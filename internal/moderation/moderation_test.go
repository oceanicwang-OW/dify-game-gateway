package moderation

import (
	"context"
	"strings"
	"testing"
)

func newMod(words ...string) *PolicyModerator {
	return NewPolicyModerator(NewKeywordPolicy(words))
}

func TestCheckInput(t *testing.T) {
	m := newMod("violence", "敏感词")
	ctx := context.Background()

	if ok, fb := m.CheckInput(ctx, "你好，铁匠"); !ok || fb != "" {
		t.Fatalf("clean input = (%v, %q), want (true, \"\")", ok, fb)
	}
	if ok, fb := m.CheckInput(ctx, "I love VIOLENCE"); ok || fb != DefaultInputFallback {
		t.Fatalf("banned input = (%v, %q), want (false, fallback)", ok, fb)
	}
	if ok, _ := m.CheckInput(ctx, "这里有敏感词存在"); ok {
		t.Fatal("CJK banned input allowed")
	}
}

func TestCheckOutput(t *testing.T) {
	m := newMod("violence")
	ctx := context.Background()
	if ok, _ := m.CheckOutput(ctx, "a peaceful sentence."); !ok {
		t.Fatal("clean output blocked")
	}
	if ok, fb := m.CheckOutput(ctx, "lots of violence here."); ok || fb != DefaultOutputFallback {
		t.Fatalf("banned output = (%v, %q), want (false, output fallback)", ok, fb)
	}
}

func TestKeywordPolicyCaseInsensitiveAndTrim(t *testing.T) {
	p := NewKeywordPolicy([]string{"  Violence  ", "", "  "})
	if len(p.banned) != 1 || p.banned[0] != "violence" {
		t.Fatalf("normalized banned = %#v, want [violence]", p.banned)
	}
	if p.Allowed(context.Background(), "ViOlEnCe") {
		t.Fatal("case-insensitive match failed")
	}
}

func TestAllowAllNeverBlocks(t *testing.T) {
	var m AllowAll
	if ok, _ := m.CheckInput(context.Background(), "anything violence"); !ok {
		t.Fatal("AllowAll blocked input")
	}
	if ok, _ := m.CheckOutput(context.Background(), "anything violence"); !ok {
		t.Fatal("AllowAll blocked output")
	}
}

func TestOutputFilterEmitsCleanSentences(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	ctx := context.Background()

	// No terminator yet -> nothing emitted.
	if emit, blocked, _ := f.Push(ctx, "Hello there"); emit != "" || blocked {
		t.Fatalf("partial push emitted %q / blocked=%v", emit, blocked)
	}
	// Terminator completes the sentence -> emitted.
	emit, blocked, _ := f.Push(ctx, ", friend. How are")
	if blocked || emit != "Hello there, friend." {
		t.Fatalf("emit = %q, blocked=%v; want 'Hello there, friend.'", emit, blocked)
	}
	// Remaining incomplete sentence emerges on flush.
	emit, blocked, _ = f.Flush(ctx)
	if blocked || emit != " How are" {
		t.Fatalf("flush emit = %q, blocked=%v; want ' How are'", emit, blocked)
	}
}

// TestOutputFilterCatchesContentSplitAcrossDeltas is the core M3-T4 acceptance:
// a banned word split across deltas must be caught once the sentence completes,
// even though no individual delta contains it.
func TestOutputFilterCatchesContentSplitAcrossDeltas(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	ctx := context.Background()

	// Sanity: neither delta alone contains the banned word.
	p := NewKeywordPolicy([]string{"violence"})
	if !p.Allowed(ctx, "I enjoy vi") || !p.Allowed(ctx, "olence in games.") {
		t.Fatal("test setup: a delta already contains the banned word")
	}

	if emit, blocked, _ := f.Push(ctx, "I enjoy vi"); emit != "" || blocked {
		t.Fatalf("first delta emitted %q / blocked=%v, want buffered", emit, blocked)
	}
	emit, blocked, fb := f.Push(ctx, "olence in games.")
	if !blocked {
		t.Fatal("split banned content not detected after sentence completed")
	}
	if emit != "" {
		t.Fatalf("blocked push emitted %q, want empty", emit)
	}
	if fb != DefaultOutputFallback {
		t.Fatalf("fallback = %q, want output fallback", fb)
	}
}

// TestOutputFilterCatchesTokenSplitAcrossTerminator covers the spurious-
// terminator hole: a banned token containing '.' (URL/decimal) lands across a
// segment boundary, yet the moderation overlap re-checks it as one unit.
func TestOutputFilterCatchesTokenSplitAcrossTerminator(t *testing.T) {
	f := NewOutputFilter(newMod("evil.com"))
	ctx := context.Background()

	// Deltas chosen so the '.' inside "evil.com" forces a segment cut, leaving
	// "evil." in one segment and "com" in the next.
	if emit, blocked, _ := f.Push(ctx, "visit ev"); emit != "" || blocked {
		t.Fatalf("first push = (%q, %v), want buffered", emit, blocked)
	}
	if _, blocked, _ := f.Push(ctx, "il.co"); blocked {
		// "visit evil." is emitted here and must NOT yet block (no full match).
		t.Fatal("blocked prematurely on partial token")
	}
	_, blocked, fb := f.Push(ctx, "m now.")
	if !blocked || fb != DefaultOutputFallback {
		t.Fatalf("token split across '.' not caught: blocked=%v fb=%q", blocked, fb)
	}
}

// TestOutputFilterOverlapCatchesAcrossCapBoundary covers the cap/space-less
// (CJK-like) hole: a banned word split across the size cap is caught via the
// moderation overlap even though no whitespace lets the tail recombine.
func TestOutputFilterOverlapCatchesAcrossCapBoundary(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	f.maxSegmentBytes = 8 // force early, space-less cap emits
	ctx := context.Background()

	// "xxxxxviol" (9B, no space) exceeds the cap and is emitted whole (tail="");
	// without the overlap, the trailing "viol" would be lost.
	if _, blocked, _ := f.Push(ctx, "xxxxxviol"); blocked {
		t.Fatal("blocked prematurely")
	}
	_, blocked, fb := f.Push(ctx, "ence now")
	if !blocked || fb != DefaultOutputFallback {
		t.Fatalf("word split across cap boundary not caught: blocked=%v fb=%q", blocked, fb)
	}
}

func TestLastRunes(t *testing.T) {
	if got := lastRunes("abcdef", 3); got != "def" {
		t.Fatalf("lastRunes = %q, want def", got)
	}
	if got := lastRunes("ab", 5); got != "ab" {
		t.Fatalf("lastRunes shorter = %q, want ab", got)
	}
	if got := lastRunes("你好世界", 2); got != "世界" {
		t.Fatalf("lastRunes CJK = %q, want 世界", got)
	}
	if got := lastRunes("abc", 0); got != "" {
		t.Fatalf("lastRunes 0 = %q, want empty", got)
	}
}

func TestOutputFilterEmitsThenBlocksLaterSentence(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	ctx := context.Background()

	emit, blocked, _ := f.Push(ctx, "All good here. ")
	if blocked || emit != "All good here." {
		t.Fatalf("first emit = %q, blocked=%v; want 'All good here.'", emit, blocked)
	}
	if e, _, _ := f.Push(ctx, "Now vio"); e != "" {
		t.Fatalf("mid push emitted %q, want buffered", e)
	}
	emit, blocked, fb := f.Push(ctx, "lence.")
	if !blocked || emit != "" || fb != DefaultOutputFallback {
		t.Fatalf("expected block with fallback, got emit=%q blocked=%v fb=%q", emit, blocked, fb)
	}
}

func TestOutputFilterStaysBlocked(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	ctx := context.Background()

	if _, blocked, _ := f.Push(ctx, "violence."); !blocked {
		t.Fatal("expected block")
	}
	// Subsequent pushes and flush must remain no-op blocked.
	if emit, blocked, _ := f.Push(ctx, "more clean text."); emit != "" || !blocked {
		t.Fatalf("post-block push = (%q, %v), want (\"\", true)", emit, blocked)
	}
	if emit, blocked, _ := f.Flush(ctx); emit != "" || !blocked {
		t.Fatalf("post-block flush = (%q, %v), want (\"\", true)", emit, blocked)
	}
	if !f.Blocked() {
		t.Fatal("Blocked() = false after hit")
	}
}

func TestOutputFilterFlushModeratesRemainder(t *testing.T) {
	ctx := context.Background()

	// Clean run-on with no terminator: buffered until flush, then emitted.
	clean := NewOutputFilter(newMod("violence"))
	if emit, _, _ := clean.Push(ctx, "a tail with no terminator"); emit != "" {
		t.Fatalf("run-on push emitted %q, want buffered", emit)
	}
	if emit, blocked, _ := clean.Flush(ctx); blocked || emit != "a tail with no terminator" {
		t.Fatalf("flush emit = %q, blocked=%v", emit, blocked)
	}

	// Banned run-on with no terminator: caught on flush.
	bad := NewOutputFilter(newMod("violence"))
	bad.Push(ctx, "ends with violence no period")
	if _, blocked, fb := bad.Flush(ctx); !blocked || fb != DefaultOutputFallback {
		t.Fatalf("flush of banned remainder not blocked: blocked=%v fb=%q", blocked, fb)
	}
}

func TestOutputFilterForcesCheckAtSegmentCap(t *testing.T) {
	f := NewOutputFilter(newMod("violence"))
	f.maxSegmentBytes = 16 // shrink cap for the test
	ctx := context.Background()

	// No terminator, but exceeding the cap forces a moderation check that
	// catches the banned content.
	_, blocked, fb := f.Push(ctx, "spreading violence everywhere across the land")
	if !blocked || fb != DefaultOutputFallback {
		t.Fatalf("cap-forced check did not block: blocked=%v fb=%q", blocked, fb)
	}
}

func TestSplitKeepingLastToken(t *testing.T) {
	head, tail := splitKeepingLastToken("alpha beta gam")
	if head != "alpha beta " || tail != "gam" {
		t.Fatalf("split = (%q, %q), want ('alpha beta ', 'gam')", head, tail)
	}
	if h, tl := splitKeepingLastToken("nospace"); h != "nospace" || tl != "" {
		t.Fatalf("no-space split = (%q, %q)", h, tl)
	}
}

func TestCutAtLastTerminatorHandlesCJK(t *testing.T) {
	text := "你好。再见"
	cut := cutAtLastTerminator(text)
	if cut < 0 || !strings.HasSuffix(text[:cut], "。") {
		t.Fatalf("cut = %d, segment = %q; want segment ending in CJK period", cut, text[:cut])
	}
	if text[cut:] != "再见" {
		t.Fatalf("remainder = %q, want 再见", text[cut:])
	}
}

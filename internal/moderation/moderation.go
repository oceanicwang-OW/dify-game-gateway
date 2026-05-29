// Package moderation implements input and streaming-output content checks
// (PDR §6.4, §9.1). Input text is checked before the upstream call; streaming
// output is buffered into complete semantic units (sentences/segments) before
// checking, so unsafe content split across multiple deltas is still detected
// (a naive per-delta check would miss it).
package moderation

import (
	"context"
	"strings"
	"unicode/utf8"
)

// Default fallbacks surfaced to the player on a moderation hit. They are
// intentionally generic and never echo the offending content.
const (
	DefaultInputFallback  = "抱歉，我无法回应这个请求。"
	DefaultOutputFallback = "（这段回复已被内容审核拦截。）"
)

// Moderator is the §9.1 content-moderation contract.
type Moderator interface {
	CheckInput(ctx context.Context, text string) (allowed bool, fallback string)
	CheckOutput(ctx context.Context, text string) (allowed bool, replacement string)
}

// Policy decides whether a piece of text is acceptable. KeywordPolicy is the
// initial implementation; a model-based policy can be slotted in later behind
// the same interface (§6.4 "预留模型审核接口").
type Policy interface {
	Allowed(ctx context.Context, text string) bool
}

// KeywordPolicy blocks any text containing a banned substring
// (case-insensitive). Matching on substrings means it also catches banned
// words embedded in a larger run of text.
type KeywordPolicy struct {
	banned []string
}

// NewKeywordPolicy builds a policy from a banned-word list (blank entries and
// surrounding whitespace are ignored; matching is case-insensitive).
func NewKeywordPolicy(words []string) *KeywordPolicy {
	normalized := make([]string, 0, len(words))
	for _, w := range words {
		w = strings.ToLower(strings.TrimSpace(w))
		if w != "" {
			normalized = append(normalized, w)
		}
	}
	return &KeywordPolicy{banned: normalized}
}

// Allowed reports whether text is free of every banned word.
func (p *KeywordPolicy) Allowed(_ context.Context, text string) bool {
	lower := strings.ToLower(text)
	for _, w := range p.banned {
		if strings.Contains(lower, w) {
			return false
		}
	}
	return true
}

// PolicyModerator applies a Policy to both input and output text and returns
// the configured fallback on a hit.
type PolicyModerator struct {
	policy         Policy
	inputFallback  string
	outputFallback string
}

// NewPolicyModerator wraps a Policy with the default fallbacks.
func NewPolicyModerator(policy Policy) *PolicyModerator {
	return &PolicyModerator{
		policy:         policy,
		inputFallback:  DefaultInputFallback,
		outputFallback: DefaultOutputFallback,
	}
}

// WithFallbacks overrides the input/output fallback messages.
func (m *PolicyModerator) WithFallbacks(input, output string) *PolicyModerator {
	m.inputFallback = input
	m.outputFallback = output
	return m
}

// CheckInput moderates a player query before any upstream call (§6.4): on a hit
// the caller must skip Dify and return the fallback directly.
func (m *PolicyModerator) CheckInput(ctx context.Context, text string) (bool, string) {
	if m.policy.Allowed(ctx, text) {
		return true, ""
	}
	return false, m.inputFallback
}

// CheckOutput moderates a buffered output segment. Callers should pass complete
// semantic units (see OutputFilter), not raw deltas.
func (m *PolicyModerator) CheckOutput(ctx context.Context, text string) (bool, string) {
	if m.policy.Allowed(ctx, text) {
		return true, ""
	}
	return false, m.outputFallback
}

// AllowAll is a no-op moderator used when moderation is disabled
// (MODERATION_ENABLED=false).
type AllowAll struct{}

func (AllowAll) CheckInput(context.Context, string) (bool, string)  { return true, "" }
func (AllowAll) CheckOutput(context.Context, string) (bool, string) { return true, "" }

// sentenceTerminators end a semantic unit eligible for moderation. Includes
// both ASCII and CJK punctuation plus newline.
const sentenceTerminators = "。！？.!?\n"

// DefaultMaxSegmentBytes caps how much output is buffered without a sentence
// terminator before it is force-moderated, bounding latency and memory for
// run-on streams.
const DefaultMaxSegmentBytes = 512

// DefaultOverlapRunes is how many trailing runes of already-emitted text are
// re-included as context when moderating the next segment, so a banned token
// straddling a segment boundary is still caught. It bounds the longest banned
// phrase reliably detected across a boundary; phrases longer than this could
// still split (an accepted, documented limit).
const DefaultOverlapRunes = 32

// OutputFilter buffers streamed deltas into complete sentences, moderates each
// completed segment, and emits only text that passed (§6.4). On a hit it latches
// blocked and yields the fallback; no further output should be forwarded.
//
// Segments are moderated together with an overlap of the previously-emitted
// text (overlapRunes), so content split across a segment boundary — a sentence
// terminator, a spurious '.' inside a URL/decimal, the size cap, or a
// space-less CJK run — is still detected. The overlap is context only and is
// never re-emitted.
type OutputFilter struct {
	mod             Moderator
	buf             strings.Builder
	maxSegmentBytes int
	overlapRunes    int
	overlap         string
	blocked         bool
}

// NewOutputFilter creates a filter over mod.
func NewOutputFilter(mod Moderator) *OutputFilter {
	return &OutputFilter{
		mod:             mod,
		maxSegmentBytes: DefaultMaxSegmentBytes,
		overlapRunes:    DefaultOverlapRunes,
	}
}

// Push buffers a delta and moderates any newly-completed sentence(s). It returns
// the text that passed and is safe to forward as a ChatChunk, whether the stream
// is now blocked, and (when blocked) the fallback to surface via ChatBlocked.
// Once blocked, subsequent Push calls are no-ops returning blocked=true.
func (f *OutputFilter) Push(ctx context.Context, delta string) (emit string, blocked bool, fallback string) {
	if f.blocked {
		return "", true, ""
	}
	f.buf.WriteString(delta)
	return f.drain(ctx, false)
}

// Flush moderates any remaining buffered text after the upstream stream ends
// (final segment without a trailing terminator). Call exactly once at stream end.
func (f *OutputFilter) Flush(ctx context.Context) (emit string, blocked bool, fallback string) {
	if f.blocked {
		return "", true, ""
	}
	return f.drain(ctx, true)
}

// Blocked reports whether the filter has latched a moderation hit.
func (f *OutputFilter) Blocked() bool { return f.blocked }

func (f *OutputFilter) drain(ctx context.Context, final bool) (emit string, blocked bool, fallback string) {
	text := f.buf.String()
	if text == "" {
		return "", false, ""
	}

	var segment, remainder string
	if final {
		// Stream ended: moderate everything that's left.
		segment, remainder = text, ""
	} else if cut := cutAtLastTerminator(text); cut >= 0 {
		segment, remainder = text[:cut], text[cut:]
	} else if len(text) >= f.maxSegmentBytes {
		// Run-on with no terminator: force a check, keeping a trailing partial
		// token so a word split at the cap can still recombine on the next push.
		segment, remainder = splitKeepingLastToken(text)
	} else {
		return "", false, "" // incomplete sentence; wait for more
	}

	// Moderate the segment together with the trailing overlap of already-emitted
	// text so a banned token straddling the previous boundary is still caught.
	// The overlap is context only; only `segment` (new text) is emitted.
	allowed, fb := f.mod.CheckOutput(ctx, f.overlap+segment)
	if !allowed {
		f.blocked = true
		f.buf.Reset()
		f.overlap = ""
		return "", true, fb
	}

	f.overlap = lastRunes(f.overlap+segment, f.overlapRunes)
	f.buf.Reset()
	f.buf.WriteString(remainder)
	return segment, false, ""
}

// cutAtLastTerminator returns the byte index just past the last sentence
// terminator in text, or -1 if none is present.
func cutAtLastTerminator(text string) int {
	idx := strings.LastIndexAny(text, sentenceTerminators)
	if idx < 0 {
		return -1
	}
	_, size := utf8.DecodeRuneInString(text[idx:])
	return idx + size
}

// lastRunes returns the last n runes of s (or all of s if shorter). It is
// rune-aware so the overlap never splits a multibyte character.
func lastRunes(s string, n int) string {
	if n <= 0 || s == "" {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[len(runes)-n:])
}

// splitKeepingLastToken splits text after the last whitespace, returning the
// head to moderate/emit and the trailing partial token to keep buffered. If
// there is no whitespace the whole text is taken.
func splitKeepingLastToken(text string) (head, tail string) {
	i := strings.LastIndexAny(text, " \t")
	if i > 0 && i < len(text)-1 {
		return text[:i+1], text[i+1:]
	}
	return text, ""
}

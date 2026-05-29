package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"dify_gateway/internal/telemetry"
)

func newTestLimiter(t *testing.T, rate RateLimit, budget int) (*RedisLimiter, func(time.Duration)) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	l := NewRedisLimiter(client, rate, budget)
	cur := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return cur }
	advance := func(d time.Duration) { cur = cur.Add(d) }
	return l, advance
}

func TestParseRate(t *testing.T) {
	cases := map[string]RateLimit{
		"1r/2s":      {Limit: 1, Window: 2 * time.Second},
		"10r/1m":     {Limit: 10, Window: time.Minute},
		" 5r/500ms ": {Limit: 5, Window: 500 * time.Millisecond},
	}
	for in, want := range cases {
		got, err := ParseRate(in)
		if err != nil || got != want {
			t.Fatalf("ParseRate(%q) = (%+v, %v), want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "1r", "abc", "0r/2s", "1x/2s", "1r/0s", "1r/bad"} {
		if _, err := ParseRate(bad); err == nil {
			t.Fatalf("ParseRate(%q) error = nil, want error", bad)
		}
	}
}

func TestRedisLimiterSlidingWindow(t *testing.T) {
	l, advance := newTestLimiter(t, RateLimit{Limit: 1, Window: 2 * time.Second}, 0)
	ctx := context.Background()

	if ok, err := l.Allow(ctx, "player-1", 0); err != nil || !ok {
		t.Fatalf("first Allow = (%v, %v), want true", ok, err)
	}
	if ok, _ := l.Allow(ctx, "player-1", 0); ok {
		t.Fatal("second Allow within window = true, want false (rate limited)")
	}
	// A different player is unaffected.
	if ok, _ := l.Allow(ctx, "player-2", 0); !ok {
		t.Fatal("other player rate limited")
	}
	// After the window slides, the first player is allowed again.
	advance(2 * time.Second)
	if ok, _ := l.Allow(ctx, "player-1", 0); !ok {
		t.Fatal("Allow after window = false, want true")
	}
}

func TestRedisLimiterTokenBudget(t *testing.T) {
	l, _ := newTestLimiter(t, RateLimit{Limit: 1000, Window: time.Second}, 100)
	ctx := context.Background()

	before := testutil.ToFloat64(telemetry.RateLimitedTotal)

	if ok, _ := l.Allow(ctx, "player-1", 60); !ok {
		t.Fatal("Allow under budget rejected")
	}
	if err := l.Record(ctx, "player-1", 60); err != nil {
		t.Fatalf("Record error = %v", err)
	}
	// 60 used + 40 est = 100 == budget -> allowed (boundary).
	if ok, _ := l.Allow(ctx, "player-1", 40); !ok {
		t.Fatal("Allow at exact budget rejected")
	}
	// 60 used + 41 est = 101 > budget -> rejected.
	if ok, _ := l.Allow(ctx, "player-1", 41); ok {
		t.Fatal("Allow over budget = true, want false")
	}

	if delta := testutil.ToFloat64(telemetry.RateLimitedTotal) - before; delta != 1 {
		t.Fatalf("RateLimitedTotal delta = %v, want 1", delta)
	}
}

func TestRedisLimiterRecordAccumulates(t *testing.T) {
	l, _ := newTestLimiter(t, RateLimit{Limit: 1000, Window: time.Second}, 100)
	ctx := context.Background()

	_ = l.Record(ctx, "player-1", 30)
	_ = l.Record(ctx, "player-1", 30)
	used, err := l.usedTokens(ctx, "player-1")
	if err != nil || used != 60 {
		t.Fatalf("usedTokens = (%d, %v), want 60", used, err)
	}
	// Over budget after accumulation.
	if ok, _ := l.Allow(ctx, "player-1", 50); ok {
		t.Fatal("Allow over accumulated budget = true, want false")
	}
}

func TestRedisLimiterDisabledChecks(t *testing.T) {
	// Rate Limit 0 and budget 0 disable both checks: always allowed.
	l, _ := newTestLimiter(t, RateLimit{Limit: 0, Window: time.Second}, 0)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if ok, err := l.Allow(ctx, "player-1", 1_000_000); err != nil || !ok {
			t.Fatalf("disabled Allow = (%v, %v), want true", ok, err)
		}
	}
}

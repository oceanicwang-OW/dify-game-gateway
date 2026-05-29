package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func newTestLimiter(t *testing.T, cfg Config) (*RedisLimiter, *fakeClock) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	clock := &fakeClock{now: time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)}
	cfg.Now = clock.Now
	if cfg.InflightLease == 0 {
		cfg.InflightLease = time.Minute
	}
	if cfg.Circuit.OpenDuration == 0 {
		cfg.Circuit.OpenDuration = time.Minute
	}

	lim, err := New(client, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return lim, clock
}

func mustAllow(t *testing.T, l *RedisLimiter, playerID string, estTokens int) {
	t.Helper()
	ok, err := l.Allow(context.Background(), playerID, estTokens)
	if err != nil || !ok {
		t.Fatalf("Allow(%q, %d) = (%v, %v), want allowed", playerID, estTokens, ok, err)
	}
}

func TestAllowRejectsOverSlidingWindow(t *testing.T) {
	l, _ := newTestLimiter(t, Config{
		RatePerPlayer:       "2r/1m",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 10,
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 0)
	if err := l.Record(ctx, "player-1", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	mustAllow(t, l, "player-1", 0)
	if err := l.Record(ctx, "player-1", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}

	ok, err := l.Allow(ctx, "player-1", 0)
	if ok || !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third Allow() = (%v, %v), want ErrRateLimited", ok, err)
	}
}

func TestAllowResumesAfterSlidingWindowExpires(t *testing.T) {
	l, clock := newTestLimiter(t, Config{
		RatePerPlayer:       "1r/2s",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 10,
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 0)
	if err := l.Record(ctx, "player-1", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}

	clock.Advance(3 * time.Second)
	mustAllow(t, l, "player-1", 0)
}

func TestAllowRejectsWhenEstimatedTokensWouldExceedDailyBudget(t *testing.T) {
	l, _ := newTestLimiter(t, Config{
		RatePerPlayer:       "100r/1m",
		TokenBudgetDaily:    100,
		MaxInflightUpstream: 10,
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 80)
	if err := l.Record(ctx, "player-1", 90); err != nil {
		t.Fatalf("Record() error = %v", err)
	}

	ok, err := l.Allow(ctx, "player-1", 11)
	if ok || !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("Allow over budget = (%v, %v), want ErrBudgetExceeded", ok, err)
	}

	mustAllow(t, l, "player-1", 10)
}

func TestAllowRejectsWhenGlobalInflightIsFullAndRecordReleasesSlot(t *testing.T) {
	l, _ := newTestLimiter(t, Config{
		RatePerPlayer:       "100r/1m",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 1,
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 0)
	ok, err := l.Allow(ctx, "player-2", 0)
	if ok || !errors.Is(err, ErrInflightFull) {
		t.Fatalf("Allow while inflight full = (%v, %v), want ErrInflightFull", ok, err)
	}

	if err := l.Record(ctx, "player-1", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	mustAllow(t, l, "player-2", 0)
}

func TestCircuitOpensOnHighErrorRateThenHalfOpenSuccessCloses(t *testing.T) {
	l, clock := newTestLimiter(t, Config{
		RatePerPlayer:       "100r/1m",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 10,
		Circuit: CircuitConfig{
			Window:           4,
			MinSamples:       4,
			FailureThreshold: 0.5,
			OpenDuration:     30 * time.Second,
		},
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 0)
	if err := l.RecordFailure(ctx); err != nil {
		t.Fatalf("RecordFailure() error = %v", err)
	}
	mustAllow(t, l, "player-2", 0)
	if err := l.RecordFailure(ctx); err != nil {
		t.Fatalf("RecordFailure() error = %v", err)
	}
	mustAllow(t, l, "player-3", 0)
	if err := l.Record(ctx, "player-3", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	mustAllow(t, l, "player-4", 0)
	if err := l.Record(ctx, "player-4", 0); err != nil {
		t.Fatalf("Record() error = %v", err)
	}

	ok, err := l.Allow(ctx, "player-5", 0)
	if ok || !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow while circuit open = (%v, %v), want ErrCircuitOpen", ok, err)
	}

	clock.Advance(31 * time.Second)
	mustAllow(t, l, "player-6", 0) // half-open probe

	ok, err = l.Allow(ctx, "player-7", 0)
	if ok || !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second half-open probe = (%v, %v), want ErrCircuitOpen", ok, err)
	}

	if err := l.Record(ctx, "player-6", 0); err != nil {
		t.Fatalf("half-open Record() error = %v", err)
	}
	mustAllow(t, l, "player-7", 0)
}

func TestRecordFailureReleasesInflightSlot(t *testing.T) {
	l, _ := newTestLimiter(t, Config{
		RatePerPlayer:       "100r/1m",
		TokenBudgetDaily:    1000,
		MaxInflightUpstream: 1,
		Circuit: CircuitConfig{
			Window:           10,
			MinSamples:       10,
			FailureThreshold: 0.9,
			OpenDuration:     time.Minute,
		},
	})
	ctx := context.Background()

	mustAllow(t, l, "player-1", 0)
	if err := l.RecordFailure(ctx); err != nil {
		t.Fatalf("RecordFailure() error = %v", err)
	}
	mustAllow(t, l, "player-2", 0)
}

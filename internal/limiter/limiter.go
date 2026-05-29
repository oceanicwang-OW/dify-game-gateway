// Package limiter implements the Redis-backed request protections from PDR
// M3-T3: per-player sliding-window rate limiting, daily token budget checks,
// a cross-process in-flight upstream semaphore, and a local circuit breaker.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"dify_gateway/internal/telemetry"
)

var (
	ErrRateLimited    = errors.New("limiter: player rate limited")
	ErrBudgetExceeded = errors.New("limiter: daily token budget exceeded")
	ErrInflightFull   = errors.New("limiter: upstream inflight limit reached")
	ErrCircuitOpen    = errors.New("limiter: circuit open")
)

const (
	defaultInflightLease = 2 * time.Minute
	budgetDateLayout     = "20060102"
)

var rateLimitScript = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, ARGV[2])
local count = redis.call("ZCARD", KEYS[1])
if count >= tonumber(ARGV[3]) then
	return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
redis.call("PEXPIRE", KEYS[1], ARGV[5])
return 1
`)

var acquireInflightScript = redis.NewScript(`
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if current >= tonumber(ARGV[1]) then
	return 0
end
redis.call("INCR", KEYS[1])
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`)

var releaseInflightScript = redis.NewScript(`
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if current <= 1 then
	redis.call("DEL", KEYS[1])
	return 0
end
return redis.call("DECR", KEYS[1])
`)

type Config struct {
	RatePerPlayer       string
	TokenBudgetDaily    int
	MaxInflightUpstream int
	InflightLease       time.Duration
	Circuit             CircuitConfig
	Now                 func() time.Time
}

type rateLimit struct {
	limit  int64
	window time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, playerID string, estTokens int) (bool, error)
	Record(ctx context.Context, playerID string, usedTokens int) error
}

// RedisLimiter implements the PDR §9.1 Limiter contract.
type RedisLimiter struct {
	client       redis.UniversalClient
	rate         rateLimit
	tokenBudget  int
	maxInflight  int
	inflightTTL  time.Duration
	now          func() time.Time
	circuit      *circuitBreaker
	memberSerial atomic.Uint64
}

var _ Limiter = (*RedisLimiter)(nil)

// New creates a Redis-backed limiter. The Redis client must be shared with the
// gateway process so rate, budget, and in-flight counters are coordinated.
func New(client redis.UniversalClient, cfg Config) (*RedisLimiter, error) {
	if client == nil {
		return nil, fmt.Errorf("limiter: redis client is required")
	}
	rate, err := parseRate(cfg.RatePerPlayer)
	if err != nil {
		return nil, err
	}
	if cfg.TokenBudgetDaily <= 0 {
		return nil, fmt.Errorf("limiter: token budget must be positive")
	}
	if cfg.MaxInflightUpstream <= 0 {
		return nil, fmt.Errorf("limiter: max inflight upstream must be positive")
	}
	if cfg.InflightLease == 0 {
		cfg.InflightLease = defaultInflightLease
	}
	if cfg.InflightLease <= 0 {
		return nil, fmt.Errorf("limiter: inflight lease must be positive")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	circuit, err := newCircuitBreaker(cfg.Circuit, cfg.Now)
	if err != nil {
		return nil, err
	}
	return &RedisLimiter{
		client:      client,
		rate:        rate,
		tokenBudget: cfg.TokenBudgetDaily,
		maxInflight: cfg.MaxInflightUpstream,
		inflightTTL: cfg.InflightLease,
		now:         cfg.Now,
		circuit:     circuit,
	}, nil
}

// Allow checks all request guards. A true result reserves one in-flight upstream
// slot; callers must later call Record or RecordFailure exactly once.
func (l *RedisLimiter) Allow(ctx context.Context, playerID string, estTokens int) (bool, error) {
	playerID = strings.TrimSpace(playerID)
	if playerID == "" {
		return false, fmt.Errorf("limiter: player ID is required")
	}
	if estTokens < 0 {
		return false, fmt.Errorf("limiter: estimated tokens cannot be negative")
	}
	if !l.circuit.ready() {
		telemetry.RateLimitedTotal.Inc()
		return false, ErrCircuitOpen
	}
	if err := l.checkBudget(ctx, playerID, estTokens); err != nil {
		if errors.Is(err, ErrBudgetExceeded) {
			telemetry.RateLimitedTotal.Inc()
			return false, err
		}
		return false, err
	}
	if !l.circuit.allow() {
		telemetry.RateLimitedTotal.Inc()
		return false, ErrCircuitOpen
	}
	if err := l.acquireInflight(ctx); err != nil {
		l.circuit.cancelProbe()
		if errors.Is(err, ErrInflightFull) {
			telemetry.RateLimitedTotal.Inc()
			return false, err
		}
		return false, err
	}
	if err := l.allowRate(ctx, playerID); err != nil {
		_ = l.releaseInflight(ctx)
		l.circuit.cancelProbe()
		if errors.Is(err, ErrRateLimited) {
			telemetry.RateLimitedTotal.Inc()
			return false, err
		}
		return false, err
	}
	return true, nil
}

// Record releases the in-flight slot, records a successful upstream call in the
// circuit breaker, and adds usedTokens to the player's daily budget counter.
func (l *RedisLimiter) Record(ctx context.Context, playerID string, usedTokens int) error {
	playerID = strings.TrimSpace(playerID)
	if playerID == "" {
		return fmt.Errorf("limiter: player ID is required")
	}
	if usedTokens < 0 {
		_ = l.releaseInflight(ctx)
		return fmt.Errorf("limiter: used tokens cannot be negative")
	}
	releaseErr := l.releaseInflight(ctx)
	recordErr := l.recordTokens(ctx, playerID, usedTokens)
	l.circuit.record(true)
	if releaseErr != nil {
		return releaseErr
	}
	return recordErr
}

// RecordFailure releases the in-flight slot and records an upstream failure for
// circuit-breaker accounting. It does not add tokens to the daily budget.
func (l *RedisLimiter) RecordFailure(ctx context.Context) error {
	releaseErr := l.releaseInflight(ctx)
	l.circuit.record(false)
	return releaseErr
}

func (l *RedisLimiter) allowRate(ctx context.Context, playerID string) error {
	nowMs := l.now().UnixMilli()
	windowMs := l.rate.window.Milliseconds()
	if windowMs < 1 {
		windowMs = 1
	}
	member := strconv.FormatInt(nowMs, 10) + ":" + strconv.FormatUint(l.memberSerial.Add(1), 10)
	result, err := rateLimitScript.Run(ctx, l.client, []string{rateKey(playerID)},
		nowMs,
		nowMs-windowMs,
		l.rate.limit,
		member,
		windowMs,
	).Int()
	if err != nil {
		return fmt.Errorf("limiter: check rate window: %w", err)
	}
	if result == 0 {
		return ErrRateLimited
	}
	return nil
}

func (l *RedisLimiter) checkBudget(ctx context.Context, playerID string, estTokens int) error {
	if estTokens == 0 {
		return nil
	}
	current, err := l.currentBudget(ctx, playerID)
	if err != nil {
		return err
	}
	if current+estTokens > l.tokenBudget {
		return ErrBudgetExceeded
	}
	return nil
}

func (l *RedisLimiter) currentBudget(ctx context.Context, playerID string) (int, error) {
	value, err := l.client.Get(ctx, budgetKey(playerID, l.now())).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("limiter: read token budget: %w", err)
	}
	current, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("limiter: parse token budget %q: %w", value, err)
	}
	return current, nil
}

func (l *RedisLimiter) recordTokens(ctx context.Context, playerID string, usedTokens int) error {
	if usedTokens == 0 {
		return nil
	}
	key := budgetKey(playerID, l.now())
	if err := l.client.IncrBy(ctx, key, int64(usedTokens)).Err(); err != nil {
		return fmt.Errorf("limiter: record token budget: %w", err)
	}
	if err := l.client.Expire(ctx, key, durationUntilNextUTCDay(l.now())).Err(); err != nil {
		return fmt.Errorf("limiter: expire token budget: %w", err)
	}
	return nil
}

func (l *RedisLimiter) acquireInflight(ctx context.Context) error {
	leaseMs := l.inflightTTL.Milliseconds()
	if leaseMs < 1 {
		leaseMs = 1
	}
	result, err := acquireInflightScript.Run(ctx, l.client, []string{inflightKey()},
		l.maxInflight,
		leaseMs,
	).Int()
	if err != nil {
		return fmt.Errorf("limiter: acquire inflight slot: %w", err)
	}
	if result == 0 {
		return ErrInflightFull
	}
	telemetry.InflightUpstream.Inc()
	return nil
}

func (l *RedisLimiter) releaseInflight(ctx context.Context) error {
	if err := releaseInflightScript.Run(ctx, l.client, []string{inflightKey()}).Err(); err != nil {
		return fmt.Errorf("limiter: release inflight slot: %w", err)
	}
	telemetry.InflightUpstream.Dec()
	return nil
}

func parseRate(raw string) (rateLimit, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "1r/2s"
	}
	countRaw, windowRaw, ok := strings.Cut(raw, "r/")
	if !ok {
		return rateLimit{}, fmt.Errorf("limiter: RATE_PER_PLAYER must look like 1r/2s")
	}
	count, err := strconv.ParseInt(strings.TrimSpace(countRaw), 10, 64)
	if err != nil || count <= 0 {
		if err != nil {
			return rateLimit{}, fmt.Errorf("limiter: rate count must be positive: %w", err)
		}
		return rateLimit{}, fmt.Errorf("limiter: rate count must be positive")
	}
	window, err := time.ParseDuration(strings.TrimSpace(windowRaw))
	if err != nil || window <= 0 {
		if err != nil {
			return rateLimit{}, fmt.Errorf("limiter: rate window must be a positive duration: %w", err)
		}
		return rateLimit{}, fmt.Errorf("limiter: rate window must be a positive duration")
	}
	return rateLimit{limit: count, window: window}, nil
}

func rateKey(playerID string) string {
	return "rate:" + playerID
}

func budgetKey(playerID string, now time.Time) string {
	return "budget:" + playerID + ":" + now.UTC().Format(budgetDateLayout)
}

func inflightKey() string {
	return "inflight:upstream"
}

func durationUntilNextUTCDay(now time.Time) time.Duration {
	now = now.UTC()
	next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	d := next.Sub(now)
	if d <= 0 {
		return 24 * time.Hour
	}
	return d
}

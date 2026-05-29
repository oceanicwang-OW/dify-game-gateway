// Package limiter implements the gateway's protection layer (PDR §6.3): a
// per-player sliding-window QPS limit and daily token budget backed by Redis, a
// global in-flight semaphore protecting the upstream, and a circuit breaker.
package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"dify_gateway/internal/telemetry"
)

// Limiter is the §9.1 per-player rate/quota contract.
type Limiter interface {
	Allow(ctx context.Context, playerID string, estTokens int) (bool, error)
	Record(ctx context.Context, playerID string, usedTokens int) error
}

// RateLimit is a request budget of Limit requests per Window.
type RateLimit struct {
	Limit  int
	Window time.Duration
}

// ParseRate parses the "<N>r/<duration>" form used by RATE_PER_PLAYER, e.g.
// "1r/2s" or "10r/1m".
func ParseRate(s string) (RateLimit, error) {
	left, right, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return RateLimit{}, fmt.Errorf("limiter: invalid rate %q, want <N>r/<duration>", s)
	}
	left = strings.TrimSpace(left)
	if !strings.HasSuffix(left, "r") {
		return RateLimit{}, fmt.Errorf("limiter: invalid rate %q, count must end in 'r'", s)
	}
	limit, err := strconv.Atoi(strings.TrimSuffix(left, "r"))
	if err != nil || limit <= 0 {
		return RateLimit{}, fmt.Errorf("limiter: invalid rate count in %q", s)
	}
	window, err := time.ParseDuration(strings.TrimSpace(right))
	if err != nil || window <= 0 {
		return RateLimit{}, fmt.Errorf("limiter: invalid rate window in %q", s)
	}
	return RateLimit{Limit: limit, Window: window}, nil
}

// slidingWindowScript implements an atomic sliding-window log: drop entries
// older than the window, reject if the window is full, else record the request.
// KEYS[1]=rate key; ARGV[1]=now(ms) ARGV[2]=window(ms) ARGV[3]=limit ARGV[4]=member
var slidingWindowScript = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, tonumber(ARGV[1]) - tonumber(ARGV[2]))
local count = redis.call("ZCARD", KEYS[1])
if count >= tonumber(ARGV[3]) then
	return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[4])
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`)

// RedisLimiter enforces the per-player sliding-window QPS limit and daily token
// budget using Redis.
type RedisLimiter struct {
	client      redis.UniversalClient
	rate        RateLimit
	dailyBudget int
	now         func() time.Time
}

var _ Limiter = (*RedisLimiter)(nil)

// NewRedisLimiter builds a limiter. A dailyBudget <= 0 disables the token budget
// check; a rate Limit <= 0 disables QPS limiting.
func NewRedisLimiter(client redis.UniversalClient, rate RateLimit, dailyBudget int) *RedisLimiter {
	return &RedisLimiter{client: client, rate: rate, dailyBudget: dailyBudget, now: time.Now}
}

// Allow reports whether a request may proceed. It first does a read-only daily
// token-budget pre-check (estTokens against remaining budget), then consumes a
// sliding-window slot. The budget check runs first so a budget rejection does
// not burn a rate slot. A rejection increments gateway_ratelimited_total.
func (l *RedisLimiter) Allow(ctx context.Context, playerID string, estTokens int) (bool, error) {
	if l.dailyBudget > 0 {
		used, err := l.usedTokens(ctx, playerID)
		if err != nil {
			return false, err
		}
		if used+estTokens > l.dailyBudget {
			telemetry.RateLimitedTotal.Inc()
			return false, nil
		}
	}

	if l.rate.Limit > 0 {
		allowed, err := l.allowRate(ctx, playerID)
		if err != nil {
			return false, err
		}
		if !allowed {
			telemetry.RateLimitedTotal.Inc()
			return false, nil
		}
	}
	return true, nil
}

// Record adds the actual token usage (from message_end) to the player's daily
// budget counter. It is a no-op when the budget is disabled or usedTokens <= 0.
func (l *RedisLimiter) Record(ctx context.Context, playerID string, usedTokens int) error {
	if l.dailyBudget <= 0 || usedTokens <= 0 {
		return nil
	}
	key := l.budgetKey(playerID)
	pipe := l.client.TxPipeline()
	pipe.IncrBy(ctx, key, int64(usedTokens))
	pipe.Expire(ctx, key, 25*time.Hour) // date-stamped key; TTL just reaps it
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("limiter: record tokens: %w", err)
	}
	return nil
}

func (l *RedisLimiter) usedTokens(ctx context.Context, playerID string) (int, error) {
	val, err := l.client.Get(ctx, l.budgetKey(playerID)).Int()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("limiter: read token budget: %w", err)
	}
	return val, nil
}

func (l *RedisLimiter) allowRate(ctx context.Context, playerID string) (bool, error) {
	nowMS := l.now().UnixMilli()
	member := strconv.FormatInt(nowMS, 10) + ":" + randToken()
	res, err := slidingWindowScript.Run(ctx, l.client,
		[]string{"ratelimit:" + playerID},
		nowMS, l.rate.Window.Milliseconds(), l.rate.Limit, member,
	).Int()
	if err != nil {
		return false, fmt.Errorf("limiter: sliding window: %w", err)
	}
	return res == 1, nil
}

func (l *RedisLimiter) budgetKey(playerID string) string {
	return "budget:" + playerID + ":" + l.now().UTC().Format("2006-01-02")
}

func randToken() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

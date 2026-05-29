// Package store implements the Redis-backed SessionStore (PDR §7.2, §9.1):
// the conv:{player}:{npc} -> conversation_id mapping with TTL, plus a
// per-(player,npc) distributed lock used to serialize first-time conversation
// creation (PDR §3.3) so concurrent first requests cannot fork a conversation.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// lockRetryInterval is how long Acquire waits between contended attempts. It is
// a var so tests can shrink it.
var lockRetryInterval = 20 * time.Millisecond

// releaseTimeout bounds the token-guarded release call so a dead Redis cannot
// block unlock for the client's default timeout.
var releaseTimeout = 5 * time.Second

// releaseScript deletes the lock key only if it still holds our token, so a
// caller can never release a lock that a different holder acquired after our
// TTL expired.
var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// RedisStore is the Redis implementation of the session store.
type RedisStore struct {
	client redis.UniversalClient
}

// New wraps a redis client.
func New(client redis.UniversalClient) *RedisStore {
	return &RedisStore{client: client}
}

func conversationKey(playerID, npcID string) string {
	return fmt.Sprintf("conv:%s:%s", playerID, npcID)
}

func lockKey(playerID, npcID string) string {
	return fmt.Sprintf("lock:conv:%s:%s", playerID, npcID)
}

// GetConversation returns the mapped conversation_id, or "" if no mapping
// exists (meaning the next request starts a new conversation, PDR §7.2).
func (s *RedisStore) GetConversation(ctx context.Context, playerID, npcID string) (string, error) {
	val, err := s.client.Get(ctx, conversationKey(playerID, npcID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: get conversation: %w", err)
	}
	return val, nil
}

// SetConversation stores the mapping with the given inactivity TTL. A
// non-positive TTL is rejected: passing it to Redis would create a mapping that
// never expires, defeating the §7.2 inactivity-expiry contract.
func (s *RedisStore) SetConversation(ctx context.Context, playerID, npcID, convID string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("store: set conversation: ttl must be positive, got %v", ttl)
	}
	if err := s.client.Set(ctx, conversationKey(playerID, npcID), convID, ttl).Err(); err != nil {
		return fmt.Errorf("store: set conversation: %w", err)
	}
	return nil
}

// DeleteConversation removes the mapping (reset / story switch, PDR §7.2).
func (s *RedisStore) DeleteConversation(ctx context.Context, playerID, npcID string) error {
	if err := s.client.Del(ctx, conversationKey(playerID, npcID)).Err(); err != nil {
		return fmt.Errorf("store: delete conversation: %w", err)
	}
	return nil
}

// AcquireConversationLock acquires the per-(player,npc) lock (SET NX PX) for the
// first-time-creation critical section (PDR §3.3). It blocks until the lock is
// obtained or ctx is cancelled. The returned unlock releases the lock and is
// safe to call once; it only deletes the key while we still hold the token, and
// the TTL guarantees the lock is reclaimable if the holder dies.
func (s *RedisStore) AcquireConversationLock(ctx context.Context, playerID, npcID string, ttl time.Duration) (func(), error) {
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	key := lockKey(playerID, npcID)

	for {
		ok, err := s.client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return nil, fmt.Errorf("store: acquire lock: %w", err)
		}
		if ok {
			return s.releaser(key, token), nil
		}

		timer := time.NewTimer(lockRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *RedisStore) releaser(key, token string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			// Best-effort, token-guarded release; ignore errors (TTL is the
			// backstop). Bounded ctx so a dead Redis can't block unlock.
			ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
			defer cancel()
			_ = releaseScript.Run(ctx, s.client, []string{key}, token).Err()
		})
	}
}

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("store: generate lock token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

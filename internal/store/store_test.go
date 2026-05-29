package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return New(client), mr
}

func TestGetConversationMissingReturnsEmpty(t *testing.T) {
	s, _ := newTestStore(t)
	got, err := s.GetConversation(context.Background(), "player-1", "npc-1")
	if err != nil {
		t.Fatalf("GetConversation() error = %v", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty (no mapping)", got)
	}
}

func TestSetGetDeleteConversation(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	// 新建: write then read back (续聊 path).
	if err := s.SetConversation(ctx, "player-1", "npc-1", "conv-abc", time.Hour); err != nil {
		t.Fatalf("SetConversation() error = %v", err)
	}
	got, err := s.GetConversation(ctx, "player-1", "npc-1")
	if err != nil || got != "conv-abc" {
		t.Fatalf("GetConversation() = %q, %v; want conv-abc", got, err)
	}

	// 隔离: a different npc has no mapping.
	if other, _ := s.GetConversation(ctx, "player-1", "npc-2"); other != "" {
		t.Fatalf("npc-2 mapping = %q, want empty", other)
	}

	// 重置: delete clears it.
	if err := s.DeleteConversation(ctx, "player-1", "npc-1"); err != nil {
		t.Fatalf("DeleteConversation() error = %v", err)
	}
	if got, _ := s.GetConversation(ctx, "player-1", "npc-1"); got != "" {
		t.Fatalf("after delete = %q, want empty", got)
	}
}

func TestConversationTTLExpires(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	if err := s.SetConversation(ctx, "player-1", "npc-1", "conv-abc", time.Minute); err != nil {
		t.Fatalf("SetConversation() error = %v", err)
	}
	// Fast-forward past the TTL.
	mr.FastForward(2 * time.Minute)

	got, err := s.GetConversation(ctx, "player-1", "npc-1")
	if err != nil {
		t.Fatalf("GetConversation() error = %v", err)
	}
	if got != "" {
		t.Fatalf("after TTL = %q, want empty", got)
	}
}

func TestAcquireConversationLockMutualExclusion(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	unlock, err := s.AcquireConversationLock(ctx, "player-1", "npc-1", time.Minute)
	if err != nil {
		t.Fatalf("first Acquire error = %v", err)
	}

	// A second acquire must block while the lock is held; with a short ctx it
	// times out rather than succeeding.
	shortCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	if _, err := s.AcquireConversationLock(shortCtx, "player-1", "npc-1", time.Minute); err == nil {
		t.Fatal("second Acquire succeeded while lock held, want timeout")
	}

	// After release, it can be acquired again.
	unlock()
	unlock2, err := s.AcquireConversationLock(ctx, "player-1", "npc-1", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire after release error = %v", err)
	}
	unlock2()
}

func TestReleaseIsTokenGuarded(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	unlock, err := s.AcquireConversationLock(ctx, "player-1", "npc-1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Acquire error = %v", err)
	}

	// Holder A's lock expires; holder B acquires the same lock.
	mr.FastForward(100 * time.Millisecond)
	unlockB, err := s.AcquireConversationLock(ctx, "player-1", "npc-1", time.Minute)
	if err != nil {
		t.Fatalf("B Acquire error = %v", err)
	}

	// A's late unlock must NOT release B's lock (token mismatch).
	unlock()
	if held, _ := s.client.Exists(ctx, lockKey("player-1", "npc-1")).Result(); held != 1 {
		t.Fatal("A's stale unlock released B's lock")
	}
	unlockB()
}

// TestConcurrentFirstRequestsCreateOnce is the core M3-T1 acceptance: two
// concurrent first requests for the same (player,npc) must create exactly one
// conversation and leave a single uncontended mapping.
func TestConcurrentFirstRequestsCreateOnce(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	var creations atomic.Int64
	// createConversation simulates the orchestration "first request" flow:
	// lock -> re-check mapping -> create+store only if still absent -> unlock.
	createConversation := func(seq int) (string, error) {
		unlock, err := s.AcquireConversationLock(ctx, "player-1", "npc-1", time.Minute)
		if err != nil {
			return "", err
		}
		defer unlock()

		if existing, err := s.GetConversation(ctx, "player-1", "npc-1"); err != nil {
			return "", err
		} else if existing != "" {
			return existing, nil // someone already created it
		}

		creations.Add(1)
		convID := "conv-created"
		if err := s.SetConversation(ctx, "player-1", "npc-1", convID, time.Hour); err != nil {
			return "", err
		}
		return convID, nil
	}

	const goroutines = 8
	var wg sync.WaitGroup
	results := make([]string, goroutines)
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = createConversation(idx)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d error = %v", i, err)
		}
		if results[i] != "conv-created" {
			t.Fatalf("goroutine %d got %q, want conv-created", i, results[i])
		}
	}
	if n := creations.Load(); n != 1 {
		t.Fatalf("conversation created %d times, want exactly 1", n)
	}

	got, _ := s.GetConversation(ctx, "player-1", "npc-1")
	if got != "conv-created" {
		t.Fatalf("final mapping = %q, want conv-created", got)
	}
}

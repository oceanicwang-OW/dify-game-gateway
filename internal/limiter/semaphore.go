package limiter

import (
	"context"

	"dify_gateway/internal/telemetry"
)

// InflightSemaphore bounds the number of concurrent in-flight upstream (Dify)
// requests per gateway instance (PDR §6.3 global concurrency). It maintains the
// gateway_inflight_upstream gauge.
type InflightSemaphore struct {
	slots chan struct{}
}

// NewInflightSemaphore creates a semaphore with the given capacity. A capacity
// <= 0 yields a semaphore that always rejects (defensive; callers should pass a
// positive MAX_INFLIGHT_UPSTREAM).
func NewInflightSemaphore(capacity int) *InflightSemaphore {
	if capacity < 0 {
		capacity = 0
	}
	return &InflightSemaphore{slots: make(chan struct{}, capacity)}
}

// TryAcquire takes a slot without blocking. It returns false immediately if the
// semaphore is full. On success the caller must call Release exactly once.
func (s *InflightSemaphore) TryAcquire() bool {
	select {
	case s.slots <- struct{}{}:
		telemetry.InflightUpstream.Inc()
		return true
	default:
		return false
	}
}

// Acquire blocks until a slot is available or ctx is cancelled. On success the
// caller must call Release exactly once.
func (s *InflightSemaphore) Acquire(ctx context.Context) error {
	select {
	case s.slots <- struct{}{}:
		telemetry.InflightUpstream.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot. It must be paired with a successful Acquire/TryAcquire.
func (s *InflightSemaphore) Release() {
	select {
	case <-s.slots:
		telemetry.InflightUpstream.Dec()
	default:
		// No slot held; ignore to stay panic-free on a double Release.
	}
}

// InFlight returns the current number of held slots.
func (s *InflightSemaphore) InFlight() int { return len(s.slots) }

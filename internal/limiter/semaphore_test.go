package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dify_gateway/internal/telemetry"
)

func TestInflightSemaphoreCapacity(t *testing.T) {
	before := testutil.ToFloat64(telemetry.InflightUpstream)
	s := NewInflightSemaphore(2)

	if !s.TryAcquire() || !s.TryAcquire() {
		t.Fatal("failed to acquire up to capacity")
	}
	if s.TryAcquire() {
		t.Fatal("acquired beyond capacity")
	}
	if s.InFlight() != 2 {
		t.Fatalf("InFlight = %d, want 2", s.InFlight())
	}
	if got := testutil.ToFloat64(telemetry.InflightUpstream) - before; got != 2 {
		t.Fatalf("InflightUpstream delta = %v, want 2", got)
	}

	s.Release()
	if s.InFlight() != 1 {
		t.Fatalf("InFlight after release = %d, want 1", s.InFlight())
	}
	if !s.TryAcquire() {
		t.Fatal("could not re-acquire after release")
	}

	// Drain back to baseline.
	s.Release()
	s.Release()
	if got := testutil.ToFloat64(telemetry.InflightUpstream) - before; got != 0 {
		t.Fatalf("InflightUpstream not balanced: delta = %v, want 0", got)
	}
}

func TestInflightSemaphoreAcquireBlocksThenCancels(t *testing.T) {
	s := NewInflightSemaphore(1)
	if !s.TryAcquire() {
		t.Fatal("first acquire failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Acquire(ctx); err == nil {
		t.Fatal("Acquire on full semaphore = nil, want ctx error")
	}
	s.Release()
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire after release = %v, want nil", err)
	}
	s.Release()
}

func TestInflightSemaphoreDoubleReleaseSafe(t *testing.T) {
	s := NewInflightSemaphore(1)
	s.TryAcquire()
	s.Release()
	s.Release() // must not panic or drive the gauge negative
	if s.InFlight() != 0 {
		t.Fatalf("InFlight = %d, want 0", s.InFlight())
	}
}

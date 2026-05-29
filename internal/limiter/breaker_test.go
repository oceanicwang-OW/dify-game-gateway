package limiter

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dify_gateway/internal/telemetry"
)

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	b := NewCircuitBreaker(3, time.Second)

	if !b.Allow() || b.State() != StateClosed {
		t.Fatal("breaker not initially closed/allowing")
	}
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatalf("state after 2 failures = %v, want closed", b.State())
	}
	b.RecordFailure() // threshold reached
	if b.State() != StateOpen {
		t.Fatalf("state after 3 failures = %v, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker allowed a request")
	}
	if got := testutil.ToFloat64(telemetry.CircuitState); got != float64(StateOpen) {
		t.Fatalf("gauge = %v, want %v (open)", got, float64(StateOpen))
	}
}

func TestCircuitBreakerHalfOpenRecovers(t *testing.T) {
	now := time.Now()
	b := NewCircuitBreaker(1, time.Second)
	b.now = func() time.Time { return now }

	b.RecordFailure() // threshold 1 -> open
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open", b.State())
	}
	// Before cooldown: still rejected.
	now = now.Add(500 * time.Millisecond)
	if b.Allow() {
		t.Fatal("allowed before cooldown elapsed")
	}
	// After cooldown: one probe admitted (half-open), others rejected.
	now = now.Add(600 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("probe not admitted after cooldown")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	if b.Allow() {
		t.Fatal("second concurrent probe admitted in half-open")
	}
	// Probe succeeds -> closed.
	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Fatalf("state after probe success = %v, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker rejected request")
	}
	if got := testutil.ToFloat64(telemetry.CircuitState); got != float64(StateClosed) {
		t.Fatalf("gauge = %v, want closed", got)
	}
}

func TestCircuitBreakerProbeFailureReopens(t *testing.T) {
	now := time.Now()
	b := NewCircuitBreaker(1, time.Second)
	b.now = func() time.Time { return now }

	b.RecordFailure() // open
	now = now.Add(2 * time.Second)
	if !b.Allow() { // half-open probe
		t.Fatal("probe not admitted")
	}
	b.RecordFailure() // probe fails -> reopen
	if b.State() != StateOpen {
		t.Fatalf("state after failed probe = %v, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("allowed immediately after reopen")
	}
	// Cooldown is measured from the reopen instant.
	now = now.Add(2 * time.Second)
	if !b.Allow() {
		t.Fatal("probe not admitted after second cooldown")
	}
}

func TestCircuitBreakerSuccessResetsFailureStreak(t *testing.T) {
	b := NewCircuitBreaker(3, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess() // resets streak
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatalf("state = %v, want closed (streak was reset)", b.State())
	}
}

package limiter

import (
	"sync"
	"time"

	"dify_gateway/internal/telemetry"
)

// CircuitState values match the gateway_circuit_state gauge (PDR §8.2).
type CircuitState int

const (
	StateClosed   CircuitState = 0 // requests flow normally
	StateHalfOpen CircuitState = 1 // a single probe is allowed
	StateOpen     CircuitState = 2 // short-circuited; requests rejected
)

// CircuitBreaker short-circuits upstream calls when Dify fails repeatedly
// (PDR §6.3). After failureThreshold consecutive failures it opens; after
// cooldown it half-opens and admits one probe; a probe success closes it, a
// probe failure re-opens it. It maintains the gateway_circuit_state gauge.
type CircuitBreaker struct {
	failureThreshold int
	cooldown         time.Duration
	now              func() time.Time

	mu                  sync.Mutex
	state               CircuitState
	consecutiveFailures int
	openedAt            time.Time
	probing             bool // a half-open probe is in flight
}

// NewCircuitBreaker creates a closed breaker. A failureThreshold <= 0 is treated
// as 1.
func NewCircuitBreaker(failureThreshold int, cooldown time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 1
	}
	telemetry.CircuitState.Set(float64(StateClosed))
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		now:              time.Now,
		state:            StateClosed,
	}
}

// Allow reports whether a request may proceed. When open it rejects until the
// cooldown elapses, then admits a single probe (half-open). While a probe is in
// flight, further requests are rejected.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if b.now().Sub(b.openedAt) >= b.cooldown {
			b.setState(StateHalfOpen)
			b.probing = true
			return true // first probe after cooldown
		}
		return false
	case StateHalfOpen:
		if b.probing {
			return false
		}
		b.probing = true
		return true
	default:
		return false
	}
}

// RecordSuccess reports a successful upstream call. In half-open it closes the
// breaker; in closed it resets the failure streak.
func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateHalfOpen:
		b.consecutiveFailures = 0
		b.probing = false
		b.setState(StateClosed)
	case StateClosed:
		b.consecutiveFailures = 0
	}
}

// RecordFailure reports a failed upstream call. In closed it trips the breaker
// once the consecutive-failure threshold is reached; in half-open a probe
// failure re-opens it.
func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		b.consecutiveFailures++
		if b.consecutiveFailures >= b.failureThreshold {
			b.open()
		}
	case StateHalfOpen:
		b.open()
	}
}

// State returns the current breaker state.
func (b *CircuitBreaker) State() CircuitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *CircuitBreaker) open() {
	b.openedAt = b.now()
	b.probing = false
	b.setState(StateOpen)
}

// setState must be called with b.mu held.
func (b *CircuitBreaker) setState(s CircuitState) {
	b.state = s
	telemetry.CircuitState.Set(float64(s))
}

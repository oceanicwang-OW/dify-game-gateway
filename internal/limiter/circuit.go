package limiter

import (
	"fmt"
	"sync"
	"time"

	"dify_gateway/internal/telemetry"
)

// CircuitState values are exported because telemetry encodes them as numbers:
// 0 closed, 1 half-open, 2 open (PDR §8.2).
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitHalfOpen
	CircuitOpen
)

const (
	defaultCircuitWindow           = 20
	defaultCircuitMinSamples       = 5
	defaultCircuitFailureThreshold = 0.5
	defaultCircuitOpenDuration     = 30 * time.Second
)

type CircuitConfig struct {
	Window           int
	MinSamples       int
	FailureThreshold float64
	OpenDuration     time.Duration
}

type circuitBreaker struct {
	mu          sync.Mutex
	now         func() time.Time
	cfg         CircuitConfig
	state       CircuitState
	openedAt    time.Time
	probeActive bool
	events      []bool // true=success, false=failure
}

func newCircuitBreaker(cfg CircuitConfig, now func() time.Time) (*circuitBreaker, error) {
	if cfg.Window == 0 {
		cfg.Window = defaultCircuitWindow
	}
	if cfg.MinSamples == 0 {
		cfg.MinSamples = defaultCircuitMinSamples
	}
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = defaultCircuitFailureThreshold
	}
	if cfg.OpenDuration == 0 {
		cfg.OpenDuration = defaultCircuitOpenDuration
	}
	if cfg.Window <= 0 {
		return nil, fmt.Errorf("limiter: circuit window must be positive")
	}
	if cfg.MinSamples <= 0 {
		return nil, fmt.Errorf("limiter: circuit min samples must be positive")
	}
	if cfg.MinSamples > cfg.Window {
		return nil, fmt.Errorf("limiter: circuit min samples cannot exceed window")
	}
	if cfg.FailureThreshold <= 0 || cfg.FailureThreshold > 1 {
		return nil, fmt.Errorf("limiter: circuit failure threshold must be in (0,1]")
	}
	if cfg.OpenDuration <= 0 {
		return nil, fmt.Errorf("limiter: circuit open duration must be positive")
	}
	if now == nil {
		now = time.Now
	}
	telemetry.CircuitState.Set(float64(CircuitClosed))
	return &circuitBreaker{now: now, cfg: cfg, state: CircuitClosed}, nil
}

func (c *circuitBreaker) ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		return c.now().Sub(c.openedAt) >= c.cfg.OpenDuration
	case CircuitHalfOpen:
		return !c.probeActive
	default:
		return false
	}
}

func (c *circuitBreaker) allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if c.now().Sub(c.openedAt) < c.cfg.OpenDuration {
			return false
		}
		c.state = CircuitHalfOpen
		c.probeActive = false
		telemetry.CircuitState.Set(float64(CircuitHalfOpen))
	}

	if c.state == CircuitHalfOpen {
		if c.probeActive {
			return false
		}
		c.probeActive = true
		return true
	}
	return false
}

func (c *circuitBreaker) cancelProbe() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == CircuitHalfOpen {
		c.probeActive = false
	}
}

func (c *circuitBreaker) record(success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case CircuitHalfOpen:
		c.probeActive = false
		if success {
			c.closeLocked()
			return
		}
		c.openLocked()
	case CircuitClosed:
		c.events = append(c.events, success)
		if len(c.events) > c.cfg.Window {
			c.events = c.events[len(c.events)-c.cfg.Window:]
		}
		if len(c.events) < c.cfg.MinSamples {
			return
		}
		failures := 0
		for _, event := range c.events {
			if !event {
				failures++
			}
		}
		if float64(failures)/float64(len(c.events)) >= c.cfg.FailureThreshold {
			c.openLocked()
		}
	case CircuitOpen:
		if !success {
			c.openedAt = c.now()
		}
	}
}

func (c *circuitBreaker) openLocked() {
	c.state = CircuitOpen
	c.openedAt = c.now()
	c.probeActive = false
	c.events = nil
	telemetry.CircuitState.Set(float64(CircuitOpen))
}

func (c *circuitBreaker) closeLocked() {
	c.state = CircuitClosed
	c.probeActive = false
	c.events = nil
	telemetry.CircuitState.Set(float64(CircuitClosed))
}

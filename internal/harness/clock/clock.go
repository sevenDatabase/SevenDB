package clock

import (
	"sync"
	"time"
)

// Clock defines a minimal time source abstraction used by the harness.
// Implementations must be deterministic under test control.
type Clock interface {
	// Now returns the current simulated time.
	Now() time.Time
	// Advance moves the clock forward by the provided duration.
	// Negative durations are ignored.
	Advance(d time.Duration)
}

// SimulatedClock is a deterministic, manual-advance clock backed by a counter.
// It starts at startTime and only moves when Advance is called.
type SimulatedClock struct {
	mu        sync.Mutex
	current   time.Time
}

// NewSimulatedClock creates a new simulated clock starting at the given time.
func NewSimulatedClock(startTime time.Time) *SimulatedClock {
	return &SimulatedClock{current: startTime}
}

// Now implements Clock.
func (c *SimulatedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// Advance implements Clock.
func (c *SimulatedClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.current = c.current.Add(d)
	c.mu.Unlock()
}


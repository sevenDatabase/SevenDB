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
	// After returns a channel that will receive the (simulated) time
	// when the clock has advanced by at least d from the moment of call.
	// The returned channel has buffer 1 and is closed after delivery.
	After(d time.Duration) <-chan time.Time
}

// SimulatedClock is a deterministic, manual-advance clock backed by a counter.
// It starts at startTime and only moves when Advance is called.
type SimulatedClock struct {
	mu        sync.Mutex
	current   time.Time
	waiters   []waiter
}

type waiter struct {
	until time.Time
	ch    chan time.Time
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
	// wake any waiters whose deadline has passed
	if len(c.waiters) > 0 {
		remaining := c.waiters[:0]
		now := c.current
		for _, w := range c.waiters {
			if !now.Before(w.until) {
				// deliver deterministically
				w.ch <- now
				close(w.ch)
			} else {
				remaining = append(remaining, w)
			}
		}
		c.waiters = remaining
	}
	c.mu.Unlock()
}

// After implements Clock.
func (c *SimulatedClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	// non-positive durations deliver immediately
	if d <= 0 {
		c.mu.Lock()
		now := c.current
		c.mu.Unlock()
		ch <- now
		close(ch)
		return ch
	}
	c.mu.Lock()
	c.waiters = append(c.waiters, waiter{until: c.current.Add(d), ch: ch})
	c.mu.Unlock()
	return ch
}


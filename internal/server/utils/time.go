package utils

import (
    "sync"
    "time"
)

// Clock provides an interface for retrieving current time. Allows tests to mock time.
type Clock interface {
    Now() time.Time
}

// RealClock implements Clock using the real system clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// MockClock is a threadsafe controllable clock for tests.
type MockClock struct {
    mu       sync.RWMutex
    CurrTime time.Time
}

func (m *MockClock) Now() time.Time {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.CurrTime
}

// SetTime sets the current time of the mock clock.
func (m *MockClock) SetTime(t time.Time) {
    m.mu.Lock()
    m.CurrTime = t
    m.mu.Unlock()
}

// CurrentTime is the global clock instance used throughout the codebase.
// Tests may replace this with a *MockClock.
var CurrentTime Clock = RealClock{}

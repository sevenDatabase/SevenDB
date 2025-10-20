package emission

import (
	"context"
	"sync"
)

// MemorySender collects sent events for assertions in tests/harness.
type MemorySender struct {
	mu     sync.Mutex
	Events []*DataEvent
}

func (m *MemorySender) Send(ctx context.Context, ev *DataEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// store a copy to isolate from caller mutation
	cp := *ev
	m.Events = append(m.Events, &cp)
	return nil
}

func (m *MemorySender) Snapshot() []*DataEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*DataEvent, len(m.Events))
	copy(out, m.Events)
	return out
}

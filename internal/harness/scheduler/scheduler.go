package scheduler

import "sync"

// Task represents a unit of work to execute serially.
type Task func()

// Scheduler runs tasks deterministically (serial, FIFO).
type Scheduler interface {
	Enqueue(t Task)
	RunAll() // runs until queue drained
}

// SerialScheduler is a minimal FIFO, single-threaded scheduler.
type SerialScheduler struct {
	mu    sync.Mutex
	queue []Task
}

// NewSerial returns a new empty serial scheduler.
func NewSerial() *SerialScheduler {
	return &SerialScheduler{}
}

// Enqueue adds a task to the queue.
func (s *SerialScheduler) Enqueue(t Task) {
	if t == nil { return }
	s.mu.Lock()
	s.queue = append(s.queue, t)
	s.mu.Unlock()
}

// RunAll runs tasks in insertion order until the queue is empty.
func (s *SerialScheduler) RunAll() {
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			return
		}
		t := s.queue[0]
		// pop
		copy(s.queue[0:], s.queue[1:])
		s.queue[len(s.queue)-1] = nil
		s.queue = s.queue[:len(s.queue)-1]
		s.mu.Unlock()
		t()
	}
}


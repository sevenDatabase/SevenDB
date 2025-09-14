package scheduler

import "sync"

// Task represents a unit of work to execute serially.
type Task func()

// Scheduler runs tasks deterministically (serial, FIFO).
type Scheduler interface {
	Enqueue(t Task)
	RunAll() // runs until queue drained
	// RunOne executes exactly one task if available and returns true when done.
	RunOne() bool
	// Len returns the number of queued tasks (not including the one currently running).
	Len() int
}

// SerialScheduler is a minimal FIFO, single-threaded scheduler.
type SerialScheduler struct {
	mu    sync.Mutex
	queue []Task
	head  int
}

// NewSerial returns a new empty serial scheduler.
func NewSerial() *SerialScheduler {
	return &SerialScheduler{}
}

// Enqueue adds a task to the queue.
func (s *SerialScheduler) Enqueue(t Task) {
	if t == nil {
		return
	}
	s.mu.Lock()
	s.queue = append(s.queue, t)
	s.mu.Unlock()
}

// pop removes and returns the next task if any.
func (s *SerialScheduler) pop() Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.head >= len(s.queue) {
		// reset slice to release memory
		s.queue = nil
		s.head = 0
		return nil
	}
	t := s.queue[s.head]
	s.head++
	// compact occasionally to avoid unbounded growth
	if s.head > 64 && s.head*2 > len(s.queue) {
		s.queue = append([]Task(nil), s.queue[s.head:]...)
		s.head = 0
	}
	return t
}

// RunOne executes exactly one queued task if present.
func (s *SerialScheduler) RunOne() bool {
	t := s.pop()
	if t == nil {
		return false
	}
	t()
	return true
}

// RunAll runs tasks in insertion order until the queue is empty.
func (s *SerialScheduler) RunAll() {
	for s.RunOne() {
	}
}

// Len returns the number of queued tasks.
func (s *SerialScheduler) Len() int {
	s.mu.Lock()
	l := len(s.queue) - s.head
	s.mu.Unlock()
	if l < 0 {
		return 0
	}
	return l
}

package raft

// Package raft contains the MVP scaffolding for per-shard Raft replication.
// This initial commit intentionally avoids choosing a concrete raft library
// (etcd/raft vs hashicorp/raft). A thin abstraction is provided so the rest
// of the codebase (buckets, server) can begin integrating. Subsequent commits
// will plug in a concrete implementation behind ShardRaftNode.

import (
	"context"
	"errors"
	"sync"
	"time"
)

// RaftLogRecord is the logical unit proposed to the shard raft group.
// It intentionally omits CommitIndex which is assigned deterministically
// on apply.
type RaftLogRecord struct {
	BucketID  string
	Type      int
	Payload   []byte
	// TODO: optional client metadata (ClientID, ReqID, TraceID) for tracing.
}

// CommittedRecord is emitted by ShardRaftNode when a raft log entry is
// durably committed (i.e. applied to the FSM). It includes the raft log
// index for durability tracking and raw record bytes.
type CommittedRecord struct {
	RaftIndex   uint64
	Term        uint64
	CommitIndex uint64        // per-bucket commit index assigned during apply
	Record      *RaftLogRecord
}

// ErrNotLeader is returned when a proposal is attempted on a follower.
var ErrNotLeader = errors.New("raft: not leader")

// ProposalResult represents the outcome of a ProposeAndWait call.
type ProposalResult struct {
	CommitIndex uint64 // per-bucket commit index assigned on apply
	RaftIndex   uint64 // raft log index for durability/location mapping
}

// ShardRaftNode is the public API surface used by the higher layers.
// Most fields are hidden until a concrete raft library is wired in.
type ShardRaftNode struct {
	shardID string

	// committedCh delivers committed records in raft log order.
	committedCh chan *CommittedRecord

	// waiters maps raft index to a channel that is closed when applied.
	waiters map[uint64]chan *ProposalResult

	mu sync.Mutex
	nextRaftIndex uint64
	perBucketCommit map[string]uint64 // last assigned per-bucket commit index
	closed bool
}

// RaftConfig captures initialization parameters required to start a shard node.
type RaftConfig struct {
	ShardID   string
	NodeID    string
	Peers     []string // static for MVP
	DataDir   string   // base dir for raft logs + snapshots
	HeartbeatMillis int
	ElectionTimeoutMillis int
}

// NewShardRaftNode creates a new in-memory stub. Follow-up commits will
// initialize the underlying raft implementation and start background goroutines.
func NewShardRaftNode(cfg RaftConfig) (*ShardRaftNode, error) {
	return &ShardRaftNode{
		shardID:     cfg.ShardID,
		committedCh: make(chan *CommittedRecord, 1024),
		waiters:     make(map[uint64]chan *ProposalResult),
		perBucketCommit: make(map[string]uint64),
	}, nil
}

func (s *ShardRaftNode) IsLeader() bool { return true /* stub until real raft wired */ }
func (s *ShardRaftNode) LeaderID() string { return "stub" }

// Propose submits a record asynchronously. For now the stub immediately
// synthesizes a commit (single-node mode) and returns. This allows integration
// with bucket runtime before full replication lands.
func (s *ShardRaftNode) Propose(ctx context.Context, rec *RaftLogRecord) (uint64, error) {
	if rec == nil { return 0, errors.New("nil raft record") }
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return 0, errors.New("raft: node closed") }
	s.nextRaftIndex++
	idx := s.nextRaftIndex
	// No waiter for plain Propose (fire-and-forget)
	s.mu.Unlock()
	// Apply asynchronously (single-node immediate commit simulation)
	go s.apply(idx, rec)
	return idx, nil
}

// ProposeAndWait behaves like Propose but also waits until the entry is applied
// (in the real implementation). In the stub we directly return the synthesized
// commit index (same as raft index for now) to allow upper layers to proceed.
func (s *ShardRaftNode) ProposeAndWait(ctx context.Context, rec *RaftLogRecord) (uint64, uint64, error) {
	if rec == nil { return 0, 0, errors.New("nil raft record") }
	ch := make(chan *ProposalResult, 1)
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return 0, 0, errors.New("raft: node closed") }
	s.nextRaftIndex++
	idx := s.nextRaftIndex
	s.waiters[idx] = ch
	s.mu.Unlock()
	go s.apply(idx, rec)
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case res := <-ch:
		if res == nil { return 0, 0, errors.New("raft: proposal aborted") }
		return res.CommitIndex, res.RaftIndex, nil
	}
}

// Committed exposes the ordered stream of committed raft records.
func (s *ShardRaftNode) Committed() <-chan *CommittedRecord { return s.committedCh }

// apply simulates the raft apply pipeline (single-node). In real integration
// this will be driven by the raft library's Ready()/Apply callbacks.
func (s *ShardRaftNode) apply(raftIndex uint64, rec *RaftLogRecord) {
	// Simulate minimal replication/commit latency (optional)
	// time.Sleep(0) // placeholder; kept for potential future injection
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return }
	// Assign per-bucket commit index
	s.perBucketCommit[rec.BucketID]++
	commitIdx := s.perBucketCommit[rec.BucketID]
	waiter := s.waiters[raftIndex]
	delete(s.waiters, raftIndex)
	s.mu.Unlock()

	cr := &CommittedRecord{RaftIndex: raftIndex, Term: 1, CommitIndex: commitIdx, Record: rec}
	// Non-blocking send with small timeout to avoid deadlock if consumer slow
	select {
	case s.committedCh <- cr:
	case <-time.After(100 * time.Millisecond):
		// Drop if channel full (should not happen with adequate buffer during MVP)
	}
	if waiter != nil {
		waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: raftIndex}
		close(waiter)
	}
}

// Close shuts down the shard node (stub). It prevents new proposals and closes channels after draining.
func (s *ShardRaftNode) Close() error {
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return nil }
	s.closed = true
	waiters := s.waiters
	s.waiters = map[uint64]chan *ProposalResult{}
	close(s.committedCh)
	s.mu.Unlock()
	// Abort any outstanding waiters
	for _, ch := range waiters { close(ch) }
	return nil
}

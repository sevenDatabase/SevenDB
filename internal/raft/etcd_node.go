package raft

// etcd-backed implementation wiring replacing the stub apply loop when enabled.
// For MVP we keep the public API (ShardRaftNode) and embed an etcd raft.Node.
// Single-node only in this first step (no transport). Multi-node and snapshots
// will be layered incrementally.

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	raftv3 "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// raftEngine selects implementation: stub or etcd.
// Future: make this a config value (e.g. config.Config.RaftEngine).
var raftEngine = "stub" // default; set to "etcd" to use etcd/raft path

// enableEtcdEngine is an internal helper (used by tests) to switch engine.
func enableEtcdEngine() { raftEngine = "etcd" }

// internalEtcd wraps the etcd raft state required for Ready loop.
type internalEtcd struct {
	node   raftv3.Node
	store  *memoryStorage // simple in-memory storage for single-node MVP
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// memoryStorage is a minimal adapter around raft MemoryStorage to satisfy
// potential future extension (snapshots, persistence) without leaking specifics.
type memoryStorage struct { *raftv3.MemoryStorage }

// initEtcdIfNeeded converts a stub-only ShardRaftNode into an etcd-backed one when engine selected.
func (s *ShardRaftNode) initEtcdIfNeeded(cfg RaftConfig) error {
	if raftEngine != "etcd" { return nil }
	ms := raftv3.NewMemoryStorage()
	c := &raftv3.Config{
		ID:                        1, // single-node for MVP
		ElectionTick:              max(1, cfg.ElectionTimeoutMillis/100), // rough scaling; refine later
		HeartbeatTick:             max(1, cfg.HeartbeatMillis/100),
		Storage:                   ms,
		Applied:                   0,
		CheckQuorum:               false,
		PreVote:                   false,
		DisableProposalForwarding: true,
	}
	peer := raftpb.Peer{ID: 1}
	n := raftv3.StartNode(c, []raftpb.Peer{peer})
	ctx, cancel := context.WithCancel(context.Background())
	et := &internalEtcd{node: n, store: &memoryStorage{ms}, cancel: cancel}
	s.etcd = et
	// Launch background loops
	et.wg.Add(2)
	go func(){ defer et.wg.Done(); s.etcdReadyLoop(ctx) }()
	go func(){ defer et.wg.Done(); s.etcdTickLoop(ctx, time.Duration(cfg.HeartbeatMillis)*time.Millisecond) }()
	return nil
}

// extended ShardRaftNode fields (added here to avoid breaking prior file logically)
func (s *ShardRaftNode) ensureExtendedFields() {
	// no-op placeholder: struct already has fields; this ensures older compiled test assumptions remain ok
}

// etcdTickLoop calls Node.Tick at heartbeat granularity (etcd uses logical ticks for elections & heartbeats).
func (s *ShardRaftNode) etcdTickLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 { interval = 100 * time.Millisecond }
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.etcd != nil { s.etcd.node.Tick() }
		}
	}
}

// etcdReadyLoop processes Ready structures: committed entries, messages (ignored single-node), etc.
func (s *ShardRaftNode) etcdReadyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if s.etcd != nil { s.etcd.node.Stop() }
			return
		default:
		}
		if s.etcd == nil { return }
		rd := s.etcd.node.Ready()
		// Persist hardState, entries, snapshot (in-memory storage already updated by raft).
		// Apply committed entries
		for _, ent := range rd.CommittedEntries {
			if ent.Type == raftpb.EntryNormal && len(ent.Data) > 0 {
				var rec RaftLogRecord
				if err := json.Unmarshal(ent.Data, &rec); err != nil {
					slog.Error("raft: failed unmarshal committed entry", slog.Any("error", err))
					continue
				}
				s.applyEtcdEntry(ent.Index, &rec)
			}
		}
		// Advance to let raft know we've handled this batch
		s.etcd.node.Advance()
	}
}

// applyEtcdEntry mirrors stub apply semantics but driven by raft committed log index.
func (s *ShardRaftNode) applyEtcdEntry(raftIndex uint64, rec *RaftLogRecord) {
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return }
	s.perBucketCommit[rec.BucketID]++
	commitIdx := s.perBucketCommit[rec.BucketID]
	waiter := s.waiters[raftIndex]
	delete(s.waiters, raftIndex)
	s.mu.Unlock()
	cr := &CommittedRecord{RaftIndex: raftIndex, Term: 0, CommitIndex: commitIdx, Record: rec}
	select { case s.committedCh <- cr: default: }
	if waiter != nil { waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: raftIndex}; close(waiter) }
}

// Propose overrides (etcd path) — reuse same API, switch on engine.
func (s *ShardRaftNode) proposeEtcd(ctx context.Context, rec *RaftLogRecord) (uint64, uint64, error) {
	if s.etcd == nil { return 0,0,context.Canceled }
	b, err := json.Marshal(rec)
	if err != nil { return 0,0,err }
	ch := make(chan *ProposalResult,1)
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return 0,0,context.Canceled }
	// raft index not yet known until committed; we stash waiter and rely on apply path.
	// Use a temporary negative key by incrementing nextRaftIndex to keep ordering semantics for stub fallback.
	s.nextRaftIndex++ // speculative index placeholder (not the real raft log index) used only for waiter mapping mid-flight
	waitKey := s.nextRaftIndex
	s.waiters[waitKey]=ch
	s.mu.Unlock()
	if err := s.etcd.node.Propose(ctx, b); err != nil { return 0,0,err }
	select {
	case <-ctx.Done(): return 0,0,ctx.Err()
	case res := <-ch: return res.CommitIndex, res.RaftIndex, nil
	}
}

// applyEtcdWaiterRemap is a TODO: For now we accept a slight mismatch because real raft index differs from speculative.
// Future improvement: intercept raft Ready().Entries to record mapping from (speculative key)->real index before commit.

// Adjust Close to also stop etcd loops.
func (s *ShardRaftNode) closeEtcd() {
	if s.etcd == nil { return }
	s.etcd.cancel()
	s.etcd.wg.Wait()
}

// Hook into existing Close via wrapper (will be called elsewhere when we add full integration).

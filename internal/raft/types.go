package raft

// Package raft contains the MVP scaffolding for per-shard Raft replication.
// This initial commit intentionally avoids choosing a concrete raft library
// (etcd/raft vs hashicorp/raft). A thin abstraction is provided so the rest
// of the codebase (buckets, server) can begin integrating. Subsequent commits
// will plug in a concrete implementation behind ShardRaftNode.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// RaftLogRecord is the logical unit proposed to the shard raft group.
// It intentionally omits CommitIndex which is assigned deterministically
// on apply.
type RaftLogRecord struct {
	BucketID string
	Type     int
	Payload  []byte
	// TODO: optional client metadata (ClientID, ReqID, TraceID) for tracing.
}

// CommittedRecord is emitted by ShardRaftNode when a raft log entry is
// durably committed (i.e. applied to the FSM). It includes the raft log
// index for durability tracking and raw record bytes.
type CommittedRecord struct {
	RaftIndex   uint64
	Term        uint64
	CommitIndex uint64 // per-bucket commit index assigned during apply
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

	mu              sync.Mutex
	nextRaftIndex   uint64
	perBucketCommit map[string]uint64 // last assigned per-bucket commit index
	closed          bool

	// engine selection ("stub" default, or "etcd")
	engine string

	// etcd/raft fields (nil when engine == stub)
	rn         etcdraft.Node
	storage    *etcdraft.MemoryStorage
	stopCh     chan struct{}
	tickEvery  time.Duration
	waitersSeq map[uint64]chan *ProposalResult // sequence -> waiter (etcd engine)
	nextSeq    uint64                          // atomic sequence counter for proposals under etcd engine

	// transport (only used for multi-node etcd raft). For single-node or stub it's nil or noop.
	transport Transport
}

// RaftConfig captures initialization parameters required to start a shard node.
type RaftConfig struct {
	ShardID               string
	NodeID                string
	Peers                 []string // static for MVP
	DataDir               string   // base dir for raft logs + snapshots
	HeartbeatMillis       int
	ElectionTimeoutMillis int
	Engine                string // "stub" | "etcd" (default stub)
}

// NewShardRaftNode creates a new in-memory stub. Follow-up commits will
// initialize the underlying raft implementation and start background goroutines.
func NewShardRaftNode(cfg RaftConfig) (*ShardRaftNode, error) {
	engine := cfg.Engine
	if engine == "" {
		engine = "stub"
	}
	s := &ShardRaftNode{
		shardID:         cfg.ShardID,
		committedCh:     make(chan *CommittedRecord, 1024),
		waiters:         make(map[uint64]chan *ProposalResult),
		perBucketCommit: make(map[string]uint64),
		engine:          engine,
		waitersSeq:      make(map[uint64]chan *ProposalResult),
	}
	if engine == "etcd" {
		if err := s.initEtcd(cfg); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *ShardRaftNode) IsLeader() bool {
	if s.engine == "etcd" && s.rn != nil {
		st := s.rn.Status()
		return st.ID == st.Lead
	}
	return true
}
func (s *ShardRaftNode) LeaderID() string {
	if s.engine == "etcd" && s.rn != nil {
		return strconv.FormatUint(uint64(s.rn.Status().Lead), 10)
	}
	return "stub"
}

// Propose submits a record asynchronously. For now the stub immediately
// synthesizes a commit (single-node mode) and returns. This allows integration
// with bucket runtime before full replication lands.
func (s *ShardRaftNode) Propose(ctx context.Context, rec *RaftLogRecord) (uint64, error) {
	if rec == nil {
		return 0, errors.New("nil raft record")
	}
	if s.engine == "etcd" && s.rn != nil {
		seq := atomic.AddUint64(&s.nextSeq, 1)
		wr := wireRecord{Seq: seq, BucketID: rec.BucketID, Type: rec.Type, Payload: rec.Payload}
		data, err := json.Marshal(&wr)
		if err != nil {
			return 0, err
		}
		if err = s.rn.Propose(ctx, data); err != nil {
			return 0, err
		}
		return 0, nil // commit index unknown yet
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("raft: node closed")
	}
	s.nextRaftIndex++
	idx := s.nextRaftIndex
	s.mu.Unlock()
	go s.apply(idx, rec)
	return idx, nil
}

// ProposeAndWait behaves like Propose but also waits until the entry is applied
// (in the real implementation). In the stub we directly return the synthesized
// commit index (same as raft index for now) to allow upper layers to proceed.
func (s *ShardRaftNode) ProposeAndWait(ctx context.Context, rec *RaftLogRecord) (uint64, uint64, error) {
	if rec == nil {
		return 0, 0, errors.New("nil raft record")
	}
	if s.engine == "etcd" && s.rn != nil {
		ch := make(chan *ProposalResult, 1)
		seq := atomic.AddUint64(&s.nextSeq, 1)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, 0, errors.New("raft: node closed")
		}
		s.waitersSeq[seq] = ch
		s.mu.Unlock()
		wr := wireRecord{Seq: seq, BucketID: rec.BucketID, Type: rec.Type, Payload: rec.Payload}
		data, err := json.Marshal(&wr)
		if err != nil {
			return 0, 0, err
		}
		if err = s.rn.Propose(ctx, data); err != nil {
			return 0, 0, err
		}
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case res := <-ch:
			if res == nil {
				return 0, 0, errors.New("raft: proposal aborted")
			}
			return res.CommitIndex, res.RaftIndex, nil
		}
	}
	ch := make(chan *ProposalResult, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, 0, errors.New("raft: node closed")
	}
	s.nextRaftIndex++
	idx := s.nextRaftIndex
	s.waiters[idx] = ch
	s.mu.Unlock()
	go s.apply(idx, rec)
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case res := <-ch:
		if res == nil {
			return 0, 0, errors.New("raft: proposal aborted")
		}
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
	if s.closed {
		s.mu.Unlock()
		return
	}
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
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	waiters := s.waiters
	s.waiters = map[uint64]chan *ProposalResult{}
	waitersSeq := s.waitersSeq
	s.waitersSeq = map[uint64]chan *ProposalResult{}
	if s.engine == "etcd" && s.stopCh != nil {
		close(s.stopCh)
	}
	close(s.committedCh)
	s.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
	for _, ch := range waitersSeq {
		close(ch)
	}
	if s.engine == "etcd" && s.rn != nil {
		s.rn.Stop()
	}
	return nil
}

// ---- etcd/raft integration ----
type wireRecord struct {
	Seq      uint64 `json:"seq"`
	BucketID string `json:"bucket_id"`
	Type     int    `json:"type"`
	Payload  []byte `json:"payload"`
}

func (s *ShardRaftNode) initEtcd(cfg RaftConfig) error {
	var id uint64 = 1
	if cfg.NodeID != "" {
		if v, err := strconv.ParseUint(cfg.NodeID, 10, 64); err == nil {
			id = v
		}
	}
	hb := cfg.HeartbeatMillis
	if hb <= 0 {
		hb = 100
	}
	et := cfg.ElectionTimeoutMillis
	if et <= 0 {
		et = 1000
	}
	electionTicks := et / hb
	if electionTicks <= 1 {
		electionTicks = 5
	}
	heartbeatTicks := 1
	c := &etcdraft.Config{ID: id, ElectionTick: int(electionTicks), HeartbeatTick: heartbeatTicks, Storage: etcdraft.NewMemoryStorage(), MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, CheckQuorum: true, PreVote: true, Logger: &etcdLoggerAdapter{}}
	s.storage = c.Storage.(*etcdraft.MemoryStorage)
	// TODO: parse cfg.Peers to build initial peer set (MVP keeps single self peer if len<=1)
	peers := []etcdraft.Peer{{ID: id}}
	// Future: when cfg.Peers provided in format id=addr, build transport + peer slice.
	s.rn = etcdraft.StartNode(c, peers)
	s.stopCh = make(chan struct{})
	s.tickEvery = time.Duration(hb) * time.Millisecond
	// Install a noop transport for now; future commit will wire HTTP/gRPC based multi-node message passing.
	if len(cfg.Peers) > 1 {
		// Placeholder: real transport will be constructed here using cfg.Peers
		s.transport = &noopTransport{}
	} else {
		s.transport = &noopTransport{}
	}
	go s.tickLoop()
	go s.readyLoop()
	return nil
}

func (s *ShardRaftNode) tickLoop() {
	ticker := time.NewTicker(s.tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if s.rn != nil {
				s.rn.Tick()
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *ShardRaftNode) readyLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		rd, ok := <-s.rn.Ready()
		if !ok {
			return
		}
		if len(rd.CommittedEntries) > 0 {
			for _, ent := range rd.CommittedEntries {
				if ent.Type == raftpb.EntryNormal && len(ent.Data) > 0 {
					var wr wireRecord
					if err := json.Unmarshal(ent.Data, &wr); err != nil {
						slog.Error("raft unmarshal", slog.Any("error", err))
						continue
					}
					rec := &RaftLogRecord{BucketID: wr.BucketID, Type: wr.Type, Payload: wr.Payload}
					s.mu.Lock()
					s.perBucketCommit[rec.BucketID]++
					commitIdx := s.perBucketCommit[rec.BucketID]
					waiter := s.waitersSeq[wr.Seq]
					delete(s.waitersSeq, wr.Seq)
					s.mu.Unlock()
					cr := &CommittedRecord{RaftIndex: ent.Index, Term: ent.Term, CommitIndex: commitIdx, Record: rec}
					select {
					case s.committedCh <- cr:
					case <-time.After(100 * time.Millisecond):
					}
					if waiter != nil {
						waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: ent.Index}
						close(waiter)
					}
				}
			}
		}
		// Send outbound raft messages (multi-node). Currently a noop in single-node mode.
		if len(rd.Messages) > 0 && s.transport != nil {
			// Best-effort send; errors will be logged inside transport implementation.
			s.transport.Send(context.Background(), rd.Messages)
		}
		s.rn.Advance()
	}
}

// Step injects an incoming raft message (used by network transport) into the raft state machine.
func (s *ShardRaftNode) Step(ctx context.Context, m raftpb.Message) error {
	if s.engine == "etcd" && s.rn != nil {
		return s.rn.Step(ctx, m)
	}
	return nil
}

// Transport defines the minimal interface for sending raft pb.Messages to peers.
// A future implementation will likely be HTTP or gRPC based with batching & backpressure.
type Transport interface {
	Send(ctx context.Context, msgs []raftpb.Message)
}

// noopTransport drops all messages (single-node / placeholder).
type noopTransport struct{}

func (n *noopTransport) Send(ctx context.Context, msgs []raftpb.Message) {
	// Intentionally no-op; metrics hook could count drops for debug.
}

type etcdLoggerAdapter struct{}

func (l *etcdLoggerAdapter) Debug(args ...interface{}) {
	slog.Debug("etcdraft", slog.Any("args", args))
}
func (l *etcdLoggerAdapter) Info(args ...interface{}) { slog.Info("etcdraft", slog.Any("args", args)) }
func (l *etcdLoggerAdapter) Warn(args ...interface{}) { slog.Warn("etcdraft", slog.Any("args", args)) }
func (l *etcdLoggerAdapter) Error(args ...interface{}) {
	slog.Error("etcdraft", slog.Any("args", args))
}

// Backwards-compatible alias names some raft versions still call
func (l *etcdLoggerAdapter) Warning(args ...interface{})                 { l.Warn(args...) }
func (l *etcdLoggerAdapter) Warningf(format string, args ...interface{}) { l.Warnf(format, args...) }
func (l *etcdLoggerAdapter) Debugf(format string, args ...interface{}) {
	slog.Debug("etcdraft", slog.String("msg", fmt.Sprintf(format, args...)))
}
func (l *etcdLoggerAdapter) Infof(format string, args ...interface{}) {
	slog.Info("etcdraft", slog.String("msg", fmt.Sprintf(format, args...)))
}
func (l *etcdLoggerAdapter) Warnf(format string, args ...interface{}) {
	slog.Warn("etcdraft", slog.String("msg", fmt.Sprintf(format, args...)))
}
func (l *etcdLoggerAdapter) Errorf(format string, args ...interface{}) {
	slog.Error("etcdraft", slog.String("msg", fmt.Sprintf(format, args...)))
}
func (l *etcdLoggerAdapter) Fatal(args ...interface{}) {
	slog.Error("etcdraft", slog.Any("fatal", args))
}
func (l *etcdLoggerAdapter) Fatalf(format string, args ...interface{}) {
	slog.Error("etcdraft", slog.String("fatal", fmt.Sprintf(format, args...)))
}
func (l *etcdLoggerAdapter) Panic(args ...interface{}) {
	slog.Error("etcdraft", slog.Any("panic", args))
}
func (l *etcdLoggerAdapter) Panicf(format string, args ...interface{}) {
	slog.Error("etcdraft", slog.String("panic", fmt.Sprintf(format, args...)))
}

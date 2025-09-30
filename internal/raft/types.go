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
	"path/filepath"
	"os"

	"hash/crc32"

	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/raftwal"
	"google.golang.org/protobuf/proto"

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

// Raft record Type values reserved for higher-level application semantics.
// Existing tests used Type=1 generically; to avoid accidental decoding of
// historical test payloads we reserve a distinct high value for application
// command replication going forward.
const (
	RaftRecordTypeAppCommand = 10 // JSON-encoded ReplicationPayload
)

// ReplicationPayload is the canonical serialized application command envelope
// carried inside a RaftLogRecord (Type==RaftRecordTypeAppCommand). It is
// deliberately minimal; version field enables future evolution without
// breaking old nodes during rolling upgrades.
type ReplicationPayload struct {
	Version   int      `json:"v"`
	Cmd       string   `json:"cmd"`
	Args      []string `json:"args"`
	ClientID  string   `json:"cid,omitempty"`
	RequestID string   `json:"rid,omitempty"`
}

// BuildReplicationRecord helper constructs a RaftLogRecord suitable for proposal
// carrying an application command. BucketID should be the logical bucket or shard-
// scoping identifier chosen by caller (currently opaque to raft layer).
func BuildReplicationRecord(bucketID string, cmd string, args []string) (*RaftLogRecord, error) {
	pl := &ReplicationPayload{Version: 1, Cmd: cmd, Args: args}
	b, err := json.Marshal(pl)
	if err != nil { return nil, err }
	return &RaftLogRecord{BucketID: bucketID, Type: RaftRecordTypeAppCommand, Payload: b}, nil
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
// NotLeaderError provides leader hint for clients to redirect proposals.
type NotLeaderError struct { LeaderID string }
func (e *NotLeaderError) Error() string { if e.LeaderID == "" { return "raft: not leader" }; return "raft: not leader (leader="+e.LeaderID+")" }
var ErrNotLeader = &NotLeaderError{}
var ErrNodeClosed = errors.New("raft: node closed")
var ErrProposalAborted = errors.New("raft: proposal aborted")

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

	// wg tracks background goroutines (readyLoop) to allow deterministic shutdown.
	wg sync.WaitGroup

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

	// persistence (etcd engine only)
	persist *raftPersistence
	// shadow unified WAL (optional during migration). We keep interface minimal for testability.
	walShadow    interface{ AppendEnvelope(uint64, []byte) error; Sync() error }
	walValidator *walValidator
	lastShadowHardState raftpb.HardState
	// snapshot management
	snapshotThreshold  int
	committedSinceSnap int
	lastSnapshotIndex  uint64
	lastAppliedIndex   uint64
	prunedThroughIndex uint64 // highest index for which on-disk log has been pruned (>= this index removed)

	// current configuration state for snapshot creation
	confState raftpb.ConfState

	// config flags
	forwardProposals bool

	// manual mode: if true, background goroutines (tickLoop/readyLoop)
	// are NOT started. Tests can drive the raft state machine deterministically
	// by calling ManualTick() and ManualProcessReady(). In production leave false.
	manual bool

	// replicationHandler (optional) is invoked for each committed application
	// command payload (Type == RaftRecordTypeAppCommand) after commit ordering
	// has been established. It is executed outside the internal mutex.
	replicationHandler func(*ReplicationPayload) error
	replicationStrict  bool // if true, handler errors are treated as fatal (panic) to avoid divergence
}

// ValidateShadowTail compares in-memory validator CRCs (if enabled) with the tail of shadow WAL.
// Returns nil if they match (prefix-match acceptable if WAL longer), or error describing first mismatch.
func (s *ShardRaftNode) ValidateShadowTail() error {
	if s.walValidator == nil || s.walShadow == nil { return nil }
	type dirGetter interface{ Dir() string }
	dg, ok := s.walShadow.(dirGetter); if !ok { return nil }
	type syncer interface{ Sync() error }
	if sy, ok := s.walShadow.(syncer); ok { _ = sy.Sync() }
	walDir := dg.Dir()
	ring := s.walValidator.snapshot()
	if len(ring) == 0 { return nil }
	tuples, err := raftwal.TailLogicalTuples(walDir, len(ring))
	if err != nil { return err }
	if len(tuples) != len(ring) { return fmt.Errorf("validator: length mismatch wal=%d ring=%d", len(tuples), len(ring)) }
	for i:=0; i<len(ring); i++ {
		if ring[i].index != tuples[i].Index || ring[i].crc != tuples[i].CRC {
			limit := 10; if len(ring) < limit { limit = len(ring) }
			var ringHead, walHead string
			for j:=0; j<limit; j++ { ringHead += fmt.Sprintf("%02d:%d:%08x ", j, ring[j].index, ring[j].crc) }
			for j:=0; j<limit; j++ { walHead += fmt.Sprintf("%02d:%d:%08x ", j, tuples[j].Index, tuples[j].CRC) }
			return fmt.Errorf("validator: mismatch at pos %d ring=(idx=%d crc=%08x) wal=(idx=%d crc=%08x) ring_head=[%s] wal_head=[%s]", i, ring[i].index, ring[i].crc, tuples[i].Index, tuples[i].CRC, ringHead, walHead)
		}
	}
	return nil
	}
func (s *ShardRaftNode) SetReplicationHandler(handler func(*ReplicationPayload) error, strict bool) {
	s.replicationHandler = handler
	s.replicationStrict = strict
}

// invokeReplicationHandler centralizes decoding & error policy for application
// command entries.
func (s *ShardRaftNode) invokeReplicationHandler(rec *RaftLogRecord) {
	if rec == nil || rec.Type != RaftRecordTypeAppCommand || s.replicationHandler == nil || len(rec.Payload) == 0 { return }
	var pl ReplicationPayload
	if err := json.Unmarshal(rec.Payload, &pl); err != nil {
		slog.Warn("replication payload decode failed", slog.String("shard", s.shardID), slog.Any("error", err))
		return
	}
	if err := s.replicationHandler(&pl); err != nil {
		if s.replicationStrict {
			slog.Error("replication handler fatal error", slog.String("shard", s.shardID), slog.Any("error", err))
			panic(fmt.Sprintf("replication handler error (strict): %v", err))
		}
		slog.Warn("replication handler error", slog.String("shard", s.shardID), slog.Any("error", err))
	}
}

// StatusSnapshot is a lightweight snapshot of raft node state for observability.
type StatusSnapshot struct {
	ShardID              string            `json:"shard_id"`
	LeaderID             string            `json:"leader_id"`
	IsLeader             bool              `json:"is_leader"`
	LastAppliedIndex     uint64            `json:"last_applied_index"`
	LastSnapshotIndex    uint64            `json:"last_snapshot_index"`
	CommittedSinceSnap   int               `json:"committed_since_snapshot"`
	PerBucketCommit      map[string]uint64 `json:"per_bucket_commit"`
	PrunedThroughIndex   uint64            `json:"pruned_through_index"`
	TransportPeersTotal  int               `json:"transport_peers_total"`
	TransportPeersConnected int            `json:"transport_peers_connected"`
}

// Status returns an immutable snapshot of internal counters useful for debug/metrics.
func (s *ShardRaftNode) Status() StatusSnapshot {
	s.mu.Lock(); defer s.mu.Unlock()
	copyMap := make(map[string]uint64, len(s.perBucketCommit))
	for k, v := range s.perBucketCommit { copyMap[k] = v }
	ss := StatusSnapshot{ShardID: s.shardID, LeaderID: s.LeaderID(), IsLeader: s.IsLeader(), LastAppliedIndex: s.lastAppliedIndex, LastSnapshotIndex: s.lastSnapshotIndex, CommittedSinceSnap: s.committedSinceSnap, PerBucketCommit: copyMap, PrunedThroughIndex: s.prunedThroughIndex}
	if gt, ok := s.transport.(*GRPCTransport); ok && gt != nil {
		ps := gt.Stats(); ss.TransportPeersTotal = ps.Total; ss.TransportPeersConnected = ps.Connected
	}
	return ss
}

// Transport abstracts how raft messages are sent to peers (multi-node). Minimal
// interface to decouple core raft application from specific networking (gRPC, etc.).
type Transport interface {
	Send(ctx context.Context, msgs []raftpb.Message)
}

// noopTransport satisfies Transport for single-node or tests where we ignore outbound messages.
type noopTransport struct{}
func (n *noopTransport) Send(ctx context.Context, msgs []raftpb.Message) {}

// etcdLoggerAdapter adapts slog to etcd/raft Logger interface (fmt-like methods).
type etcdLoggerAdapter struct{}
func (l *etcdLoggerAdapter) Debug(args ...interface{})                 { slog.Debug(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Debugf(format string, args ...interface{}) { slog.Debug(fmt.Sprintf(format, args...)) }
func (l *etcdLoggerAdapter) Info(args ...interface{})                  { slog.Info(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Infof(format string, args ...interface{})  { slog.Info(fmt.Sprintf(format, args...)) }
func (l *etcdLoggerAdapter) Warning(args ...interface{})               { slog.Warn(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Warningf(format string, args ...interface{}) { slog.Warn(fmt.Sprintf(format, args...)) }
func (l *etcdLoggerAdapter) Error(args ...interface{})                 { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Errorf(format string, args ...interface{}) { slog.Error(fmt.Sprintf(format, args...)) }
func (l *etcdLoggerAdapter) Fatal(args ...interface{})                 { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Fatalf(format string, args ...interface{}) { slog.Error(fmt.Sprintf(format, args...)) }
func (l *etcdLoggerAdapter) Panic(args ...interface{})                 { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Panicf(format string, args ...interface{}) { slog.Error(fmt.Sprintf(format, args...)) }

// walValidator (tuple-based) tracks last N (raftIndex, crc32(payload)) for deterministic ordering.
type walValidator struct {
	mu    sync.Mutex
	lastN int
	ring  []walValTuple
	idx   int
	count int
}

type walValTuple struct { index uint64; crc uint32 }

func newWalValidator(n int) *walValidator { if n <= 0 { return nil }; return &walValidator{lastN: n, ring: make([]walValTuple, n)} }

func (v *walValidator) addTuple(index uint64, payload []byte) {
	if v == nil { return }
	crc := crc32.ChecksumIEEE(payload)
	v.mu.Lock()
	v.ring[v.idx] = walValTuple{index: index, crc: crc}
	v.idx = (v.idx + 1) % v.lastN
	if v.count < v.lastN { v.count++ }
	v.mu.Unlock()
}

func (v *walValidator) snapshot() []walValTuple {
	if v == nil { return nil }
	v.mu.Lock(); defer v.mu.Unlock()
	if v.count == 0 { return nil }
	out := make([]walValTuple, 0, v.count)
	start := (v.idx - v.count + v.lastN) % v.lastN
	for i:=0; i<v.count; i++ { out = append(out, v.ring[(start+i)%v.lastN]) }
	return out
}

func (v *walValidator) seedTuples(tuples []struct{Index uint64; CRC uint32}) {
	if v == nil || len(tuples) == 0 { return }
	v.mu.Lock()
	if len(tuples) > v.lastN { tuples = tuples[len(tuples)-v.lastN:] }
	for i, t := range tuples { v.ring[i] = walValTuple{index: t.Index, crc: t.CRC} }
	v.count = len(tuples)
	v.idx = v.count % v.lastN
	v.mu.Unlock()
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
	ForwardProposals      bool   // if true, return NotLeaderError with leader hint (future: internal forwarding)
	Manual                bool   // if true, do not spawn background loops; tests drive manually
	DisablePersistence    bool   // if true (tests), skip disk persistence and load; purely in-memory
	// Migration shadow unified WAL configuration
	EnableWALShadow bool   // dual-write new WAL envelopes
	WALShadowDir    string // override directory (default DataDir/raftwal)
	ValidatorLastN  int    // if >0 track last N entries for parity validation
	// WAL tuning & test knobs
	WALSegmentMaxBytes int64 // override default 64MiB segment size (<=0 keeps default)
	WALForceRotateEvery int  // if >0 force rotation after every N appends (test only)
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
		forwardProposals: cfg.ForwardProposals,
		manual:           cfg.Manual,
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
	if s.engine == "etcd" && s.rn != nil { return strconv.FormatUint(uint64(s.rn.Status().Lead), 10) }
	return "stub"
}

// ShardID returns the identifier of this shard raft node (exported accessor).
func (s *ShardRaftNode) ShardID() string { return s.shardID }

// Step injects an incoming raft message (used by transport) into the raft state machine.
func (s *ShardRaftNode) Step(ctx context.Context, m raftpb.Message) error {
	if s.engine == "etcd" && s.rn != nil { return s.rn.Step(ctx, m) }
	return nil
}

// SetTransport installs a transport implementation (multi-node). Safe to call once.
func (s *ShardRaftNode) SetTransport(t Transport) {
	s.transport = t
}

// Propose submits a record asynchronously.
func (s *ShardRaftNode) Propose(ctx context.Context, rec *RaftLogRecord) (uint64, error) {
	if rec == nil { return 0, errors.New("nil raft record") }
	if s.engine == "etcd" && s.rn != nil && !s.IsLeader() {
		if s.forwardProposals { return 0, &NotLeaderError{LeaderID: s.LeaderID()} }
		return 0, &NotLeaderError{LeaderID: s.LeaderID()}
	}
	if s.engine == "etcd" && s.rn != nil {
		seq := atomic.AddUint64(&s.nextSeq, 1)
		wr := wireRecord{Seq: seq, BucketID: rec.BucketID, Type: rec.Type, Payload: rec.Payload}
		data, err := json.Marshal(&wr)
		if err != nil { return 0, err }
		if err = s.rn.Propose(ctx, data); err != nil { return 0, err }
		return 0, nil
	}
	s.mu.Lock()
	if s.closed { s.mu.Unlock(); return 0, errors.New("raft: node closed") }
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
	if s.engine == "etcd" && s.rn != nil && !s.IsLeader() {
		if s.forwardProposals {
			return 0, 0, &NotLeaderError{LeaderID: s.LeaderID()}
		}
		return 0, 0, &NotLeaderError{LeaderID: s.LeaderID()}
	}
	if s.engine == "etcd" && s.rn != nil {
		ch := make(chan *ProposalResult, 1)
		seq := atomic.AddUint64(&s.nextSeq, 1)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, 0, errors.New("raft: node closed")
		}
		if s.waitersSeq == nil { s.waitersSeq = make(map[uint64]chan *ProposalResult) }
		if s.waitersSeq == nil { s.waitersSeq = make(map[uint64]chan *ProposalResult) }
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
			if res == nil { return 0, 0, ErrProposalAborted }
			return res.CommitIndex, res.RaftIndex, nil
		}
	}
	ch := make(chan *ProposalResult, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, 0, errors.New("raft: node closed")
	}
	if s.waiters == nil { s.waiters = make(map[uint64]chan *ProposalResult) }
	s.nextRaftIndex++
	idx := s.nextRaftIndex
	s.waiters[idx] = ch
	s.mu.Unlock()
	go s.apply(idx, rec)
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case res := <-ch:
		if res == nil { return 0, 0, ErrProposalAborted }
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
	// Apply application-level state mutation (stub engine)
	s.invokeReplicationHandler(rec)
	// TODO: optionally mirror shadow WAL here in stub engine to allow deterministic harness parity tests for migration.
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
	waiters := s.waiters; s.waiters = map[uint64]chan *ProposalResult{}
	waitersSeq := s.waitersSeq; s.waitersSeq = map[uint64]chan *ProposalResult{}
	if s.engine == "etcd" && s.stopCh != nil { close(s.stopCh) }
	close(s.committedCh)
	s.mu.Unlock()
	// Abort all outstanding waiters.
	for _, ch := range waiters { func(c chan *ProposalResult){ defer func(){ recover() }(); c <- nil; close(c) }(ch) }
	for _, ch := range waitersSeq { func(c chan *ProposalResult){ defer func(){ recover() }(); c <- nil; close(c) }(ch) }
	if s.engine == "etcd" && s.rn != nil { s.rn.Stop() }
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
	var persist *raftPersistence
	storage := etcdraft.NewMemoryStorage()
	var loaded bool
	if !cfg.DisablePersistence {
		persistDir := cfg.DataDir
		if persistDir == "" { persistDir = "raftdata" }
		p, err := newRaftPersistence(persistDir, cfg.ShardID)
		if err != nil { return err }
		persist = p
		// Attempt load of prior state (snapshot, hard state, entries)
		var snapIdx, lastEntryIdx uint64
		loaded, snapIdx, lastEntryIdx, err = persist.Load(storage)
		if err != nil {
			slog.Warn("raft persistence load failed", slog.String("shard", cfg.ShardID), slog.Any("error", err))
		} else if loaded {
			s.lastSnapshotIndex = snapIdx
			if lastEntryIdx > snapIdx { s.lastAppliedIndex = lastEntryIdx } else { s.lastAppliedIndex = snapIdx }
		}
	}
	c := &etcdraft.Config{ID: id, ElectionTick: int(electionTicks), HeartbeatTick: heartbeatTicks, Storage: storage, MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, CheckQuorum: true, PreVote: true, Logger: &etcdLoggerAdapter{}}
	s.storage = storage
	var peers []etcdraft.Peer
	var confVoters []uint64
	if len(cfg.Peers) == 0 {
		peers = []etcdraft.Peer{{ID: id}}
		confVoters = append(confVoters, id)
	} else {
		// Expect peer specs as "<id>@<addr>"; we only need IDs here.
		for _, spec := range cfg.Peers {
			parts := spec
			at := -1
			for i, ch := range spec {
				if ch == '@' { at = i; break }
			}
			if at != -1 { parts = spec[:at] }
			if pid, err := strconv.ParseUint(parts, 10, 64); err == nil {
				peers = append(peers, etcdraft.Peer{ID: pid})
				confVoters = append(confVoters, pid)
			}
		}
		// Ensure self id is included.
		found := false
		for _, p := range peers { if p.ID == id { found = true; break } }
		if !found { peers = append(peers, etcdraft.Peer{ID: id}); confVoters = append(confVoters, id) }
	}
	if loaded { // restart path
		s.rn = etcdraft.RestartNode(c)
	} else {
		s.rn = etcdraft.StartNode(c, peers)
	}
	// If shadow WAL enabled attempt to replay last HardState envelope (non-fatal). This can override legacy file if newer.
	if cfg.EnableWALShadow && !cfg.DisablePersistence {
		shadowDir := cfg.WALShadowDir
		if shadowDir == "" { shadowDir = filepath.Join(cfg.DataDir, "raftwal") }
		if hsBytes, ok, herr := raftwal.ReplayLastHardState(shadowDir); herr == nil && ok {
			var hs raftpb.HardState
			if uerr := hs.Unmarshal(hsBytes); uerr == nil && !etcdraft.IsEmptyHardState(hs) {
				// Compare with lastShadowHardState (zero-value if not set). Accept if strictly newer in term or commit.
				if hs.Term > s.lastShadowHardState.Term || hs.Commit > s.lastShadowHardState.Commit {
					s.storage.SetHardState(hs)
					s.lastShadowHardState = hs
				}
			}
		}
	}
	// record conf state for snapshots
	s.confState = raftpb.ConfState{Voters: confVoters}
	// Shadow WAL setup (non-fatal). Only when enabled and persistence not disabled.
	if cfg.EnableWALShadow && !cfg.DisablePersistence {
		shadowDir := cfg.WALShadowDir
		if shadowDir == "" { shadowDir = filepath.Join(cfg.DataDir, "raftwal") }
		if err := os.MkdirAll(shadowDir, 0o755); err != nil { slog.Warn("wal shadow mkdir failed", slog.Any("error", err)) } else {
			segSize := cfg.WALSegmentMaxBytes
			if segSize <= 0 { segSize = 64*1024*1024 }
			wcfg := raftwalConfigCompat{Dir: shadowDir, BufMB: 4, SidecarFlushEvery: 256, SegmentMaxBytes: segSize, ForceRotateEvery: cfg.WALForceRotateEvery}
			if wr, werr := raftwalNewWriterCompat(wcfg); werr != nil { slog.Warn("wal shadow init failed", slog.Any("error", werr)) } else { s.walShadow = wr }
		}
		if cfg.ValidatorLastN > 0 { 
			s.walValidator = newWalValidator(cfg.ValidatorLastN)
			if tuples, err := raftwal.TailLogicalTuples(shadowDir, s.walValidator.lastN); err == nil && len(tuples) > 0 {
				s.walValidator.seedTuples(tuples)
			}
		}
	}
	s.stopCh = make(chan struct{})
	s.tickEvery = time.Duration(hb) * time.Millisecond
	// Install noop transport by default (caller can SetTransport later)
	s.transport = &noopTransport{}
	s.persist = persist
	// snapshot threshold: tolerate nil global config (tests with DisablePersistence)
	if config.Config != nil {
		s.snapshotThreshold = config.Config.RaftSnapshotThresholdEntries
	} else {
		// default high threshold to avoid unexpected snapshotting in tests unless explicitly configured
		s.snapshotThreshold = 0
	}
	if !s.manual { // only spawn background loops when not in manual deterministic mode
		go s.tickLoop()
		go s.readyLoop()
	}
	return nil
}

func (s *ShardRaftNode) tickLoop() {
	ticker := time.NewTicker(s.tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if s.rn != nil { s.rn.Tick() }
		case <-s.stopCh:
			return
		}
	}
}

func (s *ShardRaftNode) readyLoop() {
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		rd, ok := <-s.rn.Ready()
		if !ok { return }
		s.processReady(rd)
	}
}

// processReady centralizes handling of an etcd raft Ready. It is invoked by both
// background and manual deterministic paths to ensure identical semantics.
func (s *ShardRaftNode) processReady(rd etcdraft.Ready) {
	// If node is closed, discard any late Ready (can happen after Close races with etcd raft goroutine).
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	// capture committed channel safely (channel itself will be closed after we release lock, we guard sends later by re-checking closed state indirectly)
	committedCh := s.committedCh
	s.mu.Unlock()
	// Persist raft state (HardState, Entries, Snapshot) before applying to storage
	if s.persist != nil {
		if err := s.persist.Persist(&rd); err != nil {
			slog.Warn("raft persist failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
	}
	// Shadow WAL: write HardState envelope if changed.
	if s.walShadow != nil && !etcdraft.IsEmptyHardState(rd.HardState) {
		if rd.HardState.Term != s.lastShadowHardState.Term || rd.HardState.Vote != s.lastShadowHardState.Vote || rd.HardState.Commit != s.lastShadowHardState.Commit {
			// marshal raft HardState using protobuf (raftpb.HardState implements Marshal via gogoproto but we'll json encode for now for minimal dependency) TODO: switch to direct protobuf bytes.
			// Use protobuf marshal for fidelity.
			b, mErr := rd.HardState.Marshal()
			if mErr == nil {
				// HardState has no raft index itself; we record with raft_index = rd.HardState.Commit (best stable monotonic value) and term field for ordering in replay.
				env := &raftwal.Envelope{RaftIndex: rd.HardState.Commit, RaftTerm: rd.HardState.Term, Kind: raftwal.EntryKind_ENTRY_HARDSTATE, AppBytes: b, AppCrc: crc32.ChecksumIEEE(b)}
				if pb, perr := proto.Marshal(env); perr == nil { _ = s.walShadow.AppendEnvelope(env.RaftIndex, pb) } else { slog.Warn("wal shadow hardstate marshal envelope failed", slog.Any("error", perr)) }
				s.lastShadowHardState = rd.HardState
			} else { slog.Warn("wal shadow hardstate marshal failed", slog.Any("error", mErr)) }
		}
	}
	// Apply snapshot (if any) BEFORE appending new entries.
	if !etcdraft.IsEmptySnap(rd.Snapshot) && s.storage != nil {
		if err := s.storage.ApplySnapshot(rd.Snapshot); err != nil {
			slog.Warn("raft storage apply snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
	}
	// Append new entries (contract requirement) prior to Advance.
	if len(rd.Entries) > 0 && s.storage != nil {
		if err := s.storage.Append(rd.Entries); err != nil {
			slog.Warn("raft storage append failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
	}
	// Process committed entries.
	if len(rd.CommittedEntries) > 0 {
		for _, ent := range rd.CommittedEntries {
			switch ent.Type {
			case raftpb.EntryNormal:
				if len(ent.Data) == 0 { // noop/heartbeat
					s.mu.Lock(); s.lastAppliedIndex = ent.Index; s.mu.Unlock(); continue
				}
				var wr wireRecord
				if err := json.Unmarshal(ent.Data, &wr); err != nil { slog.Error("raft unmarshal", slog.Any("error", err)); continue }
				rec := &RaftLogRecord{BucketID: wr.BucketID, Type: wr.Type, Payload: wr.Payload}
				// Shadow WAL dual-write (best-effort, non-fatal). Only for application normal entries.
				if s.walShadow != nil && rec.Type == RaftRecordTypeAppCommand {
					// Build Envelope proto
					// Legacy payload contains JSON ReplicationPayload; we store raw bytes as app_bytes for now.
					// Future: decode and populate structured fields when cutover.
					env := &raftwal.Envelope{RaftIndex: ent.Index, RaftTerm: ent.Term, Kind: raftwal.EntryKind_ENTRY_NORMAL, AppBytes: rec.Payload, AppCrc: crc32.ChecksumIEEE(rec.Payload)}
					if b, mErr := proto.Marshal(env); mErr == nil { _ = s.walShadow.AppendEnvelope(ent.Index, b) } else { slog.Warn("wal shadow marshal failed", slog.Any("error", mErr)) }
					if s.walValidator != nil { s.walValidator.addTuple(ent.Index, rec.Payload) }
				}
				s.mu.Lock()
				s.perBucketCommit[rec.BucketID]++
				commitIdx := s.perBucketCommit[rec.BucketID]
				waiter := s.waitersSeq[wr.Seq]
				delete(s.waitersSeq, wr.Seq)
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
				cr := &CommittedRecord{RaftIndex: ent.Index, Term: ent.Term, CommitIndex: commitIdx, Record: rec}
				// Apply application mutation
				s.invokeReplicationHandler(rec)
				// Non-blocking send; avoid panic if Close closed channel between earlier capture and now by recover guard.
				if committedCh != nil {
					func() {
						defer func(){ if r:=recover(); r!=nil { /* channel closed */ } }()
						select { case committedCh <- cr: case <-time.After(100 * time.Millisecond): }
					}()
				}
				if waiter != nil { waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: ent.Index}; close(waiter) }
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if len(ent.Data) > 0 { _ = cc.Unmarshal(ent.Data) }
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock(); s.confState = *cs; s.lastAppliedIndex = ent.Index; s.mu.Unlock()
			case raftpb.EntryConfChangeV2:
				var cc raftpb.ConfChangeV2
				if len(ent.Data) > 0 { _ = cc.Unmarshal(ent.Data) }
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock(); s.confState = *cs; s.lastAppliedIndex = ent.Index; s.mu.Unlock()
			default:
				s.mu.Lock(); s.lastAppliedIndex = ent.Index; s.mu.Unlock()
			}
		}
	}
	// Send outbound messages (multi-node scenarios)
	if len(rd.Messages) > 0 && s.transport != nil { s.transport.Send(context.Background(), rd.Messages) }
	// Advance raft state machine
	s.rn.Advance()
	// Auto-campaign convenience: in non-manual single-node mode ensure a leader emerges quickly
	if !s.manual && s.rn.Status().Lead == 0 && len(s.confState.Voters) == 1 { _ = s.rn.Campaign(context.Background()) }
	// Snapshot / compaction logic
	if s.snapshotThreshold > 0 && s.committedSinceSnap >= s.snapshotThreshold && s.lastAppliedIndex > s.lastSnapshotIndex {
		if s.storage != nil {
			if snap, err := s.storage.CreateSnapshot(s.lastAppliedIndex, &s.confState, nil); err == nil && !etcdraft.IsEmptySnap(snap) {
				if s.persist != nil {
					if err := s.persist.PersistSnapshot(&snap); err != nil {
						slog.Warn("raft snapshot persist failed", slog.String("shard", s.shardID), slog.Any("error", err))
					} else {
						compactionIndex := snap.Metadata.Index
						if err := s.storage.Compact(compactionIndex); err != nil { slog.Warn("raft storage compact failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
						if err := s.persist.PruneEntries(compactionIndex); err != nil { slog.Warn("raft prune failed", slog.String("shard", s.shardID), slog.Any("error", err)) } else {
							s.mu.Lock()
	// Wait for background goroutines (readyLoop) to exit to avoid races with temp dir cleanup.
	s.wg.Wait()
							if compactionIndex > s.prunedThroughIndex { s.prunedThroughIndex = compactionIndex }
							if snap.Metadata.Index > s.lastSnapshotIndex { s.lastSnapshotIndex = snap.Metadata.Index }
							s.committedSinceSnap = 0
							s.mu.Unlock()
						}
						if s.prunedThroughIndex < compactionIndex { s.mu.Lock(); if snap.Metadata.Index > s.lastSnapshotIndex { s.lastSnapshotIndex = snap.Metadata.Index }; s.committedSinceSnap = 0; s.mu.Unlock() }
					}
				}
			} else if err != nil { slog.Warn("raft create snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
		}
	}
}

// ---- shadow WAL writer compatibility helpers ----
// Local minimal copy of raftwal.Config to avoid direct import (decouples migration path).
type raftwalConfigCompat struct {
	Dir string
	BufMB int
	SidecarFlushEvery int
	SegmentMaxBytes int64
	ForceRotateEvery int
}

// raftwalWriterCompat is satisfied by *raftwal.Writer (subset) - defined here for loose coupling.
type raftwalWriterCompat interface { AppendEnvelope(uint64, []byte) error; Sync() error }

// raftwalNewWriterCompat uses reflection-free construction by importing the real package.
// NOTE: we import inside function to avoid unused dependency when shadow disabled.
func raftwalNewWriterCompat(cfg raftwalConfigCompat) (raftwalWriterCompat, error) {
	w, err := raftwal.NewWriter(raftwal.Config{Dir: cfg.Dir, BufMB: cfg.BufMB, SidecarFlushEvery: cfg.SidecarFlushEvery, SegmentMaxBytes: cfg.SegmentMaxBytes, ForceRotateEvery: cfg.ForceRotateEvery})
	if err != nil { return nil, err }
	return w, nil
}

// Remove unused linkname approach.

// ManualTick advances the raft node one logical heartbeat/election tick.
// Only valid when RaftConfig.Manual = true.
func (s *ShardRaftNode) ManualTick() {
	if !s.manual || s.rn == nil { return }
	s.rn.Tick()
}

// ManualForceCampaign forces a leadership campaign (single-node deterministic tests).
// Safe only in manual mode; ignores errors.
func (s *ShardRaftNode) ManualForceCampaign() {
    if !s.manual || s.rn == nil { return }
    _ = s.rn.Campaign(context.Background())
}

// ManualProcessReady drains a single Ready from the raft node and processes it
// identically to the background readyLoop. Returns false if no Ready was
// available immediately. Only valid in manual mode.
func (s *ShardRaftNode) ManualProcessReady() bool {
	if !s.manual || s.rn == nil { return false }
	select {
	case rd, ok := <-s.rn.Ready():
		if !ok { return false }
		s.processReady(rd)
		return true
	default:
		return false
	}
}

// ManualProcessNextReady blocks until a Ready is available (or the raft node closes)
// and processes it. Returns true if a Ready was processed. Only valid in manual mode.
func (s *ShardRaftNode) ManualProcessNextReady() bool {
	if !s.manual || s.rn == nil { return false }
	rd, ok := <-s.rn.Ready()
	if !ok { return false }
	if s.persist != nil {
		if err := s.persist.Persist(&rd); err != nil { slog.Warn("raft persist failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
	}
	if !etcdraft.IsEmptySnap(rd.Snapshot) && s.storage != nil {
		if err := s.storage.ApplySnapshot(rd.Snapshot); err != nil { slog.Warn("raft storage apply snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
	}
	if len(rd.Entries) > 0 && s.storage != nil {
		if err := s.storage.Append(rd.Entries); err != nil { slog.Warn("raft storage append failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
	}
	if len(rd.CommittedEntries) > 0 {
		for _, ent := range rd.CommittedEntries {
			if ent.Type == raftpb.EntryNormal && len(ent.Data) > 0 {
				var wr wireRecord
				if err := json.Unmarshal(ent.Data, &wr); err != nil { slog.Error("raft unmarshal", slog.Any("error", err)); continue }
				rec := &RaftLogRecord{BucketID: wr.BucketID, Type: wr.Type, Payload: wr.Payload}
				s.mu.Lock(); s.perBucketCommit[rec.BucketID]++; commitIdx := s.perBucketCommit[rec.BucketID]; waiter := s.waitersSeq[wr.Seq]; delete(s.waitersSeq, wr.Seq); s.lastAppliedIndex = ent.Index; s.committedSinceSnap++; s.mu.Unlock()
				cr := &CommittedRecord{RaftIndex: ent.Index, Term: ent.Term, CommitIndex: commitIdx, Record: rec}
				// Apply application mutation
				s.invokeReplicationHandler(rec)
				select { case s.committedCh <- cr: case <-time.After(100 * time.Millisecond): }
				if waiter != nil { waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: ent.Index}; close(waiter) }
			} else if ent.Type == raftpb.EntryConfChange {
				var cc raftpb.ConfChange
				if len(ent.Data) > 0 { _ = cc.Unmarshal(ent.Data) }
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock(); s.confState = *cs; s.lastAppliedIndex = ent.Index; s.mu.Unlock()
			} else if ent.Type == raftpb.EntryConfChangeV2 {
				var cc raftpb.ConfChangeV2
				if len(ent.Data) > 0 { _ = cc.Unmarshal(ent.Data) }
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock(); s.confState = *cs; s.lastAppliedIndex = ent.Index; s.mu.Unlock()
			}
		}
	}
	if len(rd.Messages) > 0 && s.transport != nil { s.transport.Send(context.Background(), rd.Messages) }
	s.rn.Advance()
	if s.rn.Status().Lead == 0 && len(s.confState.Voters) == 1 { _ = s.rn.Campaign(context.Background()) }
	if s.snapshotThreshold > 0 && s.committedSinceSnap >= s.snapshotThreshold && s.lastAppliedIndex > s.lastSnapshotIndex {
		if s.storage != nil {
			if snap, err := s.storage.CreateSnapshot(s.lastAppliedIndex, &s.confState, nil); err == nil && !etcdraft.IsEmptySnap(snap) {
				if s.persist != nil {
					if err := s.persist.PersistSnapshot(&snap); err != nil { slog.Warn("raft snapshot persist failed", slog.String("shard", s.shardID), slog.Any("error", err)) } else {
						compactionIndex := snap.Metadata.Index
						if err := s.storage.Compact(compactionIndex); err != nil { slog.Warn("raft storage compact failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
						if err := s.persist.PruneEntries(compactionIndex); err != nil { slog.Warn("raft prune failed", slog.String("shard", s.shardID), slog.Any("error", err)) } else {
							s.mu.Lock(); if compactionIndex > s.prunedThroughIndex { s.prunedThroughIndex = compactionIndex }; if snap.Metadata.Index > s.lastSnapshotIndex { s.lastSnapshotIndex = snap.Metadata.Index }; s.committedSinceSnap = 0; s.mu.Unlock()
						}
						if s.prunedThroughIndex < compactionIndex { s.mu.Lock(); if snap.Metadata.Index > s.lastSnapshotIndex { s.lastSnapshotIndex = snap.Metadata.Index }; s.committedSinceSnap = 0; s.mu.Unlock() }
					}
				}
			} else if err != nil { slog.Warn("raft create snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err)) }
		}
	}
	return true
}

// ManualStep performs a single logical advancement: one Tick followed by
// draining all immediately available Ready structs. Useful for tests.
// (single implementation retained above; duplicate removed)

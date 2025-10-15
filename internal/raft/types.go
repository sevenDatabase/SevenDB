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
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/harness/clock"
	"github.com/sevenDatabase/SevenDB/internal/wal"
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

// ReplicationPayload represents the higher-level application command carried in
// a RaftLogRecord when Type == RaftRecordTypeAppCommand. This was previously
// defined elsewhere; reintroducing here after import cleanup.
type ReplicationPayload struct {
	Version int      `json:"v"`
	Cmd     string   `json:"cmd"`
	Args    []string `json:"args"`
}

// Raft record type constants.
const (
	RaftRecordTypeAppCommand = 1001 // application command replication
)

// Raft record Type values reserved for higher-level application semantics.
// Existing tests used Type=1 generically; to avoid accidental decoding of
// historical test payloads we reserve a distinct high value for application
// command replication going forward.
// (duplicate import block removed)
// carrying an application command. BucketID should be the logical bucket or shard-
// scoping identifier chosen by caller (currently opaque to raft layer).
func BuildReplicationRecord(bucketID string, cmd string, args []string) (*RaftLogRecord, error) {
	pl := &ReplicationPayload{Version: 1, Cmd: cmd, Args: args}
	b, err := json.Marshal(pl)
	if err != nil {
		return nil, err
	}
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
type NotLeaderError struct{ LeaderID string }

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "raft: not leader"
	}
	return "raft: not leader (leader=" + e.LeaderID + ")"
}

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
	closedAtomic    atomic.Bool // duplicate of closed for fast, lock-free checks in hot paths

	// engine selection ("stub" default, or "etcd")
	engine string

	// etcd/raft fields (nil when engine == stub)
	rn         etcdraft.Node
	storage    *etcdraft.MemoryStorage
	stopCh     chan struct{}
	tickEvery  time.Duration
	waitersSeq map[uint64]chan *ProposalResult // sequence -> waiter (etcd engine)
	nextSeq    uint64                          // atomic sequence counter for proposals under etcd engine
	// lastKnownLeader caches the most recent non-zero leader ID observed via Ready.SoftState.
	// This helps followers surface an up-to-date leader hint in NotLeaderError even if their
	// own etcd Status() snapshot still shows Lead=0 due to a race between election and proposal.
	// lastKnownLeader caches latest leader ID observed. Accessed atomically to avoid
	// taking s.mu inside hot-path status queries (prevents subtle self-deadlock patterns
	// when Status() is called while holding s.mu elsewhere or during test harness loops).
	lastKnownLeader uint64

	// transport (only used for multi-node etcd raft). For single-node or stub it's nil or noop.
	transport Transport

	// persistence (etcd engine only)
	persist *raftPersistence
	// shadow unified WAL (optional during migration). We keep interface minimal for testability.
	walShadow interface {
		AppendEnvelope(uint64, []byte) error
		AppendHardState(uint64, []byte) error
		Sync() error
	}
	// wal (new) is the abstracted write-ahead log interface (superset use-case). During
	// migration both walShadow (legacy shadow writer) and wal may coexist; future cleanup
	// will remove walShadow once all call sites are switched to wal.
	wal                 WAL
	walValidator        *walValidator
	walDualReadValidate bool
	// lastShadowHardState is accessed from processReady (background goroutine) and
	// initialization/replay path; guard with mu to avoid data races under -race.
	lastShadowHardState raftpb.HardState
	// snapshot management
	snapshotThreshold  int
	committedSinceSnap int
	lastSnapshotIndex  uint64
	lastAppliedIndex   uint64
	prunedThroughIndex uint64 // highest index for which on-disk log has been pruned (>= this index removed)

	// optional deterministic clock for tests (nil => real time ticker)
	clk clock.Clock

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

	// walPruneBusy guards scheduling of a background WAL prune worker to avoid
	// blocking the raft Ready loop and to prevent concurrent prune workers.
	walPruneBusy atomic.Bool
}

// Crash simulates an abrupt process crash for test scenarios. It attempts a minimal
// shutdown without flushing or closing WALs gracefully. Background goroutines are
// stopped and the node is marked closed so subsequent Status/IsLeader calls become
// no-ops. Unlike Close(), Crash() purposefully skips WAL Close() to mimic a crash.
// It is idempotent.
func (s *ShardRaftNode) Crash() {
	s.mu.Lock()
	if s.closed { // already closed/crashed
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.closedAtomic.Store(true)
	if s.engine == "etcd" && s.stopCh != nil {
		close(s.stopCh)
	}
	var rn etcdraft.Node
	if s.engine == "etcd" && s.rn != nil {
		rn = s.rn
	}
	s.mu.Unlock()
	if rn != nil {
		rn.Stop()
	}
	s.wg.Wait()
	// Intentionally DO NOT close committedCh or waiters; mimic abrupt termination where
	// in-flight proposals observe ErrProposalAborted on channel close later (ignored here).
}

// waitForLeaderHint spins briefly (bounded) to obtain a non-empty leader ID after an election.
// This reduces racy empty hints just after leadership changes. It does not block more than
// attempts * 200µs (~5ms for 25 attempts).
func (s *ShardRaftNode) waitForLeaderHint(attempts int) string {
	if attempts <= 0 {
		return ""
	}
	for i := 0; i < attempts; i++ {
		lid := s.leaderHint()
		if lid != "" {
			return lid
		}
		if s.engine == "etcd" && s.rn != nil {
			st := s.rn.Status()
			if st.Lead != 0 {
				atomic.StoreUint64(&s.lastKnownLeader, st.Lead)
				return strconv.FormatUint(uint64(st.Lead), 10)
			}
		}
		time.Sleep(200 * time.Microsecond)
	}
	return ""
}

// ValidateShadowTail compares in-memory validator CRCs (if enabled) with the tail of shadow WAL.
// Returns nil if they match (prefix-match acceptable if WAL longer), or error describing first mismatch.

// ValidateShadowTail validates the recent logical WAL entries against the validator ring.
// Migration NOTE: This function primarily targets the legacy shadow WAL; if an abstract WAL
// is present it will prefer its directory. Once cutover completes, rename to ValidateWALTail.
func (s *ShardRaftNode) ValidateShadowTail() error {
	if s.walValidator == nil {
		return nil
	}
	walDir := ""
	// Prefer abstract WAL if available.
	if s.wal != nil {
		walDir = s.wal.Dir()
	} else if s.walShadow != nil {
		type dirGetter interface{ Dir() string }
		if dg, ok := s.walShadow.(dirGetter); ok {
			walDir = dg.Dir()
		}
		// Attempt sync for legacy writer to flush buffers before tail read.
		type syncer interface{ Sync() error }
		if sy, ok := s.walShadow.(syncer); ok {
			_ = sy.Sync()
		}
	}
	if walDir == "" {
		return nil
	}
	ring := s.walValidator.snapshot()
	if len(ring) == 0 {
		return nil
	}
	tuples, err := raftwal.TailLogicalTuples(walDir, len(ring))
	if err != nil {
		return err
	}
	if len(tuples) != len(ring) {
		return fmt.Errorf("validator: length mismatch wal=%d ring=%d", len(tuples), len(ring))
	}
	for i := 0; i < len(ring); i++ {
		if ring[i].index != tuples[i].Index || ring[i].crc != tuples[i].CRC {
			limit := 10
			if len(ring) < limit {
				limit = len(ring)
			}
			var ringHead, walHead string
			for j := 0; j < limit; j++ {
				ringHead += fmt.Sprintf("%02d:%d:%08x ", j, ring[j].index, ring[j].crc)
			}
			for j := 0; j < limit; j++ {
				walHead += fmt.Sprintf("%02d:%d:%08x ", j, tuples[j].Index, tuples[j].CRC)
			}
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
	if rec == nil || rec.Type != RaftRecordTypeAppCommand || s.replicationHandler == nil || len(rec.Payload) == 0 {
		return
	}
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
	ShardID                 string            `json:"shard_id"`
	LeaderID                string            `json:"leader_id"`
	IsLeader                bool              `json:"is_leader"`
	LastAppliedIndex        uint64            `json:"last_applied_index"`
	LastSnapshotIndex       uint64            `json:"last_snapshot_index"`
	CommittedSinceSnap      int               `json:"committed_since_snapshot"`
	PerBucketCommit         map[string]uint64 `json:"per_bucket_commit"`
	PrunedThroughIndex      uint64            `json:"pruned_through_index"`
	TransportPeersTotal     int               `json:"transport_peers_total"`
	TransportPeersConnected int               `json:"transport_peers_connected"`
}

// Status returns an immutable snapshot of internal counters useful for debug/metrics.
func (s *ShardRaftNode) Status() StatusSnapshot {
	s.mu.Lock()
	copyMap := make(map[string]uint64, len(s.perBucketCommit))
	for k, v := range s.perBucketCommit {
		copyMap[k] = v
	}
	lastApplied := s.lastAppliedIndex
	lastSnap := s.lastSnapshotIndex
	commSince := s.committedSinceSnap
	pruned := s.prunedThroughIndex
	shard := s.shardID
	engine := s.engine
	closed := s.closedAtomic.Load()
	rn := s.rn
	s.mu.Unlock()

	leaderID := ""
	isLeader := false
	if engine == "etcd" && rn != nil && !closed {
		st := rn.Status()
		lead := st.Lead
		if lead != 0 {
			leaderID = strconv.FormatUint(uint64(lead), 10)
			atomic.StoreUint64(&s.lastKnownLeader, lead)
		}
		isLeader = st.ID == lead && lead != 0
		if leaderID == "" { // fallback cache
			if cached := atomic.LoadUint64(&s.lastKnownLeader); cached != 0 {
				leaderID = strconv.FormatUint(cached, 10)
			}
		}
	} else if engine != "etcd" { // stub
		leaderID = "stub"
		isLeader = true
	}
	ss := StatusSnapshot{ShardID: shard, LeaderID: leaderID, IsLeader: isLeader, LastAppliedIndex: lastApplied, LastSnapshotIndex: lastSnap, CommittedSinceSnap: commSince, PerBucketCommit: copyMap, PrunedThroughIndex: pruned}
	if gt, ok := s.transport.(*GRPCTransport); ok && gt != nil {
		ps := gt.Stats()
		ss.TransportPeersTotal = ps.Total
		ss.TransportPeersConnected = ps.Connected
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

func (l *etcdLoggerAdapter) Debug(args ...interface{}) { slog.Debug(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Debugf(format string, args ...interface{}) {
	slog.Debug(fmt.Sprintf(format, args...))
}
func (l *etcdLoggerAdapter) Info(args ...interface{}) { slog.Info(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Infof(format string, args ...interface{}) {
	slog.Info(fmt.Sprintf(format, args...))
}
func (l *etcdLoggerAdapter) Warning(args ...interface{}) { slog.Warn(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Warningf(format string, args ...interface{}) {
	slog.Warn(fmt.Sprintf(format, args...))
}
func (l *etcdLoggerAdapter) Error(args ...interface{}) { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Errorf(format string, args ...interface{}) {
	slog.Error(fmt.Sprintf(format, args...))
}
func (l *etcdLoggerAdapter) Fatal(args ...interface{}) { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Fatalf(format string, args ...interface{}) {
	slog.Error(fmt.Sprintf(format, args...))
}
func (l *etcdLoggerAdapter) Panic(args ...interface{}) { slog.Error(fmt.Sprint(args...)) }
func (l *etcdLoggerAdapter) Panicf(format string, args ...interface{}) {
	slog.Error(fmt.Sprintf(format, args...))
}

// walValidator (tuple-based) tracks last N (raftIndex, crc32(payload)) for deterministic ordering.
type walValidator struct {
	mu    sync.Mutex
	lastN int
	ring  []walValTuple
	idx   int
	count int
}

type walValTuple struct {
	index    uint64
	crc      uint32
	opcode   uint32
	sequence uint64
	bucket   string
}

func newWalValidator(n int) *walValidator {
	if n <= 0 {
		return nil
	}
	return &walValidator{lastN: n, ring: make([]walValTuple, n)}
}

func (v *walValidator) addTuple(index uint64, payload []byte) {
	if v == nil {
		return
	}
	crc := crc32.ChecksumIEEE(payload)
	v.mu.Lock()
	v.ring[v.idx] = walValTuple{index: index, crc: crc}
	v.idx = (v.idx + 1) % v.lastN
	if v.count < v.lastN {
		v.count++
	}
	v.mu.Unlock()
}

func (v *walValidator) snapshot() []walValTuple {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.count == 0 {
		return nil
	}
	out := make([]walValTuple, 0, v.count)
	start := (v.idx - v.count + v.lastN) % v.lastN
	for i := 0; i < v.count; i++ {
		out = append(out, v.ring[(start+i)%v.lastN])
	}
	return out
}

func (v *walValidator) seedTuples(tuples []struct {
	Index uint64
	CRC   uint32
}) {
	if v == nil || len(tuples) == 0 {
		return
	}
	v.mu.Lock()
	if len(tuples) > v.lastN {
		tuples = tuples[len(tuples)-v.lastN:]
	}
	for i, t := range tuples {
		v.ring[i] = walValTuple{index: t.Index, crc: t.CRC}
	}
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
	WALSegmentMaxBytes  int64 // override default 64MiB segment size (<=0 keeps default)
	WALForceRotateEvery int   // if >0 force rotation after every N appends (test only)
	// WALStrictSync if true enables strict sync mode on the shadow WAL writer (fsync each append).
	WALStrictSync bool
	// WALPrimaryRead if true (experimental) seeds in-memory raft storage from the unified WAL
	// instead of legacy raft entry log on startup. This requires that the WAL already contains
	// a contiguous prefix of raft entries and HardState. Currently limited because Envelope
	// does not yet encode full higher-level proposal metadata (TODO: extend Envelope with
	// bucket/type/sequence for deterministic reconstruction of application-level semantics).
	WALPrimaryRead bool
	// WALDualReadValidate if true runs a dual-read validator comparing legacy applied entries with
	// the shadow WAL enriched envelope metadata (index, crc, opcode, sequence, bucket) and logs mismatches.
	WALDualReadValidate bool
	// TestDeterministicClock (tests only) if non-nil injects a simulated clock used by tickLoop.
	TestDeterministicClock clock.Clock
	// WALFactory (optional) allows callers to inject a custom WAL implementation. If nil
	// and EnableWALShadow is true, the built-in shadow WAL adapter will be created. If both
	// are nil/false, no WAL dual-write occurs (legacy persistence only).
	WALFactory func(cfg RaftConfig) (WAL, error)
}

// NewShardRaftNode creates a new in-memory stub. Follow-up commits will
// initialize the underlying raft implementation and start background goroutines.
func NewShardRaftNode(cfg RaftConfig) (*ShardRaftNode, error) {
	engine := cfg.Engine
	if engine == "" {
		engine = "stub"
	}
	s := &ShardRaftNode{
		shardID:          cfg.ShardID,
		committedCh:      make(chan *CommittedRecord, 1024),
		waiters:          make(map[uint64]chan *ProposalResult),
		perBucketCommit:  make(map[string]uint64),
		engine:           engine,
		waitersSeq:       make(map[uint64]chan *ProposalResult),
		forwardProposals: cfg.ForwardProposals,
		manual:           cfg.Manual,
	}
	// Inject deterministic clock (tests) before launching loops.
	if cfg.TestDeterministicClock != nil {
		s.clk = cfg.TestDeterministicClock
	}
	if engine == "etcd" {
		if err := s.initEtcd(cfg); err != nil {
			return nil, err
		}
	}
	// Initialize abstract WAL (non-fatal on error, logs inside initEtcd if needed for shadow path).
	// We only construct here if engine!=etcd? For now we allow stub engine to also have a WAL for tests.
	if s.wal == nil && cfg.WALFactory != nil {
		if w, err := cfg.WALFactory(cfg); err == nil {
			s.wal = w
		}
	}
	return s, nil
}

func (s *ShardRaftNode) IsLeader() bool {
	if s.engine == "etcd" && s.rn != nil {
		if s.closedAtomic.Load() { // avoid calling into raft after shutdown
			return false
		}
		st := s.rn.Status()
		lead := st.Lead
		isLead := st.ID == lead
		if lead != 0 {
			atomic.StoreUint64(&s.lastKnownLeader, lead)
		}
		return isLead
	}
	return true
}

func (s *ShardRaftNode) LeaderID() string {
	if s.engine == "etcd" && s.rn != nil {
		if s.closedAtomic.Load() {
			return ""
		}
		st := s.rn.Status()
		lead := st.Lead
		if lead == 0 {
			lead = atomic.LoadUint64(&s.lastKnownLeader)
		}
		if lead == 0 { // still unknown
			return ""
		}
		return strconv.FormatUint(uint64(lead), 10)
	}
	return "stub"
}

// leaderHint returns the best-effort current leader ID string, favoring live status then cache.
func (s *ShardRaftNode) leaderHint() string { return s.LeaderID() }

// ShardID returns the identifier of this shard raft node (exported accessor).
func (s *ShardRaftNode) ShardID() string { return s.shardID }

// Step injects an incoming raft message (used by transport) into the raft state machine.
func (s *ShardRaftNode) Step(ctx context.Context, m raftpb.Message) error {
	if s.engine == "etcd" && s.rn != nil {
		return s.rn.Step(ctx, m)
	}
	return nil
}

// SetTransport installs a transport implementation (multi-node). Safe to call once.
func (s *ShardRaftNode) SetTransport(t Transport) {
	s.transport = t
}

// Propose submits a record asynchronously.
func (s *ShardRaftNode) Propose(ctx context.Context, rec *RaftLogRecord) (uint64, error) {
	if rec == nil {
		return 0, errors.New("nil raft record")
	}
	if s.engine == "etcd" && s.rn != nil && !s.IsLeader() {
		if s.forwardProposals {
			lid := s.leaderHint()
			if lid == "" {
				lid = s.waitForLeaderHint(25)
			}
			if lid == "" { // one more attempt: direct status read
				st := s.rn.Status()
				if st.Lead != 0 {
					lid = strconv.FormatUint(uint64(st.Lead), 10)
				} else if len(st.Progress) > 0 { // guess: smallest peer ID other than self
					var best uint64 = ^uint64(0)
					for id := range st.Progress {
						if id == st.ID {
							continue
						}
						if id < best {
							best = id
						}
					}
					if best != ^uint64(0) {
						lid = strconv.FormatUint(best, 10)
					}
				}
			}
			return 0, &NotLeaderError{LeaderID: lid}
		}
		lid := s.leaderHint()
		if lid == "" {
			lid = s.waitForLeaderHint(25)
		}
		if lid == "" {
			st := s.rn.Status()
			if st.Lead != 0 {
				lid = strconv.FormatUint(uint64(st.Lead), 10)
			} else if len(st.Progress) > 0 {
				var best uint64 = ^uint64(0)
				for id := range st.Progress {
					if id == st.ID {
						continue
					}
					if id < best {
						best = id
					}
				}
				if best != ^uint64(0) {
					lid = strconv.FormatUint(best, 10)
				}
			}
		}
		return 0, &NotLeaderError{LeaderID: lid}
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
		return 0, nil
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
	if s.engine == "etcd" && s.rn != nil && !s.IsLeader() {
		if s.forwardProposals {
			lid := s.leaderHint()
			if lid == "" {
				lid = s.waitForLeaderHint(25)
			}
			if lid == "" {
				st := s.rn.Status()
				if st.Lead != 0 {
					lid = strconv.FormatUint(uint64(st.Lead), 10)
				} else if len(st.Progress) > 0 {
					var best uint64 = ^uint64(0)
					for id := range st.Progress {
						if id == st.ID {
							continue
						}
						if id < best {
							best = id
						}
					}
					if best != ^uint64(0) {
						lid = strconv.FormatUint(best, 10)
					}
				}
			}
			return 0, 0, &NotLeaderError{LeaderID: lid}
		}
		lid := s.leaderHint()
		if lid == "" {
			lid = s.waitForLeaderHint(25)
		}
		if lid == "" {
			st := s.rn.Status()
			if st.Lead != 0 {
				lid = strconv.FormatUint(uint64(st.Lead), 10)
			} else if len(st.Progress) > 0 {
				var best uint64 = ^uint64(0)
				for id := range st.Progress {
					if id == st.ID {
						continue
					}
					if id < best {
						best = id
					}
				}
				if best != ^uint64(0) {
					lid = strconv.FormatUint(best, 10)
				}
			}
		}
		return 0, 0, &NotLeaderError{LeaderID: lid}
	}
	if s.engine == "etcd" && s.rn != nil {
		ch := make(chan *ProposalResult, 1)
		seq := atomic.AddUint64(&s.nextSeq, 1)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, 0, errors.New("raft: node closed")
		}
		if s.waitersSeq == nil {
			s.waitersSeq = make(map[uint64]chan *ProposalResult)
		}
		if s.waitersSeq == nil {
			s.waitersSeq = make(map[uint64]chan *ProposalResult)
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
				return 0, 0, ErrProposalAborted
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
	if s.waiters == nil {
		s.waiters = make(map[uint64]chan *ProposalResult)
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
			return 0, 0, ErrProposalAborted
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
	// Defensive: some tests inject placeholder nodes (e.g. failover replacing crashed leader)
	// without using NewShardRaftNode, so maps can be nil. Initialize lazily to avoid panics.
	if s.perBucketCommit == nil {
		s.perBucketCommit = make(map[string]uint64)
	}
	if s.waiters == nil {
		s.waiters = make(map[uint64]chan *ProposalResult)
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
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.closedAtomic.Store(true)
	var walShadowClose func() error
	if s.walShadow != nil {
		if c, ok := s.walShadow.(interface{ Close() error }); ok {
			walShadowClose = c.Close
		}
	}
	walClose := s.wal
	waiters := s.waiters
	s.waiters = map[uint64]chan *ProposalResult{}
	waitersSeq := s.waitersSeq
	s.waitersSeq = map[uint64]chan *ProposalResult{}
	if s.engine == "etcd" && s.stopCh != nil {
		close(s.stopCh)
	}
	var rn etcdraft.Node
	if s.engine == "etcd" && s.rn != nil {
		rn = s.rn
	}
	s.mu.Unlock()
	if rn != nil {
		rn.Stop()
	} // unblock Ready()
	s.wg.Wait()
	close(s.committedCh)
	for _, ch := range waiters {
		func(c chan *ProposalResult) { defer func() { recover() }(); c <- nil; close(c) }(ch)
	}
	for _, ch := range waitersSeq {
		func(c chan *ProposalResult) { defer func() { recover() }(); c <- nil; close(c) }(ch)
	}
	if walShadowClose != nil {
		_ = walShadowClose()
	}
	if walClose != nil {
		_ = walClose.Close()
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
	var persist *raftPersistence
	storage := etcdraft.NewMemoryStorage()
	var loaded bool
	if !cfg.DisablePersistence {
		persistDir := cfg.DataDir
		if persistDir == "" {
			persistDir = "raftdata"
		}
		p, err := newRaftPersistence(persistDir, cfg.ShardID)
		if err != nil {
			return err
		}
		persist = p
		// Attempt load of prior state (snapshot, hard state, entries)
		var snapIdx, lastEntryIdx uint64
		loaded, snapIdx, lastEntryIdx, err = persist.Load(storage)
		if err != nil {
			slog.Warn("raft persistence load failed", slog.String("shard", cfg.ShardID), slog.Any("error", err))
		} else if loaded {
			s.lastSnapshotIndex = snapIdx
			if lastEntryIdx > snapIdx {
				s.lastAppliedIndex = lastEntryIdx
			} else {
				s.lastAppliedIndex = snapIdx
			}
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
				if ch == '@' {
					at = i
					break
				}
			}
			if at != -1 {
				parts = spec[:at]
			}
			if pid, err := strconv.ParseUint(parts, 10, 64); err == nil {
				peers = append(peers, etcdraft.Peer{ID: pid})
				confVoters = append(confVoters, pid)
			}
		}
		// Ensure self id is included.
		found := false
		for _, p := range peers {
			if p.ID == id {
				found = true
				break
			}
		}
		if !found {
			peers = append(peers, etcdraft.Peer{ID: id})
			confVoters = append(confVoters, id)
		}
	}
	if loaded { // restart path
		s.rn = etcdraft.RestartNode(c)
	} else {
		s.rn = etcdraft.StartNode(c, peers)
	}
	// If shadow WAL enabled attempt to replay last HardState envelope (non-fatal). This can override legacy file if newer.
	if cfg.EnableWALShadow && !cfg.DisablePersistence {
		shadowDir := cfg.WALShadowDir
		if shadowDir == "" {
			shadowDir = filepath.Join(cfg.DataDir, "raftwal")
		}
		if hsBytes, ok, herr := raftwal.ReplayLastHardState(shadowDir); herr == nil && ok {
			var hs raftpb.HardState
			if uerr := hs.Unmarshal(hsBytes); uerr == nil && !etcdraft.IsEmptyHardState(hs) {
				// Compare with lastShadowHardState (zero-value if not set). Accept if strictly newer in term or commit.
				lhs := s.getLastShadowHardState()
				if hs.Term > lhs.Term || hs.Commit > lhs.Commit {
					s.storage.SetHardState(hs)
					s.setLastShadowHardState(hs)
				}
			}
		}
	}
	// record conf state for snapshots
	s.confState = raftpb.ConfState{Voters: confVoters}
	// Experimental: seed storage from WAL as primary read path.
	// Attempt Forge Unified WAL first if available; if nothing was seeded and shadow WAL is enabled,
	// fall back to shadow replay so legacy tests still pass without configuring Forge.
	if cfg.WALPrimaryRead && !cfg.DisablePersistence {
		seededEntries := 0
		seededHS := false
		// Try Forge Unified WAL (if DefaultWAL implements it)
		if uw, ok := wal.DefaultWAL.(wal.UnifiedWAL); ok && uw != nil {
			var hs raftpb.HardState
			_ = uw.ReplayItems(func(it wal.ReplayItem) error {
				if it.Kind == wal.EntryKindNormal {
					e := raftpb.Entry{Index: it.Index, Term: it.Term, Type: raftpb.EntryNormal, Data: it.RaftData}
					if err := s.storage.Append([]raftpb.Entry{e}); err == nil {
						seededEntries++
					}
				}
				return nil
			})
			if b, ok, err := uw.ReplayLastHardState(); err == nil && ok && len(b) > 0 {
				if uerr := hs.Unmarshal(b); uerr == nil && !etcdraft.IsEmptyHardState(hs) {
					s.storage.SetHardState(hs)
					s.setLastShadowHardState(hs)
					seededHS = true
				}
			}
		}
		// Fallback to shadow WAL if nothing was appended and feature is enabled
		if seededEntries == 0 && cfg.EnableWALShadow {
			shadowDir := cfg.WALShadowDir
			if shadowDir == "" { shadowDir = filepath.Join(cfg.DataDir, "raftwal") }
			_ = raftwal.Replay(shadowDir, func(env *raftwal.Envelope) error {
				if env.Kind == raftwal.EntryKind_ENTRY_NORMAL {
					_ = s.storage.Append([]raftpb.Entry{{Index: env.RaftIndex, Term: env.RaftTerm, Type: raftpb.EntryNormal, Data: env.AppBytes}})
					seededEntries++
				}
				return nil
			})
			if !seededHS {
				if hsBytes, ok, herr := raftwal.ReplayLastHardState(shadowDir); herr == nil && ok && len(hsBytes) > 0 {
					var hs raftpb.HardState
					if uerr := hs.Unmarshal(hsBytes); uerr == nil && !etcdraft.IsEmptyHardState(hs) {
						s.storage.SetHardState(hs)
						s.setLastShadowHardState(hs)
					}
				}
			}
		}
	}
	// Shadow WAL setup (non-fatal). Only when enabled and persistence not disabled.
	if cfg.EnableWALShadow && !cfg.DisablePersistence {
		shadowDir := cfg.WALShadowDir
		if shadowDir == "" {
			shadowDir = filepath.Join(cfg.DataDir, "raftwal")
		}
		if err := os.MkdirAll(shadowDir, 0o755); err != nil {
			slog.Warn("wal shadow mkdir failed", slog.Any("error", err))
		} else {
			segSize := cfg.WALSegmentMaxBytes
			if segSize <= 0 {
				segSize = 64 * 1024 * 1024
			}
			wcfg := raftwalConfigCompat{Dir: shadowDir, BufMB: 4, SidecarFlushEvery: 256, SegmentMaxBytes: segSize, ForceRotateEvery: cfg.WALForceRotateEvery, StrictSync: cfg.WALStrictSync}
			if wr, werr := raftwalNewWriterCompat(wcfg); werr != nil {
				slog.Warn("wal shadow init failed", slog.Any("error", werr))
			} else {
				s.walShadow = wr
			}
		}
		s.walDualReadValidate = cfg.WALDualReadValidate
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
		// Launch background loops. Add to waitgroup before starting goroutine to avoid race with Close().
		go s.tickLoop()
		s.wg.Add(1)
		go s.readyLoop()
	}
	return nil
}

func (s *ShardRaftNode) tickLoop() {
	if s.clk == nil { // real time path
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
	// simulated clock path: poll until logical time advances by tickEvery
	last := s.clk.Now()
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		now := s.clk.Now()
		if now.Sub(last) >= s.tickEvery {
			if s.rn != nil {
				s.rn.Tick()
			}
			last = now
		} else {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

func (s *ShardRaftNode) readyLoop() {
	defer s.wg.Done()
	for {
		if s.rn == nil {
			return
		}
		select {
		case <-s.stopCh:
			return
		case rd, ok := <-s.rn.Ready():
			if !ok {
				return
			}
			s.processReady(rd)
		}
	}
}

// processReady centralizes handling of an etcd raft Ready. It is invoked by both
// background and manual deterministic paths to ensure identical semantics.
func (s *ShardRaftNode) getLastShadowHardState() raftpb.HardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastShadowHardState
}

func (s *ShardRaftNode) setLastShadowHardState(hs raftpb.HardState) {
	s.mu.Lock()
	s.lastShadowHardState = hs
	s.mu.Unlock()
}

func (s *ShardRaftNode) processReady(rd etcdraft.Ready) {
	// If node is closed, discard any late Ready (can happen after Close races with etcd raft goroutine).
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	// Update lastKnownLeader from SoftState (if present) before unlocking so proposals racing
	// with elections get an accurate leader hint.
	if rd.SoftState != nil && rd.SoftState.Lead != 0 {
		atomic.StoreUint64(&s.lastKnownLeader, rd.SoftState.Lead)
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
	// HardState persistence to abstract WAL (and legacy shadow writer for migration).
	if !etcdraft.IsEmptyHardState(rd.HardState) {
		lhs := s.getLastShadowHardState()
		if rd.HardState.Term != lhs.Term || rd.HardState.Vote != lhs.Vote || rd.HardState.Commit != lhs.Commit {
			if b, mErr := rd.HardState.Marshal(); mErr == nil {
				// Abstract WAL path
				if s.wal != nil {
					if err := s.wal.AppendEntry(rd.HardState.Commit, rd.HardState.Term, EntryKindHardState, nil, WALMetadata{}, b); err != nil {
						slog.Warn("wal append hardstate failed", slog.String("shard", s.shardID), slog.Any("error", err))
					}
				}
				// Legacy shadow path retained for compatibility until full cutover.
				if s.walShadow != nil {
					env := &raftwal.Envelope{RaftIndex: rd.HardState.Commit, RaftTerm: rd.HardState.Term, Kind: raftwal.EntryKind_ENTRY_HARDSTATE, AppBytes: b, AppCrc: crc32.ChecksumIEEE(b)}
					if pb, perr := proto.Marshal(env); perr == nil {
						_ = s.walShadow.AppendHardState(env.RaftIndex, pb)
					} else {
						slog.Warn("wal shadow hardstate marshal envelope failed", slog.Any("error", perr))
					}
				}
				s.setLastShadowHardState(rd.HardState)
			} else {
				slog.Warn("wal hardstate marshal failed", slog.Any("error", mErr))
			}
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
	// Process committed entries (NORMAL + configuration changes). Each NORMAL entry is written
	// through the abstract WAL first (if configured) then legacy shadow WAL (migration period),
	// followed by replication handler invocation and waiter signaling.
	if len(rd.CommittedEntries) > 0 {
		for _, ent := range rd.CommittedEntries {
			switch ent.Type {
			case raftpb.EntryNormal:
				// Always write an envelope (even empty/noop) to maintain contiguous WAL indices and avoid
				// artificial conflict rotations caused by skipped heartbeat entries.
				var wr wireRecord
				var rec *RaftLogRecord
				if len(ent.Data) > 0 {
					if err := json.Unmarshal(ent.Data, &wr); err != nil {
						slog.Error("raft unmarshal", slog.Any("error", err))
						goto afterNormal
					} // fallback: still advance index
					rec = &RaftLogRecord{BucketID: wr.BucketID, Type: wr.Type, Payload: wr.Payload}
				} else {
					// heartbeat/noop placeholder
					wr = wireRecord{BucketID: "", Type: RaftRecordTypeAppCommand, Seq: 0}
					rec = &RaftLogRecord{BucketID: "", Type: RaftRecordTypeAppCommand, Payload: nil}
				}
				// Abstract WAL write (normal entry).
				if s.wal != nil {
					meta := WALMetadata{Bucket: rec.BucketID, Opcode: uint32(rec.Type), Sequence: wr.Seq}
					if err := s.wal.AppendEntry(ent.Index, ent.Term, EntryKindNormal, rec.Payload, meta, nil); err != nil {
						slog.Warn("wal append entry failed", slog.String("shard", s.shardID), slog.Uint64("index", ent.Index), slog.Any("error", err))
					}
				}
				// Legacy shadow WAL path retained for migration.
				if s.walShadow != nil {
					envPayload := rec.Payload
					crcVal := crc32.ChecksumIEEE(envPayload)
					env := &raftwal.Envelope{RaftIndex: ent.Index, RaftTerm: ent.Term, Kind: raftwal.EntryKind_ENTRY_NORMAL, AppBytes: envPayload, AppCrc: crcVal, Namespace: "default", Bucket: rec.BucketID, Opcode: uint32(rec.Type), Sequence: wr.Seq}
					if b, mErr := proto.Marshal(env); mErr == nil {
						_ = s.walShadow.AppendEnvelope(ent.Index, b)
					} else {
						slog.Warn("wal shadow marshal failed", slog.Any("error", mErr))
					}
					if s.walValidator != nil {
						s.walValidator.addTuple(ent.Index, envPayload)
					}
				}
				// Dual-read validation
				if s.walValidator != nil && s.walDualReadValidate {
					crcCalc := crc32.ChecksumIEEE(rec.Payload)
					for _, tup := range s.walValidator.snapshot() {
						if tup.index == ent.Index && tup.crc != crcCalc {
							slog.Error("wal dual-read mismatch", slog.String("shard", s.shardID), slog.Uint64("index", ent.Index), slog.Any("legacy_crc", crcCalc), slog.Any("wal_crc", tup.crc))
						}
					}
				}
				s.mu.Lock()
				if rec.BucketID != "" {
					s.perBucketCommit[rec.BucketID]++
				}
				commitIdx := uint64(0)
				if rec.BucketID != "" {
					commitIdx = s.perBucketCommit[rec.BucketID]
				}
				waiter := s.waitersSeq[wr.Seq]
				delete(s.waitersSeq, wr.Seq)
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
				if rec.BucketID != "" { // only invoke handler & channel for real app commands
					cr := &CommittedRecord{RaftIndex: ent.Index, Term: ent.Term, CommitIndex: commitIdx, Record: rec}
					s.invokeReplicationHandler(rec)
					if committedCh != nil {
						func() {
							defer func() {
								if r := recover(); r != nil {
								}
							}()
							select {
							case committedCh <- cr:
							case <-time.After(100 * time.Millisecond):
							}
						}()
					}
					if waiter != nil {
						waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: ent.Index}
						close(waiter)
					}
				} else if waiter != nil { // heartbeat with waiter (unlikely) – signal
					waiter <- &ProposalResult{CommitIndex: 0, RaftIndex: ent.Index}
					close(waiter)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if len(ent.Data) > 0 {
					_ = cc.Unmarshal(ent.Data)
				}
				// WAL: store config change like a normal entry (no bucket semantics).
				if s.wal != nil {
					meta := WALMetadata{}
					if err := s.wal.AppendEntry(ent.Index, ent.Term, EntryKindNormal, ent.Data, meta, nil); err != nil {
						slog.Warn("wal append confchange failed", slog.String("shard", s.shardID), slog.Uint64("index", ent.Index), slog.Any("error", err))
					}
				}
				if s.walShadow != nil {
					env := &raftwal.Envelope{RaftIndex: ent.Index, RaftTerm: ent.Term, Kind: raftwal.EntryKind_ENTRY_NORMAL, AppBytes: ent.Data, AppCrc: crc32.ChecksumIEEE(ent.Data), Namespace: "default"}
					if b, mErr := proto.Marshal(env); mErr == nil {
						_ = s.walShadow.AppendEnvelope(ent.Index, b)
					}
				}
				if s.walValidator != nil {
					s.walValidator.addTuple(ent.Index, ent.Data)
				}
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock()
				s.confState = *cs
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
			case raftpb.EntryConfChangeV2:
				var cc raftpb.ConfChangeV2
				if len(ent.Data) > 0 {
					_ = cc.Unmarshal(ent.Data)
				}
				if s.wal != nil {
					meta := WALMetadata{}
					if err := s.wal.AppendEntry(ent.Index, ent.Term, EntryKindNormal, ent.Data, meta, nil); err != nil {
						slog.Warn("wal append confchangeV2 failed", slog.String("shard", s.shardID), slog.Uint64("index", ent.Index), slog.Any("error", err))
					}
				}
				if s.walShadow != nil {
					env := &raftwal.Envelope{RaftIndex: ent.Index, RaftTerm: ent.Term, Kind: raftwal.EntryKind_ENTRY_NORMAL, AppBytes: ent.Data, AppCrc: crc32.ChecksumIEEE(ent.Data), Namespace: "default"}
					if b, mErr := proto.Marshal(env); mErr == nil {
						_ = s.walShadow.AppendEnvelope(ent.Index, b)
					}
				}
				if s.walValidator != nil {
					s.walValidator.addTuple(ent.Index, ent.Data)
				}
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock()
				s.confState = *cs
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
			default:
				s.mu.Lock()
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
			}
			continue
		afterNormal:
			// fallback path already advanced index; no further action
		}
	}
	// Send outbound messages (multi-node scenarios)
	if len(rd.Messages) > 0 && s.transport != nil {
		s.transport.Send(context.Background(), rd.Messages)
	}
	// Advance raft state machine
	s.rn.Advance()
	// Auto-campaign convenience: only for single-node clusters. Broadly auto-campaigning
	// on every Ready when Lead==0 in multi-node scenarios can trigger repeated pre-vote
	// storms and very high terms (observed in tests). We revert to the safer single-node
	// shortcut; multi-node elections rely on tick timeouts.
	if !s.manual && s.rn.Status().Lead == 0 && len(s.confState.Voters) == 1 {
		_ = s.rn.Campaign(context.Background())
	}
	// Snapshot / compaction logic (pruning prefers abstract WAL if present; falls back to legacy shadow WAL).
	if s.snapshotThreshold > 0 && s.committedSinceSnap >= s.snapshotThreshold && s.lastAppliedIndex > s.lastSnapshotIndex {
		if s.storage != nil {
			if snap, err := s.storage.CreateSnapshot(s.lastAppliedIndex, &s.confState, nil); err == nil && !etcdraft.IsEmptySnap(snap) {
				if s.persist != nil {
					if err := s.persist.PersistSnapshot(&snap); err != nil {
						slog.Warn("raft snapshot persist failed", slog.String("shard", s.shardID), slog.Any("error", err))
					} else {
						compactionIndex := snap.Metadata.Index
						if err := s.storage.Compact(compactionIndex); err != nil {
							slog.Warn("raft storage compact failed", slog.String("shard", s.shardID), slog.Any("error", err))
						}
						if err := s.persist.PruneEntries(compactionIndex); err != nil {
							slog.Warn("raft prune failed", slog.String("shard", s.shardID), slog.Any("error", err))
						} else {
							// Compute follower-aware prune floor: min(compactionIndex, minFollowerMatchIndex)
							floor := compactionIndex
							if mf := s.minFollowerMatchIndex(); mf > 0 && mf < floor {
								floor = mf
							}
							s.mu.Lock()
							if floor > s.prunedThroughIndex {
								s.prunedThroughIndex = floor
							}
							if snap.Metadata.Index > s.lastSnapshotIndex {
								s.lastSnapshotIndex = snap.Metadata.Index
							}
							s.committedSinceSnap = 0
							s.mu.Unlock()
							// Schedule WAL pruning asynchronously to avoid blocking ready loop.
							if s.prunedThroughIndex > 0 {
								s.scheduleWalPrune(s.prunedThroughIndex)
							}
						}
						if s.prunedThroughIndex < compactionIndex {
							s.mu.Lock()
							if snap.Metadata.Index > s.lastSnapshotIndex {
								s.lastSnapshotIndex = snap.Metadata.Index
							}
							s.committedSinceSnap = 0
							s.mu.Unlock()
						}
					}
				}
			} else if err != nil {
				slog.Warn("raft create snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err))
			}
		}
	}
}

// scheduleWalPrune launches a bounded background prune worker if none is running.
// It prunes small batches per run to minimize IO spikes.
func (s *ShardRaftNode) scheduleWalPrune(upto uint64) {
	if upto == 0 {
		return
	}
	if s.walPruneBusy.Swap(true) {
		return // already running
	}
	go func(shard string, target uint64) {
		defer s.walPruneBusy.Store(false)
		pruneFunc := func(u uint64) (int, error) { return 0, nil }
		if s.wal != nil {
			pruneFunc = s.wal.PruneThrough
		} else if s.walShadow != nil {
			if pw, ok := s.walShadow.(interface{ PruneThrough(uint64) (int, error) }); ok {
				pruneFunc = pw.PruneThrough
			}
		}
		// Prune in small batches; stop on first zero-removal pass.
		const maxBatches = 4
		for i := 0; i < maxBatches; i++ {
			removed, err := pruneFunc(target)
			if err != nil {
				slog.Warn("wal prune failed", slog.String("shard", shard), slog.Any("error", err))
				return
			}
			if removed <= 0 {
				return
			}
			slog.Info("wal segments pruned", slog.String("shard", shard), slog.Int("removed", removed), slog.Uint64("through", target))
		}
	}(s.shardID, upto)
}

// minFollowerMatchIndex returns the minimum Match index across followers for the
// current shard, as observed by the leader's etcd raft status. If not leader or
// status unavailable, returns 0 to indicate "no floor from followers".
func (s *ShardRaftNode) minFollowerMatchIndex() uint64 {
	if s.engine != "etcd" || s.rn == nil {
		return 0
	}
	st := s.rn.Status()
	if st.Lead == 0 || st.Lead != st.ID {
		// Only the leader has authoritative Progress map for followers
		return 0
	}
	min := uint64(0)
	for id, pr := range st.Progress {
		if id == st.ID {
			continue // skip leader itself
		}
		// Match is the highest index known replicated on that follower
		m := pr.Match
		if m == 0 {
			continue
		}
		if min == 0 || m < min {
			min = m
		}
	}
	return min
}

// ---- shadow WAL writer compatibility helpers ----
// Local minimal copy of raftwal.Config to avoid direct import (decouples migration path).
type raftwalConfigCompat struct {
	Dir               string
	BufMB             int
	SidecarFlushEvery int
	SegmentMaxBytes   int64
	ForceRotateEvery  int
	StrictSync        bool
}

// raftwalWriterCompat is satisfied by *raftwal.Writer (subset) - defined here for loose coupling.
type raftwalWriterCompat interface {
	AppendEnvelope(uint64, []byte) error
	AppendHardState(uint64, []byte) error
	Sync() error
}

// raftwalNewWriterCompat uses reflection-free construction by importing the real package.
// NOTE: we import inside function to avoid unused dependency when shadow disabled.
func raftwalNewWriterCompat(cfg raftwalConfigCompat) (raftwalWriterCompat, error) {
	w, err := raftwal.NewWriter(raftwal.Config{
		Dir:               cfg.Dir,
		BufMB:             cfg.BufMB,
		SidecarFlushEvery: cfg.SidecarFlushEvery,
		SegmentMaxBytes:   cfg.SegmentMaxBytes,
		ForceRotateEvery:  cfg.ForceRotateEvery,
		StrictSync:        cfg.StrictSync,
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Remove unused linkname approach.

// ManualTick advances the raft node one logical heartbeat/election tick.
// Only valid when RaftConfig.Manual = true.
func (s *ShardRaftNode) ManualTick() {
	if !s.manual || s.rn == nil {
		return
	}
	s.rn.Tick()
}

// ManualForceCampaign forces a leadership campaign (single-node deterministic tests).
// Safe only in manual mode; ignores errors.
func (s *ShardRaftNode) ManualForceCampaign() {
	if !s.manual || s.rn == nil {
		return
	}
	_ = s.rn.Campaign(context.Background())
}

// ManualProcessReady drains a single Ready from the raft node and processes it
// identically to the background readyLoop. Returns false if no Ready was
// available immediately. Only valid in manual mode.
func (s *ShardRaftNode) ManualProcessReady() bool {
	if !s.manual || s.rn == nil {
		return false
	}
	select {
	case rd, ok := <-s.rn.Ready():
		if !ok {
			return false
		}
		s.processReady(rd)
		return true
	default:
		return false
	}
}

// ManualProcessNextReady blocks until a Ready is available (or the raft node closes)
// and processes it. Returns true if a Ready was processed. Only valid in manual mode.
func (s *ShardRaftNode) ManualProcessNextReady() bool {
	if !s.manual || s.rn == nil {
		return false
	}
	rd, ok := <-s.rn.Ready()
	if !ok {
		return false
	}
	if s.persist != nil {
		if err := s.persist.Persist(&rd); err != nil {
			slog.Warn("raft persist failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
	}
	if !etcdraft.IsEmptySnap(rd.Snapshot) && s.storage != nil {
		if err := s.storage.ApplySnapshot(rd.Snapshot); err != nil {
			slog.Warn("raft storage apply snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
	}
	if len(rd.Entries) > 0 && s.storage != nil {
		if err := s.storage.Append(rd.Entries); err != nil {
			slog.Warn("raft storage append failed", slog.String("shard", s.shardID), slog.Any("error", err))
		}
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
				s.lastAppliedIndex = ent.Index
				s.committedSinceSnap++
				s.mu.Unlock()
				cr := &CommittedRecord{RaftIndex: ent.Index, Term: ent.Term, CommitIndex: commitIdx, Record: rec}
				// Apply application mutation
				s.invokeReplicationHandler(rec)
				select {
				case s.committedCh <- cr:
				case <-time.After(100 * time.Millisecond):
				}
				if waiter != nil {
					waiter <- &ProposalResult{CommitIndex: commitIdx, RaftIndex: ent.Index}
					close(waiter)
				}
			} else if ent.Type == raftpb.EntryConfChange {
				var cc raftpb.ConfChange
				if len(ent.Data) > 0 {
					_ = cc.Unmarshal(ent.Data)
				}
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock()
				s.confState = *cs
				s.lastAppliedIndex = ent.Index
				s.mu.Unlock()
			} else if ent.Type == raftpb.EntryConfChangeV2 {
				var cc raftpb.ConfChangeV2
				if len(ent.Data) > 0 {
					_ = cc.Unmarshal(ent.Data)
				}
				cs := s.rn.ApplyConfChange(cc)
				s.mu.Lock()
				s.confState = *cs
				s.lastAppliedIndex = ent.Index
				s.mu.Unlock()
			}
		}
	}
	if len(rd.Messages) > 0 && s.transport != nil {
		s.transport.Send(context.Background(), rd.Messages)
	}
	s.rn.Advance()
	if s.rn.Status().Lead == 0 && len(s.confState.Voters) == 1 {
		_ = s.rn.Campaign(context.Background())
	}
	if s.snapshotThreshold > 0 && s.committedSinceSnap >= s.snapshotThreshold && s.lastAppliedIndex > s.lastSnapshotIndex {
		if s.storage != nil {
			if snap, err := s.storage.CreateSnapshot(s.lastAppliedIndex, &s.confState, nil); err == nil && !etcdraft.IsEmptySnap(snap) {
				if s.persist != nil {
					if err := s.persist.PersistSnapshot(&snap); err != nil {
						slog.Warn("raft snapshot persist failed", slog.String("shard", s.shardID), slog.Any("error", err))
					} else {
						compactionIndex := snap.Metadata.Index
						if err := s.storage.Compact(compactionIndex); err != nil {
							slog.Warn("raft storage compact failed", slog.String("shard", s.shardID), slog.Any("error", err))
						}
						if err := s.persist.PruneEntries(compactionIndex); err != nil {
							slog.Warn("raft prune failed", slog.String("shard", s.shardID), slog.Any("error", err))
						} else {
							s.mu.Lock()
							if compactionIndex > s.prunedThroughIndex {
								s.prunedThroughIndex = compactionIndex
							}
							if snap.Metadata.Index > s.lastSnapshotIndex {
								s.lastSnapshotIndex = snap.Metadata.Index
							}
							s.committedSinceSnap = 0
							s.mu.Unlock()
						}
						if s.prunedThroughIndex < compactionIndex {
							s.mu.Lock()
							if snap.Metadata.Index > s.lastSnapshotIndex {
								s.lastSnapshotIndex = snap.Metadata.Index
							}
							s.committedSinceSnap = 0
							s.mu.Unlock()
						}
					}
				}
			} else if err != nil {
				slog.Warn("raft create snapshot failed", slog.String("shard", s.shardID), slog.Any("error", err))
			}
		}
	}
	return true
}

// ManualStep performs a single logical advancement: one Tick followed by
// draining all immediately available Ready structs. Useful for tests.
// (single implementation retained above; duplicate removed)

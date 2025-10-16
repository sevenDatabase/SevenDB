package raft

// WAL interface introduces an abstraction layer between the raft Ready processing
// and any concrete write-ahead log implementation. The current code writes directly
// to the shadow WAL (raftwal.Writer). This interface lets us plug in alternative
// implementations (e.g., encrypted WAL, compressed WAL, remote replicated WAL) without
// changing raft application logic.
//
// Integration Plan (Phase 1):
//  1. Introduce interface + adapter (this file + wal_adapter.go).
//  2. Leave ShardRaftNode still using existing s.walShadow field until migration step.
//  3. In a follow-up change, replace uses of s.walShadow with s.wal (WAL interface) and
//     supply the adapter for existing behavior.
//  4. Add configuration hook (RaftConfig.WALFactory) to inject custom WAL impls.
//  5. Harden replay / gap detection using the abstract Replay API.
//
// NOTE: We intentionally keep the interface minimal; features like tail validation
// or tuple snapshots can later be negotiated via optional sub-interfaces.

// EntryKind enumerates logical WAL record kinds.
type EntryKind int

const (
	EntryKindNormal    EntryKind = iota // replicated log entry (advances raft index)
	EntryKindHardState                  // persistent HardState mutation (term/vote/commit)
)

// WALMetadata carries supplemental application context (mirrors Envelope enrichment fields).
// Fields are optional and may be empty for noop / heartbeat entries.
type WALMetadata struct {
	Bucket   string
	Opcode   uint32
	Sequence uint64
	// Future: ClientID, TraceID, Compression flags, Encoding version, Checksums, etc.
}

// WALReplayItem represents a decoded WAL record surfaced during Replay.
type WALReplayItem struct {
	Index    uint64    // raft index (for HardState: commit index surrogate)
	Term     uint64    // raft term of the entry / hardstate
	Kind     EntryKind // normal vs hardstate
	AppBytes []byte    // raw application payload (may be empty)
	Meta     WALMetadata
	// RawHardState holds the marshaled raftpb.HardState bytes when Kind == EntryKindHardState.
	// For normal entries this will be nil.
	RawHardState []byte
}

// WAL abstracts persistence of raft log entries + HardState ordering points.
// Implementations must be safe for concurrent use by the raft Ready processing goroutine
// and potential external pruning tasks. Append methods should return only after the
// data has been written to the underlying medium according to the implementation's
// durability policy (StrictSync vs buffered). Fsync semantics are implementation-specific.
type WAL interface {
	// AppendEntry appends a NORMAL or HARDSTATE record. For NORMAL entries index must be
	// strictly contiguous with previous NORMAL entry unless this is the first append; implementations
	// may rotate segments or truncate conflicting tails on mismatch. For HARDSTATE entries index
	// is typically the commit index and need not be contiguous.
	AppendEntry(index, term uint64, kind EntryKind, appBytes []byte, meta WALMetadata, rawHardState []byte) error
	// Sync forces buffered data to stable storage (fsync). May be a no-op if StrictSync enabled per append.
	Sync() error
	// PruneThrough removes (whole segment granularity) all data strictly below or equal to uptoIndex
	// that is no longer required for follower catch-up (after snapshot compaction). Returns count of
	// segments or logical groups removed.
	PruneThrough(uptoIndex uint64) (int, error)
	// Replay iterates records in ascending order, invoking cb for each. Returning a non-nil error from cb stops replay.
	Replay(cb func(*WALReplayItem) error) error
	// ReplayLastHardState returns the last persisted HardState payload bytes (marshaled raftpb.HardState) if any.
	ReplayLastHardState() ([]byte, bool, error)
	// Dir returns underlying directory path (used for validator or external tools).
	Dir() string
	// Close releases resources; further appends should fail.
	Close() error
}

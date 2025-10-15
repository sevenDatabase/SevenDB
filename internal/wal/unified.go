package wal

import "github.com/dicedb/dicedb-go/wire"

// EntryKind denotes the type of WAL entry for the unified API.
type EntryKind int

const (
    EntryKindNormal EntryKind = iota
    EntryKindHardState
)

// ReplayItem is a unified replay structure carrying raft metadata and payloads.
type ReplayItem struct {
    Kind      EntryKind
    Index     uint64
    Term      uint64
    SubSeq    uint32
    Timestamp int64
    Cmd       *wire.Command
    // HardState carries serialized raft HardState bytes when Kind==EntryKindHardState.
    HardState []byte
}

// UnifiedWAL exposes extended operations needed by raft and unified restore.
// This is implemented incrementally by the existing WAL implementation.
type UnifiedWAL interface {
    // AppendEntry appends either a normal command or a hardstate update.
    AppendEntry(kind EntryKind, index uint64, term uint64, subSeq uint32, cmd *wire.Command, hardState []byte) error
    // ReplayItems streams unified replay items in deterministic order.
    ReplayItems(cb func(ReplayItem) error) error
    // ReplayLastHardState returns the last persisted HardState if present.
    ReplayLastHardState() (hs []byte, ok bool, err error)
    // PruneThrough removes log up to and including index (subject to safety floors).
    PruneThrough(index uint64) error
    // Dir returns the WAL directory path.
    Dir() string
}

package raft

import (
    "encoding/json"
    "errors"
    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/internal/wal"
)

// forgeWALAdapter adapts the process-wide Forge WAL (DefaultWAL) to the raft.WAL interface.
// It translates application payload bytes (ReplicationPayload JSON) into wire.Command and
// uses the unified envelope path for NORMAL entries; HARDSTATE is passed as raw bytes.
type forgeWALAdapter struct{
    uw wal.UnifiedWAL
}

// NewForgeWALAdapter constructs an adapter over the current DefaultWAL if it supports UnifiedWAL.
func NewForgeWALAdapter() (WAL, error) {
    if uw, ok := wal.DefaultWAL.(wal.UnifiedWAL); ok && uw != nil {
        return &forgeWALAdapter{uw: uw}, nil
    }
    return nil, errors.New("forge WAL not initialized or does not implement UnifiedWAL")
}

func (a *forgeWALAdapter) AppendEntry(index, term uint64, kind EntryKind, appBytes []byte, meta WALMetadata, rawHardState []byte) error {
    if a.uw == nil { return nil }
    switch kind {
    case EntryKindHardState:
        return a.uw.AppendEntry(wal.EntryKindHardState, index, term, 0, nil, rawHardState)
    case EntryKindNormal:
        // Decode application payload JSON into a wire.Command
        var pl ReplicationPayload
        if len(appBytes) > 0 {
            if err := json.Unmarshal(appBytes, &pl); err != nil {
                // If payload isn't ReplicationPayload JSON, skip logging to Forge WAL to avoid corrupt semantics
                return nil
            }
        }
        cmd := &wire.Command{Cmd: pl.Cmd, Args: pl.Args}
        // subSeq uses lower 32 bits of meta.Sequence when present
        sub := uint32(meta.Sequence & 0xffffffff)
        return a.uw.AppendEntry(wal.EntryKindNormal, index, term, sub, cmd, nil)
    default:
        return nil
    }
}

func (a *forgeWALAdapter) Sync() error {
    if d, ok := wal.DefaultWAL.(interface{ Sync() error }); ok {
        return d.Sync()
    }
    return nil
}

func (a *forgeWALAdapter) PruneThrough(uptoIndex uint64) (int, error) {
    if a.uw == nil { return 0, nil }
    if err := a.uw.PruneThrough(uptoIndex); err != nil { return 0, err }
    // Forge WAL prunes at most a small budget per call; return 1 if any progress is likely.
    return 1, nil
}

func (a *forgeWALAdapter) Replay(cb func(*WALReplayItem) error) error {
    if a.uw == nil { return nil }
    return a.uw.ReplayItems(func(it wal.ReplayItem) error {
        if it.Kind == wal.EntryKindHardState {
            // The raft-side Replay API does not stream HS here; use ReplayLastHardState instead
            return nil
        }
        wri := &WALReplayItem{Index: it.Index, Term: it.Term, Kind: EntryKindNormal, Meta: WALMetadata{}}
        if it.Cmd != nil {
            // Re-encode as appBytes (ReplicationPayload JSON) is not available here; pass empty to only seed storage when using this path in future.
            // For now this adapter is not used for WALPrimaryRead.
        }
        return cb(wri)
    })
}

func (a *forgeWALAdapter) ReplayLastHardState() ([]byte, bool, error) {
    if a.uw == nil { return nil, false, nil }
    return a.uw.ReplayLastHardState()
}

func (a *forgeWALAdapter) Dir() string {
    if a.uw == nil { return "" }
    return a.uw.Dir()
}

func (a *forgeWALAdapter) Close() error { return nil }

// ForgeWALFactory satisfies RaftConfig.WALFactory using the process-wide Forge WAL.
func ForgeWALFactory(cfg RaftConfig) (WAL, error) { return NewForgeWALAdapter() }

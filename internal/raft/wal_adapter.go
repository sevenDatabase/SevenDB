package raft

import (
    "hash/crc32"
    "path/filepath"
    "google.golang.org/protobuf/proto"
    "github.com/sevenDatabase/SevenDB/internal/raftwal"
)

// shadowWALAdapter adapts the existing raftwal.Writer (shadow WAL) to the generic WAL interface.
// This allows future pluggable WAL implementations without changing raft application logic.
type shadowWALAdapter struct {
    w *raftwal.Writer
    // dir cached for Dir() (Writer exposes Dir() but keep stable if interface evolves).
    dir string
}

// newShadowWALAdapter constructs (or continues) a shadow WAL writer rooted at dir.
// cfg mirrors existing RaftConfig shadow fields; for now we accept minimal subset.
func newShadowWALAdapter(dir string, strictSync bool, segmentMaxBytes int64, sidecarEvery int, forceRotateEvery int) (*shadowWALAdapter, error) {
    w, err := raftwal.NewWriter(raftwal.Config{
        Dir:               dir,
        BufMB:             4, // keep small reasonable default; future: surface
        SidecarFlushEvery: sidecarEvery,
        SegmentMaxBytes:   segmentMaxBytes,
        StrictSync:        strictSync,
        ForceRotateEvery:  forceRotateEvery,
    })
    if err != nil { return nil, err }
    return &shadowWALAdapter{w: w, dir: dir}, nil
}

// AppendEntry encodes a raftwal.Envelope and writes via underlying Writer.
func (a *shadowWALAdapter) AppendEntry(index, term uint64, kind EntryKind, appBytes []byte, meta WALMetadata, rawHardState []byte) error {
    if a.w == nil { return nil }
    env := &raftwal.Envelope{RaftIndex: index, RaftTerm: term, AppBytes: appBytes, AppCrc: crc32.ChecksumIEEE(appBytes), Namespace: "default", Bucket: meta.Bucket, Opcode: meta.Opcode, Sequence: meta.Sequence}
    switch kind {
    case EntryKindNormal:
        env.Kind = raftwal.EntryKind_ENTRY_NORMAL
    case EntryKindHardState:
        env.Kind = raftwal.EntryKind_ENTRY_HARDSTATE
        // For HardState we store rawHardState bytes as AppBytes; CRC already computed above on appBytes.
        if len(rawHardState) > 0 { env.AppBytes = rawHardState; env.AppCrc = crc32.ChecksumIEEE(rawHardState) }
    }
    b, err := proto.Marshal(env)
    if err != nil { return err }
    if kind == EntryKindHardState {
        return a.w.AppendHardState(index, b)
    }
    return a.w.AppendEnvelope(index, b)
}

func (a *shadowWALAdapter) Sync() error {
    if a.w == nil { return nil }
    return a.w.Sync()
}

func (a *shadowWALAdapter) PruneThrough(uptoIndex uint64) (int, error) {
    if a.w == nil { return 0, nil }
    return a.w.PruneThrough(uptoIndex)
}

func (a *shadowWALAdapter) Replay(cb func(*WALReplayItem) error) error {
    if a.w == nil { return nil }
    return raftwal.Replay(a.dir, func(env *raftwal.Envelope) error {
        item := &WALReplayItem{Index: env.RaftIndex, Term: env.RaftTerm, AppBytes: env.AppBytes, Meta: WALMetadata{Bucket: env.Bucket, Opcode: env.Opcode, Sequence: env.Sequence}}
        if env.Kind == raftwal.EntryKind_ENTRY_HARDSTATE { item.Kind = EntryKindHardState } else { item.Kind = EntryKindNormal }
        if item.Kind == EntryKindHardState { item.RawHardState = env.AppBytes }
        return cb(item)
    })
}

func (a *shadowWALAdapter) ReplayLastHardState() ([]byte, bool, error) {
    if a.w == nil { return nil, false, nil }
    return raftwal.ReplayLastHardState(a.dir)
}

func (a *shadowWALAdapter) Dir() string { if a.w == nil { return a.dir }; return a.w.Dir() }

func (a *shadowWALAdapter) Close() error { if a.w == nil { return nil }; return a.w.Close() }

// Helper to derive standard shadow wal directory given base DataDir and shard ID.
func defaultShadowWALDir(dataDir, shardID string) string { return filepath.Join(dataDir, "raftwal") }

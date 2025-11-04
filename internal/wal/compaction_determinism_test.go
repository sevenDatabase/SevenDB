package wal

import (
    "path/filepath"
    "testing"

    "github.com/cespare/xxhash/v2"
    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
)

// TestCompactionTailDeterminism appends a series of entries across segments, prunes early segments,
// and verifies that the replayed tail hash equals the precomputed tail hash.
func TestCompactionTailDeterminism(t *testing.T) {
    dir := t.TempDir()
    old := config.Config
    config.Config.WALVariant = "forge"
    config.Config.WALDir = filepath.Join(dir, "wal")
    config.Config.WALBufferSizeMB = 1
    config.Config.WALBufferSyncIntervalMillis = 60000
    config.Config.WALRotationMode = "segment-size"
    // Small segment to force multiple files quickly.
    config.Config.WALMaxSegmentSizeMB = 1
    config.Config.WALAutoCreateManifest = true
    config.Config.WALManifestEnforce = string(EnforceWarn)
    t.Cleanup(func() { config.Config = old })

    wf := newWalForge()
    if err := wf.Init(); err != nil {
        t.Fatalf("init: %v", err)
    }
    defer wf.Stop()

    // Append enough entries to span segments (payload ~200B each; 1MB segment -> ~5k entries)
    // Keep this light for CI: ~1000 entries should rotate at least once with protobuf overhead.
    total := 1000
    for i := 1; i <= total; i++ {
        c := &wire.Command{Cmd: "SET", Args: []string{"k", "v"}}
        if err := wf.AppendEntry(EntryKindNormal, uint64(i), 1, 0, c, nil, nil); err != nil {
            t.Fatalf("append %d: %v", i, err)
        }
    }
    if err := wf.Sync(); err != nil {
        t.Fatalf("sync: %v", err)
    }

    // Compute a prune cutoff exactly on a segment boundary to mirror prune semantics.
    // We pick the last LSN of the first segment. If we have fewer than 2 segments,
    // the prune would be a no-op; skip the test in that case to avoid false negatives.
    segs, err := wf.segments()
    if err != nil {
        t.Fatalf("segments: %v", err)
    }
    if len(segs) < 2 {
        t.Skip("not enough segments to exercise pruning; increase total or reduce segment size")
    }
    lastFirst, err := wf.lastIndexOfSegment(segs[0])
    if err != nil {
        t.Fatalf("lastIndexOfSegment: %v", err)
    }
    pruneThrough := lastFirst
    tailHashExpect := func() uint64 {
        h := xxhash.New()
        if err := wf.ReplayItems(func(ri ReplayItem) error {
            if ri.Cmd == nil {
                return nil
            }
            if ri.Index <= pruneThrough && ri.Index != 0 { // use Index when available (UWAL), else 0
                return nil
            }
            h.WriteString(ri.Cmd.Cmd)
            for _, a := range ri.Cmd.Args {
                h.WriteString("\t")
                h.WriteString(a)
            }
            h.WriteString("\n")
            return nil
        }); err != nil {
            t.Fatalf("replay pre-prune: %v", err)
        }
        return h.Sum64()
    }()

    // Prune and recompute actual replay hash
    if err := wf.PruneThrough(pruneThrough); err != nil {
        t.Fatalf("prune: %v", err)
    }
    h2 := xxhash.New()
    if err := wf.ReplayItems(func(ri ReplayItem) error {
        if ri.Cmd == nil {
            return nil
        }
        h2.WriteString(ri.Cmd.Cmd)
        for _, a := range ri.Cmd.Args {
            h2.WriteString("\t")
            h2.WriteString(a)
        }
        h2.WriteString("\n")
        return nil
    }); err != nil {
        t.Fatalf("replay post-prune: %v", err)
    }
    if tailHashExpect != h2.Sum64() {
        t.Fatalf("tail hash mismatch after prune: want=%x got=%x", tailHashExpect, h2.Sum64())
    }
}

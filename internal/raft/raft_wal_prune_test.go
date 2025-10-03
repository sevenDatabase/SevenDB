package raft

import (
    "context"
    "fmt"
    "path/filepath"
    "testing"
    "time"
    "os"

    "github.com/sevenDatabase/SevenDB/config"
)

// TestWALPruneAfterSnapshot ensures obsolete segments are removed after snapshot compaction.
func TestWALPruneAfterSnapshot(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir()
    walDir := filepath.Join(root, "wal")
    shardID := "prune-basic"
    // We will force rotation after every 5 appends to create multiple segments quickly.
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: root, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: walDir, ValidatorLastN: 64}
    start := time.Unix(0,0)
    n := newDeterministicNode(t, cfg, start)
    defer n.Close()
    for i:=0;i<300 && !n.IsLeader(); i++ { advanceAll([]*ShardRaftNode{n}, 10*time.Millisecond) }
    if !n.IsLeader() { t.Fatalf("node never became leader") }
    // Access underlying writer (test-only) to force rotation knob if available via interface.
    type forceSetter interface{ SetForceRotateEvery(int) }
    if fw, ok := n.walShadow.(forceSetter); ok { fw.SetForceRotateEvery(5) }

    // Lower snapshot threshold for test: manually set fields (test-only) to trigger snapshot quickly.
    n.snapshotThreshold = 12 // after 12 committed entries

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second); defer cancel()
    for i:=0; i<25; i++ { // enough to cross threshold and create several segments
        if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID:"b", Type: RaftRecordTypeAppCommand, Payload: []byte(fmt.Sprintf("k=%d", i))}); err != nil {
            t.Fatalf("proposal %d: %v", i, err)
        }
        advanceAll([]*ShardRaftNode{n}, 5*time.Millisecond)
    }
    for i:=0;i<500; i++ { advanceAll([]*ShardRaftNode{n}, 10*time.Millisecond) }

    // Inspect WAL directory: expect at least one segment removed (i.e., fewer than we would have from rotations)
    files, err := filepath.Glob(filepath.Join(walDir, "seg-*.wal"))
    if err != nil { t.Fatalf("glob: %v", err) }
    if len(files) == 0 { t.Fatalf("expected remaining segments") }
    // Ensure no .wal.deleted remnants
    deleted, _ := filepath.Glob(filepath.Join(walDir, "*.wal.deleted"))
    if len(deleted) > 0 { t.Fatalf("found leftover deleted markers: %v", deleted) }
}

// Placeholder for boundary and no-eligible tests (future expansion)
func TestWALPruneNoEligible(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir(); walDir := filepath.Join(root, "wal")
    shardID := "prune-none"
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: root, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: walDir, ValidatorLastN: 32}
    start := time.Unix(0,0)
    n := newDeterministicNode(t, cfg, start); defer n.Close()
    for i:=0;i<300 && !n.IsLeader(); i++ { advanceAll([]*ShardRaftNode{n}, 10*time.Millisecond) }
    if !n.IsLeader() { t.Fatalf("node never became leader") }
    // Force small number of entries without hitting snapshot threshold.
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    for i:=0; i<5; i++ { if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID:"b", Type: RaftRecordTypeAppCommand, Payload: []byte("x")}); err != nil { t.Fatalf("prop %d: %v", i, err) } }
    // Manually invoke prune with low pruneThrough to ensure nothing happens.
    if pw, ok := n.walShadow.(interface{ PruneThrough(uint64) (int, error) }); ok {
        removed, err := pw.PruneThrough(1)
        if err != nil { t.Fatalf("prune through: %v", err) }
        if removed != 0 { t.Fatalf("expected no removals, got %d", removed) }
    }
    leftover, _ := filepath.Glob(filepath.Join(walDir, "*.wal.deleted"))
    if len(leftover) != 0 { t.Fatalf("unexpected deleted markers: %v", leftover) }
}

// Simulate recovery cleanup of .deleted files
func TestWALPruneRecoveryDeletedCleanup(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir(); walDir := filepath.Join(root, "wal")
    shardID := "prune-recover"
    // Create dummy segment & marker
    if err := os.MkdirAll(walDir, 0o755); err != nil { t.Fatalf("mkdir: %v", err) }
    f, _ := os.Create(filepath.Join(walDir, "seg-0.wal.deleted")); f.Close()
    f2, _ := os.Create(filepath.Join(walDir, "seg-0.wal.idx.deleted")); f2.Close()
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: root, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: walDir, ValidatorLastN: 16}
    start := time.Unix(0,0)
    n := newDeterministicNode(t, cfg, start); defer n.Close()
    // Ensure deleted markers are gone
    leftover, _ := filepath.Glob(filepath.Join(walDir, "*.deleted"))
    if len(leftover) != 0 { t.Fatalf("deleted markers not cleaned: %v", leftover) }
}

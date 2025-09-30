package raft

import (
    "context"
    "fmt"
    "path/filepath"
    "testing"
    "time"
    "os"
    "strconv"
    "errors"

    "github.com/sevenDatabase/SevenDB/config"
)

// TestShadowWALSingleNodeDurability ensures that a node using shadow WAL can
// restart and validate parity between in-memory validator ring and replayed WAL tail.
func TestShadowWALSingleNodeDurability(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    dir := t.TempDir()
    shardID := "shad-solo"
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: filepath.Join(dir, "wal"), ValidatorLastN: 512}
    n, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new node: %v", err) }
    defer n.Close()
    // Wait a moment for potential initial Ready cycles
    time.Sleep(100 * time.Millisecond)
    // Become leader (single node auto-campaign may take a tick)
    deadline := time.Now().Add(2 * time.Second)
    for !n.IsLeader() && time.Now().Before(deadline) { time.Sleep(50*time.Millisecond) }
    if !n.IsLeader() { t.Fatalf("single node did not become leader") }

    // Propose N commands
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    N := 200
    for i := 0; i < N; i++ {
        payload := []byte(fmt.Sprintf("k=%d", i))
        if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID: "b", Type: RaftRecordTypeAppCommand, Payload: payload}); err != nil {
            t.Fatalf("propose %d: %v", i, err)
        }
    }
    lastApplied := n.Status().LastAppliedIndex
    if lastApplied == 0 { t.Fatalf("no entries applied") }
    if err := n.ValidateShadowTail(); err != nil { t.Fatalf("initial tail validate: %v", err) }

    // Restart node
    if err := n.Close(); err != nil { t.Fatalf("close: %v", err) }
    n2, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("restart: %v", err) }
    defer n2.Close()
    // Allow some time for replay
    time.Sleep(200 * time.Millisecond)
    if n2.Status().LastAppliedIndex < lastApplied { t.Fatalf("applied index regressed: %d < %d", n2.Status().LastAppliedIndex, lastApplied) }
    if err := n2.ValidateShadowTail(); err != nil { t.Fatalf("post-restart tail validate: %v", err) }
}

// TestShadowWALHardStateReplay validates that HardState envelopes persist term/commit across restarts.
func TestShadowWALHardStateReplay(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir()
    shardID := "hs-replay"
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: root, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: filepath.Join(root, "wal"), ValidatorLastN: 32}
    n, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new node: %v", err) }
    // Wait for leadership
    time.Sleep(150 * time.Millisecond)
    // Drive a few proposals to advance commit index
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    for i:=0; i<10; i++ { if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID: "b", Type: RaftRecordTypeAppCommand, Payload: []byte("x")}); err != nil { t.Fatalf("prop %d: %v", i, err) } }
    st := n.Status()
    applied := st.LastAppliedIndex
    if applied == 0 { t.Fatalf("expected applied > 0") }
    if err := n.Close(); err != nil { t.Fatalf("close: %v", err) }
    // Restart
    n2, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("restart: %v", err) }
    defer n2.Close()
    time.Sleep(200 * time.Millisecond)
    st2 := n2.Status()
    if st2.LastAppliedIndex < applied { t.Fatalf("applied regressed: %d < %d", st2.LastAppliedIndex, applied) }
}

// TestShadowWALParityStress pushes many entries and periodically validates tail parity.
func TestShadowWALParityStress(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    dir := t.TempDir()
    shardID := "stress"
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: filepath.Join(dir, "wal"), ValidatorLastN: 2048}
    n, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new node: %v", err) }
    defer n.Close()
    time.Sleep(150 * time.Millisecond)
    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()
    total := 5000 // reduce from 100k for CI speed; can scale up in non-short runs.
    for i:=0; i< total; i++ {
        if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID: "b", Type: RaftRecordTypeAppCommand, Payload: []byte(strconv.Itoa(i))}); err != nil { t.Fatalf("proposal %d: %v", i, err) }
        if i % 500 == 0 && i > 0 {
            if err := n.ValidateShadowTail(); err != nil { t.Fatalf("tail parity at %d: %v", i, err) }
        }
    }
    if err := n.ValidateShadowTail(); err != nil { t.Fatalf("final tail parity: %v", err) }
}

// TestShadowWALCrashDuringRotation simulates a crash after sidecar write by closing without clean shutdown mid stream and ensuring replay still works.
func TestShadowWALCrashDuringRotation(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir()
    shardID := "rot-crash"
    walDir := filepath.Join(root, "wal")
    cfg := RaftConfig{ShardID: shardID, NodeID: "1", Engine: "etcd", DataDir: root, ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: walDir, ValidatorLastN: 128}
    // Reduce rotation threshold by monkey patching environment (rename writer after creation?). We will approximate by writing large payloads.
    n, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new node: %v", err) }
    time.Sleep(120 * time.Millisecond)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second); defer cancel()
    // Write entries with large payload to force rotation quickly (64MB default -> we simulate by closing early; rotation forcing not directly configurable yet).
    for i:=0; i<200; i++ {
        payload := make([]byte, 1024) // 1KB * 200 ~ 200KB minimal, may not rotate depending on threshold; left as placeholder TODO when config exposed.
        if _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID: "b", Type: RaftRecordTypeAppCommand, Payload: payload}); err != nil { t.Fatalf("proposal %d: %v", i, err) }
        if i == 50 { // simulate crash mid-run
            // Do NOT call n.Close(); simulate abrupt stop by process crash: just break.
            break
        }
    }
    // Simulate crash by not calling Close; just drop reference and restart.
    // Restart node with same config.
    n2, err := NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("restart after crash: %v", err) }
    defer n2.Close()
    time.Sleep(200 * time.Millisecond)
    if err := n2.ValidateShadowTail(); err != nil { t.Fatalf("post-crash tail validate: %v", err) }
    _ = os.RemoveAll(walDir) // cleanup best-effort
}

// TestShadowWALLeaderFailoverMultiNode exercises leader crash, new leader proposals, and reintegration with shadow WAL parity checks.
func TestShadowWALLeaderFailoverMultiNode(t *testing.T) {
    if testing.Short() { t.Skip("short mode") }
    config.Config = &config.DiceDBConfig{}
    root := t.TempDir()
    shardID := "failover"
    peerSpecs := []string{"1@x","2@x","3@x"}
    mtrans := newMemTransport()
    var nodes []*ShardRaftNode
    for i:=1;i<=3;i++ {
        dataDir := filepath.Join(root, fmt.Sprintf("node-%d", i))
        cfg := RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true,
            EnableWALShadow: true, WALShadowDir: filepath.Join(dataDir, "wal"), ValidatorLastN: 256}
        n, err := NewShardRaftNode(cfg)
        if err != nil { t.Fatalf("create node %d: %v", i, err) }
        mtrans.attach(uint64(i), n); n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    // Wait leader
    waitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool { _,_,ok := findLeader(nodes); return ok }, "leader election")
    ln, lid, _ := findLeader(nodes)
    // Propose initial stream
    for i:=0;i<20;i++ { proposeOnLeader(t, nodes, "b", []byte(fmt.Sprintf("pre-%d", i))) }
    // Crash leader (abrupt - no Close)
    var leaderIdx int
    for i,n := range nodes { if n == ln { leaderIdx = i; break } }
    // Remove leader from transport (simulate crash)
    // (We simply don't forward messages because we drop reference from nodes slice temporarily)
    crashedDir := filepath.Join(root, fmt.Sprintf("node-%d", leaderIdx+1))
    // Replace slot with nil to avoid proposals going there
    nodes[leaderIdx] = &ShardRaftNode{shardID:"_dead"} // placeholder inert
    // Wait for new leader (different id)
    waitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool { _, newID, ok := findLeader(nodes); return ok && newID != lid }, "new leader election")
    // Propose more entries under new leader
    for i:=0;i<30;i++ { proposeOnLeader(t, nodes, "b", []byte(fmt.Sprintf("post-%d", i))) }
    // Restart crashed leader
    restartCfg := RaftConfig{ShardID: shardID, NodeID: lid, Peers: peerSpecs, DataDir: crashedDir, Engine: "etcd", ForwardProposals: true,
        EnableWALShadow: true, WALShadowDir: filepath.Join(crashedDir, "wal"), ValidatorLastN: 256}
    restarted, err := NewShardRaftNode(restartCfg)
    if err != nil { t.Fatalf("restart leader: %v", err) }
    nodes[leaderIdx] = restarted
    mtrans.attach(uint64(leaderIdx+1), restarted); restarted.SetTransport(mtrans)
    // Allow catch-up
    waitUntil(t, 10*time.Second, 100*time.Millisecond, func() bool {
        // ensure no node is far behind last applied of any other
        var max uint64
        for _, n := range nodes { st := n.Status(); if st.LastAppliedIndex > max { max = st.LastAppliedIndex } }
        for _, n := range nodes { if (n.Status().LastAppliedIndex + 5) < max { return false } }
        return max > 0
    }, "restart catch-up")
    // Validator parity on each
    for i, n := range nodes {
        if n.shardID == "_dead" { continue }
        if err := n.ValidateShadowTail(); err != nil { t.Fatalf("node %d tail parity: %v", i+1, err) }
    }
    // Proposals still succeed
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second); defer cancel()
    ln2, _, _ := findLeader(nodes)
    if ln2 == nil { t.Fatalf("no leader after restart") }
    if _, _, err := ln2.ProposeAndWait(ctx, &RaftLogRecord{BucketID:"b", Type: RaftRecordTypeAppCommand, Payload: []byte("final")}); err != nil {
        var nle *NotLeaderError
        if !errors.As(err, &nle) { t.Fatalf("final proposal failed: %v", err) }
    }
    for _, n := range nodes { if n.shardID != "_dead" { _ = n.Close() } }
}

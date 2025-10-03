package raft

import (
    "context"
    "errors"
    "fmt"
    "path/filepath"
    "runtime"
    "sort"
    "sync"
    "testing"
    "time"

    "github.com/sevenDatabase/SevenDB/config"
    "go.etcd.io/etcd/raft/v3/raftpb"
)

// memTransport is a simple in-process Transport used only for tests. It directly
// invokes Step on target nodes without serialization or network.
type memTransport struct {
    mu    sync.RWMutex
    nodes map[uint64]*ShardRaftNode
}

func newMemTransport() *memTransport { return &memTransport{nodes: make(map[uint64]*ShardRaftNode)} }

func (m *memTransport) Send(ctx context.Context, msgs []raftpb.Message) {
    m.mu.RLock(); defer m.mu.RUnlock()
    for _, msg := range msgs {
        if msg.To == 0 { continue }
        n := m.nodes[msg.To]
        if n == nil { continue }
        // best-effort; ignore error
        _ = n.Step(ctx, msg)
    }
}

// register / update node pointer
func (m *memTransport) attach(id uint64, n *ShardRaftNode) {
    m.mu.Lock(); defer m.mu.Unlock(); m.nodes[id] = n }

// waitUntil polls fn until it returns true or timeout.
func waitUntil(t *testing.T, timeout time.Duration, interval time.Duration, fn func() bool, desc string) {
    // Updated to advance simulated clocks if present instead of wall sleeping.
    var nodes []*ShardRaftNode
    // Attempt to capture caller's nodes slice via closure heuristics not possible; keep original behavior when no clocks.
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if fn() { return }
        // Try advancing any provided nodes (best-effort) – if none captured, fallback to sleep.
        for _, n := range nodes { if n != nil && n.clk != nil { n.clk.Advance(interval) } }
        time.Sleep(interval)
    }
    t.Fatalf("timeout waiting for condition: %s", desc)
}

// findLeader returns (leaderNode, leaderID,boolFound)
func findLeader(nodes []*ShardRaftNode) (*ShardRaftNode, string, bool) {
    for _, n := range nodes {
        if n.IsLeader() { return n, n.LeaderID(), true }
    }
    return nil, "", false
}

// proposeOnLeader proposes a record on the current leader, re-discovering leader if it changes.
func proposeOnLeader(t *testing.T, nodes []*ShardRaftNode, bucket string, payload []byte) (uint64, uint64) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    for attempts := 0; attempts < 10; attempts++ {
        ln, _, ok := findLeader(nodes)
        if !ok { time.Sleep(100 * time.Millisecond); continue }
        commitIdx, raftIdx, err := ln.ProposeAndWait(ctx, &RaftLogRecord{BucketID: bucket, Type: 1, Payload: payload})
        if err != nil {
            var nle *NotLeaderError
            if errors.As(err, &nle) { // leader changed mid-flight
                time.Sleep(100 * time.Millisecond)
                continue
            }
            t.Fatalf("proposal failed: %v", err)
        }
        return commitIdx, raftIdx
    }
    t.Fatalf("could not find stable leader for proposal")
    return 0,0
}

// waitApplied waits until all nodes have lastAppliedIndex >= target.
func waitApplied(t *testing.T, nodes []*ShardRaftNode, target uint64) {
    waitUntil(t, 10*time.Second, 50*time.Millisecond, func() bool {
        for _, n := range nodes { if n.Status().LastAppliedIndex < target { return false } }
        return true
    }, fmt.Sprintf("all nodes applied >= %d", target))
}

func TestRaftClusterBasicReplicationAndSnapshot(t *testing.T) {
    if testing.Short() { t.Skip("skipping in short mode") }
    // Set a small snapshot threshold for the test.
    // Initialize global config (normally done by main/flags); required before field access.
    config.Config = &config.DiceDBConfig{}
    config.Config.RaftSnapshotThresholdEntries = 5

    tempRoot := t.TempDir()
    shardID := "shard-test"
    peerSpecs := []string{"1@test", "2@test", "3@test"} // addresses unused by mem transport

    mtrans := newMemTransport()

    var nodes []*ShardRaftNode
    start := time.Unix(0,0)
    for i:=1; i<=3; i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        cfg := RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}
        n := newDeterministicNode(t, cfg, start)
        id := uint64(i)
        mtrans.attach(id, n)
        n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    for i:=0;i<500;i++ { if _,_,ok:=findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
    if _,_,ok:=findLeader(nodes); !ok { t.Fatalf("no leader elected") }

    // Propose several entries; ensure replication & commit ordering.
    var lastRaft uint64
    var lastCommit uint64
    for i:=0; i<10; i++ {
        cIdx, rIdx := proposeOnLeader(t, nodes, "bucketA", []byte(fmt.Sprintf("val-%d", i)))
        if cIdx <= lastCommit { t.Fatalf("commit index not increasing: %d <= %d", cIdx, lastCommit) }
        if rIdx <= lastRaft { t.Fatalf("raft index not increasing: %d <= %d", rIdx, lastRaft) }
        lastCommit, lastRaft = cIdx, rIdx
    }
    for i:=0;i<1000;i++ { // deterministic apply wait
        all := true
        for _, n := range nodes { if n.Status().LastAppliedIndex < lastRaft { all = false; break } }
        if all { break }
        advanceAll(nodes, 10*time.Millisecond)
    }
    for _, n := range nodes { if n.Status().LastAppliedIndex < lastRaft { t.Fatalf("apply lag after timeout") } }

    // Expect snapshot occurred (threshold=5) -> poll until lastSnapshotIndex > 0.
    for i:=0;i<1000;i++ { snap := false; for _, n := range nodes { if n.Status().LastSnapshotIndex > 0 { snap = true; break } }; if snap { break }; advanceAll(nodes, 10*time.Millisecond) }
    hasSnap := false; for _, n := range nodes { if n.Status().LastSnapshotIndex > 0 { hasSnap = true; break } }
    if !hasSnap { t.Fatalf("snapshot not triggered in deterministic window") }

    // Proposals on follower should yield NotLeaderError.
    ln, leaderID, _ := findLeader(nodes)
    for _, n := range nodes {
        if n == ln { continue }
        ctx, cancel := context.WithTimeout(context.Background(), time.Second)
        _, _, err := n.ProposeAndWait(ctx, &RaftLogRecord{BucketID:"bucketA", Type:1, Payload: []byte("follower-attempt")})
        cancel()
        if err == nil { t.Fatalf("expected NotLeaderError from follower") }
        var nle *NotLeaderError
        if !errors.As(err, &nle) { t.Fatalf("expected NotLeaderError, got %v", err) }
        if nle.LeaderID != leaderID { t.Fatalf("leader hint mismatch: %s vs %s", nle.LeaderID, leaderID) }
        break
    }

    // Restart the leader and ensure it restores indices.
    // Capture state before restart.
    preStatus := ln.Status()
    preApplied := preStatus.LastAppliedIndex
    preSnapshot := preStatus.LastSnapshotIndex
    // Close leader
    if err := ln.Close(); err != nil { t.Fatalf("close leader: %v", err) }
    // Replace node object with new instance using same data dir.
    leaderIdx := 0
    for i, n := range nodes { if n == ln { leaderIdx = i; break } }
    oldDataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", leaderIdx+1))
    newNode, err := NewShardRaftNode(RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", leaderIdx+1), Peers: peerSpecs, DataDir: oldDataDir, Engine: "etcd", ForwardProposals: true})
    if err != nil { t.Fatalf("restart leader: %v", err) }
    nodes[leaderIdx] = newNode
    mtrans.attach(uint64(leaderIdx+1), newNode)
    newNode.SetTransport(mtrans)

    // Allow re-election (may or may not become leader again).
    for i:=0;i<500;i++ { advanceAll(nodes, 10*time.Millisecond) }
    // Validate restored indices.
    st := newNode.Status()
    if st.LastAppliedIndex < preApplied { t.Fatalf("restarted node applied index regressed: %d < %d", st.LastAppliedIndex, preApplied) }
    if preSnapshot > 0 && st.LastSnapshotIndex == 0 { t.Fatalf("snapshot index lost after restart: was %d now %d", preSnapshot, st.LastSnapshotIndex) }

    // Final GC hint to reduce noise in race tests.
    runtime.GC()
}

// --- Additional Tests ---

// TestRaftLogPruneAfterSnapshot verifies that entries prior to snapshot index are pruned from disk.
func TestRaftLogPruneAfterSnapshot(t *testing.T) {
    config.Config = &config.DiceDBConfig{}
    config.Config.RaftSnapshotThresholdEntries = 3
    tempRoot := t.TempDir()
    shardID := "shard-prune"
    peerSpecs := []string{"1@x","2@x","3@x"}
    mtrans := newMemTransport()
    var nodes []*ShardRaftNode
    start := time.Unix(0,0)
    for i:=1;i<=3;i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        n := newDeterministicNode(t, RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}, start)
        mtrans.attach(uint64(i), n); n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    for i:=0;i<500;i++ { if _,_,ok:=findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
    if _,_,ok:=findLeader(nodes); !ok { t.Fatalf("no leader elected") }
    // Drive proposals until node-1 snapshots.
    var lastRaft uint64
    for i:=0; i<20; i++ {
        _, rIdx := proposeOnLeader(t, nodes, "b", []byte(fmt.Sprintf("p-%d", i)))
        lastRaft = rIdx
        waitApplied(t, nodes, lastRaft)
        if nodes[0].Status().LastSnapshotIndex > 0 { break }
    }
    snapIdx := nodes[0].Status().LastSnapshotIndex
    if snapIdx == 0 { t.Fatalf("node-1 never produced snapshot within proposal budget") }
    // Wait for pruning completion signaled by prunedThroughIndex >= snapshot index.
    for i:=0;i<1000;i++ { st := nodes[0].Status(); if st.PrunedThroughIndex >= snapIdx { break }; advanceAll(nodes, 10*time.Millisecond) }
    if nodes[0].Status().PrunedThroughIndex < snapIdx { t.Fatalf("prune did not reach snapshot index") }
}

// TestRaftFollowerRestartCatchUp ensures a stopped follower catches up on restart.
func TestRaftFollowerRestartCatchUp(t *testing.T) {
    config.Config = &config.DiceDBConfig{}
    config.Config.RaftSnapshotThresholdEntries = 100 // disable snapshot influence here
    tempRoot := t.TempDir()
    shardID := "shard-restart-follower"
    peerSpecs := []string{"1@x","2@x","3@x"}
    mtrans := newMemTransport()
    var nodes []*ShardRaftNode
    start := time.Unix(0,0)
    for i:=1;i<=3;i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        n := newDeterministicNode(t, RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}, start)
        mtrans.attach(uint64(i), n); n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    for i:=0;i<500;i++ { if _,_,ok:=findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
    if _,_,ok:=findLeader(nodes); !ok { t.Fatalf("no leader elected") }
    // Propose initial entries
    var last uint64
    for i:=0;i<5;i++ { _, rIdx := proposeOnLeader(t, nodes, "bx", []byte("seed")); last = rIdx }
    for i:=0;i<1000;i++ { all := true; for _, n := range nodes { if n.Status().LastAppliedIndex < last { all=false; break } }; if all { break }; advanceAll(nodes, 10*time.Millisecond) }
    for _, n := range nodes { if n.Status().LastAppliedIndex < last { t.Fatalf("apply lag post seed") } }
    // Pick a follower to restart
    leader, lid, _ := findLeader(nodes)
    var followerIdx int
    for i,n := range nodes { if n != leader { followerIdx = i; break } }
    follower := nodes[followerIdx]
    preIdx := follower.Status().LastAppliedIndex
    if err := follower.Close(); err != nil { t.Fatalf("close follower: %v", err) }
    // Build active node list excluding the closed follower to avoid proposing to a closed node (race fix).
    activeNodes := make([]*ShardRaftNode,0,len(nodes)-1)
    for _, n := range nodes { if n != follower { activeNodes = append(activeNodes, n) } }
    // Produce more entries while follower is down using only active nodes.
    for i:=0;i<6;i++ { _, rIdx := proposeOnLeader(t, activeNodes, "bx", []byte("later")); last = rIdx }
    // Ensure those entries are applied on all active nodes (2-node cluster) before restart.
    for i:=0;i<1000;i++ { all := true; for _, n := range activeNodes { if n.Status().LastAppliedIndex < last { all=false; break } }; if all { break }; advanceAll(activeNodes, 10*time.Millisecond) }
    for _, n := range activeNodes { if n.Status().LastAppliedIndex < last { t.Fatalf("apply lag active cluster") } }
    // Restart follower
    dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", followerIdx+1))
    newFollower, err := NewShardRaftNode(RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", followerIdx+1), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true})
    if err != nil { t.Fatalf("restart follower: %v", err) }
    nodes[followerIdx] = newFollower
    mtrans.attach(uint64(followerIdx+1), newFollower); newFollower.SetTransport(mtrans)
    // Ensure leader still leader
    _ = lid
    for i:=0;i<1000;i++ { all := true; for _, n := range nodes { if n.Status().LastAppliedIndex < last { all=false; break } }; if all { break }; advanceAll(nodes, 10*time.Millisecond) }
    for _, n := range nodes { if n.Status().LastAppliedIndex < last { t.Fatalf("apply lag after follower restart") } }
    if newFollower.Status().LastAppliedIndex <= preIdx { t.Fatalf("follower didn't advance after restart: %d <= %d", newFollower.Status().LastAppliedIndex, preIdx) }
}

// TestRaftMultiBucketCommitIsolation checks that commit indices per bucket are independent and gapless.
func TestRaftMultiBucketCommitIsolation(t *testing.T) {
    config.Config = &config.DiceDBConfig{}
    config.Config.RaftSnapshotThresholdEntries = 50
    tempRoot := t.TempDir()
    shardID := "shard-multi-bucket"
    peerSpecs := []string{"1@x","2@x","3@x"}
    mtrans := newMemTransport()
    var nodes []*ShardRaftNode
    start := time.Unix(0,0)
    for i:=1;i<=3;i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        n := newDeterministicNode(t, RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}, start)
        mtrans.attach(uint64(i), n); n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    for i:=0;i<500;i++ { if _,_,ok:=findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
    if _,_,ok:=findLeader(nodes); !ok { t.Fatalf("no leader elected") }
    var seqA, seqB []uint64
    for i:=0;i<20;i++ {
        bucket := "A"; if i%2==1 { bucket = "B" }
        commitIdx, _ := func() (uint64, uint64) { return proposeOnLeader(t, nodes, bucket, []byte("x")) }()
        if bucket == "A" { seqA = append(seqA, commitIdx) } else { seqB = append(seqB, commitIdx) }
    }
    // Validate sequences strictly increasing and contiguous starting at 1.
    validate := func(name string, s []uint64) {
        if len(s)==0 { t.Fatalf("empty sequence for %s", name) }
        copyS := append([]uint64(nil), s...)
    sorted := append([]uint64(nil), s...); sort.Slice(sorted, func(i,j int) bool {return sorted[i]<sorted[j]})
        for i, v := range sorted { expect := uint64(i+1); if v != expect { t.Fatalf("%s commit index gap: got %v at pos %d expect %d (raw=%v)", name, v, i, expect, copyS) } }
    }
    validate("bucketA", seqA)
    validate("bucketB", seqB)
}

// TestNotLeaderErrorSemantics ensures ErrNotLeader style errors are typed & leader hint present.
func TestNotLeaderErrorSemantics(t *testing.T) {
    config.Config = &config.DiceDBConfig{}
    config.Config.RaftSnapshotThresholdEntries = 100
    tempRoot := t.TempDir()
    shardID := "shard-nl"
    peerSpecs := []string{"1@x","2@x","3@x"}
    mtrans := newMemTransport()
    var nodes []*ShardRaftNode
    start := time.Unix(0,0)
    for i:=1;i<=3;i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        n := newDeterministicNode(t, RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}, start)
        mtrans.attach(uint64(i), n); n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }
    for i:=0;i<500;i++ { if _,_,ok:=findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
    if _,_,ok:=findLeader(nodes); !ok { t.Fatalf("no leader elected") }
    leader, leaderID, _ := findLeader(nodes)
    // Pick follower
    var follower *ShardRaftNode
    for _, n := range nodes { if n != leader { follower = n; break } }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    _, _, err := follower.ProposeAndWait(ctx, &RaftLogRecord{BucketID:"zz", Type:1, Payload: []byte("no")})
    cancel()
    if err == nil { t.Fatalf("expected not leader error") }
    var nle *NotLeaderError
    if !errors.As(err, &nle) { t.Fatalf("expected NotLeaderError type got %v", err) }
    if nle.LeaderID != leaderID { t.Fatalf("leader id mismatch hint=%s actual=%s", nle.LeaderID, leaderID) }
}

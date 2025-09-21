package raft

import (
    "context"
    "errors"
    "fmt"
    "path/filepath"
    "runtime"
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
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if fn() { return }
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
    for i:=1; i<=3; i++ {
        dataDir := filepath.Join(tempRoot, fmt.Sprintf("node-%d", i))
        cfg := RaftConfig{ShardID: shardID, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dataDir, Engine: "etcd", ForwardProposals: true}
        n, err := NewShardRaftNode(cfg)
        if err != nil { t.Fatalf("create node %d: %v", i, err) }
        id := uint64(i)
        mtrans.attach(id, n)
        n.SetTransport(mtrans)
        nodes = append(nodes, n)
    }

    // Wait for leader election.
    waitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool { _,_,ok := findLeader(nodes); return ok }, "leader election")

    // Propose several entries; ensure replication & commit ordering.
    var lastRaft uint64
    var lastCommit uint64
    for i:=0; i<10; i++ {
        cIdx, rIdx := proposeOnLeader(t, nodes, "bucketA", []byte(fmt.Sprintf("val-%d", i)))
        if cIdx <= lastCommit { t.Fatalf("commit index not increasing: %d <= %d", cIdx, lastCommit) }
        if rIdx <= lastRaft { t.Fatalf("raft index not increasing: %d <= %d", rIdx, lastRaft) }
        lastCommit, lastRaft = cIdx, rIdx
    }
    waitApplied(t, nodes, lastRaft)

    // Expect snapshot occurred (threshold=5) -> poll until lastSnapshotIndex > 0.
    waitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool {
        for _, n := range nodes { if n.Status().LastSnapshotIndex > 0 { return true } }
        return false
    }, "snapshot trigger")

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
    time.Sleep(500 * time.Millisecond)
    // Validate restored indices.
    st := newNode.Status()
    if st.LastAppliedIndex < preApplied { t.Fatalf("restarted node applied index regressed: %d < %d", st.LastAppliedIndex, preApplied) }
    if preSnapshot > 0 && st.LastSnapshotIndex == 0 { t.Fatalf("snapshot index lost after restart: was %d now %d", preSnapshot, st.LastSnapshotIndex) }

    // Final GC hint to reduce noise in race tests.
    runtime.GC()
}

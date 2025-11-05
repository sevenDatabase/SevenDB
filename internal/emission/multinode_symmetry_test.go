package emission_test

import (
    "context"
    "encoding/hex"
    "errors"
    "strconv"
    "strings"
    "testing"
    "time"

    "github.com/cespare/xxhash/v2"
    "github.com/sevenDatabase/SevenDB/internal/determinism"
    "github.com/sevenDatabase/SevenDB/internal/emission"
    "github.com/sevenDatabase/SevenDB/internal/harness/clock"
    "github.com/sevenDatabase/SevenDB/internal/raft"
    etcdpb "go.etcd.io/etcd/raft/v3/raftpb"
)

// test-local in-memory transport satisfying raft.Transport
type localMemTransport struct { nodes map[uint64]*raft.ShardRaftNode }

func newLocalMemTransport() *localMemTransport { return &localMemTransport{nodes: map[uint64]*raft.ShardRaftNode{}} }

func (m *localMemTransport) Send(ctx context.Context, msgs []etcdpb.Message) {
    for _, msg := range msgs {
        if msg.To == 0 { continue }
        if n := m.nodes[msg.To]; n != nil { _ = n.Step(ctx, msg) }
    }
}

func (m *localMemTransport) attach(id uint64, n *raft.ShardRaftNode) { m.nodes[id] = n }

func findLeader(nodes []*raft.ShardRaftNode) *raft.ShardRaftNode {
    for _, n := range nodes { if n.Status().IsLeader { return n } }
    return nil
}

func proposeOnLeader(t *testing.T, nodes []*raft.ShardRaftNode, rec *raft.RaftLogRecord) {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    for attempts := 0; attempts < 10; attempts++ {
        ln := findLeader(nodes)
        if ln == nil { time.Sleep(5 * time.Millisecond); continue }
        if _, _, err := ln.ProposeAndWait(ctx, rec); err != nil {
            var nle *raft.NotLeaderError
            if err == context.DeadlineExceeded { t.Fatalf("proposal timeout: %v", err) }
            if errors.As(err, &nle) { time.Sleep(5 * time.Millisecond); continue }
            t.Fatalf("proposal failed: %v", err)
        } else { return }
    }
    t.Fatalf("could not find stable leader for proposal")
}

func canonicalHash(evs []*emission.DataEvent, n int) string {
    h := xxhash.New()
    limit := n
    if len(evs) < n { limit = len(evs) }
    seq := uint64(0)
    for _, ev := range evs[:limit] {
        fp := uint64(0)
        if idx := strings.LastIndex(ev.SubID, ":"); idx >= 0 {
            if v, err := strconv.ParseUint(ev.SubID[idx+1:], 10, 64); err == nil { fp = v }
        }
        seq++
        e := determinism.Emission{Fingerprint: fp, EmitSeq: seq, Event: "DATA", Fields: map[string]string{"delta": string(ev.Delta)}}
        h.Write(determinism.CanonicalLine(e))
        h.Write([]byte{'\n'})
    }
    return hex.EncodeToString(h.Sum(nil))
}

func TestEmission_MultiReplicaSymmetry_3Nodes(t *testing.T) {
    // Deterministic clocks per node
    start := time.Unix(0, 0)
    // Set a small notifier poll interval via config if desired; defaults already small.

    tempRoot := t.TempDir()
    shardID := "sym-shard"
    peerSpecs := []string{"1@x", "2@x", "3@x"}
    trans := newLocalMemTransport()

    var nodes []*raft.ShardRaftNode
    var notifiers []*emission.Notifier
    var senders []*emission.MemorySender
    var clocks []clock.Clock

    for i := 1; i <= 3; i++ {
        clk := clock.NewSimulatedClock(start)
    cfg := raft.RaftConfig{ShardID: shardID, NodeID: strconv.Itoa(i), Peers: peerSpecs, DataDir: tempRoot, Engine: "etcd", ForwardProposals: true, DisablePersistence: true}
    cfg.TestDeterministicClock = clk
        n, err := raft.NewShardRaftNode(cfg)
        if err != nil { t.Fatalf("new raft %d: %v", i, err) }
        trans.attach(uint64(i), n)
        n.SetTransport(trans)
        mgr := emission.NewManager(shardID)
        emission.RegisterWithShard(n, mgr)
        ctx := context.Background()
        ap := emission.NewApplier(n, mgr, shardID); ap.Start(ctx)
        sender := &emission.MemorySender{}
    nt := emission.NewNotifier(mgr, nil, &emission.RaftProposer{Node: n, BucketID: shardID}, shardID)
    nt.Start(ctx)
        nodes = append(nodes, n)
        notifiers = append(notifiers, nt)
        senders = append(senders, sender)
        clocks = append(clocks, clk)
    }

    // Drive clocks until a leader emerges
    for i := 0; i < 1000 && findLeader(nodes) == nil; i++ {
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        time.Sleep(200 * time.Microsecond)
    }
    ln := findLeader(nodes)
    if ln == nil { t.Fatalf("no leader elected") }
    // Enable sender only on the leader's notifier
    for i, n := range nodes {
        if n == ln { notifiers[i].SetSender(senders[i]) } else { notifiers[i].SetSender(nil) }
    }

    // Propose fixed workload on leader
    sub := "c1:424242"
    const N = 10
    for i := 0; i < N; i++ {
        rec, _ := raft.BuildReplicationRecord(shardID, "DATA_EVENT", []string{sub, "val-"+strconv.Itoa(i)})
        proposeOnLeader(t, nodes, rec)
        // advance clocks to drive raft ticks
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        // deterministically drive the leader notifier once
        for i2, n := range nodes { if n == ln { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    // Deterministically drive a few more cycles and then check
    for i := 0; i < 5; i++ {
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        for i2, n := range nodes { if n == ln { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    deadline := time.Now().Add(3 * time.Second)
    var got int
    for time.Now().Before(deadline) {
        for i, n := range nodes {
            if n == ln { got = len(senders[i].Snapshot()) }
        }
        if got >= N { break }
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        for i2, n := range nodes { if n == ln { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    if got < N { t.Fatalf("leader delivered %d events, want %d", got, N) }

    // Compute canonical hash and assert it’s stable across two fresh 3-node clusters
    h1 := ""; h2 := ""
    for i, n := range nodes { if n == ln { h1 = canonicalHash(senders[i].Snapshot(), N) } }

    // Rebuild cluster fresh and repeat to compare
    // Teardown
    for _, n := range nodes { _ = n.Close() }

    // New cluster same config
    trans2 := newLocalMemTransport()
    nodes, notifiers, senders, clocks = nil, nil, nil, nil
    for i := 1; i <= 3; i++ {
        clk := clock.NewSimulatedClock(start)
        cfg := raft.RaftConfig{ShardID: shardID, NodeID: strconv.Itoa(i), Peers: peerSpecs, DataDir: tempRoot, Engine: "etcd", ForwardProposals: true, DisablePersistence: true}
        cfg.TestDeterministicClock = clk
        n, err := raft.NewShardRaftNode(cfg)
        if err != nil { t.Fatalf("new raft2 %d: %v", i, err) }
        trans2.attach(uint64(i), n)
        n.SetTransport(trans2)
        mgr := emission.NewManager(shardID)
        emission.RegisterWithShard(n, mgr)
        ctx := context.Background()
        ap := emission.NewApplier(n, mgr, shardID); ap.Start(ctx)
        sender := &emission.MemorySender{}
    nt := emission.NewNotifier(mgr, nil, &emission.RaftProposer{Node: n, BucketID: shardID}, shardID)
    nt.Start(ctx)
        nodes = append(nodes, n)
        notifiers = append(notifiers, nt)
        senders = append(senders, sender)
        clocks = append(clocks, clk)
    }
    for i := 0; i < 1000 && findLeader(nodes) == nil; i++ {
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        time.Sleep(200 * time.Microsecond)
    }
    ln2 := findLeader(nodes)
    if ln2 == nil { t.Fatalf("no leader in second cluster") }
    for i, n := range nodes { if n == ln2 { notifiers[i].SetSender(senders[i]) } else { notifiers[i].SetSender(nil) } }
    for i := 0; i < N; i++ {
        rec, _ := raft.BuildReplicationRecord(shardID, "DATA_EVENT", []string{sub, "val-"+strconv.Itoa(i)})
        proposeOnLeader(t, nodes, rec)
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        for i2, n := range nodes { if n == ln2 { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    deadline = time.Now().Add(3 * time.Second)
    got = 0
    for time.Now().Before(deadline) {
        for i, n := range nodes { if n == ln2 { got = len(senders[i].Snapshot()) } }
        if got >= N { break }
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        for i2, n := range nodes { if n == ln2 { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    if got < N { t.Fatalf("leader delivered (2) %d, want %d", got, N) }
    for i, n := range nodes { if n == ln2 { h2 = canonicalHash(senders[i].Snapshot(), N) } }

    if h1 != h2 { t.Fatalf("canonical emission hash mismatch across clusters: %s vs %s", h1, h2) }
}

package emission_test

import (
    "context"
    "strconv"
    "strings"
    "sync"
    "testing"
    "time"

    "github.com/sevenDatabase/SevenDB/internal/emission"
    "github.com/sevenDatabase/SevenDB/internal/raft"
)

// simple deduper keyed by (fp, commit_index)
type deduper struct{ seen sync.Map }

func (d *deduper) add(sub string, commit uint64) bool {
    fp := uint64(0)
    if idx := strings.LastIndex(sub, ":"); idx >= 0 {
        if v, err := strconv.ParseUint(sub[idx+1:], 10, 64); err == nil { fp = v }
    }
    key := strconv.FormatUint(fp, 10) + ":" + strconv.FormatUint(commit, 10)
    _, loaded := d.seen.LoadOrStore(key, struct{}{})
    return !loaded
}

// helper to wait for leader with a timeout
func waitLeader(t *testing.T, n *raft.ShardRaftNode, timeout time.Duration) {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if n.IsLeader() { return }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatalf("node did not become leader in time")
}

func TestEmission_CrashBeforeSend_ExactlyOnce(t *testing.T) {
    dir := t.TempDir()
    cfg := raft.RaftConfig{ShardID: "crash-sym", NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true}
    n, err := raft.NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new raft: %v", err) }
    t.Cleanup(func(){ _ = n.Close() })

    mgr := emission.NewManager("crash-sym")
    emission.RegisterWithShard(n, mgr)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    ap := emission.NewApplier(n, mgr, "crash-sym"); ap.Start(ctx)
    sender1 := &emission.MemorySender{}
    nt1 := emission.NewNotifier(mgr, sender1, &emission.RaftProposer{Node: n, BucketID: "crash-sym"}, "crash-sym")
    nt1.Start(ctx)

    waitLeader(t, n, 2*time.Second)

    // Crash just before the first send
    var once sync.Once
    emission.TestHookBeforeSend = func(sub string, seq emission.EmitSeq) {
        once.Do(func(){ cancel() /* stop notifier/applier */ })
    }
    t.Cleanup(func(){ emission.TestHookBeforeSend = nil })

    // Propose a single data event
    rec, _ := raft.BuildReplicationRecord("crash-sym", "DATA_EVENT", []string{"c1:424242", "payload-1"})
    if _, _, err := n.ProposeAndWait(context.Background(), rec); err != nil { t.Fatalf("propose: %v", err) }

    // Allow a few notifier ticks; since hook cancels context, nothing should be sent
    time.Sleep(50 * time.Millisecond)
    if got := len(sender1.Snapshot()); got != 0 { t.Fatalf("unexpected sends before restart: %d", got) }

    // Start a fresh notifier (simulating process restart of notifier) on the same manager
    ctx2, cancel2 := context.WithCancel(context.Background())
    defer cancel2()
    sender2 := &emission.MemorySender{}
    nt2 := emission.NewNotifier(mgr, sender2, &emission.RaftProposer{Node: n, BucketID: "crash-sym"}, "crash-sym")
    nt2.Start(ctx2)

    // Wait for exactly one delivery
    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if len(sender2.Snapshot()) >= 1 { break }
        time.Sleep(10 * time.Millisecond)
    }
    if got := len(sender2.Snapshot()); got != 1 { t.Fatalf("expected exactly one send after restart, got %d", got) }
}

func TestEmission_CrashAfterSendBeforeAck_AtLeastOnceWithDedupe(t *testing.T) {
    dir := t.TempDir()
    cfg := raft.RaftConfig{ShardID: "crash-sym2", NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true}
    n, err := raft.NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new raft: %v", err) }
    t.Cleanup(func(){ _ = n.Close() })

    mgr := emission.NewManager("crash-sym2")
    emission.RegisterWithShard(n, mgr)
    ctx, cancel := context.WithCancel(context.Background())
    ap := emission.NewApplier(n, mgr, "crash-sym2"); ap.Start(ctx)
    sender1 := &emission.MemorySender{}
    nt1 := emission.NewNotifier(mgr, sender1, &emission.RaftProposer{Node: n, BucketID: "crash-sym2"}, "crash-sym2")
    nt1.Start(ctx)

    waitLeader(t, n, 2*time.Second)

    // Crash right after the first successful send, before any ack
    var once sync.Once
    emission.TestHookAfterSendBeforeAck = func(sub string, seq emission.EmitSeq) {
        once.Do(func(){ cancel() })
    }
    t.Cleanup(func(){ emission.TestHookAfterSendBeforeAck = nil })

    rec, _ := raft.BuildReplicationRecord("crash-sym2", "DATA_EVENT", []string{"c1:424242", "payload-1"})
    if _, _, err := n.ProposeAndWait(context.Background(), rec); err != nil { t.Fatalf("propose: %v", err) }

    // Wait for first notifier to (likely) deliver once before crash hook cancels it
    time.Sleep(50 * time.Millisecond)
    firstSends := len(sender1.Snapshot())
    if firstSends < 1 { t.Fatalf("expected at least one send before crash, got %d", firstSends) }

    // Start a fresh notifier without ack (client didn’t ack yet), it should resend
    ctx2, cancel2 := context.WithCancel(context.Background())
    defer cancel2()
    sender2 := &emission.MemorySender{}
    nt2 := emission.NewNotifier(mgr, sender2, &emission.RaftProposer{Node: n, BucketID: "crash-sym2"}, "crash-sym2")
    nt2.Start(ctx2)

    // Collect resends and dedupe on client side
    d := &deduper{}
    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        take := sender2.Snapshot()
        if len(take) >= 1 { break }
        time.Sleep(10 * time.Millisecond)
    }
    totalSends := firstSends + len(sender2.Snapshot())
    if totalSends < 2 { t.Fatalf("expected duplicate resend across crash, total sends=%d", totalSends) }
    // Apply dedupe: only one unique (fp, emit_seq)
    uniq := 0
    for _, ev := range append(sender1.Snapshot(), sender2.Snapshot()...) {
        if d.add(ev.SubID, ev.EmitSeq.CommitIndex) { uniq++ }
    }
    if uniq != 1 { t.Fatalf("expected 1 unique event after dedupe, got %d", uniq) }
}

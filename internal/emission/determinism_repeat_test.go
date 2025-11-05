package emission_test

import (
    "bytes"
    "context"
    "strconv"
    "strings"
    "testing"
    "time"

    "github.com/sevenDatabase/SevenDB/internal/determinism"
    "github.com/sevenDatabase/SevenDB/internal/emission"
    "github.com/sevenDatabase/SevenDB/internal/harness/clock"
    "github.com/sevenDatabase/SevenDB/internal/raft"
)

// --- Canonical transcript builders ---

// transcriptCommitIndex renders a stable, byte-for-byte transcript using commit index as EmitSeq.
func transcriptCommitIndex(evs []*emission.DataEvent) []byte {
    var buf bytes.Buffer
    for _, ev := range evs {
        fp := subFingerprint(ev.SubID)
        line := determinism.Emission{Fingerprint: fp, EmitSeq: ev.EmitSeq.CommitIndex, Event: "DATA", Fields: map[string]string{"delta": string(ev.Delta)}}
        buf.Write(determinism.CanonicalLine(line))
        buf.WriteByte('\n')
    }
    return buf.Bytes()
}

// transcriptPositional renders a stable transcript using positional sequence (1..N) independent of commit index.
func transcriptPositional(evs []*emission.DataEvent, n int) []byte {
    var buf bytes.Buffer
    limit := n
    if len(evs) < n {
        limit = len(evs)
    }
    seq := uint64(0)
    for _, ev := range evs[:limit] {
        fp := subFingerprint(ev.SubID)
        seq++
        line := determinism.Emission{Fingerprint: fp, EmitSeq: seq, Event: "DATA", Fields: map[string]string{"delta": string(ev.Delta)}}
        buf.Write(determinism.CanonicalLine(line))
        buf.WriteByte('\n')
    }
    return buf.Bytes()
}

func subFingerprint(sub string) uint64 {
    idx := strings.LastIndex(sub, ":")
    if idx < 0 {
        return 0
    }
    v, _ := strconv.ParseUint(sub[idx+1:], 10, 64)
    return v
}

// --- Crash window, before-send ---

func runCrashBeforeSendTranscript(t *testing.T) []byte {
    t.Helper()
    dir := t.TempDir()
    cfg := raft.RaftConfig{ShardID: "crash-before", NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true}
    n, err := raft.NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new raft: %v", err) }
    t.Cleanup(func() { _ = n.Close() })

    mgr := emission.NewManager("crash-before")
    emission.RegisterWithShard(n, mgr)
    ctx, cancel := context.WithCancel(context.Background())
    ap := emission.NewApplier(n, mgr, "crash-before")
    ap.Start(ctx)

    // Hook: cancel before send
    emission.TestHookBeforeSend = func(sub string, seq emission.EmitSeq) { cancel() }
    t.Cleanup(func() { emission.TestHookBeforeSend = nil })

    // Notifier 1 (will crash before send)
    sender1 := &emission.MemorySender{}
    nt1 := emission.NewNotifier(mgr, sender1, &emission.RaftProposer{Node: n, BucketID: "crash-before"}, "crash-before")
    nt1.Start(ctx)

    waitLeader(t, n, 2*time.Second)

    // Propose a single event
    rec, _ := raft.BuildReplicationRecord("crash-before", "DATA_EVENT", []string{"c1:424242", "payload-1"})
    if _, _, err := n.ProposeAndWait(context.Background(), rec); err != nil { t.Fatalf("propose: %v", err) }
    waitPending(t, mgr, 500*time.Millisecond)
    nt1.TestTickOnce(ctx) // should not send due to crash-before-send hook

    if len(sender1.Snapshot()) != 0 {
        t.Fatalf("unexpected sends before restart: %d", len(sender1.Snapshot()))
    }

    // Restart notifier and capture transcript (exactly one send)
    ctx2, cancel2 := context.WithCancel(context.Background())
    t.Cleanup(cancel2)
    sender2 := &emission.MemorySender{}
    nt2 := emission.NewNotifier(mgr, sender2, &emission.RaftProposer{Node: n, BucketID: "crash-before"}, "crash-before")
    nt2.Start(ctx2)
    // Drive deterministically until one send observed
    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if len(sender2.Snapshot()) >= 1 { break }
        nt2.TestTickOnce(ctx2)
        time.Sleep(500 * time.Microsecond)
    }
    if len(sender2.Snapshot()) != 1 { t.Fatalf("want 1 send, got %d", len(sender2.Snapshot())) }
    return transcriptCommitIndex(sender2.Snapshot())
}

func TestDeterminism_Repeat100_CrashBeforeSend(t *testing.T) {
    var base []byte
    for i := 0; i < 100; i++ {
        got := runCrashBeforeSendTranscript(t)
        if i == 0 {
            base = got
        } else if !bytes.Equal(base, got) {
            t.Fatalf("crash-before-send transcript mismatch at run %d", i+1)
        }
    }
}

// --- Crash window, after-send-before-ack ---

func runCrashAfterSendTranscript(t *testing.T) []byte {
    t.Helper()
    dir := t.TempDir()
    cfg := raft.RaftConfig{ShardID: "crash-after", NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true}
    n, err := raft.NewShardRaftNode(cfg)
    if err != nil { t.Fatalf("new raft: %v", err) }
    t.Cleanup(func() { _ = n.Close() })

    mgr := emission.NewManager("crash-after")
    emission.RegisterWithShard(n, mgr)
    ctx, cancel := context.WithCancel(context.Background())
    ap := emission.NewApplier(n, mgr, "crash-after")
    ap.Start(ctx)

    // Hook: cancel after send before ack
    emission.TestHookAfterSendBeforeAck = func(sub string, seq emission.EmitSeq) { cancel() }
    t.Cleanup(func() { emission.TestHookAfterSendBeforeAck = nil })

    sender1 := &emission.MemorySender{}
    nt1 := emission.NewNotifier(mgr, sender1, &emission.RaftProposer{Node: n, BucketID: "crash-after"}, "crash-after")
    nt1.Start(ctx)

    waitLeader(t, n, 2*time.Second)

    rec, _ := raft.BuildReplicationRecord("crash-after", "DATA_EVENT", []string{"c1:424242", "payload-1"})
    if _, _, err := n.ProposeAndWait(context.Background(), rec); err != nil { t.Fatalf("propose: %v", err) }
    waitPending(t, mgr, 500*time.Millisecond)
    nt1.TestTickOnce(ctx) // one send, then crash via hook

    // Restart notifier (no ack yet) should resend
    ctx2, cancel2 := context.WithCancel(context.Background())
    t.Cleanup(cancel2)
    sender2 := &emission.MemorySender{}
    nt2 := emission.NewNotifier(mgr, sender2, &emission.RaftProposer{Node: n, BucketID: "crash-after"}, "crash-after")
    nt2.Start(ctx2)
    nt2.TestTickOnce(ctx2)

    // Combine transcripts: pre-crash send(s) then post-restart send(s)
    combined := append(append([]*emission.DataEvent{}, sender1.Snapshot()...), sender2.Snapshot()...)
    if len(combined) < 2 { t.Fatalf("expected duplicate resend across crash, got %d", len(combined)) }
    return transcriptCommitIndex(combined)
}

func TestDeterminism_Repeat100_CrashAfterSendBeforeAck(t *testing.T) {
    var base []byte
    for i := 0; i < 100; i++ {
        got := runCrashAfterSendTranscript(t)
        if i == 0 {
            base = got
        } else if !bytes.Equal(base, got) {
            t.Fatalf("crash-after-send transcript mismatch at run %d", i+1)
        }
    }
}

// --- Reconnect (OK) determinism ---

func runReconnectOKTranscript(t *testing.T) []byte {
    t.Helper()
    mgr := emission.NewManager("bucket-r-ok-100")
    sender := &emission.MemorySender{}
    n := emission.NewNotifier(mgr, sender, nil, "bucket-r-ok-100")
    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(cancel)
    epoch := emission.EpochID{BucketUUID: "bucket-r-ok-100", EpochCounter: 0}
    oldSub := "old:77"
    for i := uint64(1); i <= 4; i++ {
        mgr.ApplyOutboxWrite(ctx, oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: i}, []byte("x"+strconv.FormatUint(i, 10)))
    }
    mgr.ValidateAck(oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: 2})
    _, newSub, _ := mgr.RebindByFingerprint(77, "new")
    ack := mgr.Reconnect(emission.ReconnectRequest{SubID: newSub, LastProcessedEmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 2}})
    if ack.Status != emission.ReconnectOK || ack.NextCommitIndex != 3 { t.Fatalf("bad ack: %+v", ack) }
    n.SetResumeFrom(newSub, ack.NextCommitIndex)
    n.Start(ctx)
    waitUntil(t, 2*time.Second, func() bool { return len(sender.Snapshot()) >= 2 })
    return transcriptCommitIndex(sender.Snapshot())
}

func TestDeterminism_Repeat100_Reconnect_OK(t *testing.T) {
    var base []byte
    for i := 0; i < 100; i++ {
        got := runReconnectOKTranscript(t)
        if i == 0 {
            base = got
        } else if !bytes.Equal(base, got) {
            t.Fatalf("reconnect(OK) transcript mismatch at run %d", i+1)
        }
    }
}

// --- Multi-replica symmetry (3 nodes) determinism ---

func runMultiReplicaTranscript(t *testing.T) []byte {
    t.Helper()
    start := time.Unix(0, 0)
    tempRoot := t.TempDir()
    shardID := "sym-3n-100"
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
        ap := emission.NewApplier(n, mgr, shardID)
        ap.Start(ctx)
        sender := &emission.MemorySender{}
        nt := emission.NewNotifier(mgr, nil, &emission.RaftProposer{Node: n, BucketID: shardID}, shardID)
        nt.Start(ctx)
        nodes = append(nodes, n)
        notifiers = append(notifiers, nt)
        senders = append(senders, sender)
        clocks = append(clocks, clk)
    }

    // drive elections deterministically
    var leader *raft.ShardRaftNode
    for i := 0; i < 1000 && leader == nil; i++ {
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        time.Sleep(200 * time.Microsecond)
        for _, n := range nodes { if n.Status().IsLeader { leader = n; break } }
    }
    if leader == nil { t.Fatalf("no leader elected") }

    // enable sender only on leader
    for i, n := range nodes {
        if n == leader { notifiers[i].SetSender(senders[i]) } else { notifiers[i].SetSender(nil) }
    }

    // fixed workload
    sub := "c1:424242"
    const N = 10
    for i := 0; i < N; i++ {
        rec, _ := raft.BuildReplicationRecord(shardID, "DATA_EVENT", []string{sub, "val-"+strconv.Itoa(i)})
        // propose on current leader (retry if transient not-leader)
        proposeOnLeader(t, nodes, rec)
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        // drive leader notifier once
        for i2, n := range nodes { if n == leader { notifiers[i2].TestTickOnce(context.Background()) } }
    }

    // drain until N deliveries observed on leader
    deadline := time.Now().Add(3 * time.Second)
    var got int
    for time.Now().Before(deadline) {
        for i, n := range nodes { if n == leader { got = len(senders[i].Snapshot()) } }
        if got >= N { break }
        for _, c := range clocks { c.Advance(10 * time.Millisecond) }
        for i2, n := range nodes { if n == leader { notifiers[i2].TestTickOnce(context.Background()) } }
    }
    if got < N { t.Fatalf("leader delivered %d, want %d", got, N) }

    // build positional transcript for first N events
    for i, n := range nodes { if n == leader { return transcriptPositional(senders[i].Snapshot(), N) } }
    t.Fatalf("internal: leader not found in nodes slice")
    return nil
}

func TestDeterminism_Repeat100_MultiReplicaSymmetry_3Nodes(t *testing.T) {
    var base []byte
    for i := 0; i < 100; i++ {
        got := runMultiReplicaTranscript(t)
        if i == 0 {
            base = got
        } else if !bytes.Equal(base, got) {
            t.Fatalf("multi-replica transcript mismatch at run %d", i+1)
        }
    }
}

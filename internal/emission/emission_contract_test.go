package emission

import (
    "context"
    "testing"
    "time"
)

// waitUntil polls fn until it returns true or timeout elapses.
func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if fn() { return }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatalf("timeout waiting for condition")
}

func TestOutboxSendAndAck(t *testing.T) {
    mgr := NewManager()
    sender := &MemorySender{}
    n := NewNotifier(mgr, sender, nil)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    n.Start(ctx)

    epoch := EpochID{BucketUUID: "a", EpochCounter: 0}
    // Apply two outbox writes
    mgr.ApplyOutboxWrite(ctx, "c1:1", EmitSeq{Epoch: epoch, CommitIndex: 1}, []byte("d1"))
    mgr.ApplyOutboxWrite(ctx, "c1:1", EmitSeq{Epoch: epoch, CommitIndex: 2}, []byte("d2"))

    // Expect two sends in order
    waitUntil(t, 2*time.Second, func() bool { return len(sender.Snapshot()) >= 2 })
    evs := sender.Snapshot()
    if evs[0].EmitSeq.CommitIndex != 1 || evs[1].EmitSeq.CommitIndex != 2 {
        t.Fatalf("unexpected emit order: %+v", []uint64{evs[0].EmitSeq.CommitIndex, evs[1].EmitSeq.CommitIndex})
    }

    // Ack first event (should leave second pending)
    n.Ack(&ClientAck{SubID: "c1:1", EmitSeq: EmitSeq{Epoch: epoch, CommitIndex: 1}})
    waitUntil(t, time.Second, func() bool { return len(mgr.Pending("c1:1")) == 1 })
    if got := mgr.Pending("c1:1")[0].Seq.CommitIndex; got != 2 {
        t.Fatalf("expected only commit 2 pending, got %d", got)
    }

    // Ack second event (outbox empty)
    n.Ack(&ClientAck{SubID: "c1:1", EmitSeq: EmitSeq{Epoch: epoch, CommitIndex: 2}})
    waitUntil(t, time.Second, func() bool { return len(mgr.Pending("c1:1")) == 0 })
}

func TestReconnectSemantics(t *testing.T) {
    mgr := NewManager()
    // Mark compaction through index 5 for sub
    mgr.SetCompactedThrough("s1", 5)
    // Record last ack 10 via ValidateAck path
    if !mgr.ValidateAck("s1", EmitSeq{Epoch: EpochID{BucketUUID: "a"}, CommitIndex: 10}) {
        t.Fatal("unexpected ack regression")
    }

    // Stale (client behind compaction)
    st := mgr.Reconnect(ReconnectRequest{SubID: "s1", LastProcessedEmitSeq: EmitSeq{Epoch: EpochID{BucketUUID: "a"}, CommitIndex: 3}})
    if st.Status != ReconnectStaleSequence || st.NextCommitIndex != 5 {
        t.Fatalf("expected STALE->5, got %+v", st)
    }

    // OK (client at last ack)
    ok := mgr.Reconnect(ReconnectRequest{SubID: "s1", LastProcessedEmitSeq: EmitSeq{Epoch: EpochID{BucketUUID: "a"}, CommitIndex: 10}})
    if ok.Status != ReconnectOK || ok.NextCommitIndex != 11 {
        t.Fatalf("expected OK->11, got %+v", ok)
    }
}

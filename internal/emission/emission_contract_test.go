package emission_test

import (
	"context"
	"testing"
	"time"

	"github.com/sevenDatabase/SevenDB/internal/emission"
)

// waitUntil polls fn until it returns true or timeout elapses.
func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for condition")
}

func TestOutboxSendAndAck(t *testing.T) {
	mgr := emission.NewManager("a")
	sender := &emission.MemorySender{}
	n := emission.NewNotifier(mgr, sender, nil, "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	epoch := emission.EpochID{BucketUUID: "a", EpochCounter: 0}
	// Apply two outbox writes
	mgr.ApplyOutboxWrite(ctx, "c1:1", emission.EmitSeq{Epoch: epoch, CommitIndex: 1}, []byte("d1"))
	mgr.ApplyOutboxWrite(ctx, "c1:1", emission.EmitSeq{Epoch: epoch, CommitIndex: 2}, []byte("d2"))

	// Expect two sends in order; drive deterministically
	for i := 0; i < 200; i++ { if len(sender.Snapshot()) >= 2 { break }; n.TestTickOnce(ctx) }
	evs := sender.Snapshot()
	if evs[0].EmitSeq.CommitIndex != 1 || evs[1].EmitSeq.CommitIndex != 2 {
		t.Fatalf("unexpected emit order: %+v", []uint64{evs[0].EmitSeq.CommitIndex, evs[1].EmitSeq.CommitIndex})
	}

	// Ack first event (should leave second pending)
	n.Ack(&emission.ClientAck{SubID: "c1:1", EmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 1}})
	// Process acks deterministically
	n.TestProcessAcks(ctx)
	if len(mgr.Pending("c1:1")) != 1 {
		t.Fatalf("expected exactly one pending after first ack")
	}
	if got := mgr.Pending("c1:1")[0].Seq.CommitIndex; got != 2 {
		t.Fatalf("expected only commit 2 pending, got %d", got)
	}

	// Ack second event (outbox empty)
	n.Ack(&emission.ClientAck{SubID: "c1:1", EmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 2}})
	n.TestProcessAcks(ctx)
	if len(mgr.Pending("c1:1")) != 0 {
		t.Fatalf("expected no pending after second ack")
	}
}

func TestReconnectSemantics(t *testing.T) {
	mgr := emission.NewManager("a")
	// Mark compaction through index 5 for sub
	mgr.SetCompactedThrough("s1", 5)
	// Record last ack 10 via ValidateAck path
	if !mgr.ValidateAck("s1", emission.EmitSeq{Epoch: emission.EpochID{BucketUUID: "a"}, CommitIndex: 10}) {
		t.Fatal("unexpected ack regression")
	}

	// Stale (client behind compaction)
	st := mgr.Reconnect(emission.ReconnectRequest{SubID: "s1", LastProcessedEmitSeq: emission.EmitSeq{Epoch: emission.EpochID{BucketUUID: "a"}, CommitIndex: 3}})
	if st.Status != emission.ReconnectStaleSequence || st.NextCommitIndex != 5 {
		t.Fatalf("expected STALE->5, got %+v", st)
	}

	// OK (client at last ack)
	ok := mgr.Reconnect(emission.ReconnectRequest{SubID: "s1", LastProcessedEmitSeq: emission.EmitSeq{Epoch: emission.EpochID{BucketUUID: "a"}, CommitIndex: 10}})
	if ok.Status != emission.ReconnectOK || ok.NextCommitIndex != 11 {
		t.Fatalf("expected OK->11, got %+v", ok)
	}
}

package emission

import (
	"context"
	"testing"
)

func TestRebindByFingerprint_MovesEntriesAndWatermarks(t *testing.T) {
	mgr := NewManager("bucketA")
	ctx := context.Background()

	// Old subscription has two pending entries and watermarks
	oldSub := "oldClient:42"
	epoch := EpochID{BucketUUID: "bucketA", EpochCounter: 0}
	mgr.ApplyOutboxWrite(ctx, oldSub, EmitSeq{Epoch: epoch, CommitIndex: 10}, []byte("d10"))
	mgr.ApplyOutboxWrite(ctx, oldSub, EmitSeq{Epoch: epoch, CommitIndex: 11}, []byte("d11"))
	// Set lastAck and compactThrough for old sub
	if !mgr.ValidateAck(oldSub, EmitSeq{Epoch: epoch, CommitIndex: 10}) {
		t.Fatalf("unexpected ack regression")
	}
	mgr.SetCompactedThrough(oldSub, EmitSeq{Epoch: epoch, CommitIndex: 7})

	// Rebind to a new client for the same fingerprint
	from, to, moved := mgr.RebindByFingerprint(42, "", "newClient")
	if from != oldSub {
		t.Fatalf("expected from=%s, got %s", oldSub, from)
	}
	if to != "newClient:42" {
		t.Fatalf("expected to=newClient:42, got %s", to)
	}
	if moved != 2 {
		t.Fatalf("expected moved=2, got %d", moved)
	}

	// Pending should now be under new sub and ordered
	pend := mgr.Pending(to)
	if len(pend) != 2 || pend[0].Seq.CommitIndex != 10 || pend[1].Seq.CommitIndex != 11 {
		t.Fatalf("unexpected pending after rebind: %+v", pend)
	}
	if mgr.Pending(from) != nil {
		t.Fatalf("old sub should have no pending after rebind")
	}

	// Watermarks should migrate conservatively
	// lastAck for new sub should be at least 10
	// compactThrough for new sub should be at least 7
	// (validated indirectly via Reconnect)
	st := mgr.Reconnect(ReconnectRequest{SubID: to, LastProcessedEmitSeq: EmitSeq{Epoch: epoch, CommitIndex: 10}})
	if st.Status != ReconnectOK || st.NextCommitIndex != 11 {
		t.Fatalf("expected OK->11 after rebind, got %+v", st)
	}
	stale := mgr.Reconnect(ReconnectRequest{SubID: to, LastProcessedEmitSeq: EmitSeq{Epoch: epoch, CommitIndex: 6}})
	if stale.Status != ReconnectStaleSequence || stale.NextCommitIndex != 7 {
		t.Fatalf("expected STALE->7 after rebind, got %+v", stale)
	}
}

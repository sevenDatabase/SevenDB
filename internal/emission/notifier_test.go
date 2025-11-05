package emission

import (
	"context"
	"errors"
	"testing"
	"time"
)

// flakySender allows returning preset errors for first N sends, then succeeds and records events.
type flakySender struct {
	errorsLeft int
	errCount   int
	calls      int
	delivered  []*DataEvent
}

func (f *flakySender) Send(ctx context.Context, ev *DataEvent) error {
	f.calls++
	if f.errorsLeft > 0 {
		f.errorsLeft--
		f.errCount++
		return errors.New("raft: not leader")
	}
	// record a copy
	cp := *ev
	f.delivered = append(f.delivered, &cp)
	return nil
}

func waitUntil(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for condition")
}

func TestNotifier_NoResendBeforeAck(t *testing.T) {
	mgr := NewManager("b1")
	n := NewNotifier(mgr, &MemorySender{}, nil, "b1")
	ctx := context.Background()

	epoch := EpochID{BucketUUID: "b1"}
	sub := "c1:100"
	mgr.ApplyOutboxWrite(ctx, sub, EmitSeq{Epoch: epoch, CommitIndex: 1}, []byte("v1"))
	// Drive one deterministic tick: expect exactly one send
	n.processTick(ctx)
	ms := n.getSender().(*MemorySender)
	if got := len(ms.Snapshot()); got != 1 {
		t.Fatalf("expected one send, got %d", got)
	}
	// Another tick should not resend before ACK
	n.processTick(ctx)
	if got := len(ms.Snapshot()); got != 1 {
		t.Fatalf("expected no resend before ACK, got %d total", got)
	}

	// Now ack and process ACKs deterministically; outbox should empty
	n.Ack(&ClientAck{SubID: sub, EmitSeq: EmitSeq{Epoch: epoch, CommitIndex: 1}})
	n.processAcks(ctx)
	if len(mgr.Pending(sub)) != 0 {
		t.Fatalf("expected outbox empty after ACK")
	}
}

func TestNotifier_DefersOnTransientErrorsThenRetries(t *testing.T) {
	mgr := NewManager("b2")
	fs := &flakySender{errorsLeft: 1}
	n := NewNotifier(mgr, fs, nil, "b2")
	ctx := context.Background()
	// No background loop; drive ticks manually

	epoch := EpochID{BucketUUID: "b2"}
	sub := "c9:77"
	mgr.ApplyOutboxWrite(ctx, sub, EmitSeq{Epoch: epoch, CommitIndex: 5}, []byte("d5"))

	// First deterministic tick should defer on error
	n.processTick(ctx)
	if fs.errCount != 1 || len(fs.delivered) != 0 {
		t.Fatalf("expected deferral with no delivery on first tick; errs=%d delivered=%d", fs.errCount, len(fs.delivered))
	}
	// Second tick should succeed
	n.processTick(ctx)
	if len(fs.delivered) != 1 {
		t.Fatalf("expected one delivery after retry, got %d", len(fs.delivered))
	}
	if fs.delivered[0].EmitSeq.CommitIndex != 5 {
		t.Fatalf("unexpected commit index delivered: %d", fs.delivered[0].EmitSeq.CommitIndex)
	}
}

func TestNotifier_ResumeFromSkipsOlderEntries(t *testing.T) {
	mgr := NewManager("b3")
	ms := &MemorySender{}
	n := NewNotifier(mgr, ms, nil, "b3")
	ctx := context.Background()
	// deterministically drive

	epoch := EpochID{BucketUUID: "b3"}
	sub := "cX:9"
	mgr.ApplyOutboxWrite(ctx, sub, EmitSeq{Epoch: epoch, CommitIndex: 1}, []byte("d1"))
	mgr.ApplyOutboxWrite(ctx, sub, EmitSeq{Epoch: epoch, CommitIndex: 2}, []byte("d2"))

	// Request resume from index 2 so entry 1 is skipped
	n.SetResumeFrom(sub, 2)
	n.processTick(ctx)
	evs := ms.Snapshot()
	if evs[0].EmitSeq.CommitIndex != 2 {
		t.Fatalf("expected first delivered to be index 2, got %d", evs[0].EmitSeq.CommitIndex)
	}
}

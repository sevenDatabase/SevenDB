package emission_test

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/sevenDatabase/SevenDB/internal/determinism"
	"github.com/sevenDatabase/SevenDB/internal/emission"
)

func subFP(sub string) uint64 {
	idx := strings.LastIndex(sub, ":")
	if idx < 0 {
		return 0
	}
	v, _ := strconv.ParseUint(sub[idx+1:], 10, 64)
	return v
}

func reconHash(events []*emission.DataEvent) string {
	h := xxhash.New()
	for _, ev := range events {
		e := determinism.Emission{
			Fingerprint: subFP(ev.SubID),
			EmitSeq:     ev.EmitSeq.CommitIndex,
			Event:       "DATA",
			Fields:      map[string]string{"delta": string(ev.Delta)},
		}
		h.Write(determinism.CanonicalLine(e))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestEmission_Reconnect_OK_ResumesFromNextIndex(t *testing.T) {
	mgr := emission.NewManager("bucket-r")
	sender := &emission.MemorySender{}
	n := emission.NewNotifier(mgr, sender, nil, "bucket-r")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build outbox for old sub with indices 1..5
	epoch := emission.EpochID{BucketUUID: "bucket-r", EpochCounter: 0}
	oldSub := "old:42"
	for i := uint64(1); i <= 5; i++ {
		mgr.ApplyOutboxWrite(ctx, oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: i}, []byte("d"+strconv.FormatUint(i, 10)))
	}
	// Client previously acked up to 3
	mgr.ValidateAck(oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: 3})

	// Rebind to new client id for same fingerprint
	_, newSub, _ := mgr.RebindByFingerprint(42, "new")

	// Reconnect request with last processed 3 (valid)
	ack := mgr.Reconnect(emission.ReconnectRequest{SubID: newSub, LastProcessedEmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 3}})
	if ack.Status != emission.ReconnectOK || ack.NextCommitIndex != 4 {
		t.Fatalf("expected OK next=4, got %+v", ack)
	}

	// Resume from next index
	n.SetResumeFrom(newSub, ack.NextCommitIndex)
	// Drive deterministically until two sends are observed
	for i := 0; i < 200; i++ {
		if len(sender.Snapshot()) >= 2 { break }
		n.TestTickOnce(ctx)
	}
	// Expect deliveries of 4 and 5 only
	evs := sender.Snapshot()
	if len(evs) != 2 || evs[0].EmitSeq.CommitIndex != 4 || evs[1].EmitSeq.CommitIndex != 5 {
		t.Fatalf("unexpected resume stream: got %v entries with heads %v,%v", len(evs),
			func() any {
				if len(evs) > 0 {
					return evs[0].EmitSeq.CommitIndex
				}
				return nil
			}(),
			func() any {
				if len(evs) > 1 {
					return evs[1].EmitSeq.CommitIndex
				}
				return nil
			}())
	}
	_ = reconHash(evs) // compute to ensure canonicalization path stays stable
}

func runReconnectOKHash(t *testing.T) string {
	t.Helper()
	mgr := emission.NewManager("bucket-r-ok")
	sender := &emission.MemorySender{}
	n := emission.NewNotifier(mgr, sender, nil, "bucket-r-ok")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	epoch := emission.EpochID{BucketUUID: "bucket-r-ok", EpochCounter: 0}
	oldSub := "old:77"
	for i := uint64(1); i <= 4; i++ {
		mgr.ApplyOutboxWrite(ctx, oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: i}, []byte("x"+strconv.FormatUint(i, 10)))
	}
	mgr.ValidateAck(oldSub, emission.EmitSeq{Epoch: epoch, CommitIndex: 2})
	_, newSub, _ := mgr.RebindByFingerprint(77, "new")
	ack := mgr.Reconnect(emission.ReconnectRequest{SubID: newSub, LastProcessedEmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 2}})
	if ack.Status != emission.ReconnectOK || ack.NextCommitIndex != 3 {
		t.Fatalf("bad ack: %+v", ack)
	}
	n.SetResumeFrom(newSub, ack.NextCommitIndex)
	// Deterministically drive until two events delivered
	for i := 0; i < 200; i++ { if len(sender.Snapshot()) >= 2 { break }; n.TestTickOnce(ctx) }
	return reconHash(sender.Snapshot())
}

func TestEmission_Reconnect_OK_TimelineStable(t *testing.T) {
	h1 := runReconnectOKHash(t)
	h2 := runReconnectOKHash(t)
	if h1 != h2 {
		t.Fatalf("reconnect OK timeline hash mismatch: %s vs %s", h1, h2)
	}
}

func TestEmission_Reconnect_Stale_ResumesFromCompactedIndex(t *testing.T) {
	mgr := emission.NewManager("bucket-r2")
	sender := &emission.MemorySender{}
	n := emission.NewNotifier(mgr, sender, nil, "bucket-r2")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	epoch := emission.EpochID{BucketUUID: "bucket-r2", EpochCounter: 0}
	sub := "old:7"
	// Build entries 1..6; mark compaction through 4
	for i := uint64(1); i <= 6; i++ {
		// At the compacted index, simulate a snapshot payload to model reconstructor path
		payload := []byte("d" + strconv.FormatUint(i, 10))
		if i == 4 {
			payload = []byte("SNAPSHOT@4")
		}
		mgr.ApplyOutboxWrite(ctx, sub, emission.EmitSeq{Epoch: epoch, CommitIndex: i}, payload)
	}
	mgr.SetCompactedThrough(sub, 4)
	// Rebind and reconnect with stale position 2
	_, newSub, _ := mgr.RebindByFingerprint(7, "cli")
	ack := mgr.Reconnect(emission.ReconnectRequest{SubID: newSub, LastProcessedEmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 2}})
	if ack.Status != emission.ReconnectStaleSequence || ack.NextCommitIndex != 4 {
		t.Fatalf("expected STALE next=4, got %+v", ack)
	}

	n.SetResumeFrom(newSub, ack.NextCommitIndex)
	// Expect deliveries starting at 4 (snapshot) then 5,6; drive deterministically
	for i := 0; i < 300; i++ {
		if len(sender.Snapshot()) >= 3 { break }
		n.TestTickOnce(ctx)
	}
	evs := sender.Snapshot()
	if len(evs) < 3 || evs[0].EmitSeq.CommitIndex != 4 || string(evs[0].Delta) != "SNAPSHOT@4" {
		t.Fatalf("expected snapshot@4 first, got len=%d head=(%d,%s)", len(evs), evs[0].EmitSeq.CommitIndex, string(evs[0].Delta))
	}
	_ = reconHash(evs)
}

func TestEmission_Reconnect_InvalidSequence_Future(t *testing.T) {
	mgr := emission.NewManager("bucket-r3")
	ctx := context.Background()
	epoch := emission.EpochID{BucketUUID: "bucket-r3", EpochCounter: 0}
	sub := "s:9"
	// last ack is 5
	if !mgr.ValidateAck(sub, emission.EmitSeq{Epoch: epoch, CommitIndex: 5}) {
		t.Fatal("setup ack regression")
	}
	// Rebind
	_, newSub, _ := mgr.RebindByFingerprint(9, "cli")
	// Client claims it processed 10 (ahead of last+1)
	ack := mgr.Reconnect(emission.ReconnectRequest{SubID: newSub, LastProcessedEmitSeq: emission.EmitSeq{Epoch: epoch, CommitIndex: 10}})
	if ack.Status != emission.ReconnectInvalidSequence || ack.NextCommitIndex != 6 {
		t.Fatalf("expected INVALID next=6, got %+v", ack)
	}
	// No notifier started; just assert the status path here.
	_ = ctx
}

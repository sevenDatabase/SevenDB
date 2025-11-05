package emission_test

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/sevenDatabase/SevenDB/internal/determinism"
	"github.com/sevenDatabase/SevenDB/internal/emission"
	"github.com/sevenDatabase/SevenDB/internal/raft"
)

// proposeDataEvent builds and proposes a DATA_EVENT record.
func proposeDataEvent(t *testing.T, n *raft.ShardRaftNode, bucket, sub, delta string) {
	t.Helper()
	rec, err := raft.BuildReplicationRecord(bucket, "DATA_EVENT", []string{sub, delta})
	if err != nil {
		t.Fatalf("build rec: %v", err)
	}
	_, _, err = n.ProposeAndWait(context.Background(), rec)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
}

// runOnce sets up a single-node deterministic raft with emission wiring and returns a hash of emitted stream.
func runOnce(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfg := raft.RaftConfig{ShardID: "bucket-sym", NodeID: "1", Engine: "etcd", DataDir: dir, ForwardProposals: true, DisablePersistence: true, Manual: false, HeartbeatMillis: 50, ElectionTimeoutMillis: 300}
	n, err := raft.NewShardRaftNode(cfg)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	// Wait for background raft to elect a leader
	for i := 0; i < 200 && !n.IsLeader(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !n.Status().IsLeader {
		t.Fatalf("node did not become leader")
	}

	// Wire emission manager, applier, notifier with memory sender
	mgr := emission.NewManager("bucket-sym")
	emission.RegisterWithShard(n, mgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ap := emission.NewApplier(n, mgr, "bucket-sym")
	ap.Start(ctx)
	sender := &emission.MemorySender{}
	nt := emission.NewNotifier(mgr, sender, &emission.RaftProposer{Node: n, BucketID: "bucket-sym"}, "bucket-sym")
	// Deterministic progression: do NOT start the background ticker loop.
	// We'll drive sends/acks explicitly via TestTickOnce to avoid time-based races.

	// Fixed workload
	sub := "c1:424242"
	const N = 8
	for i := 0; i < N; i++ {
		proposeDataEvent(t, n, "bucket-sym", sub, "val-"+strconv.Itoa(i))
		// Drive a few deterministic ticks until this entry is observed and sent.
		// We avoid real-time sleeps and the background ticker entirely.
		for j := 0; j < 20; j++ { // up to ~20 cycles should be plenty in tests
			nt.TestTickOnce(ctx)
			if len(sender.Snapshot()) > i {
				break
			}
			// Yield briefly to allow raft apply pipeline to commit the outbox write.
			time.Sleep(500 * time.Microsecond)
		}
	}
	// Final deterministic drain to catch any last application lag
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if len(sender.Snapshot()) >= N {
			break
		}
		nt.TestTickOnce(ctx)
		time.Sleep(500 * time.Microsecond)
	}
	evs := sender.Snapshot()
	if len(evs) < N {
		t.Fatalf("captured %d events, want %d", len(evs), N)
	}

	// Canonical hash of first N events
	h := xxhash.New()
	seq := uint64(0)
	for _, ev := range evs[:N] {
		fp := uint64(0)
		if idx := strings.LastIndex(ev.SubID, ":"); idx >= 0 {
			if v, err := strconv.ParseUint(ev.SubID[idx+1:], 10, 64); err == nil {
				fp = v
			}
		}
		seq++
		e := determinism.Emission{Fingerprint: fp, EmitSeq: seq, Event: "DATA", Fields: map[string]string{"delta": string(ev.Delta)}}
		h.Write(determinism.CanonicalLine(e))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestEmission_MultiReplicaSymmetry_SingleNodeRuns(t *testing.T) {
	h1 := runOnce(t)
	h2 := runOnce(t)
	h3 := runOnce(t)
	if h1 != h2 || h2 != h3 {
		t.Fatalf("hash mismatch across runs: %s, %s, %s", h1, h2, h3)
	}
}

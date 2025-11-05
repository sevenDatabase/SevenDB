package emission

import (
	"context"
	"testing"
	"time"

	raftimpl "github.com/sevenDatabase/SevenDB/internal/raft"
)

func TestApplier_DataEventAndAckFlow(t *testing.T) {
	// Create a stub raft node for a single shard/bucket
	node, err := raftimpl.NewShardRaftNode(raftimpl.RaftConfig{ShardID: "sh1", Engine: "stub"})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer node.Close()

	mgr := NewManager("sh1")
	ap := NewApplier(node, mgr, "sh1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ap.Start(ctx)

	sub := "c1:55"

	// Propose a DATA_EVENT; Applier should in turn propose OUTBOX_WRITE and apply it.
	rec, _ := raftimpl.BuildReplicationRecord("sh1", "DATA_EVENT", []string{sub, "hello"})
	if _, _, err := node.ProposeAndWait(ctx, rec); err != nil {
		t.Fatalf("propose DATA_EVENT: %v", err)
	}

	// Wait for outbox to have the entry with commit index 1
	wait := func(fn func() bool) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if fn() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for condition")
	}
	wait(func() bool { ps := mgr.Pending(sub); return len(ps) == 1 && ps[0].Seq.CommitIndex == 1 })

	// Now propose an ACK for that entry; Applier should propose OUTBOX_PURGE and apply purge
	ackRec, _ := raftimpl.BuildReplicationRecord("sh1", "ACK", []string{sub, "sh1", "0", "1"})
	if _, _, err := node.ProposeAndWait(ctx, ackRec); err != nil {
		t.Fatalf("propose ACK: %v", err)
	}
	wait(func() bool { return len(mgr.Pending(sub)) == 0 })
}

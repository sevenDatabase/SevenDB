package bucket

import (
    "context"
    "testing"
    "time"

    raftimpl "github.com/sevenDatabase/SevenDB/internal/raft"
)

// TestRaftBucketLog_AppendRead ensures that commit indices provided by the
// stub ShardRaftNode are monotonic per bucket and that Read filters by bucket.
func TestRaftBucketLog_AppendRead(t *testing.T) {
    rn, _ := raftimpl.NewShardRaftNode(raftimpl.RaftConfig{ShardID: "s1"})
    rlog := NewRaftBucketLog(rn)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Append 3 entries for bucket A, 2 for bucket B interleaved.
    buckets := []BucketID{"A","B","A","B","A"}
    for i, b := range buckets {
        e := &WALEntry{BucketID: b, Type: RecDataUpdate, Payload: []byte{byte(i)}}
        ci, _, err := rlog.Append(ctx, e)
        if err != nil { t.Fatalf("append: %v", err) }
        if ci == 0 { t.Fatalf("expected non-zero commit index") }
    }
    // Consume for bucket A only
    chA, _ := rlog.Read(ctx, "A", 1)
    var seenA []uint64
loop:
    for {
        select {
        case e, ok := <-chA:
            if !ok { break loop }
            seenA = append(seenA, e.CommitIndex)
            if len(seenA) == 3 { break loop }
        case <-time.After(2 * time.Second):
            t.Fatalf("timeout waiting for bucket A entries")
        }
    }
    if len(seenA) != 3 { t.Fatalf("expected 3 entries for A, got %d", len(seenA)) }
    for i := 1; i <= 3; i++ { if seenA[i-1] != uint64(i) { t.Fatalf("expected commit index %d at position %d, got %d", i, i-1, seenA[i-1]) } }
}

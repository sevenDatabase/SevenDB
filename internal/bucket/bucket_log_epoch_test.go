package bucket

import (
    "context"
    "testing"
    "time"
)

// Verifies that epoch counter is always stamped from internal state (currently 0) and
// file offsets are strictly increasing for a bucket.
func TestFileBucketLog_EpochAndOffsetMonotonic(t *testing.T) {
    setTestWalDir(t)
    fbl, err := NewFileBucketLog()
    if err != nil { t.Fatalf("new file bucket log: %v", err) }
    defer fbl.Close()
    b := BucketID("epoch1")
    ctx := context.Background()
    var lastOffset uint64
    for i:=0;i<5;i++ {
        ci, off, err := fbl.Append(ctx, &WALEntry{BucketID:b, Type:RecDataUpdate})
        if err != nil { t.Fatalf("append: %v", err) }
        if ci != uint64(i+1) { t.Fatalf("commit index expected %d got %d", i+1, ci) }
        if off < lastOffset { t.Fatalf("file offset not monotonic: %d < %d", off, lastOffset) }
        lastOffset = off
    }
    ch, err := fbl.Read(ctx, b, 1)
    if err != nil { t.Fatalf("read: %v", err) }
    for e := range ch {
        if e.Epoch.Counter != 0 { t.Fatalf("unexpected epoch counter %d", e.Epoch.Counter) }
        if e.Epoch.Bucket != b { t.Fatalf("epoch bucket mismatch %s != %s", e.Epoch.Bucket, b) }
    }
}

// Smoke test that exported Run functions.
func TestBucket_RunExported(t *testing.T) {
    setTestWalDir(t)
    fbl, _ := NewFileBucketLog(); defer fbl.Close()
    b := NewBucket("runexp", fbl)
    ctx, cancel := context.WithCancel(context.Background())
    go b.Run(ctx)
    for i:=0;i<3;i++ { _,_,_ = fbl.Append(ctx,&WALEntry{BucketID:b.ID,Type:RecDataUpdate}) }
    time.Sleep(100*time.Millisecond)
    if b.lastApplied != 3 { t.Fatalf("expected lastApplied=3 got %d", b.lastApplied) }
    cancel()
    time.Sleep(20*time.Millisecond)
}

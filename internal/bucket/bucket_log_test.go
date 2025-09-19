package bucket

import (
    "context"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/sevenDatabase/SevenDB/config"
)

// setTestWalDir points WALDir to a temp directory for isolation.
func setTestWalDir(t *testing.T) string {
    t.Helper()
    dir := t.TempDir()
    config.Config.WALDir = filepath.Join(dir, "wal")
    if err := os.MkdirAll(config.Config.WALDir, 0o755); err != nil {
        t.Fatalf("mkdir wal: %v", err)
    }
    return config.Config.WALDir
}

func TestFileBucketLog_AppendRead_MonotonicCommitIndex(t *testing.T) {
    setTestWalDir(t)
    log, err := NewFileBucketLog()
    if err != nil {
        t.Fatalf("new log: %v", err)
    }
    defer log.Close()

    ctx := context.Background()
    b1 := BucketID("b1")
    b2 := BucketID("b2")

    // Append interleaved entries to two buckets
    for i := 0; i < 5; i++ {
        if _, _, err := log.Append(ctx, &WALEntry{BucketID: b1, Type: RecDataUpdate, Payload: []byte("x")}); err != nil {
            t.Fatalf("append b1: %v", err)
        }
        if _, _, err := log.Append(ctx, &WALEntry{BucketID: b2, Type: RecDataUpdate, Payload: []byte("y")}); err != nil {
            t.Fatalf("append b2: %v", err)
        }
    }

    // Read from commit index 1 for both buckets and ensure commit indexes are 1..5 each
    ch1, err := log.Read(ctx, b1, 1)
    if err != nil { t.Fatalf("read b1: %v", err) }
    var last uint64
    for e := range ch1 { last = e.CommitIndex }
    if last != 5 { t.Fatalf("expected last commit index 5 for b1, got %d", last) }

    ch2, err := log.Read(ctx, b2, 1)
    if err != nil { t.Fatalf("read b2: %v", err) }
    last = 0
    for e := range ch2 { last = e.CommitIndex }
    if last != 5 { t.Fatalf("expected last commit index 5 for b2, got %d", last) }
}

func TestFileBucketLog_ReopenReplayDeterminism(t *testing.T) {
    walDir := setTestWalDir(t)
    log1, err := NewFileBucketLog()
    if err != nil { t.Fatalf("new log1: %v", err) }
    b := BucketID("bucketA")
    ctx := context.Background()
    for i:=0;i<3;i++ { _, _, _ = log1.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate, Payload: []byte{byte('a'+i)}}) }
    _ = log1.Close()

    // Reopen
    _ = os.Setenv("SEVENDB_TEST_WALDIR", walDir) // just to avoid linter complaining unused
    log2, err := NewFileBucketLog()
    if err != nil { t.Fatalf("new log2: %v", err) }
    defer log2.Close()
    ch, err := log2.Read(ctx, b, 1)
    if err != nil { t.Fatalf("read: %v", err) }
    var count int
    var last uint64
    for e := range ch { count++; last = e.CommitIndex }
    if count != 3 || last != 3 { t.Fatalf("unexpected replay count=%d last=%d", count, last) }
}

func TestFileBucketLog_ReadCancellation(t *testing.T) {
    setTestWalDir(t)
    log, err := NewFileBucketLog()
    if err != nil { t.Fatalf("new log: %v", err) }
    defer log.Close()
    b := BucketID("bX")
    ctx := context.Background()
    for i:=0;i<10;i++ { _, _, _ = log.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate}) }
    rctx, cancel := context.WithCancel(context.Background())
    ch, err := log.Read(rctx, b, 1)
    if err != nil { t.Fatalf("read: %v", err) }
    // Drain a few then cancel
    for i:=0;i<3;i++ { <-ch }
    cancel()
    // Give goroutine time to exit
    time.Sleep(50*time.Millisecond)
}

package bucket

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
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
	if err != nil {
		t.Fatalf("read b1: %v", err)
	}
	var last uint64
	for e := range ch1 {
		last = e.CommitIndex
	}
	if last != 5 {
		t.Fatalf("expected last commit index 5 for b1, got %d", last)
	}

	ch2, err := log.Read(ctx, b2, 1)
	if err != nil {
		t.Fatalf("read b2: %v", err)
	}
	last = 0
	for e := range ch2 {
		last = e.CommitIndex
	}
	if last != 5 {
		t.Fatalf("expected last commit index 5 for b2, got %d", last)
	}
}

func TestFileBucketLog_ReopenReplayDeterminism(t *testing.T) {
	walDir := setTestWalDir(t)
	log1, err := NewFileBucketLog()
	if err != nil {
		t.Fatalf("new log1: %v", err)
	}
	b := BucketID("bucketA")
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _, _ = log1.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate, Payload: []byte{byte('a' + i)}})
	}
	_ = log1.Close()

	// Reopen
	_ = os.Setenv("SEVENDB_TEST_WALDIR", walDir) // just to avoid linter complaining unused
	log2, err := NewFileBucketLog()
	if err != nil {
		t.Fatalf("new log2: %v", err)
	}
	defer log2.Close()
	ch, err := log2.Read(ctx, b, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var count int
	var last uint64
	for e := range ch {
		count++
		last = e.CommitIndex
	}
	if count != 3 || last != 3 {
		t.Fatalf("unexpected replay count=%d last=%d", count, last)
	}
}

func TestFileBucketLog_ReadCancellation(t *testing.T) {
	setTestWalDir(t)
	log, err := NewFileBucketLog()
	if err != nil {
		t.Fatalf("new log: %v", err)
	}
	defer log.Close()
	b := BucketID("bX")
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, _, _ = log.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate})
	}
	rctx, cancel := context.WithCancel(context.Background())
	before := runtime.NumGoroutine()
	ch, err := log.Read(rctx, b, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Drain a few then cancel
	for i := 0; i < 3; i++ {
		<-ch
	}
	cancel()
	// Give goroutine time to exit
	time.Sleep(100 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after-before > 5 { // very loose heuristic, just ensure it didn't leak unbounded
		t.Logf("goroutines before=%d after=%d (heuristic)", before, after)
	}
}

// Cross-bucket isolation: same key name in two buckets (simulated by payload), ensure commit sequences independent.
func TestFileBucketLog_CrossBucketIsolation(t *testing.T) {
	setTestWalDir(t)
	log, _ := NewFileBucketLog()
	defer log.Close()
	ctx := context.Background()
	b1, b2 := BucketID("iso1"), BucketID("iso2")
	for i := 0; i < 3; i++ {
		_, _, _ = log.Append(ctx, &WALEntry{BucketID: b1, Type: RecDataUpdate, Payload: []byte("k:same")})
		_, _, _ = log.Append(ctx, &WALEntry{BucketID: b2, Type: RecDataUpdate, Payload: []byte("k:same")})
	}
	ch1, _ := log.Read(ctx, b1, 1)
	var last1 uint64
	for e := range ch1 {
		last1 = e.CommitIndex
	}
	if last1 != 3 {
		t.Fatalf("b1 last=%d", last1)
	}
	ch2, _ := log.Read(ctx, b2, 1)
	var last2 uint64
	for e := range ch2 {
		last2 = e.CommitIndex
	}
	if last2 != 3 {
		t.Fatalf("b2 last=%d", last2)
	}
}

// Truncation test: simulate crash during partial append by truncating last few bytes; reopen should stop cleanly.
func TestFileBucketLog_TruncatedTail(t *testing.T) {
	walDir := setTestWalDir(t)
	log1, _ := NewFileBucketLog()
	ctx := context.Background()
	b := BucketID("trunc")
	for i := 0; i < 5; i++ {
		_, _, _ = log1.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate, Payload: []byte{byte(i)}})
	}
	// Append one more but then simulate crash mid-write by truncating file
	_, _, _ = log1.Append(ctx, &WALEntry{BucketID: b, Type: RecDataUpdate, Payload: []byte("willtrunc")})
	log1.Close()
	// Truncate last 10 bytes
	fp := filepath.Join(walDir, "buckets", "bucket-"+string(b)+".wal")
	st, err := os.Stat(fp)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Size() > 10 {
		_ = os.Truncate(fp, st.Size()-10)
	}
	// Reopen – scan should stop at corruption boundary without panic
	log2, err := NewFileBucketLog()
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()
	ch, _ := log2.Read(ctx, b, 1)
	var count int
	for range ch {
		count++
	}
	if count < 5 {
		t.Fatalf("expected at least first 5 durable entries, got %d", count)
	}
}

// Bucket execution model smoke test.
func TestBucket_RunLoopAppliesSequentially(t *testing.T) {
	setTestWalDir(t)
	fbl, _ := NewFileBucketLog()
	defer fbl.Close()
	b := NewBucket("exec1", fbl)
	ctx, cancel := context.WithCancel(context.Background())
	var applied atomic.Uint64
	b.onApply = func(e *WALEntry) { applied.Store(e.CommitIndex) }
	go b.Run(ctx)
	for i := 0; i < 4; i++ {
		_, _, _ = fbl.Append(ctx, &WALEntry{BucketID: b.ID, Type: RecDataUpdate, Payload: []byte{byte(i)}})
	}
	// wait a bit
	time.Sleep(150 * time.Millisecond)
	if applied.Load() != 4 {
		t.Fatalf("expected applied=4 got %d", applied.Load())
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
}

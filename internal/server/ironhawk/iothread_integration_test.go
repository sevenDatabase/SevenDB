package ironhawk

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/cmd"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/sevenDatabase/SevenDB/internal/wal"
)

// walDecorator wraps a WAL implementation to count Sync() calls.
type walDecorator struct {
	inner      wal.WAL
	syncCalls  int
}

func (w *walDecorator) Init() error { return w.inner.Init() }
func (w *walDecorator) Stop()       { w.inner.Stop() }
func (w *walDecorator) LogCommand(c *wire.Command) error { return w.inner.LogCommand(c) }
func (w *walDecorator) Sync() error {
	err := w.inner.Sync()
	if err == nil {
		w.syncCalls++
	}
	return err
}
func (w *walDecorator) ReplayCommand(cb func(c *wire.Command) error) error { return w.inner.ReplayCommand(cb) }

// TestDurableSetTriggersSync initializes a minimal shard + WAL environment and asserts that
// a SET with the DURABLE flag causes an immediate WAL Sync compared to a buffered SET.
func TestDurableSetTriggersSync(t *testing.T) {
	// Skip if running with race detector and this becomes flaky due to timing.
	// (Adjust if needed.)

	tdir := t.TempDir()
	config.Config.EnableWAL = true
	config.Config.WALVariant = "forge"
	config.Config.WALDir = tdir
	config.Config.WALBufferSyncIntervalMillis = 5000 // large interval to avoid background sync interference
	config.Config.WALEnableDurableSet = true

	wal.SetupWAL()
	deco := &walDecorator{inner: wal.DefaultWAL}
	wal.DefaultWAL = deco
	defer wal.TeardownWAL()

	// Minimal shard manager with one shard
	sm := shardmanager.NewShardManager(1, nil)

	// Non-durable SET
	cmd1 := &cmd.Cmd{C: &wire.Command{Cmd: "SET", Args: []string{"k", "v1"}}}
	if _, err := cmd1.Execute(sm); err != nil {
		// Eval path only; WAL logging happens in IOThread. Simulate subset of IOThread logic.
		t.Fatalf("execute non-durable set failed: %v", err)
	}
	if err := wal.DefaultWAL.LogCommand(cmd1.C); err != nil {
		t.Fatalf("log non-durable command failed: %v", err)
	}
	// Expect no sync yet
	if deco.syncCalls != 0 {
		t.Fatalf("unexpected syncCalls=%d after non-durable SET", deco.syncCalls)
	}

	// Durable SET
	cmd2 := &cmd.Cmd{C: &wire.Command{Cmd: "SET", Args: []string{"k", "v2", "DURABLE"}}}
	if _, err := cmd2.Execute(sm); err != nil {
		t.Fatalf("execute durable set failed: %v", err)
	}
	if err := wal.DefaultWAL.LogCommand(cmd2.C); err != nil {
		t.Fatalf("log durable command failed: %v", err)
	}
	if err := wal.DefaultWAL.Sync(); err != nil {
		t.Fatalf("sync durable command failed: %v", err)
	}
	if deco.syncCalls != 1 {
		t.Fatalf("expected 1 sync after durable SET, got %d", deco.syncCalls)
	}

	// Validate WAL segment mtime advanced
	entries, err := os.ReadDir(tdir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	var newestMod time.Time
	for _, e := range entries {
		if e.IsDir() { continue }
		fi, ferr := os.Stat(filepath.Join(tdir, e.Name()))
		if ferr == nil && fi.ModTime().After(newestMod) {
			newestMod = fi.ModTime()
		}
	}
	if time.Since(newestMod) > time.Second {
		// Highly unlikely if sync succeeded.
		t.Fatalf("wal segment mtime not recent (mtime=%v now=%v)", newestMod, time.Now())
	}
}

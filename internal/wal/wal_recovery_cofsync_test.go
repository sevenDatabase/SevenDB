package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
)

// TestCrashAfterFlushBeforeFsync_NoHardStatePersisted
// Simulates a crash after buffered data is flushed but before segment fsync.
// Verifies that HardState is NOT persisted (co-fsync invariant: HS never points
// past the last durable WAL entry).
func TestCrashAfterFlushBeforeFsync_NoHardStatePersisted(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	// Configure forge WAL with long sync interval and segment-size rotation to minimize background activity
	old := config.Config
	config.Config.WALVariant = "forge"
	config.Config.WALDir = walDir
	config.Config.WALBufferSizeMB = 1
	config.Config.WALBufferSyncIntervalMillis = 60000
	config.Config.WALRotationMode = "segment-size"
	config.Config.WALMaxSegmentSizeMB = 16
	config.Config.WALAutoCreateManifest = true
	config.Config.WALManifestEnforce = string(EnforceWarn)
	t.Cleanup(func() { config.Config = old })

	wf := newWalForge()
	if err := wf.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Append a normal entry
	cmd := &wire.Command{Cmd: "SET", Args: []string{"k", "v"}}
	if err := wf.AppendEntry(EntryKindNormal, 1, 1, 0, cmd, nil, []byte("rv")); err != nil {
		t.Fatalf("append normal: %v", err)
	}
	// Stage a HardState commit=1 term=1
	hs := []byte{0x08, 0x01, 0x10, 0x00, 0x18, 0x01}
	if err := wf.AppendEntry(EntryKindHardState, 1, 1, 0, nil, hs, nil); err != nil {
		t.Fatalf("append hardstate: %v", err)
	}

	// Inject crash after data flushed but before fsync
	TestHookAfterDataBeforeFsync = func() { panic("inject:after-data-before-fsync") }
	defer func() { TestHookAfterDataBeforeFsync = nil }()

	func() {
		defer func() { _ = recover() }()
		_ = wf.Sync() // triggers panic via hook
	}()

	// Simulate crash: do NOT call Stop() (which would sync and persist HS).
	// Instead, tear down background goroutines without invoking wl.sync().
	if wf.bufferSyncTicker != nil {
		wf.bufferSyncTicker.Stop()
	}
	if wf.segmentRotationTicker != nil {
		wf.segmentRotationTicker.Stop()
	}
	if wf.csf != nil {
		_ = wf.csf.Close()
	}
	if wf.cancel != nil {
		wf.cancel()
	}
	TestHookAfterDataBeforeFsync = nil

	// Verify HardState file does not exist on disk, indicating it wasn't durably persisted
	if _, err := os.Stat(filepath.Join(walDir, "hardstate")); err == nil {
		t.Fatalf("unexpected hardstate file persisted after crash before fsync")
	}

	// Re-open a fresh WAL instance from disk to ensure replay still works
	wf2 := newWalForge()
	if err := wf2.Init(); err != nil {
		t.Fatalf("reinit: %v", err)
	}
	defer wf2.Stop()
}

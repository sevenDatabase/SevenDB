package wal_test

import (
    "runtime/debug"
    "testing"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
    "github.com/sevenDatabase/SevenDB/internal/wal"
)

// TestWAL_MidFsyncCrash_Recovery ensures that if a crash occurs after data is flushed
// but before fsync, replay on restart still sees a consistent WAL (at least the
// flushed entries), and no panic occurs.
func TestWAL_MidFsyncCrash_Recovery(t *testing.T) {
    t.Parallel()
    tmp := t.TempDir()
    // Point WAL to temp dir for isolation
    config.Config.WALDir = tmp
    config.Config.WALVariant = "forge"

    // Initialize WAL
    wal.SetupWAL()
    t.Cleanup(func() {
        if wal.DefaultWAL != nil {
            wal.DefaultWAL.Stop()
        }
    })

    // Log a command to create one frame
    if err := wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"}); err != nil {
        t.Fatalf("log command: %v", err)
    }

    // Inject crash: after data is flushed to file but before fsync
    panicked := false
    wal.TestHookAfterDataBeforeFsync = func() { panic("inject:after-data-before-fsync") }
    defer func() { wal.TestHookAfterDataBeforeFsync = nil }()

    func() {
        defer func() {
            if r := recover(); r != nil {
                _ = r
                _ = debug.Stack()
                panicked = true
            }
        }()
        _ = wal.DefaultWAL.Sync() // expect panic from hook
    }()

    if !panicked {
        t.Fatalf("expected panic from TestHookAfterDataBeforeFsync, got none")
    }

    // Simulate restart by stopping and reinitializing WAL
    if wal.DefaultWAL != nil {
        wal.DefaultWAL.Stop()
    }
    wal.SetupWAL()
    t.Cleanup(func() {
        if wal.DefaultWAL != nil {
            wal.DefaultWAL.Stop()
        }
    })

    // Replay should succeed and deliver exactly one command
    var count int
    if err := wal.DefaultWAL.ReplayCommand(func(c *wire.Command) error {
        count++
        return nil
    }); err != nil {
        t.Fatalf("replay: %v", err)
    }
    if count != 1 {
        t.Fatalf("expected 1 replayed command, got %d", count)
    }
}

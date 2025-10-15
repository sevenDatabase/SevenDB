package wal_test

import (
    "path/filepath"
    "sort"
    "strings"
    "testing"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
    "github.com/sevenDatabase/SevenDB/internal/wal"
)

// TestWAL_PruneThrough_RemovesCompleteSegments verifies that pruning removes
// only fully completed (closed) segments whose last LSN is <= the prune index,
// and never deletes the currently open segment.
func TestWAL_PruneThrough_RemovesCompleteSegments(t *testing.T) {
    tmp := t.TempDir()
    config.Config.WALDir = tmp
    config.Config.WALVariant = "forge"
    // Force rotation on every append by making max segment size 0MB
    config.Config.WALMaxSegmentSizeMB = 0

    wal.SetupWAL()
    t.Cleanup(func() {
        if wal.DefaultWAL != nil {
            wal.DefaultWAL.Stop()
        }
    })

    // Append 3 commands. With 0MB max segment size, each append lands in a new segment.
    for i := 0; i < 3; i++ {
        if err := wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "INCR", Args: []string{"k"}}); err != nil {
            t.Fatalf("log command %d: %v", i, err)
        }
    }

    // Use unified interface to prune through LSN=1 (should remove the segment containing the first entry)
    uw, ok := wal.DefaultWAL.(wal.UnifiedWAL)
    if !ok {
        t.Fatalf("wal does not implement UnifiedWAL")
    }
    if err := uw.PruneThrough(1); err != nil {
        t.Fatalf("prune through: %v", err)
    }

    // List remaining segments and normalize to basenames for assertions
    segs, err := filepath.Glob(filepath.Join(tmp, "seg-*.wal"))
    if err != nil {
        t.Fatalf("glob segments: %v", err)
    }
    bases := make([]string, 0, len(segs))
    for _, s := range segs {
        parts := strings.Split(s, string(filepath.Separator))
        bases = append(bases, parts[len(parts)-1])
    }
    sort.Strings(bases)

    // Expect that seg-1.wal (first entry) is removed. seg-0.wal may still exist (empty initial file),
    // and seg-2.wal or higher should still exist containing later entries.
    for _, banned := range []string{"seg-1.wal"} {
        for _, b := range bases {
            if b == banned {
                t.Fatalf("expected %s to be pruned, but it exists: %v", banned, bases)
            }
        }
    }
    // Ensure at least one later segment remains
    foundLater := false
    for _, b := range bases {
        if b == "seg-2.wal" || b == "seg-3.wal" || b == "seg-4.wal" {
            foundLater = true
            break
        }
    }
    if !foundLater {
        t.Fatalf("expected a later segment to remain, got: %v", bases)
    }
}

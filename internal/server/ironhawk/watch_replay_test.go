package ironhawk

import (
    "path/filepath"
    "strconv"
    "testing"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
    "github.com/sevenDatabase/SevenDB/internal/cmd"
    "github.com/sevenDatabase/SevenDB/internal/wal"
    "strings"
)

// TestWatchSubscribeUnsubscribeReplay ensures SUBSCRIBE/UNSUBSCRIBE events logged by the watch manager
// can be replayed to reconstruct (and remove) subscriptions deterministically.
func TestWatchSubscribeUnsubscribeReplay(t *testing.T) {
    dir := t.TempDir()
    old := config.Config
    config.Config.WALVariant = "forge"
    config.Config.WALDir = filepath.Join(dir, "wal")
    config.Config.WALBufferSizeMB = 1
    config.Config.WALBufferSyncIntervalMillis = 60000
    config.Config.WALRotationMode = "segment-size"
    config.Config.WALMaxSegmentSizeMB = 16
    config.Config.WALAutoCreateManifest = true
    config.Config.WALManifestEnforce = string(wal.EnforceWarn)
    t.Cleanup(func() { config.Config = old })

    // Fresh WAL
    wal.SetupWAL()
    t.Cleanup(func() { wal.TeardownWAL() })

    wm := NewWatchManager()
    io := &IOThread{ClientID: "clientA"}

    // Register a watch (logs SUBSCRIBE + Sync)
    wc := &cmd.Cmd{C: &wire.Command{Cmd: "GET.WATCH", Args: []string{"k"}}}
    if err := wm.HandleWatch(wc, io); err != nil {
        t.Fatalf("handle watch: %v", err)
    }

    // Now UNWATCH using the same fingerprint value used by the manager
    fp := wc.Fingerprint()
    uc := &cmd.Cmd{C: &wire.Command{Cmd: "UNWATCH", Args: []string{strconv.FormatUint(fp, 10)}}}
    if err := wm.HandleUnwatch(uc, io); err != nil {
        t.Fatalf("handle unwatch: %v", err)
    }

    // Restart: reconstruct subscriptions by scanning WAL
    wm2 := NewWatchManager()
    // Replay in-order and rebuild state
    if err := wal.DefaultWAL.ReplayCommand(func(c *wire.Command) error {
        switch c.Cmd {
        case "SUBSCRIBE":
            if len(c.Args) != 3 { // clientID, repr, fp
                return nil
            }
            cid := c.Args[0]
            repr := c.Args[1]
            nfp, _ := strconv.ParseUint(c.Args[2], 10, 64)
            // Recreate minimal cmd.Cmd from stored repr
            parts := strings.Fields(repr)
            wc := &wire.Command{Cmd: parts[0]}
            if len(parts) > 1 {
                wc.Args = parts[1:]
            }
            cm := &cmd.Cmd{C: wc, IsReplay: true}
            // Restore maps
            // We don't have a direct method to set explicit fp mapping; use helper
            _ = wm2.RestoreSubscription(cid, cm.String(), nfp)
        case "UNSUBSCRIBE":
            if len(c.Args) != 2 {
                return nil
            }
            cid := c.Args[0]
            nfp, _ := strconv.ParseUint(c.Args[1], 10, 64)
            _ = wm2.RemoveSubscription(cid, nfp)
        }
        return nil
    }); err != nil {
        t.Fatalf("replay: %v", err)
    }

    // Expect no active mapping for fingerprint after replay (unsubscribed)
    wm2.mu.RLock()
    _, has := wm2.fpClientMap[fp]
    wm2.mu.RUnlock()
    if has {
        t.Fatalf("subscription unexpectedly present after UNSUBSCRIBE replay")
    }
}

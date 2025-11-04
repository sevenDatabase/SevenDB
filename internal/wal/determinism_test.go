package wal

import (
    "path/filepath"
    "testing"

    "github.com/cespare/xxhash/v2"
    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
)

// TestReplayDeterminism_StableHash ensures that replaying the same WAL yields the same
// byte-for-byte sequence of commands and a stable hash across process restarts.
func TestReplayDeterminism_StableHash(t *testing.T) {
    dir := t.TempDir()
    // Configure forge WAL in a temp dir with background activity minimized.
    old := config.Config
    config.Config.WALVariant = "forge"
    config.Config.WALDir = filepath.Join(dir, "wal")
    config.Config.WALBufferSizeMB = 1
    config.Config.WALBufferSyncIntervalMillis = 60000 // long sync to avoid flusher races
    config.Config.WALRotationMode = "segment-size"
    config.Config.WALMaxSegmentSizeMB = 16
    config.Config.WALAutoCreateManifest = true
    config.Config.WALManifestEnforce = string(EnforceWarn)
    t.Cleanup(func() { config.Config = old })

    wf := newWalForge()
    if err := wf.Init(); err != nil {
        t.Fatalf("init: %v", err)
    }
    defer wf.Stop()

    // Append a deterministic sequence of commands
    cmds := []*wire.Command{
        {Cmd: "SET", Args: []string{"k1", "v1"}},
        {Cmd: "SET", Args: []string{"k2", "v2"}},
        {Cmd: "DEL", Args: []string{"k1"}},
        {Cmd: "SET", Args: []string{"k3", "v3"}},
    }
    for i, c := range cmds {
        if err := wf.AppendEntry(EntryKindNormal, uint64(i+1), 1, 0, c, nil, nil); err != nil {
            t.Fatalf("append %d: %v", i, err)
        }
    }
    if err := wf.Sync(); err != nil { // durable barrier
        t.Fatalf("sync: %v", err)
    }

    hashOnce := func(t *testing.T) uint64 {
        hasher := xxhash.New()
        if err := wf.ReplayItems(func(ri ReplayItem) error {
            if ri.Cmd == nil {
                return nil
            }
            // Canonicalize as CMD\targ1\targ2... to avoid whitespace differences
            hasher.WriteString(ri.Cmd.Cmd)
            hasher.Write([]byte{'\t'})
            for i, a := range ri.Cmd.Args {
                if i > 0 {
                    hasher.Write([]byte{'\t'})
                }
                hasher.WriteString(a)
            }
            hasher.Write([]byte{'\n'})
            return nil
        }); err != nil {
            t.Fatalf("replay: %v", err)
        }
        return hasher.Sum64()
    }

    h1 := hashOnce(t)

    // Simulate process restart by creating a fresh WAL instance against the same dir
    wf.Stop()
    wf2 := newWalForge()
    if err := wf2.Init(); err != nil {
        t.Fatalf("reinit: %v", err)
    }
    defer wf2.Stop()
    // rebind local var for helper
    wf = wf2
    h2 := hashOnce(t)

    if h1 != h2 {
        t.Fatalf("replay determinism violated: h1=%x h2=%x", h1, h2)
    }
}

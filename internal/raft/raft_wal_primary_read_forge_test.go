package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	wPkg "github.com/sevenDatabase/SevenDB/internal/wal"
)

// TestForgeWALPrimaryReadSeedsStorage writes a few UWAL entries via Forge WAL and then boots
// a raft node with WALPrimaryRead=true to verify etcd storage is seeded with entries and HS.
func TestForgeWALPrimaryReadSeedsStorage(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	// Configure global WAL to forge variant rooted at temp dir
	oldCfg := config.Config
	config.Config.WALVariant = "forge"
	config.Config.WALDir = walDir
	config.Config.WALBufferSizeMB = 1
	config.Config.WALBufferSyncIntervalMillis = 5
	// Use a positive value; wal.newWalForge creates tickers unconditionally.
	config.Config.WALSegmentRotationTimeSec = 1
	config.Config.WALMaxSegmentSizeMB = 16
	t.Cleanup(func() { config.Config = oldCfg })

	wPkg.SetupWAL()
	t.Cleanup(func() { wPkg.TeardownWAL() })

	uw, ok := wPkg.DefaultWAL.(wPkg.UnifiedWAL)
	if !ok || uw == nil {
		t.Fatalf("default wal not unified/forge")
	}

	// Append 3 normal entries with raft metadata and data bytes
	for i := 1; i <= 3; i++ {
		idx := uint64(i)
		term := uint64(1)
		cmd := &wire.Command{Cmd: "SET", Args: []string{"k", string('a' + byte(i-1))}}
		data := []byte{byte('a' + i - 1)}
		if err := uw.AppendEntry(wPkg.EntryKindNormal, idx, term, 0, cmd, nil, data); err != nil {
			t.Fatalf("append normal %d: %v", i, err)
		}
	}
	// Persist a HardState commit=3 term=1
	hs := []byte{0x08, 0x01, 0x10, 0x00, 0x18, 0x03} // raftpb.HardState{Term:1, Vote:0, Commit:3} protobuf bytes
	if err := uw.AppendEntry(wPkg.EntryKindHardState, 3, 1, 0, nil, hs, nil); err != nil {
		t.Fatalf("append hardstate: %v", err)
	}
	// Ensure data flushed to disk
	if s, ok := wPkg.DefaultWAL.(interface{ Sync() error }); ok {
		_ = s.Sync()
	}

	// Start a node with WALPrimaryRead=true and verify seeded entries
	rcfg := RaftConfig{ShardID: "s1", NodeID: "1", Peers: []string{"1"}, DataDir: dir, Engine: "etcd", WALPrimaryRead: true, Manual: true}
	n, err := NewShardRaftNode(rcfg)
	if err != nil {
		t.Fatalf("new shard node: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })

	ents, eerr := n.storage.Entries(1, 4, 1<<20)
	if eerr != nil {
		t.Fatalf("entries: %v", eerr)
	}
	if len(ents) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(ents))
	}
	for i, e := range ents {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry index mismatch at %d: %d", i, e.Index)
		}
		if e.Term != 1 {
			t.Fatalf("entry term mismatch at %d: %d", i, e.Term)
		}
		if len(e.Data) != 1 || e.Data[0] != byte('a'+i) {
			t.Fatalf("entry data mismatch at %d: %v", i, e.Data)
		}
	}
}

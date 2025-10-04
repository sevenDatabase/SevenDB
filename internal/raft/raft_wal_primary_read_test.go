package raft

import (
	"os"
	"path/filepath"
	"testing"

	raftwal "github.com/sevenDatabase/SevenDB/internal/raftwal"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// TestWALPrimaryReadSeedsStorage writes a few envelopes directly via writer, then constructs
// a Raft node with WALPrimaryRead enabled and verifies storage reflects those entries.
func TestWALPrimaryReadSeedsStorage(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "raftwal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	w, err := raftwal.NewWriter(raftwal.Config{Dir: walDir, BufMB: 1, SidecarFlushEvery: 2, StrictSync: true})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	// Write 3 normal entries
	for i := 1; i <= 3; i++ {
		env := &raftwal.Envelope{RaftIndex: uint64(i), RaftTerm: 1, Kind: raftwal.EntryKind_ENTRY_NORMAL, AppBytes: []byte{byte('a' + i - 1)}, AppCrc: uint32(i)}
		b, _ := proto.Marshal(env)
		if err := w.AppendEnvelope(uint64(i), b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// HardState commit=3 term=1
	hs := raftpb.HardState{Commit: 3, Term: 1}
	hsBytes, _ := hs.Marshal()
	hsEnv := &raftwal.Envelope{Kind: raftwal.EntryKind_ENTRY_HARDSTATE, AppBytes: hsBytes}
	hb, _ := proto.Marshal(hsEnv)
	if err := w.AppendHardState(3, hb); err != nil {
		t.Fatalf("append hs: %v", err)
	}
	_ = w.Close()

	rcfg := RaftConfig{ShardID: "s1", NodeID: "n1", Peers: []string{"n1"}, DataDir: dir, Engine: "etcd", EnableWALShadow: true, WALPrimaryRead: true}
	n, err := NewShardRaftNode(rcfg)
	if err != nil {
		t.Fatalf("new shard node: %v", err)
	}
	// Storage should now contain the entries 1..3
	entries, eerr := n.storage.Entries(1, 4, 1<<20)
	if eerr != nil {
		t.Fatalf("entries: %v", eerr)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry index mismatch at %d: %d", i, e.Index)
		}
	}
	// HardState applied to storage was also tracked in node.lastShadowHardState during seeding.
	if n.lastShadowHardState.Commit != 3 || n.lastShadowHardState.Term != 1 {
		t.Fatalf("unexpected hardstate commit/term: %+v", n.lastShadowHardState)
	}
	_ = n.Close()
}

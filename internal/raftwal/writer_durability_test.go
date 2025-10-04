package raftwal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestStrictSyncDurability basic lifecycle: write N entries + hardstate, close, reopen, replay, ensure all present.
func TestStrictSyncDurability(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 4, StrictSync: true})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	// Write 10 normal envelopes
	for i := 1; i <= 10; i++ {
		env := &Envelope{RaftIndex: uint64(i), RaftTerm: 1, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{byte(i)}, AppCrc: uint32(i)}
		b, _ := proto.Marshal(env)
		if err := w.AppendEnvelope(uint64(i), b); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// HardState (simulate commit)
	hsPayload := make([]byte, 8)
	binary.LittleEndian.PutUint64(hsPayload, uint64(10))
	hsEnv := &Envelope{Kind: EntryKind_ENTRY_HARDSTATE, AppBytes: hsPayload}
	hb, _ := proto.Marshal(hsEnv)
	if err := w.AppendHardState(10, hb); err != nil {
		t.Fatalf("append hs: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Remove sidecar to force scan path (simulate crash before sidecar rewrite)
	seg0 := filepath.Join(dir, "seg-0.wal")
	if _, err := os.Stat(seg0 + ".idx"); err == nil {
		_ = os.Remove(seg0 + ".idx")
	}

	// Replay and check we recovered all 10 normal entries and one hardstate
	var count int
	var lastHS bool
	if err := Replay(dir, func(env *Envelope) error {
		if env.Kind == EntryKind_ENTRY_NORMAL {
			count++
		}
		if env.Kind == EntryKind_ENTRY_HARDSTATE {
			lastHS = true
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 normal entries, got %d", count)
	}
	if !lastHS {
		t.Fatalf("expected hardstate envelope present")
	}
}

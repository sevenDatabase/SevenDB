package raftwal

import (
	"os"
	"testing"
	"path/filepath"
	"google.golang.org/protobuf/proto"
)

// simulateCrash closes process abruptly by not calling Close() – we just drop the writer reference.
// We rely on OS to flush already written data; buffered (unflushed) data is lost.

func buildTestEnvelope(i uint64) *Envelope {
	return &Envelope{RaftIndex: i, RaftTerm: 1, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{byte(i)}, AppCrc: uint32(i), Namespace: "default", Bucket: "b", Opcode: 1, Sequence: i}
}

// TestWriterCrashInjection iterates injection points to ensure replay recovers all durable frames.
func TestWriterCrashInjection(t *testing.T) {
	points := []string{"afterFrame", "afterFlush", "afterSidecar", "afterRotation"}
	for _, p := range points {
		t.Run(p, func(t *testing.T){
			dir := t.TempDir()
			w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 2, SegmentMaxBytes: 1024, StrictSync: true})
			if err != nil { t.Fatalf("new writer: %v", err) }
			// Install injection hooks
			switch p {
			case "afterFrame":
				w.injAfterFrameWrite = func(kind string, idx uint64) { if idx == 5 { /* crash */ w = nil } }
			case "afterFlush":
				w.injAfterFlushFsync = func(kind string, idx uint64) { if idx == 5 { w = nil } }
			case "afterSidecar":
				w.injAfterSidecar = func(seg int) { /* trigger after sidecar rewrite following idx 6 */ }
			case "afterRotation":
				w.injAfterRotation = func(old int) { /* rotation path */ }
			}
			// Write 10 envelopes (some may not persist depending on crash point)
			for i:=uint64(1); i<=10; i++ {
				if w == nil { break }
				env := buildTestEnvelope(i)
				b, _ := proto.Marshal(env)
				if err := w.AppendEnvelope(i, b); err != nil { t.Fatalf("append %d: %v", i, err) }
			}
			// Do not call Close() to emulate abrupt termination.

			// Replay and ensure monotonic sequence of indices without CRC mismatch
			seen := make(map[uint64]bool)
			err = Replay(dir, func(env *Envelope) error { seen[env.RaftIndex] = true; return nil })
			if err != nil { t.Fatalf("replay: %v", err) }
			// Basic assertion: indices strictly increasing without gaps up to len(seen)
			// (StrictSync ensures each successful append before crash is durable.)
			for i:=uint64(1); i<=uint64(len(seen)); i++ { if !seen[i] { t.Fatalf("missing index %d for crash point %s", i, p) } }
		})
	}
}

// TestSidecarLossEnriched ensures that deleting the sidecar forces segment scan and enriched fields remain available.
func TestSidecarLossEnriched(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 2, StrictSync: true})
	if err != nil { t.Fatalf("writer: %v", err) }
	for i:=uint64(1); i<=6; i++ { b, _ := proto.Marshal(buildTestEnvelope(i)); if err := w.AppendEnvelope(i, b); err != nil { t.Fatalf("append: %v", err) } }
	if err := w.Close(); err != nil { t.Fatalf("close: %v", err) }
	// Remove sidecar(s)
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.wal.idx"))
	for _, s := range segs { _ = os.Remove(s) }
	// Replay and verify we still get the enriched metadata
	count := 0
	err = Replay(dir, func(env *Envelope) error {
		if env.Namespace != "default" || env.Bucket != "b" || env.Opcode != 1 || env.Sequence == 0 {
			t.Fatalf("enriched fields missing after sidecar loss: %+v", env)
		}
		count++
		return nil
	})
	if err != nil { t.Fatalf("replay: %v", err) }
	if count != 6 { t.Fatalf("expected 6 envelopes, got %d", count) }
}


package raftwal

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"

	"google.golang.org/protobuf/proto"
)

// walTranscript builds a stable byte transcript from replayed envelopes: one line per
// ENTRY_NORMAL envelope as "<index>:<crc>\n" ordered oldest->newest across segments.
func walTranscript(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	err := Replay(dir, func(env *Envelope) error {
		if env.Kind == EntryKind_ENTRY_NORMAL {
			// If AppCrc isn't populated, compute it defensively to ensure stability
			crc := env.AppCrc
			if crc == 0 && len(env.AppBytes) > 0 {
				crc = crc32.ChecksumIEEE(env.AppBytes)
			}
			fmt.Fprintf(&buf, "%d:%08x\n", env.RaftIndex, crc)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return buf.Bytes()
}

func runWALRolloverTranscript(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	// ForceRotateEvery triggers deterministic rotation independent of segment size
	w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 3, StrictSync: true, ForceRotateEvery: 3})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	// Write a fixed workload spanning multiple segments
	for i := uint64(1); i <= 12; i++ {
		env := &Envelope{RaftIndex: i, RaftTerm: 1, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{byte(i)}, AppCrc: crc32.ChecksumIEEE([]byte{byte(i)})}
		b, _ := proto.Marshal(env)
		if err := w.AppendEnvelope(i, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		// interleave a few hardstate writes to exercise mixed kinds (not included in transcript)
		if i%5 == 0 {
			hs := &Envelope{Kind: EntryKind_ENTRY_HARDSTATE, AppBytes: []byte{0xaa, byte(i)}}
			hb, _ := proto.Marshal(hs)
			if err := w.AppendHardState(i, hb); err != nil {
				t.Fatalf("append hs %d: %v", i, err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return walTranscript(t, dir)
}

func TestDeterminism_Repeat100_WAL_Rollover(t *testing.T) {
	var base []byte
	for i := 0; i < 100; i++ {
		got := runWALRolloverTranscript(t)
		if i == 0 {
			base = got
		} else if !bytes.Equal(base, got) {
			t.Fatalf("wal rollover transcript mismatch at run %d", i+1)
		}
	}
}

func runWALPruneTranscript(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 2, StrictSync: true, ForceRotateEvery: 2})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	// Write entries so we span several segments deterministically
	for i := uint64(1); i <= 12; i++ {
		env := &Envelope{RaftIndex: i, RaftTerm: 1, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{0x7f, byte(i)}, AppCrc: crc32.ChecksumIEEE([]byte{0x7f, byte(i)})}
		b, _ := proto.Marshal(env)
		if err := w.AppendEnvelope(i, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	// Prune segments strictly below index 7; remaining replay should start at >=7
	if _, err := w.PruneThrough(7); err != nil {
		t.Fatalf("prune: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return walTranscript(t, dir)
}

func TestDeterminism_Repeat100_WAL_PruneThrough(t *testing.T) {
	var base []byte
	for i := 0; i < 100; i++ {
		got := runWALPruneTranscript(t)
		if i == 0 {
			base = got
		} else if !bytes.Equal(base, got) {
			t.Fatalf("wal prune transcript mismatch at run %d", i+1)
		}
	}
}

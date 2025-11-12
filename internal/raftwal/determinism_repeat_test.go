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

// walFullTranscript is a stricter canonical transcript including multiple fields to
// make serialization invariants explicit: "<index>:<term>:<kind>:<applen>:<crc>\n".
func walFullTranscript(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	err := Replay(dir, func(env *Envelope) error {
		if env.Kind == EntryKind_ENTRY_NORMAL {
			crc := env.AppCrc
			if crc == 0 && len(env.AppBytes) > 0 {
				crc = crc32.ChecksumIEEE(env.AppBytes)
			}
			// Include more fields to ensure canonical ordering/encoding is stable
			// across rotations, sessions, and with/without precomputed AppCrc.
			fmt.Fprintf(&buf, "%d:%d:%d:%d:%08x\n", env.RaftIndex, env.RaftTerm, env.Kind, len(env.AppBytes), crc)
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

// Same as rollover test but intentionally omit AppCrc to prove canonicalization is stable.
func runWALRolloverTranscriptNoAppCrc(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 3, StrictSync: true, ForceRotateEvery: 3})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	for i := uint64(1); i <= 12; i++ {
		// AppCrc omitted deliberately
		env := &Envelope{RaftIndex: i, RaftTerm: 1, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{byte(i)}}
		b, _ := proto.Marshal(env)
		if err := w.AppendEnvelope(i, b); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if i%4 == 0 {
			hs := &Envelope{Kind: EntryKind_ENTRY_HARDSTATE, AppBytes: []byte{0xbb, byte(i)}}
			hb, _ := proto.Marshal(hs)
			if err := w.AppendHardState(i, hb); err != nil {
				t.Fatalf("append hs %d: %v", i, err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return walFullTranscript(t, dir)
}

func TestDeterminism_Repeat100_WAL_Rollover_NoAppCrc(t *testing.T) {
	var base []byte
	for i := 0; i < 100; i++ {
		got := runWALRolloverTranscriptNoAppCrc(t)
		if i == 0 {
			base = got
		} else if !bytes.Equal(base, got) {
			t.Fatalf("wal rollover (no AppCrc) transcript mismatch at run %d", i+1)
		}
	}
}

// Reopen the WAL and continue writing to ensure canonical ordering/serialization across sessions.
func runWALReopenContinueTranscript(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()

	cfg := Config{Dir: dir, BufMB: 1, SidecarFlushEvery: 2, StrictSync: true, ForceRotateEvery: 3}

	// First session: write 1..6
	w1, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	for i := uint64(1); i <= 6; i++ {
		env := &Envelope{RaftIndex: i, RaftTerm: 2, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{0x11, byte(i)}, AppCrc: crc32.ChecksumIEEE([]byte{0x11, byte(i)})}
		b, _ := proto.Marshal(env)
		if err := w1.AppendEnvelope(i, b); err != nil {
			t.Fatalf("append1 %d: %v", i, err)
		}
		if i%3 == 0 {
			hs := &Envelope{Kind: EntryKind_ENTRY_HARDSTATE, AppBytes: []byte{0xcc, byte(i)}}
			hb, _ := proto.Marshal(hs)
			if err := w1.AppendHardState(i, hb); err != nil {
				t.Fatalf("append1 hs %d: %v", i, err)
			}
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	// Second session: continue 7..12
	w2, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	for i := uint64(7); i <= 12; i++ {
		env := &Envelope{RaftIndex: i, RaftTerm: 2, Kind: EntryKind_ENTRY_NORMAL, AppBytes: []byte{0x11, byte(i)}, AppCrc: crc32.ChecksumIEEE([]byte{0x11, byte(i)})}
		b, _ := proto.Marshal(env)
		if err := w2.AppendEnvelope(i, b); err != nil {
			t.Fatalf("append2 %d: %v", i, err)
		}
		if i%4 == 1 {
			hs := &Envelope{Kind: EntryKind_ENTRY_HARDSTATE, AppBytes: []byte{0xdd, byte(i)}}
			hb, _ := proto.Marshal(hs)
			if err := w2.AppendHardState(i, hb); err != nil {
				t.Fatalf("append2 hs %d: %v", i, err)
			}
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	return walFullTranscript(t, dir)
}

func TestDeterminism_Repeat100_WAL_Reopen_Continue(t *testing.T) {
	var base []byte
	for i := 0; i < 100; i++ {
		got := runWALReopenContinueTranscript(t)
		if i == 0 {
			base = got
		} else if !bytes.Equal(base, got) {
			t.Fatalf("wal reopen+continue transcript mismatch at run %d", i+1)
		}
	}
}

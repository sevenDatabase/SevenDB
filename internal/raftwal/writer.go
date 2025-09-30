package raftwal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Physical entry framing layout written to WAL segment:
// [4B physical CRC32 over payload][4B payload length][payload bytes = Envelope proto]
// This mirrors existing WAL Forge framing for consistency.

// Writer implements shadow-mode append of raft Envelope records.
type Writer struct {
	mu       sync.Mutex
	dir      string
	segIdx   int
	file     *os.File
	buf      *bufio.Writer
	bufSize  int
	closed   bool
	lastIndex uint64

    // sidecar index in-memory (dense list of entries for current segment)
    entries []entryMeta
    entriesSinceFlush int
    sidecarFlushEvery int // config threshold

	maxSegmentBytes int64
	dirFsync bool // if true fsync directory after metadata updates (rotation, sidecar rename)
	// test/diagnostic knobs (not for production traffic paths)
	forceRotateEvery int // if >0 rotate after this many successful appends (post-append)
	appendCount      int // internal counter of appends for forceRotateEvery
}

// Dir returns the base directory the writer operates in.
func (w *Writer) Dir() string { return w.dir }

// Config for Writer initialization.
type Config struct {
	Dir     string
	BufMB   int
    SidecarFlushEvery int // number of entries between sidecar rewrites (>=1)
	SegmentMaxBytes   int64 // rotate when current segment size exceeds this (>0)
	DirFsync bool // if true fsync directory after segment create & sidecar rename
	// ForceRotateEvery is a test-only knob: if >0 a segment rotation is forced
	// after every N successful AppendEnvelope calls. Rotation happens after the
	// append (so the just-written entry resides in the completed segment). This
	// enables deterministic reproduction of crash windows around rotation in
	// tests without relying on size thresholds.
	ForceRotateEvery int
}

// NewWriter creates (or continues) the latest segment in shadow mode.
func NewWriter(cfg Config) (*Writer, error) {
	if cfg.Dir == "" { return nil, errors.New("raftwal: empty dir") }
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil { return nil, err }
	w := &Writer{dir: cfg.Dir, bufSize: cfg.BufMB * 1024 * 1024, sidecarFlushEvery: cfg.SidecarFlushEvery, maxSegmentBytes: cfg.segmentMaxBytesOrDefault(), dirFsync: cfg.DirFsync, forceRotateEvery: cfg.ForceRotateEvery}
	if w.bufSize == 0 { w.bufSize = 1 * 1024 * 1024 }
    if w.sidecarFlushEvery <= 0 { w.sidecarFlushEvery = 256 }
	if err := w.openLastOrCreate(); err != nil { return nil, err }
	return w, nil
}

func (c *Config) segmentMaxBytesOrDefault() int64 { if c.SegmentMaxBytes > 0 { return c.SegmentMaxBytes }; return 64 * 1024 * 1024 }

func (w *Writer) openLastOrCreate() error {
	// MVP: find highest seg-*.wal and continue; else start at seg-0.
	matches, _ := filepath.Glob(filepath.Join(w.dir, "seg-*.wal"))
	highest := -1
	for _, m := range matches {
		// parse number between 'seg-' and '.wal'
		base := filepath.Base(m)
		// expected form seg-<n>.wal
		var n int
		if _, err := fmt.Sscanf(base, "seg-%d.wal", &n); err == nil {
			if n > highest { highest = n }
		}
	}
	if highest >= 0 { w.segIdx = highest } else { w.segIdx = 0 }
	name := w.segmentFileName()
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil { return err }
	w.file = f
	w.buf = bufio.NewWriterSize(f, w.bufSize)
	return nil
}

// AppendEnvelope writes an Envelope proto payload already marshaled into b.
// Caller is responsible for providing the serialized Envelope (future: we will marshal internally).
func (w *Writer) AppendEnvelope(raftIndex uint64, b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed { return errors.New("raftwal: writer closed") }
	// Enforce sequential index (shadow mode safety). Allow first append at any index.
	if w.lastIndex != 0 && raftIndex != w.lastIndex+1 {
		// Conflict: start conflict rotation path (rotate new segment; later we will delete conflicting ones).
		if err := w.rotateForConflictLocked(raftIndex); err != nil { return err }
	}
	payloadLen := len(b)
	frameLen := 8 + payloadLen
	buf := make([]byte, frameLen)
	crc := crc32.ChecksumIEEE(b)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))
	copy(buf[8:], b)
	offsetBefore := w.currentOffsetUnsafe()
	if _, err := w.buf.Write(buf); err != nil { return err }
	w.lastIndex = raftIndex
    w.entries = append(w.entries, entryMeta{Index: raftIndex, Offset: uint32(offsetBefore), Length: uint32(frameLen)})
    w.entriesSinceFlush++
    if w.entriesSinceFlush >= w.sidecarFlushEvery { _ = w.writeSidecarLocked() }
	// Size-based rotation check
	if w.maxSegmentBytes > 0 && (offsetBefore+int64(frameLen)) >= w.maxSegmentBytes {
		if err := w.rotateLocked(); err != nil { return err }
	}
	// Deterministic test rotation knob: rotate after N appends
	if w.forceRotateEvery > 0 {
		w.appendCount++
		if (w.appendCount % w.forceRotateEvery) == 0 {
			if err := w.rotateLocked(); err != nil { return err }
		}
	}
	return nil
}

// Flush forces buffered frames to disk (but does not fsync underlying file).
func (w *Writer) Flush() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return nil }
	return w.buf.Flush()
}

// Sync flushes and fsyncs the segment file.
func (w *Writer) Sync() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return nil }
	if err := w.buf.Flush(); err != nil { return err }
	return w.file.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return nil }
	_ = w.buf.Flush()
	_ = w.writeSidecarLocked()
	_ = w.file.Sync()
	w.closed = true
	return w.file.Close()
}

// entryMeta holds simple offset mapping inside a segment.
type entryMeta struct {
	Index  uint64
	Offset uint32
	Length uint32
}

// currentOffsetUnsafe returns current file size (approx) by syncing writer buffer length.
func (w *Writer) currentOffsetUnsafe() int64 {
	// NOTE: We estimate using Stat().Size() + buffered bytes. This is acceptable for shadow mode.
	if w.file == nil { return 0 }
	fi, err := w.file.Stat()
	if err != nil { return 0 }
	return fi.Size() + int64(w.buf.Buffered())
}

// writeSidecarLocked writes the in-memory entries index for current segment.
func (w *Writer) writeSidecarLocked() error {
	if w.file == nil { return nil }
	if len(w.entries) == 0 { return nil }
	name := w.segmentFileName()
	sidecar := name + ".idx"
	tmp := sidecar + ".tmp"
	// Build binary sidecar (simplified version: header + fixed triplets + trailer CRC)
	var buf bytes.Buffer
	// Header
	buf.Write([]byte{'W','I','D','X'})
	// version uint16 + flags uint16
	hdr := make([]byte, 12) // version(2)+flags(2)+count(4)+reserved(4 for future)
	binary.LittleEndian.PutUint16(hdr[0:2], uint16(1))
	// flags=0
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(w.entries)))
	buf.Write(hdr)
	// Entries
	entryBytes := make([]byte, 16) // index(8)+offset(4)+length(4)
	for _, em := range w.entries {
		binary.LittleEndian.PutUint64(entryBytes[0:8], em.Index)
		binary.LittleEndian.PutUint32(entryBytes[8:12], em.Offset)
		binary.LittleEndian.PutUint32(entryBytes[12:16], em.Length)
		buf.Write(entryBytes)
	}
	crc := crc32.ChecksumIEEE(buf.Bytes())
	tail := make([]byte, 4)
	binary.LittleEndian.PutUint32(tail, crc)
	buf.Write(tail)
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil { return err }
	if err := os.Rename(tmp, sidecar); err != nil { return err }
	if w.dirFsync { _ = syncDir(w.dir) }
	w.entriesSinceFlush = 0
	return nil
}

func (w *Writer) segmentFileName() string { return filepath.Join(w.dir, fmt.Sprintf("seg-%d.wal", w.segIdx)) }

// rotateLocked closes current segment (flushing sidecar) and opens a new segment.
func (w *Writer) rotateLocked() error {
	if err := w.buf.Flush(); err != nil { return err }
	_ = w.writeSidecarLocked()
	if err := w.file.Sync(); err != nil { return err }
	_ = w.file.Close()
	w.segIdx++
	name := w.segmentFileName()
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil { return err }
	w.file = f
	w.buf = bufio.NewWriterSize(f, w.bufSize)
	w.entries = w.entries[:0]
	w.entriesSinceFlush = 0
	if w.dirFsync { _ = syncDir(w.dir) }
	return nil
}

// rotateForConflictLocked forces a rotation before appending a non-sequential index.
// Later cleanup logic will be responsible for pruning conflicting higher index segments.
func (w *Writer) rotateForConflictLocked(_ uint64) error {
	if w.lastIndex == 0 { return nil }
	return w.rotateLocked()
}

// Replay scans all segments in directory (ascending) and invokes cb for each decoded Envelope.
func Replay(dir string, cb func(*Envelope) error) error {
	files, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil { return err }
	sort.Strings(files)
	for _, fpath := range files {
		if err := replaySegment(fpath, cb); err != nil { return err }
	}
	return nil
}

func replaySegment(path string, cb func(*Envelope) error) error {
	f, err := os.Open(path)
	if err != nil { return err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) { return nil }
			if err == io.ErrUnexpectedEOF { return nil }
			return err
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return err }
		if crc32.ChecksumIEEE(payload) != crc { return fmt.Errorf("wal: crc mismatch in %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return err }
		if err := cb(&env); err != nil { return err }
	}
}

// TailLogicalCRCs replays segments from newest backwards until it has collected up to limit
// logical CRCs (app_crc) of ENTRY_NORMAL envelopes, returned oldest->newest.
func TailLogicalCRCs(dir string, limit int) ([]uint32, error) {
	if limit <= 0 { return nil, nil }
	files, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil { return nil, err }
	sort.Strings(files)
	// iterate backwards
	crcsRev := make([]uint32, 0, limit)
	for i := len(files) - 1; i >= 0 && len(crcsRev) < limit; i-- {
		fpath := files[i]
		// read segment entries sequentially (can't reverse-read easily due to variable length framing)
		segCRCs, err := collectSegmentCRCs(fpath, limit-len(crcsRev))
		if err != nil { return nil, err }
		// prepend by appending to reverse slice
		for j := len(segCRCs)-1; j >= 0; j-- { crcsRev = append(crcsRev, segCRCs[j]) }
	}
	// Now crcsRev holds newest->oldest; invert to oldest->newest limited.
	out := make([]uint32, len(crcsRev))
	for i := range crcsRev { out[len(crcsRev)-1-i] = crcsRev[i] }
	return out, nil
}

// TailLogicalTuples returns up to limit tuples (index, crc) for ENTRY_NORMAL envelopes, oldest->newest.
// This is used by the upgraded validator that tracks raft index ordering explicitly.
func TailLogicalTuples(dir string, limit int) ([]struct{Index uint64; CRC uint32}, error) {
	if limit <= 0 { return nil, nil }
	files, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil { return nil, err }
	sort.Strings(files)
	rev := make([]struct{Index uint64; CRC uint32}, 0, limit)
	for i := len(files)-1; i >=0 && len(rev) < limit; i-- {
		fpath := files[i]
		tuples, err := collectSegmentTuples(fpath, limit-len(rev))
		if err != nil { return nil, err }
		for j:=len(tuples)-1; j>=0; j-- { rev = append(rev, tuples[j]) }
	}
	// reverse
	out := make([]struct{Index uint64; CRC uint32}, len(rev))
	for i:=range rev { out[len(rev)-1-i] = rev[i] }
	return out, nil
}

func collectSegmentTuples(path string, capLeft int) ([]struct{Index uint64; CRC uint32}, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	var res []struct{Index uint64; CRC uint32}
	for {
		if capLeft <= 0 { break }
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
			return res, err
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return res, err }
		if crc32.ChecksumIEEE(payload) != crc { return res, fmt.Errorf("wal: crc mismatch tail scan %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return res, err }
		if env.Kind == EntryKind_ENTRY_NORMAL {
			res = append(res, struct{Index uint64; CRC uint32}{Index: env.RaftIndex, CRC: env.AppCrc})
			capLeft--
		}
	}
	return res, nil
}

func collectSegmentCRCs(path string, capLeft int) ([]uint32, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	var res []uint32
	for {
		if capLeft <= 0 { break }
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
			return res, err
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return res, err }
		if crc32.ChecksumIEEE(payload) != crc { return res, fmt.Errorf("wal: crc mismatch tail scan %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return res, err }
		if env.Kind == EntryKind_ENTRY_NORMAL {
			res = append(res, env.AppCrc)
			capLeft--
		}
	}
	return res, nil
}

// syncDir fsyncs a directory to persist metadata updates (file create/rename) to stable storage.
func syncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil { return err }
	defer df.Close()
	return df.Sync()
}

// ReplayLastHardState scans envelopes in order and returns last HARDSTATE envelope's payload bytes.
func ReplayLastHardState(dir string) ([]byte, bool, error) {
	files, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil { return nil, false, err }
	sort.Strings(files)
	var hs []byte
	for _, fpath := range files {
		f, err := os.Open(fpath); if err != nil { return nil, false, err }
		r := bufio.NewReader(f)
		header := make([]byte, 8)
		for {
			if _, err := io.ReadFull(r, header); err != nil {
				if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
				_ = f.Close(); return nil, false, err
			}
			crc := binary.LittleEndian.Uint32(header[0:4])
			sz := binary.LittleEndian.Uint32(header[4:8])
			if sz == 0 { continue }
			payload := make([]byte, sz)
			if _, err := io.ReadFull(r, payload); err != nil { _ = f.Close(); return nil, false, err }
			if crc32.ChecksumIEEE(payload) != crc { _ = f.Close(); return nil, false, fmt.Errorf("wal: crc mismatch %s", fpath) }
			var env Envelope
			if err := proto.Unmarshal(payload, &env); err != nil { _ = f.Close(); return nil, false, err }
			if env.Kind == EntryKind_ENTRY_HARDSTATE { hs = env.AppBytes }
		}
		_ = f.Close()
	}
	if hs == nil { return nil, false, nil }
	return hs, true, nil
}

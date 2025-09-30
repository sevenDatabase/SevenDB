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
}

// Config for Writer initialization.
type Config struct {
	Dir     string
	BufMB   int
    SidecarFlushEvery int // number of entries between sidecar rewrites (>=1)
	SegmentMaxBytes   int64 // rotate when current segment size exceeds this (>0)
}

// NewWriter creates (or continues) the latest segment in shadow mode.
func NewWriter(cfg Config) (*Writer, error) {
	if cfg.Dir == "" { return nil, errors.New("raftwal: empty dir") }
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil { return nil, err }
	w := &Writer{dir: cfg.Dir, bufSize: cfg.BufMB * 1024 * 1024, sidecarFlushEvery: cfg.SidecarFlushEvery, maxSegmentBytes: cfg.SegmentMaxBytes}
	if w.bufSize == 0 { w.bufSize = 1 * 1024 * 1024 }
    if w.sidecarFlushEvery <= 0 { w.sidecarFlushEvery = 256 }
	if w.maxSegmentBytes <= 0 { w.maxSegmentBytes = 64 * 1024 * 1024 } // default 64MB
	if err := w.openLastOrCreate(); err != nil { return nil, err }
	return w, nil
}

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

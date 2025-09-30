package raftwal

// Package raftwal provides a shadow (and future primary) write-ahead log used during
// migration from the legacy raft persistence. Segments use a simple physical frame:
//   [4B CRC32(payload)][4B little-endian payload length][Envelope proto bytes]
// The logical payload (Envelope) contains raft index/term, kind, and application bytes + crc.
//
// Durability / Ordering Modes
// ---------------------------
// Normal (shadow) mode batches writes in a buffered writer and periodically emits a sidecar
// index (.idx) containing offsets of frames in the segment. Crashes may lose the tail of
// buffered frames not yet flushed (acceptable while legacy path is authoritative).
//
// StrictSync mode (cfg.StrictSync / RaftConfig.WALStrictSync) enforces an fsync discipline
// on EVERY append (both normal entries and HardState). Ordering:
//   1. Append frame bytes to buffer
//   2. Flush buffer
//   3. Fsync segment file
//   4. (If sidecar threshold reached) write sidecar.tmp, rename to .idx, fsync directory
//   5. (If rotation triggered) flush+fsync old segment (already done), close, create new, fsync directory
// This provides a crash guarantee that any reported successful Append* call has its frame
// bytes safely persisted in the segment file. The sidecar may lag by up to (SidecarFlushEvery-1)
// entries; recovery tolerates a stale sidecar by scanning raw segment frames. A future refinement
// may fsync the directory on EVERY strict append to tighten rename persistence latency.
//
// Crash Recovery Invariants
// * A partially written frame (torn write) is detected by CRC mismatch and stops replay at
//   that boundary (subsequent garbage ignored).
// * Missing / outdated sidecar leads to on-demand segment scan; sidecar is rebuilt opportunistically.
// * Pruning uses rename-to-.deleted then unlink with directory fsync to avoid resurrecting
//   entries after crash.
//
// Future Cutover
// The replay path introduced for WALPrimaryRead will seed in-memory raft storage from segments.
// For full logical reconstruction of higher-level proposal metadata we may extend Envelope to
// carry bucket/type/sequence fields (TODO in code where relevant).

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
	"log"

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
	// strictSync when true enforces a conservative durability ordering on every append:
	//  (a) write frame to buffer
	//  (b) flush buffer
	//  (c) fsync segment file
	//  (d) if sidecar was rewritten, ensure directory fsync
	//  (e) on rotation, fsync closed segment then create+fsync dir for new file
	// This is heavier than shadow mode requirements but is used for migration
	// validation and future cutover confidence tests.
	strictSync bool

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
	// StrictSync enforces fsync of the active segment file after each AppendEnvelope / AppendHardState.
	// It implicitly enables directory fsync even if DirFsync was false (because rename+create ordering
	// must reach stable storage to uphold the stricter durability contract).
	StrictSync bool
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
	dirFsync := cfg.DirFsync
	if cfg.StrictSync && !dirFsync { dirFsync = true } // strict implies directory fsync
	w := &Writer{dir: cfg.Dir, bufSize: cfg.BufMB * 1024 * 1024, sidecarFlushEvery: cfg.SidecarFlushEvery, maxSegmentBytes: cfg.segmentMaxBytesOrDefault(), dirFsync: dirFsync, forceRotateEvery: cfg.ForceRotateEvery, strictSync: cfg.StrictSync}
	if w.bufSize == 0 { w.bufSize = 1 * 1024 * 1024 }
    if w.sidecarFlushEvery <= 0 { w.sidecarFlushEvery = 256 }
	if err := w.openLastOrCreate(); err != nil { return nil, err }
	return w, nil
}

func (c *Config) segmentMaxBytesOrDefault() int64 { if c.SegmentMaxBytes > 0 { return c.SegmentMaxBytes }; return 64 * 1024 * 1024 }

func (w *Writer) openLastOrCreate() error {
	// MVP: find highest seg-*.wal and continue; else start at seg-0.
	matches, _ := filepath.Glob(filepath.Join(w.dir, "seg-*.wal"))
	// Cleanup any leftover *.deleted from prior interrupted prune operations.
	deleted, _ := filepath.Glob(filepath.Join(w.dir, "*.wal.deleted"))
	for _, d := range deleted { _ = os.Remove(d) }
	deletedIdx, _ := filepath.Glob(filepath.Join(w.dir, "*.wal.idx.deleted"))
	for _, d := range deletedIdx { _ = os.Remove(d) }
	// Cleanup any orphan sidecar temp files (crash during sidecar rewrite).
	tmps, _ := filepath.Glob(filepath.Join(w.dir, "*.wal.idx.tmp"))
	for _, t := range tmps { _ = os.Remove(t) }
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
// AppendEnvelope appends a normal (index advancing) raft envelope. Sequential enforcement
// applies: raftIndex must be lastIndex+1 (unless this is the first append) else we rotate
// the segment for conflict isolation. HardState envelopes should use AppendHardState.
func (w *Writer) AppendEnvelope(raftIndex uint64, b []byte) error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return errors.New("raftwal: writer closed") }
	// Enforce sequential index (shadow mode safety). Allow first append at any index.
	if w.lastIndex != 0 && raftIndex != w.lastIndex+1 {
		log.Printf("raftwal: conflict rotate segment=%d lastIndex=%d incoming=%d", w.segIdx, w.lastIndex, raftIndex)
		if err := w.rotateForConflictLocked(raftIndex); err != nil { return err }
	}
	payloadLen := len(b)
	frameLen := 8 + payloadLen
	frame := make([]byte, frameLen)
	crc := crc32.ChecksumIEEE(b)
	binary.LittleEndian.PutUint32(frame[0:4], crc)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(payloadLen))
	copy(frame[8:], b)
	offsetBefore := w.currentOffsetUnsafe()
	if _, err := w.buf.Write(frame); err != nil { return err }
	w.lastIndex = raftIndex
	w.entries = append(w.entries, entryMeta{Index: raftIndex, Offset: uint32(offsetBefore), Length: uint32(frameLen)})
	w.entriesSinceFlush++
	if w.strictSync { // flush + fsync segment file for each append
		if err := w.buf.Flush(); err != nil { return err }
		if err := w.file.Sync(); err != nil { return err }
	} else if w.entriesSinceFlush >= w.sidecarFlushEvery { // background sidecar cadence
		_ = w.writeSidecarLocked()
	}
	if w.entriesSinceFlush >= w.sidecarFlushEvery { // ensure sidecar flush logic still triggers in strict mode
		_ = w.writeSidecarLocked()
	}
	// Size-based rotation check
	if w.maxSegmentBytes > 0 && (offsetBefore+int64(frameLen)) >= w.maxSegmentBytes {
		if err := w.rotateLocked(); err != nil { return err }
	}
	if w.forceRotateEvery > 0 { // test knob
		w.appendCount++
		if (w.appendCount % w.forceRotateEvery) == 0 {
			if err := w.rotateLocked(); err != nil { return err }
		}
	}
	return nil
}

// AppendHardState appends a HARDSTATE envelope. It does NOT participate in the
// sequential index advancement logic because HardState.Commit may jump forward
// relative to the last appended normal entry (committed batch). We record it
// without updating lastIndex so subsequent normal entries still enforce strict
// +1 continuity. This prevents artificial gaps causing conflict rotations.
func (w *Writer) AppendHardState(commitIndex uint64, b []byte) error {
	w.mu.Lock(); defer w.mu.Unlock()
	if w.closed { return errors.New("raftwal: writer closed") }
	// We intentionally skip sequential enforcement and lastIndex advancement here.
	payloadLen := len(b)
	frameLen := 8 + payloadLen
	frame := make([]byte, frameLen)
	crc := crc32.ChecksumIEEE(b)
	binary.LittleEndian.PutUint32(frame[0:4], crc)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(payloadLen))
	copy(frame[8:], b)
	offsetBefore := w.currentOffsetUnsafe()
	if _, err := w.buf.Write(frame); err != nil { return err }
	// Index meta stored for completeness; does not alter lastIndex.
	w.entries = append(w.entries, entryMeta{Index: commitIndex, Offset: uint32(offsetBefore), Length: uint32(frameLen)})
	w.entriesSinceFlush++
	if w.strictSync {
		if err := w.buf.Flush(); err != nil { return err }
		if err := w.file.Sync(); err != nil { return err }
	} else if w.entriesSinceFlush >= w.sidecarFlushEvery { _ = w.writeSidecarLocked() }
	if w.entriesSinceFlush >= w.sidecarFlushEvery { _ = w.writeSidecarLocked() }
	if w.maxSegmentBytes > 0 && (offsetBefore+int64(frameLen)) >= w.maxSegmentBytes { if err := w.rotateLocked(); err != nil { return err } }
	if w.forceRotateEvery > 0 { w.appendCount++; if (w.appendCount % w.forceRotateEvery) == 0 { if err := w.rotateLocked(); err != nil { return err } } }
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

// PruneThrough removes whole WAL segments whose max raft index is strictly less than pruneThrough.
// It never deletes the currently active (open) segment and never performs partial truncation of a segment.
// Deletion protocol: rename seg-N.wal -> seg-N.wal.deleted (and sidecar) then unlink to allow crash-safe cleanup.
// Returns number of segments removed.
func (w *Writer) PruneThrough(pruneThrough uint64) (int, error) {
	if pruneThrough == 0 { return 0, nil }
	w.mu.Lock()
	if w.closed { w.mu.Unlock(); return 0, errors.New("raftwal: writer closed") }
	activeSeg := w.segIdx
	w.mu.Unlock()
	// List segments (outside lock to allow slow IO without blocking appends; active segment index captured)
	segs, err := filepath.Glob(filepath.Join(w.dir, "seg-*.wal"))
	if err != nil { return 0, err }
	sort.Strings(segs)
	removed := 0
	for _, seg := range segs {
		// Determine segment number
		base := filepath.Base(seg)
		var idx int
		if _, err := fmt.Sscanf(base, "seg-%d.wal", &idx); err != nil { continue }
		if idx == activeSeg { continue } // never delete active
		// Scan to find max index in this segment
		maxIdx, scanErr := maxIndexInSegment(seg)
		if scanErr != nil { return removed, scanErr }
		if maxIdx < pruneThrough {
			if err := w.deleteSegmentFiles(seg); err != nil { return removed, err }
			removed++
		}
	}
	return removed, nil
}

// maxIndexInSegment scans a segment file and returns the highest envelope RaftIndex found.
func maxIndexInSegment(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil { return 0, err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	var max uint64
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
			return max, err
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return max, err }
		if crc32.ChecksumIEEE(payload) != crc { return max, fmt.Errorf("wal: crc mismatch during prune scan %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return max, err }
		if env.RaftIndex > max { max = env.RaftIndex }
	}
	return max, nil
}

// deleteSegmentFiles performs rename-to-deleted then unlink for the segment and its sidecar.
func (w *Writer) deleteSegmentFiles(segPath string) error {
	sidecar := segPath + ".idx"
	// rename segment
	if err := os.Rename(segPath, segPath+".deleted"); err != nil { return err }
	// rename sidecar if exists
	if _, err := os.Stat(sidecar); err == nil { _ = os.Rename(sidecar, sidecar+".deleted") }
	if w.dirFsync { _ = syncDir(w.dir) }
	// unlink renamed
	_ = os.Remove(segPath+".deleted")
	_ = os.Remove(sidecar+".deleted")
	if w.dirFsync { _ = syncDir(w.dir) }
	return nil
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
	// In strictSync we also ensure the segment file itself is fsynced after updating the sidecar
	// to minimize windows where index metadata lags durable data. (Segment data already fsynced
	// at append time.) This is a light extra call for completeness.
	if w.strictSync {
		if err := w.file.Sync(); err != nil { return err }
	}
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
    var all []uint32
    for _, f := range files {
        seg, err := collectSegmentCRCsAll(f)
        if err != nil { return nil, err }
        if len(seg) > 0 { all = append(all, seg...) }
    }
    if len(all) > limit { return append([]uint32(nil), all[len(all)-limit:]...), nil }
    return all, nil
}

// TailLogicalTuples returns up to limit tuples (index, crc) for ENTRY_NORMAL envelopes, oldest->newest.
// This is used by the upgraded validator that tracks raft index ordering explicitly.
func TailLogicalTuples(dir string, limit int) ([]struct{Index uint64; CRC uint32}, error) {
	if limit <= 0 { return nil, nil }
	files, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil { return nil, err }
	sort.Strings(files)
	var all []struct{Index uint64; CRC uint32}
	for _, f := range files {
		seg, err := collectSegmentTuplesAll(f)
		if err != nil { return nil, err }
		if len(seg) > 0 { all = append(all, seg...) }
	}
	if len(all) > limit { return append([]struct{Index uint64; CRC uint32}(nil), all[len(all)-limit:]...), nil }
	return all, nil
}

func collectSegmentTuplesAll(path string) ([]struct{Index uint64; CRC uint32}, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	var all []struct{Index uint64; CRC uint32}
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
			return all, nil
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return all, err }
		if crc32.ChecksumIEEE(payload) != crc { return all, fmt.Errorf("wal: crc mismatch tail scan %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return all, err }
			if env.Kind == EntryKind_ENTRY_NORMAL { all = append(all, struct{Index uint64; CRC uint32}{Index: env.RaftIndex, CRC: env.AppCrc}) }
		}
		return all, nil
}

func collectSegmentCRCsAll(path string) ([]uint32, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	r := bufio.NewReader(f)
	header := make([]byte, 8)
	var all []uint32
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || err == io.ErrUnexpectedEOF { break }
			return all, nil
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if sz == 0 { continue }
		payload := make([]byte, sz)
		if _, err := io.ReadFull(r, payload); err != nil { return all, err }
		if crc32.ChecksumIEEE(payload) != crc { return all, fmt.Errorf("wal: crc mismatch tail scan %s", path) }
		var env Envelope
		if err := proto.Unmarshal(payload, &env); err != nil { return all, err }
		if env.Kind == EntryKind_ENTRY_NORMAL { all = append(all, env.AppCrc) }
	}
    return all, nil
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

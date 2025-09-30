package raftwal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
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
}

// Config for Writer initialization.
type Config struct {
	Dir     string
	BufMB   int
}

// NewWriter creates (or continues) the latest segment in shadow mode.
func NewWriter(cfg Config) (*Writer, error) {
	if cfg.Dir == "" { return nil, errors.New("raftwal: empty dir") }
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil { return nil, err }
	w := &Writer{dir: cfg.Dir, bufSize: cfg.BufMB * 1024 * 1024}
	if w.bufSize == 0 { w.bufSize = 1 * 1024 * 1024 }
	if err := w.openLastOrCreate(); err != nil { return nil, err }
	return w, nil
}

func (w *Writer) openLastOrCreate() error {
	// For MVP: always create seg-0.wal if none (no recovery scan yet). Recovery logic will be added later.
	name := filepath.Join(w.dir, "seg-0.wal")
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
	// TODO: enforce sequential raftIndex in future.
	payloadLen := len(b)
	frameLen := 8 + payloadLen
	buf := make([]byte, frameLen)
	crc := crc32.ChecksumIEEE(b)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))
	copy(buf[8:], b)
	if _, err := w.buf.Write(buf); err != nil { return err }
	w.lastIndex = raftIndex
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
	_ = w.file.Sync()
	w.closed = true
	return w.file.Close()
}

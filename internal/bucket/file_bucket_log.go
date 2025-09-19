package bucket

import (
    "bufio"
    "context"
    "encoding/binary"
    "encoding/json"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "log/slog"
    "os"
    "path/filepath"
    "sync"
    "time"

    "github.com/sevenDatabase/SevenDB/config"
)

// File layout (per bucket file bucket-<bucketID>.wal):
// Repeated entries of: | uint32 payloadLen | uint32 crc32(payload) | payload(bytes) |
// Where payload is JSON serialized WALEntry without GlobalOffset & CommitIndex initially
// (CommitIndex & GlobalOffset filled before encoding).
//
// For MVP simplicity we maintain one file per bucket; this avoids interleaving
// and scanning cost per append. A future implementation may switch to a shared
// segment file with an index.
type FileBucketLog struct {
    dir string

    mu sync.Mutex
    // per bucket state (opened lazily)
    files      map[BucketID]*os.File
    writers    map[BucketID]*bufio.Writer
    commitIdx  map[BucketID]uint64 // last assigned commit index per bucket
    epochCount map[BucketID]uint64 // current epoch counter per bucket
}

func NewFileBucketLog() (*FileBucketLog, error) {
    dir := filepath.Join(config.Config.WALDir, "buckets")
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, fmt.Errorf("create bucket wal dir: %w", err)
    }
    fbl := &FileBucketLog{
        dir:       dir,
        files:     map[BucketID]*os.File{},
        writers:   map[BucketID]*bufio.Writer{},
        commitIdx: map[BucketID]uint64{},
        epochCount: map[BucketID]uint64{},
    }
    return fbl, nil
}

// openBucket loads/opens a bucket file (creating if absent) and, if newly
// created or commitIdx unknown, scans the tail to determine last commit index.
func (f *FileBucketLog) openBucket(b BucketID) error {
    if _, ok := f.files[b]; ok {
        return nil
    }
    path := filepath.Join(f.dir, fmt.Sprintf("bucket-%s.wal", string(b)))
    file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
    if err != nil {
        return fmt.Errorf("open bucket file: %w", err)
    }
    // Determine last commit index by scanning file once (MVP). For large files
    // this is O(n); can be optimized with an index or trailer later.
    var last uint64
    reader := bufio.NewReader(file)
    for {
        header := make([]byte, 8)
        if _, err := io.ReadFull(reader, header); err != nil {
            if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
                break
            }
            file.Close()
            return fmt.Errorf("scan bucket %s: %w", b, err)
        }
        ln := binary.LittleEndian.Uint32(header[0:4])
        crc := binary.LittleEndian.Uint32(header[4:8])
        if ln == 0 { // corruption or placeholder
            break
        }
        buf := make([]byte, ln)
        if _, err := io.ReadFull(reader, buf); err != nil {
            if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
                break
            }
            file.Close()
            return fmt.Errorf("scan payload bucket %s: %w", b, err)
        }
        if crc32.ChecksumIEEE(buf) != crc {
            slog.Error("crc mismatch while scanning bucket log", slog.String("bucket", string(b)))
            break // stop at first corruption; truncate logic could be added later
        }
        var e WALEntry
        if err := json.Unmarshal(buf, &e); err != nil {
            slog.Error("unmarshal wal entry", slog.String("bucket", string(b)), slog.Any("error", err))
            continue
        }
        if e.CommitIndex > last {
            last = e.CommitIndex
        }
    }
    // Seek to end for appends
    if _, err := file.Seek(0, io.SeekEnd); err != nil {
        file.Close()
        return fmt.Errorf("seek end: %w", err)
    }
    f.files[b] = file
    f.writers[b] = bufio.NewWriterSize(file, 128*1024) // small buffer; tune later
    f.commitIdx[b] = last
    return nil
}

func (f *FileBucketLog) Append(ctx context.Context, entry *WALEntry) (uint64, uint64, error) {
    f.mu.Lock()
    defer f.mu.Unlock()

    if entry == nil {
        return 0, 0, fmt.Errorf("nil entry")
    }
    if err := f.openBucket(entry.BucketID); err != nil {
        return 0, 0, err
    }
    // Assign commit index & timestamp
    entry.CommitIndex = f.commitIdx[entry.BucketID] + 1
    entry.Timestamp = time.Now().UnixNano()
    // Stamp epoch deterministically from internal counter; caller supplied counter ignored for safety.
    entry.Epoch = EpochID{Bucket: entry.BucketID, Counter: f.epochCount[entry.BucketID]}

    bw := f.writers[entry.BucketID]
    file := f.files[entry.BucketID]

    // FileOffset = current file size (seek)
    off, err := file.Seek(0, io.SeekCurrent)
    if err != nil {
        return 0, 0, fmt.Errorf("seek current: %w", err)
    }
    entry.FileOffset = uint64(off)

    payload, err := json.Marshal(entry)
    if err != nil {
        return 0, 0, fmt.Errorf("marshal entry: %w", err)
    }
    if len(payload) > int(^uint32(0)>>1) { // very defensive
        return 0, 0, fmt.Errorf("entry too large: %d", len(payload))
    }
    header := make([]byte, 8)
    binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
    binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

    if _, err := bw.Write(header); err != nil {
        return 0, 0, fmt.Errorf("write header: %w", err)
    }
    if _, err := bw.Write(payload); err != nil {
        return 0, 0, fmt.Errorf("write payload: %w", err)
    }
    if err := bw.Flush(); err != nil { // ensure durability ordering semantics for MVP
        return 0, 0, fmt.Errorf("flush: %w", err)
    }
    if err := file.Sync(); err != nil { // fsync per entry (slow); optimize later with group commit
        return 0, 0, fmt.Errorf("fsync: %w", err)
    }
    f.commitIdx[entry.BucketID] = entry.CommitIndex
    return entry.CommitIndex, entry.FileOffset, nil
}

// Read streams entries for a bucket starting at fromCommitIndex. Caller must
// drain channel until closed. Cancelling ctx stops early.
func (f *FileBucketLog) Read(ctx context.Context, bucket BucketID, fromCommitIndex uint64) (<-chan *WALEntry, error) {
    f.mu.Lock()
    if err := f.openBucket(bucket); err != nil {
        f.mu.Unlock()
        return nil, err
    }
    // Duplicate file handle for read so concurrent appends can proceed.
    path := filepath.Join(f.dir, fmt.Sprintf("bucket-%s.wal", string(bucket)))
    rf, err := os.Open(path)
    if err != nil {
        f.mu.Unlock()
        return nil, fmt.Errorf("open for read: %w", err)
    }
    f.mu.Unlock()

    out := make(chan *WALEntry, 64)
    go func() {
        defer close(out)
        defer rf.Close()
        reader := bufio.NewReader(rf)
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }
            header := make([]byte, 8)
            if _, err := io.ReadFull(reader, header); err != nil {
                if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
                    return
                }
                slog.Error("bucket log read header", slog.String("bucket", string(bucket)), slog.Any("error", err))
                return
            }
            ln := binary.LittleEndian.Uint32(header[0:4])
            crc := binary.LittleEndian.Uint32(header[4:8])
            buf := make([]byte, ln)
            if _, err := io.ReadFull(reader, buf); err != nil {
                if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
                    return
                }
                slog.Error("bucket log read payload", slog.String("bucket", string(bucket)), slog.Any("error", err))
                return
            }
            if crc32.ChecksumIEEE(buf) != crc {
                slog.Error("bucket log crc mismatch", slog.String("bucket", string(bucket)))
                return
            }
            var e WALEntry
            if err := json.Unmarshal(buf, &e); err != nil {
                slog.Error("bucket log unmarshal", slog.String("bucket", string(bucket)), slog.Any("error", err))
                continue
            }
            if e.CommitIndex < fromCommitIndex {
                continue
            }
            select {
            case out <- &e:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out, nil
}

// Snapshot is a stub for the MVP – returns a generated snapshot ID without creating an actual snapshot.
func (f *FileBucketLog) Snapshot(ctx context.Context, bucket BucketID) (string, error) {
    return fmt.Sprintf("snapshot-%s-%d", bucket, time.Now().UnixNano()), nil
}

// Compact currently a no-op (MVP). Future work: rewrite file excluding entries before beforeCommitIndex.
func (f *FileBucketLog) Compact(ctx context.Context, bucket BucketID, beforeCommitIndex uint64) error {
    return nil
}

func (f *FileBucketLog) Close() error {
    f.mu.Lock()
    defer f.mu.Unlock()
    for b, w := range f.writers {
        _ = w.Flush()
        if f.files[b] != nil {
            _ = f.files[b].Close()
        }
    }
    return nil
}

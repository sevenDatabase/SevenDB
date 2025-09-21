package raft

import (
    "bufio"
    "encoding/base64"
    "encoding/json"
    "errors"
    "io"
    "log/slog"
    "os"
    "path/filepath"
    "sync"
    "strings"

    gogoproto "github.com/gogo/protobuf/proto"
    etcdraft "go.etcd.io/etcd/raft/v3"
    "go.etcd.io/etcd/raft/v3/raftpb"
)

type raftPersistence struct {
    dir          string
    shardID      string
    entriesPath  string
    hsPath       string
    snapPath     string
    mu           sync.Mutex
}

func newRaftPersistence(baseDir, shardID string) (*raftPersistence, error) {
    d := filepath.Join(baseDir, shardID)
    if err := os.MkdirAll(d, 0o755); err != nil { return nil, err }
    return &raftPersistence{dir: d, shardID: shardID, entriesPath: filepath.Join(d, "entries.log"), hsPath: filepath.Join(d, "hardstate.json"), snapPath: filepath.Join(d, "snapshot.bin")}, nil
}

// Load previous snapshot, hardstate, and entries into storage. Returns true if any state loaded.
func (p *raftPersistence) Load(storage *etcdraft.MemoryStorage) (bool, error) {
    p.mu.Lock(); defer p.mu.Unlock()
    loaded := false
    // Snapshot
    if b, err := os.ReadFile(p.snapPath); err == nil && len(b) > 0 {
        var snap raftpb.Snapshot
        if err := gogoproto.Unmarshal(b, &snap); err == nil && !etcdraft.IsEmptySnap(snap) {
            if err := storage.ApplySnapshot(snap); err != nil {
                slog.Warn("apply snapshot failed", slog.String("shard", p.shardID), slog.Any("error", err))
            } else { loaded = true }
        }
    }
    // HardState
    if b, err := os.ReadFile(p.hsPath); err == nil && len(b) > 0 {
        var hs raftpb.HardState
        if err := json.Unmarshal(b, &hs); err == nil && !etcdraft.IsEmptyHardState(hs) {
            storage.SetHardState(hs); loaded = true
        }
    }
    // Entries
    f, err := os.Open(p.entriesPath)
    if err == nil {
        defer f.Close()
        r := bufio.NewReader(f)
        var entries []raftpb.Entry
        for {
            line, err := r.ReadString('\n')
            if errors.Is(err, io.EOF) && line == "" { break }
            if line == "" { break }
            if len(line) > 0 && line[len(line)-1] == '\n' { line = line[:len(line)-1] }
            if line == "" { if errors.Is(err, io.EOF) { break }; continue }
            b, decErr := base64.StdEncoding.DecodeString(line)
            if decErr != nil { continue }
            var e raftpb.Entry
            if uErr := gogoproto.Unmarshal(b, &e); uErr == nil {
                entries = append(entries, e)
            }
            if errors.Is(err, io.EOF) { break }
        }
        if len(entries) > 0 {
            if err := storage.Append(entries); err == nil { loaded = true }
        }
    }
    return loaded, nil
}

// Persist writes raft Ready components (HardState, Entries, Snapshot) atomically enough for MVP.
func (p *raftPersistence) Persist(rd *etcdraft.Ready) error {
    p.mu.Lock(); defer p.mu.Unlock()
    // Snapshot
    if !etcdraft.IsEmptySnap(rd.Snapshot) && rd.Snapshot.Metadata.Index > 0 {
        if err := p.writeSnapshot(&rd.Snapshot); err != nil { return err }
    }
    // HardState
    if !etcdraft.IsEmptyHardState(rd.HardState) {
        if err := os.WriteFile(p.hsPath+".tmp", mustJSON(&rd.HardState), 0o644); err == nil {
            _ = os.Rename(p.hsPath+".tmp", p.hsPath)
        }
    }
    // Entries
    if len(rd.Entries) > 0 {
        f, err := os.OpenFile(p.entriesPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
        if err != nil { return err }
        bw := bufio.NewWriter(f)
        for _, e := range rd.Entries {
            b, _ := gogoproto.Marshal(&e)
            enc := base64.StdEncoding.EncodeToString(b)
            _, _ = bw.WriteString(enc+"\n")
        }
        _ = bw.Flush()
        _ = f.Close()
    }
    return nil
}

// PersistSnapshot persists a snapshot produced outside of Ready (our manual trigger).
func (p *raftPersistence) PersistSnapshot(snap *raftpb.Snapshot) error {
    p.mu.Lock(); defer p.mu.Unlock()
    return p.writeSnapshot(snap)
}

func (p *raftPersistence) writeSnapshot(snap *raftpb.Snapshot) error {
    b, err := gogoproto.Marshal(snap)
    if err != nil { return err }
    tmp := p.snapPath+".tmp"
    if err := os.WriteFile(tmp, b, 0o644); err != nil { return err }
    return os.Rename(tmp, p.snapPath)
}

// PruneEntries rewrites the entries log keeping only entries with index > upto.
// Best-effort; on failure the original file is left intact. This is a coarse
// approach (line scan) adequate for MVP scale. Future optimization: segment the log.
func (p *raftPersistence) PruneEntries(upto uint64) error {
    p.mu.Lock(); defer p.mu.Unlock()
    src, err := os.Open(p.entriesPath)
    if err != nil { return err }
    defer src.Close()
    tmpPath := p.entriesPath + ".compact.tmp"
    tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
    if err != nil { return err }
    w := bufio.NewWriter(tmp)
    r := bufio.NewReader(src)
    for {
        line, err := r.ReadString('\n')
        if line == "" && errors.Is(err, io.EOF) { break }
        if line == "" { if errors.Is(err, io.EOF) { break }; continue }
        // Trim newline
        if line[len(line)-1] == '\n' { line = line[:len(line)-1] }
        if line == "" { if errors.Is(err, io.EOF) { break }; continue }
        b, decErr := base64.StdEncoding.DecodeString(line)
        if decErr != nil { continue }
        var e raftpb.Entry
        if uErr := gogoproto.Unmarshal(b, &e); uErr != nil { continue }
        if e.Index <= upto { continue }
        // Write original line back (ensure newline)
        _, _ = w.WriteString(strings.TrimRight(line, "\n") + "\n")
        if errors.Is(err, io.EOF) { break }
    }
    _ = w.Flush()
    _ = tmp.Close()
    // Atomic replace
    if err := os.Rename(tmpPath, p.entriesPath); err != nil {
        // Best-effort cleanup
        _ = os.Remove(tmpPath)
        return err
    }
    return nil
}

// PruneEntries rewrites the entries log keeping only entries with Index > upto (inclusive prune).
// Called after successful snapshot / in-memory compaction. Best-effort: on error, we log and leave file un-pruned.
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

package wal

import (
    "bufio"
    "crypto/sha256"
    "encoding/binary"
    "io"
    "os"
    "path/filepath"
    "testing"

    w "github.com/dicedb/dicedb-go/wal"
    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
    "google.golang.org/protobuf/proto"
)

// helper to configure forge WAL pointing at a temp wal dir
func setupForgeWALForTest(t *testing.T, walDir string) {
    t.Helper()
    old := config.Config
    config.Config.WALVariant = "forge"
    config.Config.WALDir = walDir
    config.Config.WALBufferSizeMB = 1
    config.Config.WALBufferSyncIntervalMillis = 5
    config.Config.WALSegmentRotationTimeSec = 1 // positive; tickers are created unconditionally
    config.Config.WALMaxSegmentSizeMB = 16
    config.Config.WALAutoCreateManifest = true
    config.Config.WALManifestEnforce = string(EnforceWarn)
    t.Cleanup(func(){ config.Config = old })
}

func readFirstElement(t *testing.T, segPath string) *w.Element {
    t.Helper()
    f, err := os.Open(segPath)
    if err != nil { t.Fatalf("open seg: %v", err) }
    defer f.Close()
    r := bufio.NewReader(f)
    hdr := make([]byte, 8)
    if _, err := io.ReadFull(r, hdr); err != nil { t.Fatalf("read hdr: %v", err) }
    sz := binary.LittleEndian.Uint32(hdr[4:8])
    payload := make([]byte, sz)
    if _, err := io.ReadFull(r, payload); err != nil { t.Fatalf("read payload: %v", err) }
    var el w.Element
    if err := proto.Unmarshal(payload, &el); err != nil { t.Fatalf("unmarshal el: %v", err) }
    return &el
}

func TestManifestPreambleWrittenAndMatchesHash(t *testing.T) {
    dir := t.TempDir()
    walDir := filepath.Join(dir, "wal")
    if err := os.MkdirAll(walDir, 0o755); err != nil { t.Fatalf("mkdir wal: %v", err) }
    setupForgeWALForTest(t, walDir)

    SetupWAL()
    // Ensure any buffered preamble is flushed
    if s, ok := DefaultWAL.(interface{ Sync() error }); ok { _ = s.Sync() }
    t.Cleanup(func(){ if DefaultWAL != nil { DefaultWAL.Stop() } })

    // Compute expected manifest hash from file contents
    manPath := filepath.Join(walDir, manifestFileName)
    b, err := os.ReadFile(manPath)
    if err != nil { t.Fatalf("read manifest: %v", err) }
    sum := sha256.Sum256(b)

    // Read first element of first segment and verify it's the manifest preamble with matching hash
    segPath := filepath.Join(walDir, segmentPrefix+"0"+".wal")
    // Wait briefly for writer to flush bytes in case of scheduling delays
    for i := 0; i < 10; i++ {
        if fi, err := os.Stat(segPath); err == nil && fi.Size() >= 8 {
            break
        }
    }
    el := readFirstElement(t, segPath)
    if el.ElementType != w.ElementType_ELEMENT_TYPE_COMMAND { t.Fatalf("unexpected element type: %v", el.ElementType) }
    if !isUWAL(el.Payload) { t.Fatalf("first payload not UWAL") }
    kind, _, _, _, inner, _, err := decodeUWAL(el.Payload)
    if err != nil { t.Fatalf("decode uwal: %v", err) }
    if kind != uwalKindManifest { t.Fatalf("first entry not manifest preamble: kind=%d", kind) }
    if string(inner) != string(sum[:]) { t.Fatalf("manifest hash mismatch: wal=%x want=%x", inner, sum[:]) }
}

func TestManifestPreambleMismatchStrictFailsInit(t *testing.T) {
    dir := t.TempDir()
    walDir := filepath.Join(dir, "wal")
    if err := os.MkdirAll(walDir, 0o755); err != nil { t.Fatalf("mkdir wal: %v", err) }
    setupForgeWALForTest(t, walDir)

    // First init in warn mode to create manifest and preamble
    wf := newWalForge()
    if err := wf.Init(); err != nil { t.Fatalf("init warn: %v", err) }
    wf.Stop()

    // Load manifest, flip to strict and change writerVersion to alter hash, save atomically
    mf, _, err := LoadManifest(walDir)
    if err != nil || mf == nil { t.Fatalf("load manifest: %v, mf=%v", err, mf) }
    mf.Enforce = EnforceStrict
    mf.WriterVersion = mf.WriterVersion + "-changed"
    if _, err := SaveManifestAtomic(walDir, mf); err != nil { t.Fatalf("save manifest: %v", err) }

    // Now a fresh Init should fail due to preamble mismatch in strict mode
    wf2 := newWalForge()
    if err := wf2.Init(); err == nil {
        wf2.Stop()
        t.Fatalf("expected strict mismatch error, got nil")
    }
}

func TestManifestPreambleSkippedInReplayAndLastIndex(t *testing.T) {
    dir := t.TempDir()
    walDir := filepath.Join(dir, "wal")
    if err := os.MkdirAll(walDir, 0o755); err != nil { t.Fatalf("mkdir wal: %v", err) }
    setupForgeWALForTest(t, walDir)

    SetupWAL()
    t.Cleanup(func(){ if DefaultWAL != nil { DefaultWAL.Stop() } })

    uw, ok := DefaultWAL.(UnifiedWAL)
    if !ok || uw == nil { t.Fatalf("default wal not unified/forge") }

    // Append a single normal entry and sync
    cmd := &wire.Command{Cmd: "SET", Args: []string{"k", "v"}}
    if err := uw.AppendEntry(EntryKindNormal, 42, 7, 0, cmd, nil, []byte("rv")); err != nil {
        t.Fatalf("append: %v", err)
    }
    if s, ok := DefaultWAL.(interface{ Sync() error }); ok { _ = s.Sync() }

    // ReplayItems should yield only the normal entry (manifest preamble is skipped)
    count := 0
    var last ReplayItem
    if err := uw.ReplayItems(func(ri ReplayItem) error { count++; last = ri; return nil }); err != nil { t.Fatalf("replay: %v", err) }
    if count < 1 { t.Fatalf("expected at least 1 replay item, got %d", count) }
    if last.Index != 42 || last.Term != 7 { t.Fatalf("unexpected last replay item: %+v", last) }

    // lastIndexOfSegment should ignore the preamble and return 42 for seg-0
    wf, ok := DefaultWAL.(*walForge)
    if !ok { t.Fatalf("default wal not *walForge") }
    segPath := filepath.Join(walDir, segmentPrefix+"0"+".wal")
    idx, err := wf.lastIndexOfSegment(segPath)
    if err != nil { t.Fatalf("lastIndexOfSegment: %v", err) }
    if idx != 42 { t.Fatalf("last index mismatch: got %d want %d", idx, 42) }
}

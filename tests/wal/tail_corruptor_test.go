package wal_test

import (
    "io"
    "os"
    "path/filepath"
    "strings"
    "testing"

    "github.com/sevenDatabase/SevenDB/config"
)

// CorruptOrDuplicateTail duplicates the last valid frame or corrupts the last frame's CRC.
// This is a test-only helper and should be used only from tests that are already stopped or paused.
func CorruptOrDuplicateTail(t *testing.T, duplicate bool) {
    t.Helper()
    dir := config.Config.WALDir
    pattern := filepath.Join(dir, "seg-*.wal")
    files, err := filepath.Glob(pattern)
    if err != nil || len(files) == 0 {
        t.Fatalf("no wal segments found: %v", err)
    }
    // pick last numerically by index
    last := files[0]
    for _, f := range files[1:] {
        bi := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(last), "seg-"), ".wal")
        bj := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "seg-"), ".wal")
        if bi < bj { // lexical is fine since both are numeric-like strings; if not, adjust parsing
            last = f
        }
    }
    // Open read-write to alter tail
    f, err := os.OpenFile(last, os.O_RDWR, 0644)
    if err != nil {
        t.Fatalf("open last segment: %v", err)
    }
    defer f.Close()
    // Read entire file to locate last frame boundary by scanning headers
    data, err := io.ReadAll(f)
    if err != nil {
        t.Fatalf("read file: %v", err)
    }
    // Frames are: 4 bytes CRC + 4 bytes length + payload
    off := 0
    lastStart := -1
    lastLen := 0
    for off+8 <= len(data) {
        sz := int(uint32(data[off+4]) | uint32(data[off+5])<<8 | uint32(data[off+6])<<16 | uint32(data[off+7])<<24)
        if off+8+sz > len(data) {
            break
        }
        lastStart = off
        lastLen = 8 + sz
        off += 8 + sz
    }
    if lastStart < 0 {
        t.Fatalf("no complete frame found in %s", last)
    }
    if duplicate {
        // append a duplicate of the last frame to the end
        if _, err := f.WriteAt(data[lastStart:lastStart+lastLen], int64(len(data)));
            err != nil {
            t.Fatalf("append duplicate: %v", err)
        }
    } else {
        // corrupt CRC: flip a bit in the first CRC byte
        if data[lastStart] == 0xFF {
            data[lastStart] = 0x00
        } else {
            data[lastStart] ^= 0xFF
        }
        if _, err := f.WriteAt(data[lastStart:lastStart+1], int64(lastStart)); err != nil {
            t.Fatalf("write corrupt crc: %v", err)
        }
    }
    if err := f.Sync(); err != nil {
        t.Fatalf("sync: %v", err)
    }
    t.Logf("tail %s on %s", map[bool]string{true: "duplicated", false: "corrupted"}[duplicate], filepath.Base(last))
}

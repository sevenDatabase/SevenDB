// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sevenDatabase/SevenDB/config"
	"google.golang.org/protobuf/proto"

	w "github.com/dicedb/dicedb-go/wal"
	"github.com/dicedb/dicedb-go/wire"
)

const (
	segmentPrefix = "seg-"
)

var bb []byte

func init() {
	// Pre-allocate a buffer to avoid re-allocating it
	// This will hold one WAL Forge Entry Before it is written to the buffer
	bb = make([]byte, 10*1024)
}

type walForge struct {
	// Current Segment File and its writer
	csf      *os.File
	csWriter *bufio.Writer
	csIdx    int
	csSize   uint32

	// TODO: Come up with a way to generate a LSN that is
	// monotonically increasing and even after restart it
	// resumes from the last LSN and not start from 0.
	lsn uint64

	maxSegmentSizeBytes uint32

	bufferSyncTicker      *time.Ticker
	segmentRotationTicker *time.Ticker

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	// testClock allows tests to inject a deterministic time source for features
	// like replay budget. If nil, time.Now() is used directly.
	testClock func() time.Time

	// hardstate persistence (unified WAL behavior)
	// hsPath is the canonical file where the last durable HardState is stored.
	// pendingHS buffers the next HardState bytes to persist at the next Sync() barrier.
	hsPath   string
	pendingHS []byte

	// manifest holds the loaded/created manifest and hash for enforcement and UWAL headers
	manifest     *Manifest
	manifestHash []byte
}

// Test-only hooks (safe no-ops in production)
// Set these from tests to simulate failures and race conditions deterministically.
// Called after buffered data is flushed but before fsync to disk.
// For example, tests can set TestHookAfterDataBeforeFsync = func(){ panic("inject:after-data-before-fsync") }
var TestHookAfterDataBeforeFsync func()

// Placeholder for future HardState co-fsync testing when HS is written.
// If/when HS is persisted separately, invoke this hook after HS is durable in
// memory/buffer but before the final fsync that groups HS with the last entry.
var TestHookAfterHSBeforeFsync func()

func newWalForge() *walForge {
	ctx, cancel := context.WithCancel(context.Background())
	return &walForge{
		ctx:    ctx,
		cancel: cancel,

		bufferSyncTicker:      time.NewTicker(time.Duration(config.Config.WALBufferSyncIntervalMillis) * time.Millisecond),
		segmentRotationTicker: time.NewTicker(time.Duration(config.Config.WALSegmentRotationTimeSec) * time.Second),

		maxSegmentSizeBytes: uint32(config.Config.WALMaxSegmentSizeMB) * 1024 * 1024,
	}
}

// Ensure walForge implements UnifiedWAL (compile-time check via var assignment)
var _ UnifiedWAL = (*walForge)(nil)

// AppendEntry appends a unified entry. NORMAL entries are written using a
// lightweight envelope inside the Forge Element payload that carries raft
// metadata (index/term/subSeq). HARDSTATE bytes are staged for co-fsync via
// sync() and persisted separately in wl.hsPath.
func (wl *walForge) AppendEntry(kind EntryKind, index uint64, term uint64, subSeq uint32, cmd *wire.Command, hardState []byte, raftData []byte) error {
	switch kind {
	case EntryKindNormal:
		// Enforce manifest policy: in strict legacy/mixed modes, disallow UWAL appends
		if wl.manifest != nil && wl.manifest.Enforce == EnforceStrict && !wl.manifest.IsUWAL1() {
			return fmt.Errorf("wal manifest(strict %s) forbids UWAL appends: migrate or relax policy", wl.manifest.Format)
		}
		if cmd == nil {
			return fmt.Errorf("AppendEntry: nil command for normal entry")
		}
		// Marshal command and wrap in unified envelope, then wrap in Element
		b, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
	payload := encodeUWAL(uwalKindNormal, index, term, subSeq, b, raftData)
		wl.mu.Lock()
		defer wl.mu.Unlock()
		wl.lsn += 1
		ts := time.Now().UnixNano()
		if wl.testClock != nil { ts = wl.testClock().UnixNano() }
		el := &w.Element{Lsn: wl.lsn, Timestamp: ts, ElementType: w.ElementType_ELEMENT_TYPE_COMMAND, Payload: payload}
	eb, err := proto.Marshal(el)
	if err != nil { return err }
		return wl.writePayloadLocked(eb)
	case EntryKindHardState:
		if len(hardState) == 0 {
			return fmt.Errorf("AppendEntry: empty hardState payload")
		}
		wl.mu.Lock()
		// Stage the hardstate to be made durable on next Sync() barrier.
		wl.pendingHS = append(wl.pendingHS[:0], hardState...)
		wl.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("AppendEntry: unknown kind %d", kind)
	}
}

// ReplayItems maps existing ReplayCommand to ReplayItem for normal commands.
func (wl *walForge) ReplayItems(cb func(ReplayItem) error) error {
	var crc, entrySize uint32
	var el w.Element

	// Buffers to hold the header and the element bytes
	bb1h := make([]byte, 8)
	bb1ElementBytes := make([]byte, 10*1024)

	// Get list of segment files ordered by index ascending
	segments, err := wl.segments()
	if err != nil {
		return fmt.Errorf("error getting wal-segment files: %w", err)
	}

	// Process each segment file in order
	for _, segment := range segments {
		file, err := os.Open(segment)
		if err != nil {
			return fmt.Errorf("error opening wal-segment file %s: %w", segment, err)
		}

		reader := bufio.NewReader(file)
		// Format: CRC32 (4 bytes) | Size of WAL entry (4 bytes) | WAL data
		for {
			if _, err := io.ReadFull(reader, bb1h); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				return fmt.Errorf("error reading WAL: %w", err)
			}
			crc = binary.LittleEndian.Uint32(bb1h[0:4])
			entrySize = binary.LittleEndian.Uint32(bb1h[4:8])

			if _, err := io.ReadFull(reader, bb1ElementBytes[:entrySize]); err != nil {
				file.Close()
				return fmt.Errorf("error reading WAL data: %w", err)
			}

			expectedCRC := crc32.ChecksumIEEE(bb1ElementBytes[:entrySize])
			if crc != expectedCRC {
				file.Close()
				return fmt.Errorf("CRC32 mismatch: expected %d, got %d", crc, expectedCRC)
			}

			if err := proto.Unmarshal(bb1ElementBytes[:entrySize], &el); err != nil {
				file.Close()
				return fmt.Errorf("error unmarshaling WAL entry: %w", err)
			}

			// Only process command entries for now
			if el.ElementType != w.ElementType_ELEMENT_TYPE_COMMAND {
				continue
			}

			// Detect unified envelope vs legacy raw command
			var item ReplayItem
			if isUWAL(el.Payload) {
				kind, idx, term, sub, inner, raftBytes, derr := decodeUWAL(el.Payload)
				if derr != nil {
					file.Close()
					return fmt.Errorf("decode uwal: %w", derr)
				}
				if kind == uwalKindManifest {
					// Skip manifest preamble entries
					continue
				}
				if kind == uwalKindHardState {
					// HardState envelopes (if ever present) are not yielded as commands
					continue
				}
				var c wire.Command
				if err := proto.Unmarshal(inner, &c); err != nil {
					file.Close()
					return fmt.Errorf("error unmarshaling uwal command: %w", err)
				}
				item = ReplayItem{Kind: EntryKindNormal, Index: idx, Term: term, SubSeq: sub, Timestamp: el.Timestamp, Cmd: &c, RaftData: raftBytes}
			} else {
				var c wire.Command
				if err := proto.Unmarshal(el.Payload, &c); err != nil {
					file.Close()
					return fmt.Errorf("error unmarshaling command: %w", err)
				}
				item = ReplayItem{Kind: EntryKindNormal, Timestamp: el.Timestamp, Cmd: &c, RaftData: el.Payload}
			}
			if err := cb(item); err != nil {
				file.Close()
				return fmt.Errorf("error replaying item: %w", err)
			}
		}
	}
	return nil
}

// ReplayLastHardState is not yet persisted by walForge; return not found.
func (wl *walForge) ReplayLastHardState() ([]byte, bool, error) {
	// Read the last persisted hardstate from hsPath if present.
	// No locking: reading a fully persisted file is safe. If a concurrent Sync is renaming,
	// either we read the old file or the new one; both are valid. We do not read temp files.
	if wl.hsPath == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(wl.hsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return b, true, nil
}

// PruneThrough removes fully durable segments whose last entry position (LSN) is
// less than or equal to the provided index. It serializes with appends via wl.mu
// and performs crash-safe deletion (rename + dir fsync + unlink + dir fsync).
// Note: Currently we treat the 'index' parameter as a WAL LSN. Once unified
// raft metadata is embedded, the caller can pass the appropriate mapping.
func (wl *walForge) PruneThrough(index uint64) error {
	// Limit work per invocation to bound blocking time in raft ready loop.
	const pruneBudgetSegments = 1

	wl.mu.Lock()
	// Ensure buffered data is durable before pruning old segments
	if err := wl.sync(); err != nil {
		wl.mu.Unlock()
		return err
	}
	// Snapshot candidate list under lock, then release lock before IO-heavy work.
	segments, err := wl.segments()
	if err != nil {
		wl.mu.Unlock()
		return err
	}
	// Collect candidates strictly older than current open segment.
	candidates := make([]string, 0, len(segments))
	for _, seg := range segments {
		base := filepath.Base(seg)
		idxStr := strings.TrimSuffix(strings.TrimPrefix(base, segmentPrefix), ".wal")
		sIdx, _ := strconv.Atoi(idxStr)
		if sIdx < wl.csIdx { // exclude current open segment and any newer
			candidates = append(candidates, seg)
		}
	}
	wl.mu.Unlock()

	if len(candidates) == 0 {
		return nil
	}

	// Helper to fsync directory after metadata changes
	fsyncDir := func() error {
		dir, err := os.Open(config.Config.WALDir)
		if err != nil {
			return err
		}
		if err := dir.Sync(); err != nil {
			dir.Close()
			return err
		}
		return dir.Close()
	}

	removed := 0
	for _, seg := range candidates {
		if removed >= pruneBudgetSegments {
			break
		}
		base := filepath.Base(seg)
		last, err := wl.lastIndexOfSegment(seg)
		if err != nil {
			slog.Error("prune: failed to read last LSN of segment", slog.String("segment", base), slog.Any("error", err))
			continue
		}
		if last == 0 || last > index {
			// Not safe to delete yet
			continue
		}
		renamed := seg + ".deleted"
		if err := os.Rename(seg, renamed); err != nil {
			slog.Error("prune: rename failed", slog.String("segment", base), slog.Any("error", err))
			continue
		}
		if err := fsyncDir(); err != nil {
			slog.Error("prune: dir fsync after rename failed", slog.Any("error", err))
			_ = os.Rename(renamed, seg)
			_ = fsyncDir()
			continue
		}
		if err := os.Remove(renamed); err != nil {
			slog.Error("prune: unlink failed", slog.String("segment", filepath.Base(renamed)), slog.Any("error", err))
			_ = os.Rename(renamed, seg)
			_ = fsyncDir()
			continue
		}
		if err := fsyncDir(); err != nil {
			slog.Error("prune: dir fsync after unlink failed", slog.Any("error", err))
		}
		removed++
	}
	return nil
}

// lastIndexOfSegment scans a segment and returns the last raft index observed
// from unified envelopes. Falls back to physical LSN when envelope metadata is
// absent. It tolerates tail corruption by returning the last successfully
// verified entry.
func (wl *walForge) lastIndexOfSegment(path string) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var last uint64
	header := make([]byte, 8)
	buf := make([]byte, 10*1024)
	var el w.Element
	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				// partial header: ignore tail
				break
			}
			return last, err
		}
		crc := binary.LittleEndian.Uint32(header[:4])
		sz := binary.LittleEndian.Uint32(header[4:8])
		if int(sz) > len(buf) {
			buf = make([]byte, sz)
		}
		if _, err := io.ReadFull(reader, buf[:sz]); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				// partial payload: ignore tail
				break
			}
			return last, err
		}
		if crc32.ChecksumIEEE(buf[:sz]) != crc {
			// corrupted tail; stop here
			break
		}
		if err := proto.Unmarshal(buf[:sz], &el); err != nil {
			// corrupted tail; stop here
			break
		}
		// Prefer raft index from unified envelope when present
		if el.ElementType == w.ElementType_ELEMENT_TYPE_COMMAND && isUWAL(el.Payload) {
			kind, idx, _, _, _, _, derr := decodeUWAL(el.Payload)
			if derr == nil {
				if kind != uwalKindManifest { // ignore preamble entries
					last = idx
				}
				continue
			}
		}
		// Fallback: use physical LSN
		last = el.GetLsn()
	}
	return last, nil
}

// Dir returns the configured WAL directory.
func (wl *walForge) Dir() string { return config.Config.WALDir }

func (wl *walForge) Init() error {
	// TODO: Once the checkpoint is implemented
	// Load the initial state of the database from this checkpoint
	// and then reply the WAL files that happened after this checkpoint.

	// Make sure the WAL directory exists
	if err := os.MkdirAll(config.Config.WALDir, 0755); err != nil {
		return err
	}

	// Initialize hardstate path under the WAL directory
	wl.hsPath = filepath.Join(config.Config.WALDir, "hardstate")

	// Load or create manifest and enforce on startup
	{
		mf, hash, err := LoadManifest(config.Config.WALDir)
		if err != nil {
			return fmt.Errorf("manifest load: %w", err)
		}
		if mf == nil {
			// Detect on missing manifest
			detected, derr := DetectFormat(config.Config.WALDir, 2, 16)
			if derr != nil { return fmt.Errorf("manifest detect: %w", derr) }
			// Enforce config policies
			if config.Config.WALRequireUWAL1 && detected != WALFormatUWAL1 {
				return fmt.Errorf("wal requires UWAL1, but detected %s without manifest; refuse startup (set wal-require-uwal1=false to allow)", detected)
			}
			if config.Config.WALAutoCreateManifest {
				enforce := EnforceWarn
				if config.Config.WALManifestEnforce == string(EnforceStrict) { enforce = EnforceStrict }
				mf = &Manifest{SchemaVersion: 1, Format: detected, UWALVersion: func() int { if detected==WALFormatUWAL1 {return 1}; return 0 }(), Enforce: enforce}
				hash, err = SaveManifestAtomic(config.Config.WALDir, mf)
				if err != nil { return fmt.Errorf("manifest save: %w", err) }
			} else {
				// No manifest created; proceed, but treat as warn mode in memory
				mf = &Manifest{SchemaVersion: 1, Format: detected, UWALVersion: func() int { if detected==WALFormatUWAL1 {return 1}; return 0 }(), Enforce: EnforceWarn}
			}
		} else {
			// Verify directory format against manifest
			detected, derr := DetectFormat(config.Config.WALDir, 2, 16)
			if derr == nil {
				if verr := mf.VerifyAgainst(detected); verr != nil {
					return fmt.Errorf("wal format enforcement failed: %w", verr)
				}
			}
		}
		wl.manifest = mf
		wl.manifestHash = hash
	}

	// Get the list of log segment files in the WAL directory
	sfs, err := wl.segments()
	if err != nil {
		return err
	}
	slog.Debug("Loading WAL segments", slog.Any("total_segments", len(sfs)))

	// Verify manifest preamble if present in the first segment
	if err := wl.verifyManifestPreambleIfPresent(sfs); err != nil {
		if wl.manifest != nil && wl.manifest.Enforce == EnforceStrict {
			return err
		}
		slog.Warn("wal manifest preamble mismatch (continuing in warn mode)", slog.Any("error", err))
	}

	// Determine the current active segment. If existing segments are present we
	// append to the highest numbered one and set wl.csIdx & wl.csSize accordingly.
	var targetPath string
	if len(sfs) == 0 { // start fresh at seg-0.wal
		targetPath = filepath.Join(config.Config.WALDir, segmentPrefix+"0"+".wal")
		wl.csIdx = 0
	} else {
		// segments() already returns sorted ascending by index.
		last := sfs[len(sfs)-1]
		// Extract index from filename
		base := filepath.Base(last)
		idxStr := strings.TrimSuffix(strings.TrimPrefix(base, segmentPrefix), ".wal")
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return fmt.Errorf("invalid wal segment index in %s: %w", base, err)
		}
		wl.csIdx = idx
		// Determine current size so rotateLogIfNeeded works for partial filled segment.
		fi, err := os.Stat(last)
		if err != nil {
			return fmt.Errorf("stat last wal segment: %w", err)
		}
		if fi.Size() > 0 {
			// Safe cast because segment size vs maxSegmentSizeBytes (uint32) check uses uint32.
			wl.csSize = uint32(fi.Size())
		}
		targetPath = last
	}

	sf, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	wl.csf = sf
	wl.csWriter = bufio.NewWriterSize(wl.csf, config.Config.WALBufferSizeMB*1024*1024)

	// If this is a brand new segment (size==0), write the manifest preamble first
	if wl.csSize == 0 {
		wl.mu.Lock()
		if err := wl.writeManifestPreambleLocked(); err != nil {
			wl.mu.Unlock()
			return fmt.Errorf("write manifest preamble: %w", err)
		}
		wl.mu.Unlock()
	}

	go wl.periodicSyncBuffer()

	switch config.Config.WALRotationMode {
	case "time":
		go wl.periodicRotateSegment()
	}
	return nil
}

// LogCommand writes a command to the WAL with a monotonically increasing LSN.
func (wl *walForge) LogCommand(c *wire.Command) error {
	// Lock once for the entire LSN operation
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// marshal the command to bytes
	b, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	// TODO: This logic changes as we change the LSN format
	wl.lsn += 1
	ts := time.Now().UnixNano()
	if wl.testClock != nil {
		ts = wl.testClock().UnixNano()
	}
	el := &w.Element{
		Lsn:         wl.lsn,
		Timestamp:   ts,
		ElementType: w.ElementType_ELEMENT_TYPE_COMMAND,
		Payload:     b,
	}

	// marshal the WAL Element to bytes and write via helper
	eb, err := proto.Marshal(el)
	if err != nil {
		return err
	}
	return wl.writePayloadLocked(eb)
}

// rotateLogIfNeeded checks if the current segment size + the entry size is
// greater than the max segment size, and if so, it rotates the log.
// This method is not thread safe and hence should be called with the lock held.
func (wl *walForge) rotateLogIfNeeded(entrySize uint32) error {
	// If the current segment size + the entry size is greater than the max segment size,
	// we need to rotate the log.
	if wl.csSize+entrySize > wl.maxSegmentSizeBytes {
		if err := wl.rotateLog(); err != nil {
			return err
		}
	}
	return nil
}

// rotateLog rotates the log by closing the current segment file,
// incrementing the current segment index, and opening a new segment file.
func (wl *walForge) rotateLog() error {
	fmt.Println("rotating log")
	// TODO: Ideally this function should not return any error
	// Check for the conditions where it can return an error
	// and handle them gracefully.
	// I fear that we will need to do some cleanup operations in case of errors.

	// Sync the current segment file to disk
	if err := wl.sync(); err != nil {
		return err
	}

	// Close the current segment file
	if err := wl.csf.Close(); err != nil {
		return err
	}

	// Increment the current segment index
	wl.csIdx++

	// Open a new segment file
	sf, err := os.OpenFile(
		filepath.Join(config.Config.WALDir, fmt.Sprintf("%s%d.wal", segmentPrefix, wl.csIdx)),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// TODO: We are panicking here because we are not handling the error
		// and we want to make sure that the WAL is not corrupted.
		// We need to handle this error gracefully.
		panic(fmt.Errorf("failed opening file: %w", err))
	}

	// Reset the trackers
	wl.csf = sf
	wl.csSize = 0
	wl.csWriter = bufio.NewWriter(sf)

	// After opening a fresh segment, write the manifest preamble first
	if err := wl.writeManifestPreambleLocked(); err != nil {
		return err
	}

	return nil
}

// Writes out any data in the WAL's in-memory buffer to the segment file.
// and syncs the segment file to disk.
func (wl *walForge) sync() error {
	// NOTE: caller must hold wl.mu to serialize with append/rotate and protect pendingHS.
	// Flush the buffer to the segment file
	if err := wl.csWriter.Flush(); err != nil {
		return err
	}

	// Test hook: simulate a crash right after data hits the page cache but before durable fsync
	if TestHookAfterDataBeforeFsync != nil {
		TestHookAfterDataBeforeFsync()
	}

	// First, fsync the segment to ensure no hardstate can ever point past
	// the last durable WAL entry.
	if err := wl.csf.Sync(); err != nil {
		return err
	}

	// If we have a staged HardState, persist it now using a durable rename.
	if len(wl.pendingHS) > 0 && wl.hsPath != "" {
		tmp := wl.hsPath + ".tmp"
		// Write temp file
		if err := os.WriteFile(tmp, wl.pendingHS, 0644); err != nil {
			return err
		}
		// fsync the temp file to ensure contents are durable
		f, err := os.OpenFile(tmp, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}

		// Atomically replace the hardstate file
		if err := os.Rename(tmp, wl.hsPath); err != nil {
			return err
		}

		// Optional test hook: after HS persisted but before directory fsync
		if TestHookAfterHSBeforeFsync != nil {
			TestHookAfterHSBeforeFsync()
		}

		// fsync the directory to ensure the rename is durable
		dir, err := os.Open(config.Config.WALDir)
		if err != nil {
			return err
		}
		if err := dir.Sync(); err != nil {
			dir.Close()
			return err
		}
		_ = dir.Close()

		// Clear the staged HS only after all persistence steps succeed
		wl.pendingHS = wl.pendingHS[:0]
	}

	// TODO: Evaluate if DIRECT_IO is needed here.
	// If we are using a file system that supports direct IO,
	// we can use it to improve the performance.
	// If we are using a file system that does not support direct IO,
	// we can use the buffer to improve the performance.
	return nil
}

func (wl *walForge) periodicSyncBuffer() {
	for {
		select {
		case <-wl.bufferSyncTicker.C:
			wl.mu.Lock()
			err := wl.sync()
			if err != nil {
				slog.Error("failed to sync buffer", slog.String("error", err.Error()))
			}
			wl.mu.Unlock()
		case <-wl.ctx.Done():
			return
		}
	}
}

func (wl *walForge) periodicRotateSegment() {
	fmt.Println("rotating segment")
	for {
		select {
		case <-wl.segmentRotationTicker.C:
			// TODO: Remove this error handling once we clean up the error handling in the rotateLog function.
			wl.mu.Lock()
			if err := wl.rotateLog(); err != nil {
				slog.Error("failed to rotate segment", slog.String("error", err.Error()))
			}
			wl.mu.Unlock()
		case <-wl.ctx.Done():
			return
		}
	}
}

func (wl *walForge) segments() ([]string, error) {
	pattern := filepath.Join(config.Config.WALDir, segmentPrefix+"*"+".wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	// Filter out any unexpected names that do not conform to seg-<n>.wal
	valid := files[:0]
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasPrefix(base, segmentPrefix) || !strings.HasSuffix(base, ".wal") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(base, segmentPrefix), ".wal")
		if _, err := strconv.Atoi(idxStr); err != nil {
			continue
		}
		valid = append(valid, f)
	}
	files = valid
	// Sort numerically by extracted index (NOT lexicographically by full path)
	sort.Slice(files, func(i, j int) bool {
		bi := filepath.Base(files[i])
		bj := filepath.Base(files[j])
		iStr := strings.TrimSuffix(strings.TrimPrefix(bi, segmentPrefix), ".wal")
		jStr := strings.TrimSuffix(strings.TrimPrefix(bj, segmentPrefix), ".wal")
		iVal, _ := strconv.Atoi(iStr)
		jVal, _ := strconv.Atoi(jStr)
		return iVal < jVal
	})
	return files, nil
}

// segmentsInDir returns segments under an explicit directory (used by manifest detection before Init).
func (wl *walForge) segmentsInDir(dir string) ([]string, error) {
	pattern := filepath.Join(dir, segmentPrefix+"*"+".wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	// reuse same filtering/sorting as segments()
	valid := files[:0]
	for _, f := range files {
		base := filepath.Base(f)
		if !strings.HasPrefix(base, segmentPrefix) || !strings.HasSuffix(base, ".wal") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(base, segmentPrefix), ".wal")
		if _, err := strconv.Atoi(idxStr); err != nil {
			continue
		}
		valid = append(valid, f)
	}
	files = valid
	sort.Slice(files, func(i, j int) bool {
		bi := filepath.Base(files[i])
		bj := filepath.Base(files[j])
		iStr := strings.TrimSuffix(strings.TrimPrefix(bi, segmentPrefix), ".wal")
		jStr := strings.TrimSuffix(strings.TrimPrefix(bj, segmentPrefix), ".wal")
		iVal, _ := strconv.Atoi(iStr)
		jVal, _ := strconv.Atoi(jStr)
		return iVal < jVal
	})
	return files, nil
}

// ReplayCommand replays the commands from the WAL files.
// This method is thread safe.
func (wl *walForge) ReplayCommand(cb func(*wire.Command) error) error {
	var crc, entrySize uint32
	var el w.Element

	// Buffers to hold the header and the element bytes
	bb1h := make([]byte, 8)
	bb1ElementBytes := make([]byte, 10*1024)

	// Get list of segment files ordered by timestamp in ascending order
	segments, err := wl.segments()
	if err != nil {
		return fmt.Errorf("error getting wal-segment files: %w", err)
	}

	// Process each segment file in order
	for _, segment := range segments {
		file, err := os.Open(segment)
		if err != nil {
			return fmt.Errorf("error opening wal-segment file %s: %w", segment, err)
		}

		reader := bufio.NewReader(file)
		// Format: CRC32 (4 bytes) | Size of WAL entry (4 bytes) | WAL data

		// TODO: Replace this infinite loop with a more elegant solution
		for {
			// Read CRC32 (4 bytes) + entrySize (4 bytes)
			if _, err := io.ReadFull(reader, bb1h); err != nil {
				// TODO: this terminating connection should be handled in a better way
				// and the loop should not be infinite.
				// Edge case: this EOF error can happen even in the next step when
				// we are reading the WAL element from the file.
				if err == io.EOF {
					break
				}
				file.Close()
				return fmt.Errorf("error reading WAL: %w", err)
			}
			crc = binary.LittleEndian.Uint32(bb1h[0:4])
			entrySize = binary.LittleEndian.Uint32(bb1h[4:8])

			if _, err := io.ReadFull(reader, bb1ElementBytes[:entrySize]); err != nil {
				file.Close()
				return fmt.Errorf("error reading WAL data: %w", err)
			}

			// Calculate CRC32 only on the payload
			expectedCRC := crc32.ChecksumIEEE(bb1ElementBytes[:entrySize])
			if crc != expectedCRC {
				// TODO: We are reprtitively closing the file here
				// A better solution would be to move this logic to a function
				// and use defer to close the file.
				// The function. thus, in a way processes (replays) one segment at a time.
				file.Close()

				// TODO: THis is where we should trigger the WAL recovery
				// Recovery process is all about truncating the segment file
				// till this point and ignoring the rest.
				// Log appropriate messages when this happens.
				// Evaluate if this recovery mode should be a command line flag
				// that would suggest if we should truncate, ignore, or stop the boot process.
				return fmt.Errorf("CRC32 mismatch: expected %d, got %d", crc, expectedCRC)
			}

			// Unmarshal the WAL entry to get the payload
			if err := proto.Unmarshal(bb1ElementBytes[:entrySize], &el); err != nil {
				file.Close()
				return fmt.Errorf("error unmarshaling WAL entry: %w", err)
			}

			// Detect unified envelope and unwrap to command bytes if present
			var cmdBytes []byte
			if isUWAL(el.Payload) {
				kind, _, _, _, inner, _, derr := decodeUWAL(el.Payload)
				if derr != nil {
					file.Close()
					return fmt.Errorf("decode uwal: %w", derr)
				}
				if kind == uwalKindManifest {
					// Skip manifest preamble
					continue
				}
				if kind == uwalKindHardState {
					// unified hardstate is not surfaced via ReplayCommand
					continue
				}
				cmdBytes = inner
			} else {
				cmdBytes = el.Payload
			}
			var c wire.Command
			if err := proto.Unmarshal(cmdBytes, &c); err != nil {
				file.Close()
				return fmt.Errorf("error unmarshaling command: %w", err)
			}
			// Call provided replay function with parsed command
			if err := cb(&c); err != nil {
				file.Close()
				return fmt.Errorf("error replaying command: %w", err)
			}
		}
	}

	return nil
}

// Stop stops the WAL Forge.
// This method is thread safe.
func (wl *walForge) Stop() {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// Stop the tickers
	wl.bufferSyncTicker.Stop()
	wl.segmentRotationTicker.Stop()

	// Cancel the context
	wl.cancel()

	// Sync the current segment file to disk
	// Temporarily disable test hooks to avoid synthetic panics during teardown.
	oldHookData := TestHookAfterDataBeforeFsync
	oldHookHS := TestHookAfterHSBeforeFsync
	TestHookAfterDataBeforeFsync = nil
	TestHookAfterHSBeforeFsync = nil
	if err := wl.sync(); err != nil {
		slog.Error("failed to sync current segment file", slog.String("error", err.Error()))
	}
	// Restore hooks
	TestHookAfterDataBeforeFsync = oldHookData
	TestHookAfterHSBeforeFsync = oldHookHS

	wl.csf.Close()

	// TODO: See if we are missing any other cleanup operations.
}

// Sync flushes the WAL buffer and fsyncs the current segment to ensure durability.
func (wl *walForge) Sync() error {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	return wl.sync()
}

// --- unified envelope helpers (non-exported) ---

const (
	uwalKindNormal    = byte(0)
	uwalKindHardState = byte(1)
	uwalKindManifest  = byte(2) // preamble record carrying manifest hash; ignored during replay
)

var uwalMagic = []byte{'U', 'W', 'A', 'L', '1'}

func isUWAL(b []byte) bool {
	if len(b) < len(uwalMagic) { return false }
	for i := range uwalMagic {
		if b[i] != uwalMagic[i] { return false }
	}
	return true
}

// encodeUWAL builds a minimal envelope:
// magic(5) | kind(1) | index(8 LE) | term(8 LE) | subSeq(4 LE) | cmdLen(4 LE) | cmd | raftLen(4 LE) | raftData
func encodeUWAL(kind byte, index, term uint64, subSeq uint32, cmd []byte, raftData []byte) []byte {
	total := 5 + 1 + 8 + 8 + 4 + 4 + len(cmd) + 4 + len(raftData)
	out := make([]byte, total)
	copy(out[:5], uwalMagic)
	out[5] = kind
	off := 6
	binary.LittleEndian.PutUint64(out[off:off+8], index)
	off += 8
	binary.LittleEndian.PutUint64(out[off:off+8], term)
	off += 8
	binary.LittleEndian.PutUint32(out[off:off+4], subSeq)
	off += 4
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(len(cmd)))
	off += 4
	copy(out[off:], cmd)
	off += len(cmd)
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(len(raftData)))
	off += 4
	copy(out[off:], raftData)
	return out
}

// decodeUWAL parses the minimal envelope and returns (kind,index,term,subSeq,cmdBytes,raftData).
func decodeUWAL(b []byte) (byte, uint64, uint64, uint32, []byte, []byte, error) {
	if !isUWAL(b) { return 0, 0, 0, 0, nil, nil, fmt.Errorf("not uwal") }
	if len(b) < 6+8+8+4+4 { return 0, 0, 0, 0, nil, nil, fmt.Errorf("uwal: short header") }
	kind := b[5]
	off := 6
	idx := binary.LittleEndian.Uint64(b[off:off+8]); off += 8
	term := binary.LittleEndian.Uint64(b[off:off+8]); off += 8
	sub := binary.LittleEndian.Uint32(b[off:off+4]); off += 4
	ln := binary.LittleEndian.Uint32(b[off:off+4]); off += 4
	if int(off)+int(ln) > len(b) { return 0, 0, 0, 0, nil, nil, fmt.Errorf("uwal: length exceeds payload") }
	cmd := b[off : off+int(ln)]
	off += int(ln)
	if len(b) < off+4 { return kind, idx, term, sub, cmd, nil, nil }
	rln := binary.LittleEndian.Uint32(b[off:off+4]); off += 4
	if int(off)+int(rln) > len(b) { return 0, 0, 0, 0, nil, nil, fmt.Errorf("uwal: raft length exceeds payload") }
	raft := b[off : off+int(rln)]
	return kind, idx, term, sub, cmd, raft, nil
}

// writePayloadLocked writes a fully formed Element payload 'eb' to the segment with framing and rotation.
// Caller must hold wl.mu.
func (wl *walForge) writePayloadLocked(eb []byte) error {
	// Wrap the element with Checksum and Size, and write via buffer
	entrySize := uint32(4 + 4 + len(eb))
	if err := wl.rotateLogIfNeeded(entrySize); err != nil {
		return err
	}
	if entrySize > uint32(cap(bb)) {
		panic(fmt.Errorf("buffer too small, %d > %d", entrySize, len(bb)))
	}
	bb = bb[:8+len(eb)]
	chk := crc32.ChecksumIEEE(eb)
	binary.LittleEndian.PutUint32(bb[0:4], chk)
	binary.LittleEndian.PutUint32(bb[4:8], uint32(len(eb)))
	copy(bb[8:], eb)
	_, _ = wl.csWriter.Write(bb)
	wl.csSize += entrySize
	return nil
}

// writeManifestPreambleLocked writes a UWAL manifest preamble as the first record
// of an empty segment. Caller must hold wl.mu. No-op if manifestHash is empty
// or the current segment already has data.
func (wl *walForge) writeManifestPreambleLocked() error {
	if len(wl.manifestHash) == 0 {
		return nil
	}
	if wl.csSize != 0 {
		return nil
	}
	// Encode the manifest hash as the inner command bytes with uwalKindManifest
	payload := encodeUWAL(uwalKindManifest, 0, 0, 0, wl.manifestHash, nil)
	wl.lsn += 1
	ts := time.Now().UnixNano()
	if wl.testClock != nil {
		ts = wl.testClock().UnixNano()
	}
	el := &w.Element{Lsn: wl.lsn, Timestamp: ts, ElementType: w.ElementType_ELEMENT_TYPE_COMMAND, Payload: payload}
	eb, err := proto.Marshal(el)
	if err != nil {
		return err
	}
	return wl.writePayloadLocked(eb)
}

// verifyManifestPreambleIfPresent reads the first segment's first entry (if any)
// and verifies the uwalKindManifest payload hash matches the current manifest hash.
// Returns an error on mismatch in strict UWAL1 mode; otherwise returns nil.
func (wl *walForge) verifyManifestPreambleIfPresent(segments []string) error {
	if len(segments) == 0 {
		return nil
	}
	// Only enforce for UWAL1 strict manifests; warn-only is permissive.
	if wl.manifest == nil || !wl.manifest.IsUWAL1() || wl.manifest.Enforce != EnforceStrict {
		return nil
	}
	f, err := os.Open(segments[0])
	if err != nil {
		return nil // don't block startup if we can't read; segment scanning will catch issues later
	}
	defer f.Close()
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil // empty or unreadable first record; nothing to verify
	}
	sz := binary.LittleEndian.Uint32(hdr[4:8])
	if sz == 0 || sz > 16*1024*1024 {
		return nil
	}
	payload := make([]byte, sz)
	if _, err := io.ReadFull(f, payload); err != nil {
		return nil
	}
	var el w.Element
	if err := proto.Unmarshal(payload, &el); err != nil {
		return nil
	}
	if !isUWAL(el.Payload) {
		return nil
	}
	kind, _, _, _, inner, _, derr := decodeUWAL(el.Payload)
	if derr != nil {
		return nil
	}
	if kind != uwalKindManifest {
		return nil
	}
	// inner contains the manifest hash bytes
	if len(inner) != len(wl.manifestHash) || (len(inner) > 0 && string(inner) != string(wl.manifestHash)) {
		return fmt.Errorf("manifest preamble hash mismatch: wal=%x manifest=%x", inner, wl.manifestHash)
	}
	return nil
}

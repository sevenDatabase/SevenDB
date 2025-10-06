package raft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sevenDatabase/SevenDB/internal/harness/clock"
)

// local copies of small interfaces from types.go (kept private there) to avoid import cycle
type dirGetter interface{ Dir() string }

// closeAll closes every non-placeholder node and asserts that each shadow WAL directory
// contains only expected segment/sidecar files (seg-*.wal, seg-*.wal.idx) with no stray tmp files.
// It fails the test if unexpected files remain, which helps catch resource or cleanup regressions.
func closeAll(t *testing.T, nodes []*ShardRaftNode) {
	t.Helper()
	for _, n := range nodes {
		if n == nil || n.shardID == "_dead" {
			continue
		}
		_ = n.Close()
		// If WAL shadow disabled or interface missing dir getter skip validation.
		if n.walShadow == nil {
			continue
		}
		dg, ok := n.walShadow.(dirGetter)
		if !ok {
			continue
		}
		dir := dg.Dir()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read wal dir %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			// Allow segment and sidecar files only.
			if strings.HasPrefix(name, "seg-") && (strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, ".wal.idx")) {
				continue
			}
			t.Fatalf("unexpected file remaining in wal dir %s: %s", dir, name)
		}
		// Additionally assert no leftover temporary sidecar rename (.tmp) files.
		matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
		if len(matches) > 0 {
			t.Fatalf("temporary files not cleaned in wal dir %s: %v", dir, matches)
		}
	}
}

// newDeterministicNode creates a raft node with a simulated clock (if etcd engine) for fast tests.
func newDeterministicNode(t *testing.T, cfg RaftConfig, start time.Time) *ShardRaftNode {
	clk := clock.NewSimulatedClock(start)
	cfg.TestDeterministicClock = clk
	n, err := NewShardRaftNode(cfg)
	if err != nil {
		t.Fatalf("new deterministic node: %v", err)
	}
	// Ensure the node is always closed even if a test forgets to explicitly call closeAll/Close.
	// This prevents background raft goroutines (ready/tick) or WAL segment writers from
	// racing with the testing package's TempDir cleanup (which led to ENOTEMPTY errors).
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// advanceAll advances the simulated clocks for nodes that have one.
func advanceAll(nodes []*ShardRaftNode, d time.Duration) {
	for _, n := range nodes {
		if n != nil && n.clk != nil {
			n.clk.Advance(d)
		}
	}
}

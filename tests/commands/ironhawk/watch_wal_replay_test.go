package ironhawk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
	"github.com/sevenDatabase/SevenDB/config"
	derrors "github.com/sevenDatabase/SevenDB/internal/errors"
	serverih "github.com/sevenDatabase/SevenDB/internal/server/ironhawk"
	"github.com/sevenDatabase/SevenDB/internal/shardmanager"
	"github.com/sevenDatabase/SevenDB/internal/wal"
)

// mockWAL is a controllable WAL used to test durable-ack semantics.
type mockWAL struct {
	mu       sync.Mutex
	entries  []*wire.Command
	syncCh   chan struct{}
	initOnce sync.Once
}

// ensureConfig guarantees config.Config is initialized when running a single test file directly.
func ensureConfig() {
	if config.Config == nil {
		config.ForceInit(&config.DiceDBConfig{})
	}
}

func (m *mockWAL) Init() error { m.initOnce.Do(func() {}); return nil }
func (m *mockWAL) Stop()       {}
func (m *mockWAL) LogCommand(c *wire.Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, c)
	return nil
}
func (m *mockWAL) Sync() error {
	if m.syncCh != nil {
		<-m.syncCh // block until unblocked
	}
	return nil
}
func (m *mockWAL) ReplayCommand(cb func(*wire.Command) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		if err := cb(e); err != nil {
			return err
		}
	}
	return nil
}

// unblock releases any waiting Sync callers.
func (m *mockWAL) unblock() {
	if m.syncCh != nil {
		close(m.syncCh)
		m.syncCh = nil
	}
}

// startTestServer spins up a minimal ironhawk server bound to config.Config.Port and returns a cancel func and waitgroup join.
func startTestServer(t *testing.T, wm *serverih.WatchManager) (cancel func(), join func()) {
	t.Helper()
	log.Printf("[test] starting test server host=%s port=%d", config.Config.Host, config.Config.Port)

	gec := make(chan error, 1)
	// Keep 1 shard for simpler tests
	sm := shardmanager.NewShardManager(1, gec)
	ioMgr := serverih.NewIOThreadManager()
	srv := serverih.NewServer(sm, ioMgr, wm)

	ctx, cancelFn := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// shard manager
	wg.Add(1)
	go func() {
		defer wg.Done()
		sm.Run(ctx)
	}()

	// server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("[test] server Run entering")
		_ = srv.Run(ctx)
		log.Printf("[test] server Run exited")
	}()

	// propagate abort (non-blocking loop that exits on context cancel)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Printf("[test] error-listener goroutine exiting on ctx cancel")
				return
			case err := <-gec:
				if err == nil {
					continue
				}
				if errors.Is(err, derrors.ErrAborted) {
					log.Printf("[test] received abort error; canceling context: %v", err)
					cancelFn()
				}
			}
		}
	}()

	return cancelFn, func() { cancelFn(); wg.Wait() }
}

// replaySubscriptions replays WAL entries into the provided watch manager to restore subscriptions.
func replaySubscriptions(t *testing.T, wm *serverih.WatchManager) {
	t.Helper()
	cb := func(cd *wire.Command) error {
		switch cd.Cmd {
		case "SUBSCRIBE":
			if len(cd.Args) < 3 {
				return nil
			}
			clientID := cd.Args[0]
			commandStr := cd.Args[1]
			fpStr := cd.Args[2]
			fp, err := strconv.ParseUint(fpStr, 10, 64)
			if err != nil {
				return nil
			}
			return wm.RestoreSubscription(clientID, commandStr, fp)
		case "UNSUBSCRIBE":
			if len(cd.Args) < 2 {
				return nil
			}
			clientID := cd.Args[0]
			fpStr := cd.Args[1]
			fp, err := strconv.ParseUint(fpStr, 10, 64)
			if err != nil {
				return nil
			}
			return wm.RemoveSubscription(clientID, fp)
		default:
			// ignore non-subscription commands during replay in tests
			return nil
		}
	}
	if wal.DefaultWAL != nil {
		if err := wal.DefaultWAL.ReplayCommand(cb); err != nil {
			t.Fatalf("replay failed: %v", err)
		}
	}
}

func TestWatch_AckAfterSync(t *testing.T) {
	// Ensure global config is initialized when running this file standalone.
	ensureConfig()
	// Use a mock WAL with a blocking Sync to ensure no ack is sent before durability.
	config.Config.EnableWatch = true
	prev := wal.DefaultWAL
	mw := &mockWAL{syncCh: make(chan struct{})}
	wal.DefaultWAL = mw
	t.Cleanup(func() { wal.DefaultWAL = prev })

	// Isolate port for this test
	config.Config.Port = 7482
	wm := serverih.NewWatchManager()
	cancel, join := startTestServer(t, wm)
	defer join()

	log.Printf("[test] dialing client localhost:%d", config.Config.Port)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
			 t.Fatalf("client connect failed: %v", err)
	}
	defer client.Close()
	log.Printf("[test] client connected, firing GET.WATCH (should block)")

	// Fire a GET.WATCH; it should block until WAL.Sync is unblocked
	resCh := make(chan *wire.Result, 1)
	go func() {
		log.Printf("[test] goroutine issuing GET.WATCH")
		res := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"k_ack_sync"}})
		log.Printf("[test] GET.WATCH returned status=%v", res.Status)
		resCh <- res
	}()

	select {
	case <-resCh:
		t.Fatal("received watch response before WAL.Sync — ack should be after durable log")
	case <-time.After(300 * time.Millisecond):
		// expected: still blocked
	}

	// Now unblock WAL sync; the response should come through
	log.Printf("[test] unblocking WAL Sync")
	mw.unblock()
	select {
	case res := <-resCh:
		if res.Status != wire.Status_OK {
			t.Fatalf("unexpected status: %+v", res)
		}
		// Ensure a SUBSCRIBE entry was logged before ack
		mw.mu.Lock()
		found := false
		for _, e := range mw.entries {
			if e.Cmd == "SUBSCRIBE" {
				found = true
				break
			}
		}
		mw.mu.Unlock()
		if !found {
			t.Fatalf("expected SUBSCRIBE to be logged in WAL before ack")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch response after WAL.Sync unblock")
	}

	// Cleanup promptly (avoid lingering server if join waits on channels)
	log.Printf("[test] canceling server context after success")
	cancel()
}

func TestWatch_ReplayRestoresSubscription(t *testing.T) {
	ensureConfig()
	// Use a real WAL in a temp directory
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7483
	config.Config.EnableWatch = true

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Server 1: create a subscription
	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	clientPub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("client connect failed: %v", err)
	}
	defer clientPub.Close()

	// Give server a moment to bind
	time.Sleep(150 * time.Millisecond)

	// Identify client for persistence
	cid := "client-replay-1"
	r := clientPub.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}})
	if r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}

	// Seed a value and subscribe
	_ = clientPub.Fire(&wire.Command{Cmd: "SET", Args: []string{"k_replay", "v1"}})
	resSub := clientPub.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"k_replay"}})
	if resSub.Status != wire.Status_OK {
		t.Fatalf("watch subscribe failed: %+v", resSub)
	}

	// Stop server 1 (simulate restart)
	cancel1()
	join1()

	// Server 2: restore from WAL
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	// Recreate a watch connection for the same client ID so notifications have a destination
	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	resHs := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}})
	if resHs.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", resHs)
	}

	// Now publish an update
	// Reconnect a publisher client (server restarted)
	clientPub2, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher reconnect failed: %v", err)
	}
	defer clientPub2.Close()

	// Send a SET that should trigger a watch notification to the restored subscription
	_ = clientPub2.Fire(&wire.Command{Cmd: "SET", Args: []string{"k_replay", "v2"}})

	// Read from the watch connection: the next response should be the GET result (v2)
	// We send a benign PING to drive a read; the server might have already queued the notification.
	// If we receive PONG, try a few times to allow for scheduling.
	try := 0
	for {
		try++
		res := watchConn.Fire(&wire.Command{Cmd: "PING"})
		// If it's a GET response, verify and break
		if getRes := res.GetGETRes(); getRes != nil {
			if getRes.Value != "v2" {
				t.Fatalf("expected restored subscription to deliver v2, got %q", getRes.Value)
			}
			break
		}
		if try > 5 {
			t.Fatalf("did not receive watch notification after retries; last: %+v", res)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Cleanup
	cancel2()
}

// waitForWatchValue polls until it receives a GET result with the expected value or times out.
func waitForWatchValue(t *testing.T, c *dicedb.Client, expected string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res := c.Fire(&wire.Command{Cmd: "PING"})
		if getRes := res.GetGETRes(); getRes != nil {
			if getRes.Value == expected {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for watch value %q", expected)
}

// waitForWatchValues waits until it has observed all values in expected set, or times out.
func waitForWatchValues(t *testing.T, c *dicedb.Client, expected map[string]struct{}, timeout time.Duration) error {
	t.Helper()
	remaining := make(map[string]struct{}, len(expected))
	for k := range expected {
		remaining[k] = struct{}{}
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res := c.Fire(&wire.Command{Cmd: "PING"})
		if getRes := res.GetGETRes(); getRes != nil {
			if _, ok := remaining[getRes.Value]; ok {
				delete(remaining, getRes.Value)
				if len(remaining) == 0 {
					return nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for watch values, still missing: %v", remaining)
}

func TestWAL_CorruptedReplayFails(t *testing.T) {
	ensureConfig()
	// Use real WAL and then corrupt the CRC header to force a replay error.
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_corrupt")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Log a simple command and sync to create a segment file
	_ = wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"})
	_ = wal.DefaultWAL.Sync()

	// Corrupt the CRC32 of the first entry in seg-0.wal
	seg := filepath.Join(config.Config.WALDir, "seg-0.wal")
	f, err := os.OpenFile(seg, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open wal segment failed: %v", err)
	}
	// Overwrite first 4 bytes (CRC32) with zeros
	if _, err := f.WriteAt([]byte{0, 0, 0, 0}, 0); err != nil {
		_ = f.Close()
		t.Fatalf("failed to corrupt wal: %v", err)
	}
	_ = f.Close()

	// Expect replay to fail loudly due to CRC mismatch
	err = wal.DefaultWAL.ReplayCommand(func(c *wire.Command) error { return nil })
	if err == nil {
		t.Fatalf("expected replay to fail due to corruption, got nil error")
	}
	// Be precise: ensure this is a CRC mismatch and not some unrelated error
	if !strings.Contains(err.Error(), "CRC32 mismatch") {
		t.Fatalf("unexpected replay error for corruption: %v", err)
	}
}

func TestWatch_MultipleSubscriptionsAcrossKeys_Replay(t *testing.T) {
	ensureConfig()
	// Real WAL with time-based rotation to ensure replay over multiple entries
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_multi")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.WALSegmentRotationTimeSec = 1
	config.Config.WALRotationMode = "time"
	config.Config.Port = 7484
	config.Config.EnableWatch = true

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Server 1: create two subscriptions on different keys
	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client1, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client1.Close()
	time.Sleep(150 * time.Millisecond)

	cid := "cid-multi"
	if r := client1.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}
	_ = client1.Fire(&wire.Command{Cmd: "SET", Args: []string{"k1", "v1a"}})
	_ = client1.Fire(&wire.Command{Cmd: "SET", Args: []string{"k2", "v2a"}})
	if r := client1.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"k1"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch k1 failed: %+v", r)
	}
	if r := client1.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"k2"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch k2 failed: %+v", r)
	}

	// Deterministically wait for at least 2 WAL segments before proceeding
	if err := waitForWALSegments(t, config.Config.WALDir, 2, 5*time.Second); err != nil {
		t.Fatalf("wal rotation not observed in time: %v", err)
	}

	cancel1()
	join1()

	// Server 2: replay and verify both deliver updates
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"k1", "v1b"}})
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"k2", "v2b"}})

	want := map[string]struct{}{"v1b": {}, "v2b": {}}
	if err := waitForWatchValues(t, watchConn, want, 3*time.Second); err != nil {
		t.Fatal(err)
	}

	cancel2()
}

// waitForWALSegments waits until at least minCount seg-*.wal files exist in dir or times out.
func waitForWALSegments(t *testing.T, dir string, minCount int, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	pattern := filepath.Join(dir, "seg-*.wal")
	for time.Now().Before(deadline) {
		files, _ := filepath.Glob(pattern)
		if len(files) >= minCount {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	files, _ := filepath.Glob(pattern)
	return fmt.Errorf("have %d segments, want >= %d", len(files), minCount)
}

// Simulate an unclean crash: do not stop the WAL (no final flush), then restart and replay.
func TestWatch_ReplayAfterCrash_NoWALStop(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_crash")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7487
	config.Config.EnableWatch = true

	wal.SetupWAL()
	// Intentionally DO NOT call wal.DefaultWAL.Stop() before restart to simulate crash.
	// We'll stop it only at the very end of the test to release resources.
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Server 1: establish a subscription which is durably synced as part of watch flow
	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()
	time.Sleep(150 * time.Millisecond)

	cid := "cid-crash"
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}
	_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc", "v0"}})
	if r := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"kc"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch kc failed: %+v", r)
	}

	// Simulate crash: stop the server goroutines without stopping WAL to avoid its final sync.
	cancel1()
	join1()

	// Server 2: replay and verify subscription still restored (because watch path fsynced before ack)
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc", "v1"}})

	if err := waitForWatchValue(t, watchConn, "v1", 2*time.Second); err != nil {
		t.Fatal(err)
	}

	cancel2()
}

// Optional performance guardrail: large replay under load. Skipped on -short to keep CI fast.
func TestWatch_ReplayUnderLoad_SkippedOnShort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in -short mode")
	}

	ensureConfig()

	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_load")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7488
	config.Config.EnableWatch = true
	// Encourage rotations during load
	config.Config.WALRotationMode = "time"
	config.Config.WALSegmentRotationTimeSec = 1

	wal.SetupWAL()
	defer func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	}()

	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()
	time.Sleep(200 * time.Millisecond)

	cid := "cid-load"
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}

	// Create many subscriptions
	N := 1000 // increase locally if needed; 10k may be too heavy for CI
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("kl-%d", i)
		_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "seed"}})
		if r := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}}); r.Status != wire.Status_OK {
			t.Fatalf("watch %s failed: %+v", key, r)
		}
	}

	// Wait for at least 2 segments (should rotate under time)
	if err := waitForWALSegments(t, config.Config.WALDir, 2, 10*time.Second); err != nil {
		t.Logf("warning: rotation not observed: %v", err)
	}

	cancel1()
	join1()

	// Replay and ensure we can deliver to a subset within a bounded time
	wm2 := serverih.NewWatchManager()
	start := time.Now()
	replaySubscriptions(t, wm2)
	replayDur := time.Since(start)
	t.Logf("replay time for %d subscriptions: %s", N, replayDur)
	// Soft performance guardrail (tunable): fail if too slow to catch regressions
	if replayDur > 5*time.Second {
		t.Fatalf("replay too slow: %s (N=%d)", replayDur, N)
	}

	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()

	// Update a handful of keys and assert notifications arrive quickly
	want := map[string]struct{}{}
	for i := 0; i < 10; i++ {
		val := fmt.Sprintf("load-%d", i)
		key := fmt.Sprintf("kl-%d", i)
		_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, val}})
		want[val] = struct{}{}
	}

	if err := waitForWatchValues(t, watchConn, want, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	cancel2()
}

func TestWatch_SubscribeUnsubscribe_Replay(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_unsub")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7485
	config.Config.EnableWatch = true

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()
	time.Sleep(150 * time.Millisecond)

	cid := "cid-unsub"
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}

	_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"ka", "a1"}})
	_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"kb", "b1"}})
	rA := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"ka"}})
	if rA.Status != wire.Status_OK {
		t.Fatalf("watch ka failed: %+v", rA)
	}
	rB := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"kb"}})
	if rB.Status != wire.Status_OK {
		t.Fatalf("watch kb failed: %+v", rB)
	}

	// Unsubscribe kb using fingerprint
	fpB := strconv.FormatUint(rB.Fingerprint64, 10)
	if r := client.Fire(&wire.Command{Cmd: "UNWATCH", Args: []string{fpB}}); r.Status != wire.Status_OK {
		t.Fatalf("unwatch kb failed: %+v", r)
	}

	cancel1()
	join1()

	// Replay and verify only ka receives updates
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"ka", "a2"}})
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kb", "b2"}})

	// We should see a2 but not b2 within the window
	if err := waitForWatchValue(t, watchConn, "a2", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// Ensure b2 does not arrive in a grace window
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		res := watchConn.Fire(&wire.Command{Cmd: "PING"})
		if getRes := res.GetGETRes(); getRes != nil && getRes.Value == "b2" {
			t.Fatalf("unexpected notification for unsubscribed key kb: %v", getRes.Value)
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel2()
}

func TestWatch_ConcurrentSubscriptions_Replay(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_concurrent")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7486
	config.Config.EnableWatch = true

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()
	time.Sleep(150 * time.Millisecond)

	cid := "cid-conc"
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}

	// Fire multiple subscriptions concurrently
	keys := []string{"kc1", "kc2", "kc3", "kc4", "kc5"}
	var wg sync.WaitGroup
	for _, k := range keys {
		k := k
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{k, "seed"}})
			res := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{k}})
			if res.Status != wire.Status_OK {
				t.Errorf("watch %s failed: %+v", k, res)
			}
		}()
	}
	wg.Wait()

	cancel1()
	join1()

	// Replay and verify updates for all keys
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	want := map[string]struct{}{}
	for i, k := range keys {
		val := fmt.Sprintf("cval-%d", i)
		_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{k, val}})
		want[val] = struct{}{}
	}
	if err := waitForWatchValues(t, watchConn, want, 4*time.Second); err != nil {
		t.Fatal(err)
	}

	cancel2()
}

// Timestamp determinism placeholder: skipped until timestamps are surfaced to callbacks.
func TestWAL_TimestampDeterminism_Skipped(t *testing.T) {
	t.Skip("wire.Command callbacks do not expose WAL entry timestamps; enable once available")
}

// Truncated EOF at the end of the WAL should cause replay to fail loudly.
// Truncated EOF at the end of the WAL should cause replay to fail loudly with an EOF/UnexpectedEOF.
func TestWAL_TruncatedEOF_ReplayFails(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_trunc")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Write a couple of entries and sync
	_ = wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"})
	_ = wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"})
	_ = wal.DefaultWAL.Sync()

	seg := filepath.Join(config.Config.WALDir, "seg-0.wal")
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if st.Size() < 2 {
		t.Fatalf("wal segment unexpectedly small (%d)", st.Size())
	}
	// Truncate the file by 1 byte to simulate partial last entry payload
	if err := os.Truncate(seg, st.Size()-1); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}

	// Expect replay to fail due to EOF while reading last entry
	err = wal.DefaultWAL.ReplayCommand(func(c *wire.Command) error { return nil })
	if err == nil {
		t.Fatalf("expected replay error due to truncated EOF, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) &&
		!strings.Contains(err.Error(), "error reading WAL data") && !strings.Contains(err.Error(), "error reading WAL") {
		t.Fatalf("unexpected error when replaying truncated WAL: %v", err)
	}
}

// Truncating inside the header (CRC/length) should also error with UnexpectedEOF during header read.
func TestWAL_TruncatedHeader_ReplayFails(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_trunc_header")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Write entries and sync
	_ = wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"})
	_ = wal.DefaultWAL.LogCommand(&wire.Command{Cmd: "PING"})
	_ = wal.DefaultWAL.Sync()

	seg := filepath.Join(config.Config.WALDir, "seg-0.wal")
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if st.Size() < 8 {
		t.Fatalf("wal segment too small for header truncation (%d)", st.Size())
	}
	// Truncate last 4 bytes to force header read failure on next iteration
	if err := os.Truncate(seg, st.Size()-4); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}

	err = wal.DefaultWAL.ReplayCommand(func(c *wire.Command) error { return nil })
	if err == nil {
		t.Fatalf("expected replay error due to truncated header, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !strings.Contains(err.Error(), "error reading WAL") {
		t.Fatalf("unexpected error when replaying header-truncated WAL: %v", err)
	}
}

// Subscribe -> Unsubscribe -> Resubscribe within one segment; only the final subscription should be active after replay.
// Subscribe -> Unsubscribe -> Resubscribe within one segment; only the final subscription should be active after replay.
func TestWatch_SubUnsubResubscribe_SingleSegment_Replay(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_subunsub")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7489
	config.Config.EnableWatch = true
	// Keep default time-based rotation (60s) so all ops stay in seg-0

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	client, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()
	time.Sleep(150 * time.Millisecond)

	cid := "cid-order"
	if r := client.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake failed: %+v", r)
	}

	key := "ko"
	_ = client.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "o0"}})

	// SUBSCRIBE #1
	res1 := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	if res1.Status != wire.Status_OK {
		t.Fatalf("watch #1 failed: %+v", res1)
	}
	fp1 := strconv.FormatUint(res1.Fingerprint64, 10)

	// UNSUBSCRIBE #1
	if r := client.Fire(&wire.Command{Cmd: "UNWATCH", Args: []string{fp1}}); r.Status != wire.Status_OK {
		t.Fatalf("unwatch #1 failed: %+v", r)
	}

	// RESUBSCRIBE #2
	res2 := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	if res2.Status != wire.Status_OK {
		t.Fatalf("watch #2 failed: %+v", res2)
	}

	cancel1()
	join1()

	// Replay and verify only the final subscription is active
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	watchConn, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect failed: %v", err)
	}
	defer watchConn.Close()
	if r := watchConn.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{cid, "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake failed: %+v", r)
	}

	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "o1"}})

	if err := waitForWatchValue(t, watchConn, "o1", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// Ensure we don't get duplicate notifications for the same update (only final subscription active).
	dupDeadline := time.Now().Add(400 * time.Millisecond)
	var dupCount int
	for time.Now().Before(dupDeadline) {
		res := watchConn.Fire(&wire.Command{Cmd: "PING"})
		if getRes := res.GetGETRes(); getRes != nil && getRes.Value == "o1" {
			dupCount++
			if dupCount > 0 {
				t.Fatalf("received duplicate notification for %s after replay", key)
			}
		}
		time.Sleep(40 * time.Millisecond)
	}

	cancel2()
}

func TestWatch_MultiClientSameKey_Replay(t *testing.T) {
	ensureConfig()
	dir := t.TempDir()
	config.Config.WALDir = filepath.Join(dir, "wal_multi_client")
	_ = os.MkdirAll(config.Config.WALDir, 0o755)
	config.Config.WALVariant = "forge"
	config.Config.Port = 7490
	config.Config.EnableWatch = true

	wal.SetupWAL()
	t.Cleanup(func() {
		if wal.DefaultWAL != nil {
			wal.DefaultWAL.Stop()
		}
	})

	// Server 1: two clients subscribe to the same key
	wm1 := serverih.NewWatchManager()
	cancel1, join1 := startTestServer(t, wm1)
	time.Sleep(150 * time.Millisecond)

	clientA, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("client A connect failed: %v", err)
	}
	defer clientA.Close()
	if r := clientA.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"cid-A", "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake A failed: %+v", r)
	}

	clientB, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("client B connect failed: %v", err)
	}
	defer clientB.Close()
	if r := clientB.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"cid-B", "command"}}); r.Status != wire.Status_OK {
		t.Fatalf("handshake B failed: %+v", r)
	}

	key := "the_same_key"
	_ = clientA.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "v0"}})
	if r := clientA.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}}); r.Status != wire.Status_OK {
		t.Fatalf("client A watch failed: %+v", r)
	}
	if r := clientB.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}}); r.Status != wire.Status_OK {
		t.Fatalf("client B watch failed: %+v", r)
	}

	// Simulate crash
	cancel1()
	join1()

	// Server 2: replay and verify both clients get updates
	wm2 := serverih.NewWatchManager()
	replaySubscriptions(t, wm2)
	cancel2, join2 := startTestServer(t, wm2)
	defer join2()

	// Re-establish watch connections for both client IDs
	watchConnA, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect A failed: %v", err)
	}
	defer watchConnA.Close()
	if r := watchConnA.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"cid-A", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake A failed: %+v", r)
	}

	watchConnB, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("watch connect B failed: %v", err)
	}
	defer watchConnB.Close()
	if r := watchConnB.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"cid-B", "watch"}}); r.Status != wire.Status_OK {
		t.Fatalf("watch handshake B failed: %+v", r)
	}

	// Publisher updates the key
	pub, err := dicedb.NewClient("localhost", config.Config.Port)
	if err != nil {
		t.Fatalf("publisher connect failed: %v", err)
	}
	defer pub.Close()
	_ = pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "final-value"}})

	// Verify both clients receive the notification
	if err := waitForWatchValue(t, watchConnA, "final-value", 2*time.Second); err != nil {
		t.Errorf("client A did not receive notification: %v", err)
	}
	if err := waitForWatchValue(t, watchConnB, "final-value", 2*time.Second); err != nil {
		t.Errorf("client B did not receive notification: %v", err)
	}

	cancel2()
}

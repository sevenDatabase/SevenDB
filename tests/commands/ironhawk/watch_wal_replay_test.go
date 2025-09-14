package ironhawk

import (
    "context"
    "errors"
    "os"
    "path/filepath"
    "strconv"
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
        _ = srv.Run(ctx)
    }()

    // propagate abort
    wg.Add(1)
    go func() {
        defer wg.Done()
        for err := range gec {
            if err != nil && errors.Is(err, derrors.ErrAborted) {
                cancelFn()
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
    // Use a mock WAL with a blocking Sync to ensure no ack is sent before durability.
    mw := &mockWAL{syncCh: make(chan struct{})}
    wal.DefaultWAL = mw

    // Isolate port for this test
    config.Config.Port = 7482
    wm := serverih.NewWatchManager()
    cancel, join := startTestServer(t, wm)
    defer join()

    client, err := dicedb.NewClient("localhost", config.Config.Port)
    if err != nil {
        t.Fatalf("client connect failed: %v", err)
    }
    defer client.Close()

    // Fire a GET.WATCH; it should block until WAL.Sync is unblocked
    resCh := make(chan *wire.Result, 1)
    go func() {
        res := client.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"k_ack_sync"}})
        resCh <- res
    }()

    select {
    case <-resCh:
        t.Fatal("received watch response before WAL.Sync — ack should be after durable log")
    case <-time.After(300 * time.Millisecond):
        // expected: still blocked
    }

    // Now unblock WAL sync; the response should come through
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

    // Cleanup
    cancel()
}

func TestWatch_ReplayRestoresSubscription(t *testing.T) {
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

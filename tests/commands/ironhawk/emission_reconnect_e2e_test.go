package ironhawk

import (
    "strconv"
    "testing"
    "time"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
)

// Test that a client can disconnect and resume from the exact next commit index using EMITRECONNECT.
func TestEmissionContract_Reconnect_OK_Smoke(t *testing.T) {
    // Enable emission-contract and stub raft for single shard
    config.Config.RaftEnabled = true
    config.Config.RaftEngine = "stub"
    config.Config.EmissionContractEnabled = true
    config.Config.EnableWatch = true
    // Tighten notifier poll for test responsiveness
    config.Config.EmissionNotifierPollMs = 2

    RunTestServer(nil)
    time.Sleep(150 * time.Millisecond)

    pub := getLocalSdk()
    defer pub.Close()

    // Initial watch session
    watch := getLocalSdk()
    if r := watch.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"w1", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("watch handshake failed: %+v", r)
    }
    subRes := watch.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"kc"}})
    if subRes.Status != wire.Status_OK || subRes.Fingerprint64 == 0 {
        t.Fatalf("GET.WATCH failed or missing fp: %+v", subRes)
    }
    subID := "w1:" + strconv.FormatUint(subRes.Fingerprint64, 10)

    // Emit first update and observe
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc", "v1"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET failed: %+v", r)
    }
    got := false
    for i := 0; i < 40; i++ {
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "v1" {
            got = true
            break
        }
        time.Sleep(25 * time.Millisecond)
    }
    if !got {
        t.Fatal("did not receive first emission v1 before reconnect")
    }
    watch.Close()

    // New session with same client id, resume from commit index 1
    watch2 := getLocalSdk()
    defer watch2.Close()
    if r := watch2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"w1", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("re-handshake failed: %+v", r)
    }
    rec := watch2.Fire(&wire.Command{Cmd: "EMITRECONNECT", Args: []string{"kc", subID, "1"}})
    if rec.Status != wire.Status_OK {
        t.Fatalf("EMITRECONNECT expected OK, got: %+v", rec)
    }

    // Produce next update and expect it after reconnect
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc", "v2"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET v2 failed: %+v", r)
    }
    got2 := false
    for i := 0; i < 40; i++ {
        res := watch2.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "v2" {
            got2 = true
            break
        }
        time.Sleep(25 * time.Millisecond)
    }
    if !got2 {
        t.Fatal("did not receive resumed emission v2 after EMITRECONNECT")
    }
}

// Test that reconnect detects a stale sequence when the outbox has been purged past the client position.
func TestEmissionContract_Reconnect_StaleSequence(t *testing.T) {
    // Enable emission-contract and stub raft for single shard
    config.Config.RaftEnabled = true
    config.Config.RaftEngine = "stub"
    config.Config.EmissionContractEnabled = true
    config.Config.EnableWatch = true
    config.Config.EmissionNotifierPollMs = 2

    RunTestServer(nil)
    time.Sleep(150 * time.Millisecond)

    pub := getLocalSdk()
    defer pub.Close()

    watch := getLocalSdk()
    if r := watch.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"w2", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("watch handshake failed: %+v", r)
    }
    subRes := watch.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"kc2"}})
    if subRes.Status != wire.Status_OK || subRes.Fingerprint64 == 0 {
        t.Fatalf("GET.WATCH failed or missing fp: %+v", subRes)
    }
    subID := "w2:" + strconv.FormatUint(subRes.Fingerprint64, 10)

    // v1
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc2", "v1"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET v1 failed: %+v", r)
    }
    // Wait delivery of v1
    for i := 0; i < 40; i++ {
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "v1" {
            break
        }
        time.Sleep(25 * time.Millisecond)
    }
    // v2
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc2", "v2"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET v2 failed: %+v", r)
    }
    // Poll until v2 shows up
    for i := 0; i < 40; i++ {
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "v2" {
            break
        }
        time.Sleep(25 * time.Millisecond)
    }
    // ACK up to commit index 2 (MVP expectations) to purge outbox entries
    ack := watch.Fire(&wire.Command{Cmd: "EMITACK", Args: []string{"kc2", subID, "2"}})
    if ack.Status != wire.Status_OK {
        t.Fatalf("EMITACK failed: %+v", ack)
    }
    watch.Close()

    // Reconnect from a position older than compaction
    watch2 := getLocalSdk()
    defer watch2.Close()
    if r := watch2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"w2", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("re-handshake failed: %+v", r)
    }
    rec := watch2.Fire(&wire.Command{Cmd: "EMITRECONNECT", Args: []string{"kc2", subID, "1"}})
    if rec.Status != wire.Status_ERR || rec.Message != "STALE_SEQUENCE" {
        t.Fatalf("expected STALE_SEQUENCE, got: %+v", rec)
    }
}

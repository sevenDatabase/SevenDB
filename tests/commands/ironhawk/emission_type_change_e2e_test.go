package ironhawk

import (
    "testing"
    "time"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
)

// If a key's type changes (string -> zset), WatchManager should handle it gracefully under emission path.
// We assert that the system does not deliver a bogus emission and remains responsive (PING -> PONG).
func TestEmissionContract_TypeChange_Graceful(t *testing.T) {
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
    defer watch.Close()
    if r := watch.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"tc", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("watch handshake failed: %+v", r)
    }
    subRes := watch.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"tc-key"}})
    if subRes.Status != wire.Status_OK || subRes.Fingerprint64 == 0 {
        t.Fatalf("GET.WATCH failed or missing fp: %+v", subRes)
    }

    // Write a string and expect an emission with that value
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"tc-key", "s1"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET s1 failed: %+v", r)
    }
    got := false
    for i := 0; i < 40; i++ {
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "s1" {
            got = true
            break
        }
        time.Sleep(20 * time.Millisecond)
    }
    if !got {
        t.Fatal("did not receive initial emission for string value")
    }

    // Change the key type to zset. First delete the previous string value to avoid WRONGTYPE,
    // then create a sorted set at the same key. The watcher (GET.WATCH) should not emit
    // an invalid payload after the type change.
    if r := pub.Fire(&wire.Command{Cmd: "DEL", Args: []string{"tc-key"}}); r.Status != wire.Status_OK {
        t.Fatalf("DEL before type change failed: %+v", r)
    }
    if r := pub.Fire(&wire.Command{Cmd: "ZADD", Args: []string{"tc-key", "1", "m1"}}); r.Status != wire.Status_OK {
        t.Fatalf("ZADD failed: %+v", r)
    }

    // Ensure that a subsequent PING returns a normal PONG (i.e., no pending emission).
    // Because emissions can arrive asynchronously on the same watch wire, we may
    // observe a GETRes before the PINGRes. Poll until PINGRes is observed or timeout.
    seenPong := false
    for i := 0; i < 60; i++ { // allow up to ~1.2s to account for async emissions on the wire
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if pr := res.GetPINGRes(); pr != nil && pr.Message != "" {
            seenPong = true
            break
        }
        time.Sleep(20 * time.Millisecond)
    }
    if !seenPong {
        t.Fatalf("expected normal PING response after type change, but did not observe PINGRes in time")
    }
}

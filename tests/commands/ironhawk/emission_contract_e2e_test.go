package ironhawk

import (
    "strconv"
    "sync"
    "testing"
    "time"

    "github.com/dicedb/dicedb-go/wire"
    "github.com/sevenDatabase/SevenDB/config"
)

func TestEmissionContract_WatchEmitAck_Smoke(t *testing.T) {
    // Enable emission-contract and stub raft for single shard
    config.Config.RaftEnabled = true
    config.Config.RaftEngine = "stub"
    config.Config.EmissionContractEnabled = true
    config.Config.EnableWatch = true

    var wg sync.WaitGroup
    RunTestServer(&wg)
    time.Sleep(150 * time.Millisecond)

    pub := getLocalSdk()
    defer pub.Close()

    watch := getLocalSdk()
    defer watch.Close()
    if r := watch.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{"w1", "watch"}}); r.Status != wire.Status_OK {
        t.Fatalf("watch handshake failed: %+v", r)
    }
    // Subscribe and capture fingerprint from server response
    subRes := watch.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{"kc"}})
    if subRes.Status != wire.Status_OK || subRes.Fingerprint64 == 0 {
        t.Fatalf("GET.WATCH failed or missing fp: %+v", subRes)
    }

    // Trigger an update
    if r := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{"kc", "v1"}}); r.Status != wire.Status_OK {
        t.Fatalf("SET failed: %+v", r)
    }

    // Poll for the watch notification (sent via BridgeSender)
    // Use small sleeps to keep test deterministic without flakes
    got := false
    for i := 0; i < 20; i++ {
        res := watch.Fire(&wire.Command{Cmd: "PING"})
        if res.Message == "v1" {
            got = true
            break
        }
        time.Sleep(25 * time.Millisecond)
    }
    if !got {
        t.Fatal("did not receive watch emission for SET kc v1 under emission contract")
    }

    // Ack up to commit index 1 for this sub (clientID:fp). With stub raft, first commit index per bucket is 1.
    subID := "w1:" + strconv.FormatUint(subRes.Fingerprint64, 10)
    ack := watch.Fire(&wire.Command{Cmd: "EMITACK", Args: []string{"kc", subID, "1"}})
    if ack.Status != wire.Status_OK {
        t.Fatalf("EMITACK failed: %+v", ack)
    }
}

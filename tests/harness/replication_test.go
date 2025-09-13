package harness_test

import (
    "encoding/hex"
    "testing"
    "time"

    h "github.com/dicedb/dice/internal/harness"
    hclock "github.com/dicedb/dice/internal/harness/clock"
    hnet "github.com/dicedb/dice/internal/harness/network"
    hsched "github.com/dicedb/dice/internal/harness/scheduler"
)

func TestReplicationHappyPath(t *testing.T) {
    clock := hclock.NewSimulatedClock(time.Unix(0, 0))
    net := hnet.NewSimulatedNetwork()
    sched := hsched.NewSerial()
    harn := h.New(clock, net, sched)

    // wire 3 nodes into cluster and harness
    setupCluster(harn, "A", "B", "C")

    script := h.Script{Actions: []h.TimedAction{
        h.StartNode("A")(0),
        h.StartNode("B")(0),
        h.StartNode("C")(0),
        // normal client ops to leader A
        h.ClientSet("A", "user:1", "Alice")(5 * time.Millisecond),
        h.ClientSubscribe("A", "orders")(7 * time.Millisecond),
        h.ClientSet("A", "user:2", "Bob")(10 * time.Millisecond),
        // allow deliveries
        h.AdvanceBy(5 * time.Millisecond)(12 * time.Millisecond),
        // verify convergence
        h.VerifyEqual("A", "B", "C")(20 * time.Millisecond),
    }}

    script.Run(harn)

    // direct hash check as well
    ha := hex.EncodeToString(harn.Nodes["A"].StateHash())
    hb := hex.EncodeToString(harn.Nodes["B"].StateHash())
    hc := hex.EncodeToString(harn.Nodes["C"].StateHash())
    if ha != hb || hb != hc { t.Fatalf("hashes differ: A=%s B=%s C=%s", ha, hb, hc) }
}

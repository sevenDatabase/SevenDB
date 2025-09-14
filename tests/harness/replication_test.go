package harness_test

import (
    "encoding/hex"
    "testing"
    "time"

    h "github.com/sevenDatabase/SevenDB/internal/harness"
    hclock "github.com/sevenDatabase/SevenDB/internal/harness/clock"
    hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
    hsched "github.com/sevenDatabase/SevenDB/internal/harness/scheduler"
)

func TestReplicationHappyPath(t *testing.T) {
    clock := hclock.NewSimulatedClock(time.Unix(0, 0))
    net := hnet.NewSimulatedNetwork()
    sched := hsched.NewSerial()
    harn := h.New(clock, net, sched)

    // wire 3 nodes into cluster and harness
    setupCluster(harn, hnet.NodeID("A"), hnet.NodeID("B"), hnet.NodeID("C"))

    script := h.Script{Actions: []h.TimedAction{
    h.StartNode(hnet.NodeID("A"))(0),
    h.StartNode(hnet.NodeID("B"))(0),
    h.StartNode(hnet.NodeID("C"))(0),
        // normal client ops to leader A
    h.ClientSet(hnet.NodeID("A"), "user:1", "Alice")(5 * time.Millisecond),
    h.ClientSubscribe(hnet.NodeID("A"), "orders")(7 * time.Millisecond),
    h.ClientSet(hnet.NodeID("A"), "user:2", "Bob")(10 * time.Millisecond),
        // allow deliveries
        h.AdvanceBy(5 * time.Millisecond)(12 * time.Millisecond),
        // verify convergence
        h.VerifyEqual(hnet.NodeID("A"), hnet.NodeID("B"), hnet.NodeID("C"))(20 * time.Millisecond),
    }}

    script.Run(harn)

    // direct hash check as well
    ha := hex.EncodeToString(harn.Nodes[hnet.NodeID("A")].StateHash())
    hb := hex.EncodeToString(harn.Nodes[hnet.NodeID("B")].StateHash())
    hc := hex.EncodeToString(harn.Nodes[hnet.NodeID("C")].StateHash())
    if ha != hb || hb != hc { t.Fatalf("hashes differ: A=%s B=%s C=%s", ha, hb, hc) }
}

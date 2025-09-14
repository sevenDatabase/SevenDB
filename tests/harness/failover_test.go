package harness_test

import (
	"testing"
	"time"

	h "github.com/sevenDatabase/SevenDB/internal/harness"
	hclock "github.com/sevenDatabase/SevenDB/internal/harness/clock"
	hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
	hsched "github.com/sevenDatabase/SevenDB/internal/harness/scheduler"
)

func TestFailoverChaos(t *testing.T) {
	clock := hclock.NewSimulatedClock(time.Unix(0, 0))
	net := hnet.NewSimulatedNetwork()
	sched := hsched.NewSerial()
	harn := h.New(clock, net, sched)

	// wire 3 nodes
	setupCluster(harn, "A", "B", "C")

	script := h.Script{Actions: []h.TimedAction{
		// Start cluster
		h.StartNode("A")(0),
		h.StartNode("B")(0),
		h.StartNode("C")(0),
		// subscribe on A so notifier will deliver client updates
		h.ClientSubscribe("A", "orders")(5 * time.Millisecond),
		// enqueue a write but crash A before deliveries
		h.ClientSet("A", "order:1", "open")(10 * time.Millisecond),
		h.CrashNode("A")(11 * time.Millisecond),
		// advance clock to trigger failover election and notifier change
		h.AdvanceBy(10 * time.Millisecond)(20 * time.Millisecond),
		// ensure B and C converge
		h.VerifyEqual("B", "C")(25 * time.Millisecond),
	}}

	script.Run(harn)

	// After failover, cluster.clientLog should contain the update once
	if len(cluster.clientLog) == 0 || cluster.clientLog[0] != "order:1=open" {
		t.Fatalf("expected client log to contain delivered update, got %#v", cluster.clientLog)
	}
}

package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	hclock "github.com/sevenDatabase/SevenDB/internal/harness/clock"
	hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
	hsched "github.com/sevenDatabase/SevenDB/internal/harness/scheduler"
)

// Node abstracts the minimal hooks the harness needs from a replica.
// Integrate your real node implementation by adapting to this interface.
type Node interface {
	ID() hnet.NodeID
	Start()
	Stop()
	// HandleMessage is invoked by the harness when a message arrives.
	HandleMessage(from hnet.NodeID, msg hnet.Message)
	// Client operations used by scripts (MVP: minimal subset)
	ClientSet(key, value string)
	ClientSubscribe(key string)
	// StateHash returns a deterministic hash representing replica state.
	StateHash() []byte
}

// Harness wires clock, network, and scheduler to run deterministic scripts.
type Harness struct {
	Clock     hclock.Clock
	Network   hnet.Network
	Scheduler hsched.Scheduler
	Nodes     map[hnet.NodeID]Node
}

// New creates a new Harness with provided components; if nil, defaults are used.
func New(c hclock.Clock, n hnet.Network, s hsched.Scheduler) *Harness {
	if c == nil {
		c = hclock.NewSimulatedClock(time.Unix(0, 0))
	}
	if n == nil {
		n = hnet.NewSimulatedNetwork()
	}
	if s == nil {
		s = hsched.NewSerial()
	}
	return &Harness{
		Clock:     c,
		Network:   n,
		Scheduler: s,
		Nodes:     make(map[hnet.NodeID]Node),
	}
}

// RegisterNode adds a node into the harness's registry.
func (h *Harness) RegisterNode(n Node) {
	h.Nodes[n.ID()] = n
}

// DeliverNext delivers one message (if available) to dst node via scheduler.
func (h *Harness) DeliverNext(dst hnet.NodeID) bool {
	from, msg, ok := h.Network.Recv(dst)
	if !ok { return false }
	node, exists := h.Nodes[dst]
	if !exists { return false }
	h.Scheduler.Enqueue(func() { node.HandleMessage(from, msg) })
	return true
}

// DrainAll delivers all pending messages to their destinations, serially.
func (h *Harness) DrainAll() {
	// Keep trying until no mailbox has messages and queue drains.
	for {
		progressed := false
		for id := range h.Nodes {
			if h.DeliverNext(id) { progressed = true }
		}
		h.Scheduler.RunAll()
		if !progressed {
			break
		}
	}
}

// ComputeClusterHash returns a hex hash summarizing all nodes' state hashes.
func (h *Harness) ComputeClusterHash() string {
	// Sort ids for determinism
	ids := make([]string, 0, len(h.Nodes))
	for id := range h.Nodes { ids = append(ids, string(id)) }
	sort.Strings(ids)
	hasher := sha256.New()
	for _, sid := range ids {
		hasher.Write([]byte(sid))
		hasher.Write(h.Nodes[hnet.NodeID(sid)].StateHash())
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// MustEqualHashes panics if the provided node IDs do not share equal state hashes.
func (h *Harness) MustEqualHashes(ids ...hnet.NodeID) {
	if len(ids) <= 1 { return }
	base := h.Nodes[ids[0]].StateHash()
	for _, id := range ids[1:] {
		other := h.Nodes[id].StateHash()
		if hex.EncodeToString(base) != hex.EncodeToString(other) {
			panic(fmt.Errorf("hash mismatch: %s vs %s on node %s", hex.EncodeToString(base), hex.EncodeToString(other), id))
		}
	}
}


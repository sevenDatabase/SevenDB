package harness_test

// avoid t.parallel() in tests using shared cluster state

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"

	h "github.com/sevenDatabase/SevenDB/internal/harness"
	hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
)

// testCluster provides shared state for the fake nodes within a single test.
type testCluster struct {
	nodes     map[hnet.NodeID]*fakeNode
	network   hnet.Network
	notifier  hnet.NodeID
	delivered map[int]struct{}
	nextSeq   int
	// clientLog collects notifications delivered by the active notifier
	clientLog []string
	subs      map[string]struct{}
}

// fakeNode implements h.Node and the optional tickable (OnTick) hook.
type fakeNode struct {
	id      hnet.NodeID
	started bool
	state   map[string]string // replicated state
	outbox  map[int]string    // replicated outbox entries (seq -> payload)
	cluster *testCluster      // back-pointer to owning test cluster
}

func newFakeNode(id hnet.NodeID, c *testCluster) *fakeNode {
	return &fakeNode{id: id, state: make(map[string]string), outbox: make(map[int]string), cluster: c}
}

func (n *fakeNode) ID() hnet.NodeID { return n.id }
func (n *fakeNode) Start()          { n.started = true }
func (n *fakeNode) Stop()           { n.started = false }

// HandleMessage applies replicated state updates from the leader.
// Format: "SET|key|value|seq"
func (n *fakeNode) HandleMessage(_ hnet.NodeID, msg hnet.Message) {
	if !n.started {
		return
	}
	parts := strings.SplitN(string(msg), "|", 4)
	if len(parts) == 4 && parts[0] == "SET" {
		key, value, seqStr := parts[1], parts[2], parts[3]
		seq, _ := strconv.Atoi(seqStr)
		n.state[key] = value
		// Followers also record outbox entries via replication to enable notifier failover
		n.outbox[seq] = key + "=" + value
	}
}

// ClientSet runs on the leader. It appends to local state/outbox and replicates to followers.
func (n *fakeNode) ClientSet(key, value string) {
	if !n.started {
		return
	}
	seq := n.cluster.nextSeq
	n.cluster.nextSeq++
	event := key + "=" + value
	// Append locally (leader)
	n.state[key] = value
	n.outbox[seq] = event
	// Replicate via network to followers only
	payload := hnet.Message([]byte(fmt.Sprintf("SET|%s|%s|%d", key, value, seq)))
	for _, dst := range sortedNodeIDs(n.cluster.nodes) {
		if dst == n.id {
			continue
		}
		n.cluster.network.Send(n.id, dst, payload)
	}
}

func (n *fakeNode) ClientSubscribe(key string) {
	if !n.started {
		return
	}
	if n.cluster.subs == nil {
		n.cluster.subs = make(map[string]struct{})
	}
	n.cluster.subs[key] = struct{}{}
}

func (n *fakeNode) StateHash() []byte {
	keys := make([]string, 0, len(n.state))
	for k := range n.state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hsh := sha256.New()
	for _, k := range keys {
		hsh.Write([]byte(k))
		hsh.Write([]byte("="))
		hsh.Write([]byte(n.state[k]))
		hsh.Write([]byte(";"))
	}
	return hsh.Sum(nil)
}

// OnTick elects a notifier if needed and flushes outbox from the active notifier.
func (n *fakeNode) OnTick() {
	c := n.cluster
	// Elect notifier if none or stopped
	if c.notifier == "" || !c.nodes[c.notifier].started {
		// choose lexicographically smallest started node
		ids := make([]string, 0, len(c.nodes))
		for id, node := range c.nodes {
			if node.started {
				ids = append(ids, string(id))
			}
		}
		sort.Strings(ids)
		if len(ids) > 0 {
			c.notifier = hnet.NodeID(ids[0])
		}
	}
	// Only active notifier flushes
	if n.id != c.notifier || !n.started {
		return
	}
	if c.delivered == nil {
		c.delivered = make(map[int]struct{})
	}
	// Flush in seq order for determinism
	seqs := make([]int, 0, len(n.outbox))
	for seq := range n.outbox {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	for _, seq := range seqs {
		if _, ok := c.delivered[seq]; ok {
			continue
		}
		// deliver to client log if any node has subscriptions
		if len(c.subs) > 0 { // any subscription in cluster
			c.clientLog = append(c.clientLog, n.outbox[seq])
		}
		c.delivered[seq] = struct{}{}
	}
}

// Helper to wire nodes into harness and return a per-test cluster handle.
func setupCluster(harn *h.Harness, ids ...hnet.NodeID) *testCluster {
	c := &testCluster{
		nodes:     make(map[hnet.NodeID]*fakeNode),
		network:   harn.Network,
		notifier:  "",
		delivered: make(map[int]struct{}),
		nextSeq:   1,
		clientLog: nil,
		subs:      make(map[string]struct{}),
	}
	for _, id := range ids {
		n := newFakeNode(id, c)
		c.nodes[id] = n
		harn.RegisterNode(n)
	}
	return c
}

// sortedNodeIDs returns the node IDs in deterministic lexicographic order.
func sortedNodeIDs(m map[hnet.NodeID]*fakeNode) []hnet.NodeID {
	ids := make([]hnet.NodeID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
	return ids
}

package harness_test

import (
    "crypto/sha256"
    "sort"
    "strings"

    h "github.com/sevenDatabase/SevenDB/internal/harness"
    hnet "github.com/sevenDatabase/SevenDB/internal/harness/network"
)

// testCluster provides shared state for the fake nodes across tests.
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

var cluster testCluster

// fakeNode implements h.Node and the optional tickable (OnTick) hook.
type fakeNode struct {
    id      hnet.NodeID
    started bool
    state   map[string]string        // replicated state
    outbox  map[int]string           // replicated outbox entries (seq -> payload)
}

func newFakeNode(id hnet.NodeID) *fakeNode {
    return &fakeNode{id: id, state: make(map[string]string), outbox: make(map[int]string)}
}

func (n *fakeNode) ID() hnet.NodeID { return n.id }
func (n *fakeNode) Start()          { n.started = true }
func (n *fakeNode) Stop()           { n.started = false }

// HandleMessage applies replicated state updates. Format: "SET|key|value"
func (n *fakeNode) HandleMessage(_ hnet.NodeID, msg hnet.Message) {
    if !n.started { return }
    parts := strings.SplitN(string(msg), "|", 3)
    if len(parts) == 3 && parts[0] == "SET" {
        n.state[parts[1]] = parts[2]
    }
}

// ClientSet is invoked on the leader in tests. It performs two actions:
// 1) Replicates state update to all nodes via the harness network.
// 2) Appends to the replicated outbox (same seq on all nodes) for client delivery.
func (n *fakeNode) ClientSet(key, value string) {
    if !n.started { return }
    // Replicate state via network
    payload := hnet.Message([]byte("SET|"+key+"|"+value))
    for dst := range cluster.nodes {
        cluster.network.Send(n.id, dst, payload)
    }
    // Replicate outbox entry to all nodes with the same sequence
    seq := cluster.nextSeq
    cluster.nextSeq++
    event := key + "=" + value
    for _, node := range cluster.nodes {
        node.outbox[seq] = event
    }
}

func (n *fakeNode) ClientSubscribe(key string) {
    if !n.started { return }
    if cluster.subs == nil { cluster.subs = make(map[string]struct{}) }
    cluster.subs[key] = struct{}{}
}

func (n *fakeNode) StateHash() []byte {
    keys := make([]string, 0, len(n.state))
    for k := range n.state { keys = append(keys, k) }
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
    // Elect notifier if none or stopped
    if cluster.notifier == "" || !cluster.nodes[cluster.notifier].started {
        // choose lexicographically smallest started node
        ids := make([]string, 0, len(cluster.nodes))
        for id, node := range cluster.nodes {
            if node.started { ids = append(ids, string(id)) }
        }
        sort.Strings(ids)
        if len(ids) > 0 { cluster.notifier = hnet.NodeID(ids[0]) }
    }
    // Only active notifier flushes
    if n.id != cluster.notifier || !n.started { return }
    if cluster.delivered == nil { cluster.delivered = make(map[int]struct{}) }
    // Flush in seq order for determinism
    seqs := make([]int, 0, len(n.outbox))
    for seq := range n.outbox { seqs = append(seqs, seq) }
    sort.Ints(seqs)
    for _, seq := range seqs {
        if _, ok := cluster.delivered[seq]; ok { continue }
        // deliver to client log if any node has subscriptions
        if len(cluster.subs) > 0 { // any subscription in cluster
            cluster.clientLog = append(cluster.clientLog, n.outbox[seq])
        }
        cluster.delivered[seq] = struct{}{}
    }
}

// Helper to wire nodes into harness and cluster for tests
func setupCluster(harn *h.Harness, ids ...hnet.NodeID) {
    cluster.nodes = make(map[hnet.NodeID]*fakeNode)
    cluster.network = harn.Network
    cluster.notifier = ""
    cluster.delivered = make(map[int]struct{})
    cluster.nextSeq = 1
    cluster.clientLog = nil
    cluster.subs = make(map[string]struct{})
    for _, id := range ids {
        n := newFakeNode(id)
        cluster.nodes[id] = n
        harn.RegisterNode(n)
    }
}

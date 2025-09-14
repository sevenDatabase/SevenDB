package network

import "sync"

// NodeID uniquely identifies a node/replica in the harness network.
type NodeID string

// Message is an opaque payload delivered between nodes.
// For MVP, treat as []byte to avoid coupling.
type Message []byte

// Network defines a deterministic message passing interface for replicas.
type Network interface {
	// Send enqueues a message from src to dst. Delivery is FIFO and reliable by default.
	Send(src, dst NodeID, msg Message)
	// Recv dequeues the next message for the given node, if any.
	// Returns ok=false when the mailbox is empty.
	Recv(dst NodeID) (from NodeID, msg Message, ok bool)
	// DropAll clears the mailbox for a node (useful for crashes in tests).
	DropAll(dst NodeID)
	// DeliverOne pops and returns the next message for dst without invoking handlers.
	// Useful for harnesses to schedule handling via a scheduler.
	DeliverOne(dst NodeID) (from NodeID, msg Message, ok bool)
	// MailboxSize returns the number of queued messages for dst.
	MailboxSize(dst NodeID) int
	// Block and Unblock can be used to simulate partitions or drops between pairs.
	Block(src, dst NodeID)
	Unblock(src, dst NodeID)
}

// SimulatedNetwork is an in-memory FIFO, reliable message bus.
type SimulatedNetwork struct {
	mu       sync.Mutex
	mailboxes map[NodeID][]queued
	blocked  map[[2]NodeID]bool // blocked[src,dst] == true means drop on send
}

type queued struct {
	from NodeID
	msg  Message
}

// NewSimulatedNetwork constructs an empty simulated network.
func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{mailboxes: make(map[NodeID][]queued), blocked: make(map[[2]NodeID]bool)}
}

// Send implements Network.
func (n *SimulatedNetwork) Send(src, dst NodeID, msg Message) {
	n.mu.Lock()
	if n.blocked[[2]NodeID{src, dst}] {
		n.mu.Unlock()
		return
	}
	n.mailboxes[dst] = append(n.mailboxes[dst], queued{from: src, msg: append(Message(nil), msg...)})
	n.mu.Unlock()
}

// Recv implements Network.
func (n *SimulatedNetwork) Recv(dst NodeID) (NodeID, Message, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	q := n.mailboxes[dst]
	if len(q) == 0 {
		return "", nil, false
	}
	head := q[0]
	// Pop FIFO
	if len(q) == 1 {
		delete(n.mailboxes, dst)
	} else {
		n.mailboxes[dst] = q[1:]
	}
	return head.from, head.msg, true
}

// DropAll implements Network.
func (n *SimulatedNetwork) DropAll(dst NodeID) {
	n.mu.Lock()
	delete(n.mailboxes, dst)
	n.mu.Unlock()
}

// DeliverOne pops one message for dst if any.
func (n *SimulatedNetwork) DeliverOne(dst NodeID) (NodeID, Message, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	q := n.mailboxes[dst]
	if len(q) == 0 { return "", nil, false }
	head := q[0]
	if len(q) == 1 {
		delete(n.mailboxes, dst)
	} else {
		n.mailboxes[dst] = q[1:]
	}
	return head.from, head.msg, true
}

// MailboxSize returns number of messages queued for dst.
func (n *SimulatedNetwork) MailboxSize(dst NodeID) int {
	n.mu.Lock()
	l := len(n.mailboxes[dst])
	n.mu.Unlock()
	return l
}

// Block marks communication from src to dst as dropped.
func (n *SimulatedNetwork) Block(src, dst NodeID) {
	n.mu.Lock()
	n.blocked[[2]NodeID{src, dst}] = true
	n.mu.Unlock()
}

// Unblock removes a previously set block rule between src and dst.
func (n *SimulatedNetwork) Unblock(src, dst NodeID) {
	n.mu.Lock()
	delete(n.blocked, [2]NodeID{src, dst})
	n.mu.Unlock()
}


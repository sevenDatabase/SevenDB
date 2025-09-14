package harness

import (
	"sort"
	"time"

	hnet "github.com/sevendatabase/sevendb/internal/harness/network"
)

// TimedAction represents an action executed when the simulated clock reaches At.
// At is an absolute time relative to the harness clock's start.
type TimedAction struct {
	At time.Duration
	Fn func(h *Harness)
}

// Script is a list of timed actions to execute deterministically.
type Script struct {
	Actions []TimedAction
}

// Run executes the script against the provided harness deterministically.
func (s Script) Run(h *Harness) {
	// sort by time, stable to preserve insertion order for identical At
	actions := append([]TimedAction(nil), s.Actions...)
	sort.SliceStable(actions, func(i, j int) bool { return actions[i].At < actions[j].At })

	var now time.Duration
	for _, a := range actions {
		if a.At > now {
			h.Clock.Advance(a.At - now)
			// allow nodes to react to time advancing deterministically
			onTick(h)
			now = a.At
		}
		if a.Fn != nil {
			a.Fn(h)
		}
		// after each step, drain deliveries and run pending tasks
		h.DrainAll()
	}
}

// Helpers to construct common actions for scripts

func StartNode(id hnet.NodeID) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) {
			if n, ok := h.Nodes[id]; ok { n.Start() }
		}}
	}
}

func StopNode(id hnet.NodeID) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) {
			if n, ok := h.Nodes[id]; ok { n.Stop(); h.Network.DropAll(id) }
		}}
	}
}

func ClientSet(id hnet.NodeID, key, value string) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) {
			if n, ok := h.Nodes[id]; ok { n.ClientSet(key, value) }
		}}
	}
}

func ClientSubscribe(id hnet.NodeID, key string) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) {
			if n, ok := h.Nodes[id]; ok { n.ClientSubscribe(key) }
		}}
	}
}

// CrashNode simulates a crash of a node by stopping it and dropping queued messages.
func CrashNode(id hnet.NodeID) TimedActionBuilder {
	return StopNode(id)
}

// AdvanceBy advances the clock by delta at the given At time (useful for explicit waits).
func AdvanceBy(delta time.Duration) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) {
			h.Clock.Advance(delta)
			onTick(h)
		}}
	}
}

// VerifyEqual asserts nodes share identical state hashes at given time.
func VerifyEqual(ids ...hnet.NodeID) TimedActionBuilder {
	return func(at time.Duration) TimedAction {
		return TimedAction{At: at, Fn: func(h *Harness) { h.MustEqualHashes(ids...) }}
	}
}

// TimedActionBuilder produces a timed action bound to an At timestamp when invoked.
type TimedActionBuilder func(at time.Duration) TimedAction

// Example usage:
//
// script := Script{Actions: []TimedAction{
//     StartNode("A")(0),
//     StartNode("B")(0),
//     StartNode("C")(0),
//     ClientSubscribe("A", "user:1")(5 * time.Millisecond),
//     ClientSet("B", "user:1", "Alice")(10 * time.Millisecond),
//     CrashNode("A")(15 * time.Millisecond),
//     // at 20 ms we just advance/deliver; add an explicit advance if needed
//     VerifyEqual("B", "C")(25 * time.Millisecond),
// }}
// script.Run(h)

// tickable allows nodes to process time-driven events deterministically.
// Nodes used in tests may implement this (optional).
type tickable interface { OnTick() }

// onTick invokes OnTick on all registered nodes that support it.
func onTick(h *Harness) {
	for _, n := range h.Nodes {
		if t, ok := n.(tickable); ok {
			t.OnTick()
		}
	}
}


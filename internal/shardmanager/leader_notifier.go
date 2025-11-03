package shardmanager

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sevenDatabase/SevenDB/internal/emission"
	"github.com/sevenDatabase/SevenDB/internal/raft"
)

// gatedSender wraps an emission.Sender and allows enabling/disabling delivery
// without swapping the sender instance from the Notifier. When disabled, Send is a no-op.
type gatedSender struct {
	enabled    atomic.Bool
	mu         sync.RWMutex
	underlying emission.Sender
}

func newGatedSender(under emission.Sender) *gatedSender {
	g := &gatedSender{}
	if under != nil {
		g.underlying = under
	}
	// default disabled; controller will toggle based on leadership
	g.enabled.Store(false)
	return g
}

func (g *gatedSender) SetEnabled(on bool) { g.enabled.Store(on) }

// SetUnderlying replaces the transport implementation while preserving gating.
func (g *gatedSender) SetUnderlying(under emission.Sender) {
	g.mu.Lock()
	g.underlying = under
	g.mu.Unlock()
}

// Send implements emission.Sender with gating semantics.
func (g *gatedSender) Send(ctx context.Context, ev *emission.DataEvent) error {
	if !g.enabled.Load() {
		// follower: signal not-leader so notifier will retry later and not mark as delivered
		return &notLeaderError{}
	}
	g.mu.RLock()
	s := g.underlying
	g.mu.RUnlock()
	if s == nil {
		// leader but no transport yet; log via Notifier's path will warn/handle backoff
		return &noTransportError{}
	}
	return s.Send(ctx, ev)
}

type noTransportError struct{}

func (e *noTransportError) Error() string { return "emission: no transport" }

type notLeaderError struct{}

func (e *notLeaderError) Error() string { return "emission: not leader" }

// leaderNotifierController observes raft leadership for a shard and toggles the gate.
// It provides a clean seam to swap out later with an independent election mechanism.
type leaderNotifierController struct {
	shardID  string
	rn       *raft.ShardRaftNode
	gate     *gatedSender
	interval time.Duration
}

func newLeaderNotifierController(shardID string, rn *raft.ShardRaftNode, gate *gatedSender) *leaderNotifierController {
	// poll interval chosen to be lightweight but responsive
	return &leaderNotifierController{shardID: shardID, rn: rn, gate: gate, interval: 100 * time.Millisecond}
}

func (c *leaderNotifierController) Start(ctx context.Context) {
	// set initial state
	st := c.rn.Status()
	c.gate.SetEnabled(st.IsLeader)
	go c.loop(ctx)
}

func (c *leaderNotifierController) loop(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	prev := c.rn.Status().IsLeader
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st := c.rn.Status()
			if st.IsLeader != prev {
				prev = st.IsLeader
				c.gate.SetEnabled(st.IsLeader)
				role := "follower"
				if st.IsLeader {
					role = "leader"
				}
				slog.Info("notifier role updated", slog.String("shard", c.shardID), slog.String("role", role))
			}
		}
	}
}

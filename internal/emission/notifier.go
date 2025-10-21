package emission

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Sender abstracts transport to clients. Implementation can be gRPC/Websocket/etc.
type Sender interface {
	Send(ctx context.Context, ev *DataEvent) error
}

// Proposer abstracts proposing a purge back into raft.
type Proposer interface {
	ProposePurge(ctx context.Context, sub string, upTo EmitSeq) error
}

// Notifier is a single-holder loop that delivers pending outbox entries and
// processes client acknowledgments.
type Notifier struct {
	mgr      *Manager
	sender   Sender
	senderMu sync.RWMutex
	proposer Proposer
	// ackCh receives client acks; in real system wired via RPC.
	ackCh chan *ClientAck
	// simple ticker-based poll for pending.
	interval time.Duration
	stopCh   chan struct{}
}

func NewNotifier(mgr *Manager, sender Sender, proposer Proposer) *Notifier {
	// Use a small poll interval to keep delivery latency low in tests and stub-raft mode
	return &Notifier{mgr: mgr, sender: sender, proposer: proposer, ackCh: make(chan *ClientAck, 1024), interval: 5 * time.Millisecond, stopCh: make(chan struct{})}
}

// Ack injects a client ack (test/simulated path for now).
func (n *Notifier) Ack(ack *ClientAck) { n.ackCh <- ack }

// SetSender swaps the sender implementation safely at runtime.
func (n *Notifier) SetSender(s Sender) {
	n.senderMu.Lock()
	defer n.senderMu.Unlock()
	n.sender = s
}

func (n *Notifier) getSender() Sender {
	n.senderMu.RLock()
	defer n.senderMu.RUnlock()
	return n.sender
}

func (n *Notifier) Start(ctx context.Context) {
	go n.loop(ctx)
}

func (n *Notifier) Stop() { close(n.stopCh) }

func (n *Notifier) loop(ctx context.Context) {
	t := time.NewTicker(n.interval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ctx.Done():
			return
		case ack := <-n.ackCh:
			// Validate monotonic ack and propose purge
			if ok := n.mgr.ValidateAck(ack.SubID, ack.EmitSeq); !ok {
				slog.Warn("ACK regression or duplicate", slog.String("sub_id", ack.SubID), slog.String("emit_seq", ack.EmitSeq.String()))
				continue
			}
			if n.proposer != nil {
				if err := n.proposer.ProposePurge(ctx, ack.SubID, ack.EmitSeq); err != nil {
					slog.Error("propose purge failed", slog.Any("error", err))
				}
			} else {
				// Test/local fallback: apply purge directly when no raft proposer is wired.
				n.mgr.ApplyOutboxPurge(ctx, ack.SubID, ack.EmitSeq)
			}
		case <-t.C:
			// scan pending and send, sub by sub
			subs := n.mgr.SubsWithPending()
			for _, sub := range subs {
				entries := n.mgr.Pending(sub)
				for _, e := range entries {
					ev := &DataEvent{SubID: sub, EmitSeq: e.Seq, Delta: e.Delta}
					sender := n.getSender()
					if sender == nil {
						slog.Warn("no sender set; skipping delivery", slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
						break
					}
					if err := sender.Send(ctx, ev); err != nil {
						slog.Warn("send failed; will retry on next tick", slog.Any("error", err), slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
						break // backoff this sub until next tick
					}
					slog.Debug("SEND", slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
				}
			}
		}
	}
}

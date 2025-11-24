package emission

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/sevenDatabase/SevenDB/config"
	"github.com/sevenDatabase/SevenDB/internal/logging"
)

// Test hooks for crash window simulations in tests. These are no-ops in production
// unless set by tests. Keep exported so external package tests can assign closures.
// They MUST be fast and side-effect free unless intentionally used to simulate failures.
var (
	// TestHookBeforeSend fires right before a DataEvent is sent to the client.
	TestHookBeforeSend func(sub string, seq EmitSeq)
	// TestHookAfterSendBeforeAck fires immediately after a successful send, before any ACK is processed.
	TestHookAfterSendBeforeAck func(sub string, seq EmitSeq)
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
	// test hooks captured at start to avoid races with package-level test vars
	hookBeforeSend         func(sub string, seq EmitSeq)
	hookAfterSendBeforeAck func(sub string, seq EmitSeq)
	// resumeFrom optionally holds the next commit index to resume from per sub after reconnect
	resumeMu   sync.Mutex
	resumeFrom map[string]uint64
	// sentThrough tracks the highest sequence already sent to a sub (but not necessarily ACKed yet).
	// This prevents resending the same entry on every poll tick and spamming clients when ACKs lag.
	sentMu      sync.Mutex
	sentThrough map[string]EmitSeq
	// bucketID labels metrics for this notifier's shard/bucket
	bucketID string
}

func NewNotifier(mgr *Manager, sender Sender, proposer Proposer, bucketID string) *Notifier {
	// Use configured poll interval when available; default to 5ms.
	pollMs := 5
	if config.Config != nil && config.Config.EmissionNotifierPollMs > 0 {
		pollMs = config.Config.EmissionNotifierPollMs
	}
	return &Notifier{mgr: mgr, sender: sender, proposer: proposer, ackCh: make(chan *ClientAck, 1024), interval: time.Duration(pollMs) * time.Millisecond, stopCh: make(chan struct{}), resumeFrom: make(map[string]uint64), sentThrough: make(map[string]EmitSeq), bucketID: bucketID}
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
	// Capture current test hooks once to avoid data races if tests mutate globals later.
	n.hookBeforeSend = TestHookBeforeSend
	n.hookAfterSendBeforeAck = TestHookAfterSendBeforeAck
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
			// Advance sentThrough watermark up to the acked index for this sub.
			n.sentMu.Lock()
			// Compare full EmitSeq (Epoch + CommitIndex)
			if last, ok := n.sentThrough[ack.SubID]; !ok || (ack.EmitSeq.Epoch.EpochCounter > last.Epoch.EpochCounter) || (ack.EmitSeq.Epoch.EpochCounter == last.Epoch.EpochCounter && ack.EmitSeq.CommitIndex > last.CommitIndex) {
				n.sentThrough[ack.SubID] = ack.EmitSeq
			}
			n.sentMu.Unlock()
			if n.bucketID != "" {
				Metrics.IncAckFor(n.bucketID)
			} else {
				Metrics.IncAck()
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
			n.processTick(ctx)
		}
	}
}

// TestTickOnce executes one deterministic cycle: drain acks and perform a single send scan.
// Exported strictly for tests to avoid reliance on time.Ticker and sleeps.
func (n *Notifier) TestTickOnce(ctx context.Context) {
	n.processAcks(ctx)
	n.processTick(ctx)
}

// TestProcessAcks drains the ack channel once (for tests).
func (n *Notifier) TestProcessAcks(ctx context.Context) { n.processAcks(ctx) }

// SetResumeFrom sets the next commit index to resume sending for a subscription.
// Note: This assumes the resume is for the CURRENT epoch or a known valid epoch.
// Since we don't pass epoch here, we construct a synthetic one or just update the index if epoch matches.
// For MVP, we will assume the caller wants to resume in the current epoch context or reset.
// However, since sentThrough now tracks EmitSeq, we need to be careful.
// If we are resuming, we likely want to reset the watermark to (CurrentEpoch, nextCommitIdx-1).
// But we don't know CurrentEpoch here easily without querying Manager or Applier.
// Workaround: We will invalidate the sentThrough entry if we are forcing a resume,
// effectively clearing the watermark so the Notifier picks up from the resume point.
func (n *Notifier) SetResumeFrom(sub string, nextCommitIdx uint64) {
	n.resumeMu.Lock()
	defer n.resumeMu.Unlock()
	if nextCommitIdx == 0 {
		delete(n.resumeFrom, sub)
		return
	}
	n.resumeFrom[sub] = nextCommitIdx
	
	// Clear sentThrough to allow Notifier to re-evaluate based on resumeFrom
	n.sentMu.Lock()
	delete(n.sentThrough, sub)
	n.sentMu.Unlock()
}

// ClearWatermarksForClient removes sentThrough entries for a given client ID prefix.
// This ensures that if a client disconnects and reconnects without explicit resume,
// we don't skip sending pending entries due to stale watermarks.
func (n *Notifier) ClearWatermarksForClient(clientID string) {
	prefix := clientID + ":"
	n.sentMu.Lock()
	defer n.sentMu.Unlock()
	for sub := range n.sentThrough {
		if strings.HasPrefix(sub, prefix) {
			delete(n.sentThrough, sub)
		}
	}
}

// processAcks drains the ack channel once and applies purges/metrics. Intended for deterministic tests.
func (n *Notifier) processAcks(ctx context.Context) {
	for {
		select {
		case ack := <-n.ackCh:
			if ok := n.mgr.ValidateAck(ack.SubID, ack.EmitSeq); !ok {
				slog.Warn("ACK regression or duplicate", slog.String("sub_id", ack.SubID), slog.String("emit_seq", ack.EmitSeq.String()))
				continue
			}
			n.sentMu.Lock()
			last, ok := n.sentThrough[ack.SubID]
			isNewer := !ok || ack.EmitSeq.Epoch.EpochCounter > last.Epoch.EpochCounter || (ack.EmitSeq.Epoch.EpochCounter == last.Epoch.EpochCounter && ack.EmitSeq.CommitIndex > last.CommitIndex)
			if isNewer {
				n.sentThrough[ack.SubID] = ack.EmitSeq
			}
			n.sentMu.Unlock()
			if n.bucketID != "" {
				Metrics.IncAckFor(n.bucketID)
			} else {
				Metrics.IncAck()
			}
			if n.proposer != nil {
				if err := n.proposer.ProposePurge(ctx, ack.SubID, ack.EmitSeq); err != nil {
					slog.Error("propose purge failed", slog.Any("error", err))
				}
			} else {
				n.mgr.ApplyOutboxPurge(ctx, ack.SubID, ack.EmitSeq)
			}
		default:
			return
		}
	}
}

// processTick performs one deterministic scan-send pass over pending entries. Intended for tests.
func (n *Notifier) processTick(ctx context.Context) {
	// scan pending and send, sub by sub
	subs := n.mgr.SubsWithPending()
	if n.bucketID != "" {
		Metrics.SetSubsWithPendingFor(n.bucketID, len(subs))
	} else {
		Metrics.SetSubsWithPending(len(subs))
	}
	for _, sub := range subs {
		entries := n.mgr.Pending(sub)
		for _, e := range entries {
			// Skip resending entries already sent up to this commit index.
			n.sentMu.Lock()
			lastSent := n.sentThrough[sub]
			n.sentMu.Unlock()
			
			n.resumeMu.Lock()
			resumeIdx := n.resumeFrom[sub]
			n.resumeMu.Unlock()

			shouldSkip := false
			if e.Seq.Epoch.EpochCounter < lastSent.Epoch.EpochCounter {
				shouldSkip = true
			} else if e.Seq.Epoch.EpochCounter == lastSent.Epoch.EpochCounter {
				if e.Seq.CommitIndex <= lastSent.CommitIndex {
					shouldSkip = true
				}
			}

			if shouldSkip {
				logging.VInfo("verbose", "emission-notifier: skipping sent entry",
					slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()), slog.String("last_sent", lastSent.String()))
				continue
			}
			
			if resumeIdx > 0 && e.Seq.CommitIndex < resumeIdx {
				logging.VInfo("verbose", "emission-notifier: skipping older entry",
					slog.String("sub_id", sub), slog.Uint64("commit_index", e.Seq.CommitIndex), slog.Uint64("resume_idx", resumeIdx))
				continue // skip older entries until reaching resume point
			}
			ev := &DataEvent{SubID: sub, EmitSeq: e.Seq, Delta: e.Delta}
			sender := n.getSender()
			if sender == nil {
				// follower or sender not yet wired; elevate log to Info temporarily for field diagnosis
				logging.VInfo("verbose", "emission-notifier: no sender set; skipping delivery",
					slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
				break
			}
			logging.VInfo("verbose", "emission-notifier: sending event",
				slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
			// Resolve hook: prefer instance-captured, else fall back to package-level for tests
			hookBefore := n.hookBeforeSend
			if hookBefore == nil {
				hookBefore = TestHookBeforeSend
			}
			if hookBefore != nil {
				// Best-effort; hooks should not panic the notifier loop.
				func() { defer func() { _ = recover() }(); hookBefore(sub, e.Seq) }()
				// If a test simulates a crash via context cancellation in the hook,
				// skip sending on this tick to model crash-before-send.
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			start := time.Now()
			if err := sender.Send(ctx, ev); err != nil {
				// Differentiate expected transient conditions to avoid noisy logs.
				msg := err.Error()
					if strings.Contains(msg, "not leader") || strings.Contains(msg, "no transport") ||
					strings.Contains(msg, "no active thread") || strings.Contains(msg, "no recipients") {
					// Elevate to Info to aid diagnosis when deliveries are missing
					logging.VInfo("verbose", "emission-notifier: send deferred",
						slog.String("reason", msg), slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
				} else {
					slog.Warn("send failed; will retry on next tick", slog.Any("error", err), slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
				}
				break // backoff this sub until next tick
			}
			hookAfter := n.hookAfterSendBeforeAck
			if hookAfter == nil {
				hookAfter = TestHookAfterSendBeforeAck
			}
			if hookAfter != nil {
				func() { defer func() { _ = recover() }(); hookAfter(sub, e.Seq) }()
			}
			// Mark this commit index as sent for this subscription to avoid spamming until ACK.
			n.sentMu.Lock()
			if lastSent.Epoch.EpochCounter < e.Seq.Epoch.EpochCounter || (lastSent.Epoch.EpochCounter == e.Seq.Epoch.EpochCounter && lastSent.CommitIndex < e.Seq.CommitIndex) {
				n.sentThrough[sub] = e.Seq
			}
			n.sentMu.Unlock()
			if n.bucketID != "" {
				Metrics.ObserveSendFor(n.bucketID, time.Since(start))
			} else {
				Metrics.ObserveSend(time.Since(start))
			}
				// Temporary elevated log to confirm every emission send; revert to Debug after investigation
				logging.VInfo("verbose", "emission-notifier sent",
					slog.String("sub_id", sub), slog.String("emit_seq", e.Seq.String()))
			// If we had a resume threshold and we reached/surpassed it, clear it
			if resumeIdx > 0 && e.Seq.CommitIndex >= resumeIdx {
				n.resumeMu.Lock()
				delete(n.resumeFrom, sub)
				n.resumeMu.Unlock()
			}
		}
	}
}

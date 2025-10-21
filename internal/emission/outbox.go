package emission

import (
	"context"
	"log/slog"
	"sort"
	"sync"
)

// OutboxEntry represents a durable emission awaiting delivery/ack.
type OutboxEntry struct {
	SubID string
	Seq   EmitSeq
	Delta []byte
}

// outboxState is the in-memory representation. Persistence is via raft apply
// of OutboxWrite/OutboxPurge records; this map is rebuilt on replay.
type outboxState struct {
	mu     sync.RWMutex
	bySub  map[string]map[uint64]*OutboxEntry // sub -> commit_index -> entry
	epochs map[string]EpochID                 // bucket -> epoch
}

func newOutboxState() *outboxState {
	return &outboxState{bySub: make(map[string]map[uint64]*OutboxEntry), epochs: make(map[string]EpochID)}
}

func (o *outboxState) write(e *OutboxEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.bySub[e.SubID]; !ok {
		o.bySub[e.SubID] = make(map[uint64]*OutboxEntry)
	}
	o.bySub[e.SubID][e.Seq.CommitIndex] = e
}

// purge removes all entries up to and including the given commit index for sub.
func (o *outboxState) purge(sub string, upTo uint64) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	m := o.bySub[sub]
	if m == nil {
		return 0
	}
	removed := 0
	for idx := range m {
		if idx <= upTo {
			delete(m, idx)
			removed++
		}
	}
	if len(m) == 0 {
		delete(o.bySub, sub)
	}
	return removed
}

// pendingSorted returns pending entries for a sub ordered by commit index.
func (o *outboxState) pendingSorted(sub string) []*OutboxEntry {
	o.mu.RLock()
	defer o.mu.RUnlock()
	m := o.bySub[sub]
	if m == nil {
		return nil
	}
	idxs := make([]int, 0, len(m))
	for k := range m {
		idxs = append(idxs, int(k))
	}
	sort.Ints(idxs)
	out := make([]*OutboxEntry, 0, len(m))
	for _, i := range idxs {
		out = append(out, m[uint64(i)])
	}
	return out
}

// Manager orchestrates writes/purges by proposing to raft and applying back.
// For MVP we expose Apply* to be called from the raft apply pipeline.
type Manager struct {
	ob *outboxState
	// lastAck tracks highest acked commit index per subscription for validation.
	mu      sync.RWMutex
	lastAck map[string]uint64
	// compaction watermark for validation; in MVP this can be set by caller.
	compactThrough map[string]uint64
}

func NewManager() *Manager {
	return &Manager{ob: newOutboxState(), lastAck: make(map[string]uint64), compactThrough: make(map[string]uint64)}
}

// ApplyOutboxWrite records a durable outbox entry as per raft apply.
func (m *Manager) ApplyOutboxWrite(ctx context.Context, sub string, seq EmitSeq, delta []byte) {
	m.ob.write(&OutboxEntry{SubID: sub, Seq: seq, Delta: delta})
	slog.Debug("OUTBOX_WRITE",
		slog.String("sub_id", sub),
		slog.String("emit_seq", seq.String()),
		slog.Int("delta_bytes", len(delta)))
}

// ApplyOutboxPurge removes entries up to the provided position (inclusive).
func (m *Manager) ApplyOutboxPurge(ctx context.Context, sub string, upTo EmitSeq) {
	removed := m.ob.purge(sub, upTo.CommitIndex)
	// Update compaction watermark so reconnect can detect stale sequences
	m.SetCompactedThrough(sub, upTo.CommitIndex)
	slog.Debug("OUTBOX_PURGE", slog.String("sub_id", sub), slog.String("up_to", upTo.String()), slog.Int("removed", removed))
}

// ValidateAck returns whether ack is strictly monotonic; updates lastAck if valid.
func (m *Manager) ValidateAck(sub string, seq EmitSeq) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	last := m.lastAck[sub]
	if seq.CommitIndex <= last {
		return false
	}
	m.lastAck[sub] = seq.CommitIndex
	return true
}

// Pending returns a snapshot of pending entries for a given sub.
func (m *Manager) Pending(sub string) []*OutboxEntry { return m.ob.pendingSorted(sub) }

// SubsWithPending returns a snapshot of sub IDs that currently have pending entries.
func (m *Manager) SubsWithPending() []string {
	m.ob.mu.RLock()
	defer m.ob.mu.RUnlock()
	out := make([]string, 0, len(m.ob.bySub))
	for sub := range m.ob.bySub {
		out = append(out, sub)
	}
	sort.Strings(out)
	return out
}

// SetCompactedThrough updates per-sub compacted watermark.
func (m *Manager) SetCompactedThrough(sub string, idx uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactThrough[sub] = idx
}

// Reconnect validates the client position and returns status with resume point.
func (m *Manager) Reconnect(req ReconnectRequest) ReconnectAck {
	m.mu.RLock()
	compacted := m.compactThrough[req.SubID]
	last := m.lastAck[req.SubID]
	m.mu.RUnlock()

	// If client is ahead of last ack (or future), treat as invalid for MVP.
	if req.LastProcessedEmitSeq.CommitIndex > last+1 && last != 0 {
		return ReconnectAck{Status: ReconnectInvalidSequence, CurrentEpoch: req.LastProcessedEmitSeq.Epoch, NextCommitIndex: last + 1}
	}
	// If client is older than compaction watermark, stale.
	if req.LastProcessedEmitSeq.CommitIndex < compacted {
		return ReconnectAck{Status: ReconnectStaleSequence, CurrentEpoch: req.LastProcessedEmitSeq.Epoch, NextCommitIndex: compacted}
	}
	// OK: resume from next index
	next := req.LastProcessedEmitSeq.CommitIndex + 1
	return ReconnectAck{Status: ReconnectOK, CurrentEpoch: req.LastProcessedEmitSeq.Epoch, NextCommitIndex: next}
}

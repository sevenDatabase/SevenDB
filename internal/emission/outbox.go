package emission

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"github.com/sevenDatabase/SevenDB/internal/logging"
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

func (o *outboxState) write(e *OutboxEntry) (newSub bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.bySub[e.SubID]; !ok {
		o.bySub[e.SubID] = make(map[uint64]*OutboxEntry)
		newSub = true
	}
	o.bySub[e.SubID][e.Seq.CommitIndex] = e
	return
}

// purge removes all entries up to and including the given commit index for sub.
func (o *outboxState) purge(sub string, upTo uint64) (int, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	m := o.bySub[sub]
	if m == nil {
		return 0, false
	}
	removed := 0
	for idx := range m {
		if idx <= upTo {
			delete(m, idx)
			removed++
		}
	}
	emptied := false
	if len(m) == 0 {
		delete(o.bySub, sub)
		emptied = true
	}
	return removed, emptied
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
	lastAck map[string]EmitSeq
	// compaction watermark for validation; in MVP this can be set by caller.
	compactThrough map[string]EmitSeq
	// bucketID labels metrics updates for this manager's shard/bucket
	bucketID string
	// currentEpoch tracks the active epoch for this manager
	currentEpoch EpochID
}

func NewManager(bucketID string) *Manager {
	return &Manager{ob: newOutboxState(), lastAck: make(map[string]EmitSeq), compactThrough: make(map[string]EmitSeq), bucketID: bucketID}
}

func (m *Manager) SetCurrentEpoch(e EpochID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentEpoch = e
}

// ApplyOutboxWrite records a durable outbox entry as per raft apply.
func (m *Manager) ApplyOutboxWrite(ctx context.Context, sub string, seq EmitSeq, delta []byte) {
	if newSub := m.ob.write(&OutboxEntry{SubID: sub, Seq: seq, Delta: delta}); newSub {
		// a new sub now has pending entries; update gauge best-effort under read lock
		m.ob.mu.RLock()
		if m.bucketID != "" {
			Metrics.SetSubsWithPendingFor(m.bucketID, len(m.ob.bySub))
		} else {
			Metrics.SetSubsWithPending(len(m.ob.bySub))
		}
		m.ob.mu.RUnlock()
	}
	if m.bucketID != "" {
		Metrics.AddPendingFor(m.bucketID, 1)
	} else {
		Metrics.AddPending(1)
	}
	slog.Debug("OUTBOX_WRITE",
		slog.String("sub_id", sub),
		slog.String("emit_seq", seq.String()),
		slog.Int("delta_bytes", len(delta)))
}

// ApplyOutboxPurge removes entries up to the provided position (inclusive).
func (m *Manager) ApplyOutboxPurge(ctx context.Context, sub string, upTo EmitSeq) {
	removed, emptied := m.ob.purge(sub, upTo.CommitIndex)
	// Update compaction watermark so reconnect can detect stale sequences
	m.SetCompactedThrough(sub, upTo)
	if removed > 0 {
		if m.bucketID != "" {
			Metrics.AddPendingFor(m.bucketID, -int64(removed))
		} else {
			Metrics.AddPending(-int64(removed))
		}
	}
	if emptied {
		m.ob.mu.RLock()
		if m.bucketID != "" {
			Metrics.SetSubsWithPendingFor(m.bucketID, len(m.ob.bySub))
		} else {
			Metrics.SetSubsWithPending(len(m.ob.bySub))
		}
		m.ob.mu.RUnlock()
	}
	slog.Debug("OUTBOX_PURGE", slog.String("sub_id", sub), slog.String("up_to", upTo.String()), slog.Int("removed", removed))
}

// ValidateAck returns whether ack is strictly monotonic; updates lastAck if valid.
func (m *Manager) ValidateAck(sub string, seq EmitSeq) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	last, ok := m.lastAck[sub]
	if ok && (seq.Epoch.EpochCounter < last.Epoch.EpochCounter || (seq.Epoch.EpochCounter == last.Epoch.EpochCounter && seq.CommitIndex <= last.CommitIndex)) {
		return false
	}
	m.lastAck[sub] = seq
	return true
}

// SubsWithPending returns a list of subscription IDs that have pending entries.
func (m *Manager) SubsWithPending() []string {
	m.ob.mu.RLock()
	defer m.ob.mu.RUnlock()
	subs := make([]string, 0, len(m.ob.bySub))
	for sub := range m.ob.bySub {
		subs = append(subs, sub)
	}
	return subs
}

// Pending returns pending entries for a sub ordered by commit index.
func (m *Manager) Pending(sub string) []*OutboxEntry {
	return m.ob.pendingSorted(sub)
}

// RebindByFingerprint migrates all pending entries and watermarks for a subscription identified by
// its fingerprint suffix to a new clientID (forming newSubID = clientID+":"+fp).
// If oldClientID is provided, it is used to locate the old subscription.
// If fp is 0 and oldClientID is provided, it searches for any subscription starting with oldClientID.
// Returns the old subID it migrated from (if any), the new subID, and the number of entries moved.
func (m *Manager) RebindByFingerprint(fp uint64, oldClientID, newClientID string) (string, string, int) {
	slog.Info("RebindByFingerprint called", slog.Uint64("fp", fp), slog.String("oldClient", oldClientID), slog.String("newClient", newClientID))
	var oldSub string
	var newSub string

	if fp != 0 {
		newSub = newClientID + ":" + strconv.FormatUint(fp, 10)
		if oldClientID != "" {
			candidate := oldClientID + ":" + strconv.FormatUint(fp, 10)
			// Verify existence
			m.ob.mu.RLock()
			_, exists := m.ob.bySub[candidate]
			m.ob.mu.RUnlock()
			if exists {
				oldSub = candidate
			} else {
				// Check watermarks
				m.mu.RLock()
				_, existsAck := m.lastAck[candidate]
				_, existsCompact := m.compactThrough[candidate]
				m.mu.RUnlock()
				if existsAck || existsCompact {
					oldSub = candidate
				} else {
						logging.VInfo("verbose", "RebindByFingerprint: exact match not found, falling back to search", slog.String("candidate", candidate))
				}
			}
		}
	} else if oldClientID != "" {
		// Fallback: try to find oldSub by prefix if fp is 0
		prefix := oldClientID + ":"
		// Helper to find by prefix
		findByPrefix := func() string {
			m.ob.mu.RLock()
			for sub := range m.ob.bySub {
				if strings.HasPrefix(sub, prefix) {
					m.ob.mu.RUnlock()
					return sub
				}
			}
			m.ob.mu.RUnlock()
			m.mu.RLock()
			defer m.mu.RUnlock()
			for sub := range m.lastAck {
				if strings.HasPrefix(sub, prefix) {
					return sub
				}
			}
			for sub := range m.compactThrough {
				if strings.HasPrefix(sub, prefix) {
					return sub
				}
			}
			return ""
		}
		oldSub = findByPrefix()
		if oldSub != "" {
			// Infer fp from oldSub
			parts := strings.SplitN(oldSub, ":", 2)
			if len(parts) == 2 {
				if v, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
					fp = v
					newSub = newClientID + ":" + parts[1]
				}
			}
		}
	}

	// Legacy fallback: search by suffix if oldSub still not found and fp != 0
	if oldSub == "" && fp != 0 {
		fpSuffix := ":" + strconv.FormatUint(fp, 10)
		// Locate an existing sub that matches the fingerprint and has pending entries.
		m.ob.mu.RLock()
		for sub := range m.ob.bySub {
			if len(sub) > len(fpSuffix) && sub[len(sub)-len(fpSuffix):] == fpSuffix {
				oldSub = sub
				break
			}
		}
		m.ob.mu.RUnlock()
		if oldSub == "" {
			// If no pending entries exist under the old sub, try to locate by watermarks
			m.mu.RLock()
			for sub := range m.lastAck {
				if len(sub) > len(fpSuffix) && sub[len(sub)-len(fpSuffix):] == fpSuffix {
					oldSub = sub
					break
				}
			}
			if oldSub == "" {
				for sub := range m.compactThrough {
					if len(sub) > len(fpSuffix) && sub[len(sub)-len(fpSuffix):] == fpSuffix {
						oldSub = sub
						break
					}
				}
			}
			m.mu.RUnlock()
		}
		
		// Also check pending entries in outbox if not found in watermarks (double check)
		if oldSub == "" {
			m.ob.mu.RLock()
			for sub := range m.ob.bySub {
				if len(sub) > len(fpSuffix) && sub[len(sub)-len(fpSuffix):] == fpSuffix {
					oldSub = sub
					break
				}
			}
			m.ob.mu.RUnlock()
		}
	}

	if oldSub == "" {
		// Nothing to migrate
		logging.VInfo("verbose", "RebindByFingerprint: oldSub not found", slog.Uint64("fp", fp), slog.String("oldClient", oldClientID))
		
		m.ob.mu.RLock()
		keys := make([]string, 0, len(m.ob.bySub))
		for k := range m.ob.bySub {
			keys = append(keys, k)
		}
		m.ob.mu.RUnlock()
		logging.VInfo("verbose", "RebindByFingerprint: available subs", slog.Any("keys", keys))

		if newSub == "" && fp != 0 {
			newSub = newClientID + ":" + strconv.FormatUint(fp, 10)
		}
		return "", newSub, 0
	}
	
	if newSub == "" {
		// Should have been set by now if oldSub was found
		parts := strings.SplitN(oldSub, ":", 2)
		if len(parts) == 2 {
			newSub = newClientID + ":" + parts[1]
		} else {
			// Should not happen if logic is correct
			return "", "", 0 
		}
	}

	logging.VInfo("verbose", "RebindByFingerprint: resolving", slog.String("oldSub", oldSub), slog.String("newSub", newSub))

	if oldSub == newSub {
		return oldSub, newSub, 0
	}


	// Move entries under lock
	moved := 0
	m.ob.mu.Lock()
	oldMap := m.ob.bySub[oldSub]
	if oldMap != nil {
		if _, ok := m.ob.bySub[newSub]; !ok {
			m.ob.bySub[newSub] = make(map[uint64]*OutboxEntry)
		}
		for idx, e := range oldMap {
			// rewrite subID and move
			if e != nil {
				e.SubID = newSub
			}
			m.ob.bySub[newSub][idx] = e
			delete(oldMap, idx)
			moved++
		}
		if len(oldMap) == 0 {
			delete(m.ob.bySub, oldSub)
		}
	}
	m.ob.mu.Unlock()

	// Move watermarks
	m.mu.Lock()
	if v, ok := m.lastAck[oldSub]; ok {
		cur, ok2 := m.lastAck[newSub]
		isNewer := !ok2 || v.Epoch.EpochCounter > cur.Epoch.EpochCounter || (v.Epoch.EpochCounter == cur.Epoch.EpochCounter && v.CommitIndex > cur.CommitIndex)
		if isNewer {
			m.lastAck[newSub] = v
		}
		delete(m.lastAck, oldSub)
	}
	if v, ok := m.compactThrough[oldSub]; ok {
		cur, ok2 := m.compactThrough[newSub]
		isNewer := !ok2 || v.Epoch.EpochCounter > cur.Epoch.EpochCounter || (v.Epoch.EpochCounter == cur.Epoch.EpochCounter && v.CommitIndex > cur.CommitIndex)
		if isNewer {
			m.compactThrough[newSub] = v
		}
		delete(m.compactThrough, oldSub)
	}
	m.mu.Unlock()

	logging.VInfo("verbose", "rebinding subscription", slog.String("from", oldSub), slog.String("to", newSub), slog.Int("moved_entries", moved))
	return oldSub, newSub, moved
}

// SetCompactedThrough updates per-sub compacted watermark.
func (m *Manager) SetCompactedThrough(sub string, seq EmitSeq) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactThrough[sub] = seq
}

// Reconnect validates the client position and returns status with resume point.
func (m *Manager) Reconnect(req ReconnectRequest) ReconnectAck {
	m.mu.RLock()
	compacted := m.compactThrough[req.SubID]
	last, hasLast := m.lastAck[req.SubID]
	currentEpoch := m.currentEpoch
	m.mu.RUnlock()

	// If client is from a different epoch, force restart from 0.
	if req.LastProcessedEmitSeq.Epoch.EpochCounter != currentEpoch.EpochCounter {
		return ReconnectAck{Status: ReconnectOK, CurrentEpoch: currentEpoch, NextCommitIndex: 0}
	}

	// If client is ahead of last ack (or future), treat as invalid for MVP.
	if hasLast && req.LastProcessedEmitSeq.CommitIndex > last.CommitIndex {
		return ReconnectAck{Status: ReconnectInvalidSequence, CurrentEpoch: currentEpoch, NextCommitIndex: last.CommitIndex + 1}
	}
	// If client is older than compaction watermark, stale.
	if req.LastProcessedEmitSeq.CommitIndex < compacted.CommitIndex {
		return ReconnectAck{Status: ReconnectStaleSequence, CurrentEpoch: currentEpoch, NextCommitIndex: compacted.CommitIndex}
	}
	// OK: resume from next index
	next := req.LastProcessedEmitSeq.CommitIndex + 1
	return ReconnectAck{Status: ReconnectOK, CurrentEpoch: currentEpoch, NextCommitIndex: next}
}

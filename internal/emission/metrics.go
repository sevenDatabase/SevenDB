package emission

import (
	"sync"
	"sync/atomic"
	"time"
)

// EmissionMetrics provides lightweight counters, gauges, and a basic latency summary.
// This struct is used both for the global aggregate and per-bucket instances.
type EmissionMetrics struct {
	// Gauges
	pendingEntries  atomic.Int64
	subsWithPending atomic.Int64

	// Counters and windowed rates (per second)
	sendsTotal          atomic.Int64
	sendsWindowStartSec atomic.Int64
	sendsWindowCount    atomic.Int64
	sendsPerSec         atomic.Int64

	acksTotal          atomic.Int64
	acksWindowStartSec atomic.Int64
	acksWindowCount    atomic.Int64
	acksPerSec         atomic.Int64

	// Send latency summary in milliseconds
	latMu      sync.Mutex
	latCount   int64
	latTotalMs int64
	latMinMs   int64
	latMaxMs   int64

	// Reconnect outcomes
	recOK       atomic.Int64
	recStale    atomic.Int64
	recInvalid  atomic.Int64
	recNotFound atomic.Int64
}

// MetricsRegistry holds the global metrics and an optional per-bucket breakdown.
type MetricsRegistry struct {
	global  EmissionMetrics
	mu      sync.RWMutex
	buckets map[string]*EmissionMetrics
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{buckets: make(map[string]*EmissionMetrics)}
}

var Metrics = NewMetricsRegistry()

// getBucket returns the per-bucket metrics struct, creating it if missing.
func (r *MetricsRegistry) getBucket(bucket string) *EmissionMetrics {
	if bucket == "" {
		return nil
	}
	r.mu.RLock()
	bm := r.buckets[bucket]
	r.mu.RUnlock()
	if bm != nil {
		return bm
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if bm = r.buckets[bucket]; bm == nil {
		bm = &EmissionMetrics{}
		r.buckets[bucket] = bm
	}
	return bm
}

// Gauge helpers (aggregate-only)
func (r *MetricsRegistry) AddPending(delta int64)   { r.global.pendingEntries.Add(delta) }
func (r *MetricsRegistry) SetSubsWithPending(n int) { r.global.subsWithPending.Store(int64(n)) }

// Gauge helpers with bucket label
func (r *MetricsRegistry) AddPendingFor(bucket string, delta int64) {
	r.global.pendingEntries.Add(delta)
	if bm := r.getBucket(bucket); bm != nil {
		bm.pendingEntries.Add(delta)
	}
}
func (r *MetricsRegistry) SetSubsWithPendingFor(bucket string, n int) {
	r.global.subsWithPending.Store(int64(n))
	if bm := r.getBucket(bucket); bm != nil {
		bm.subsWithPending.Store(int64(n))
	}
}

// Windowed rate helpers
func updateWindow(nowSec int64, start *atomic.Int64, windowCount *atomic.Int64, perSec *atomic.Int64) {
	s := start.Load()
	if s == 0 {
		// initialize window
		start.Store(nowSec)
		return
	}
	if nowSec > s {
		// close the window and publish rate
		c := windowCount.Swap(0)
		perSec.Store(c)
		start.Store(nowSec)
	}
}

// ObserveSend updates send counters and latency for aggregate and labeled bucket.
func (r *MetricsRegistry) ObserveSend(d time.Duration) { observeSend(&r.global, d) }
func (r *MetricsRegistry) ObserveSendFor(bucket string, d time.Duration) {
	observeSend(&r.global, d)
	if bm := r.getBucket(bucket); bm != nil {
		observeSend(bm, d)
	}
}

func observeSend(m *EmissionMetrics, d time.Duration) {
	// total count
	m.sendsTotal.Add(1)
	// windowed rate
	nowSec := time.Now().Unix()
	updateWindow(nowSec, &m.sendsWindowStartSec, &m.sendsWindowCount, &m.sendsPerSec)
	m.sendsWindowCount.Add(1)
	// latency summary
	ms := d.Milliseconds()
	m.latMu.Lock()
	if m.latCount == 0 || ms < m.latMinMs {
		m.latMinMs = ms
	}
	if ms > m.latMaxMs {
		m.latMaxMs = ms
	}
	m.latCount++
	m.latTotalMs += ms
	m.latMu.Unlock()
}

// ACK counters
func (r *MetricsRegistry) IncAck() { incAck(&r.global) }
func (r *MetricsRegistry) IncAckFor(bucket string) {
	incAck(&r.global)
	if bm := r.getBucket(bucket); bm != nil {
		incAck(bm)
	}
}

func incAck(m *EmissionMetrics) {
	m.acksTotal.Add(1)
	nowSec := time.Now().Unix()
	updateWindow(nowSec, &m.acksWindowStartSec, &m.acksWindowCount, &m.acksPerSec)
	m.acksWindowCount.Add(1)
}

// Reconnect outcomes
func (r *MetricsRegistry) IncReconnect(st ReconnectStatus) { incReconnect(&r.global, st) }
func (r *MetricsRegistry) IncReconnectFor(bucket string, st ReconnectStatus) {
	incReconnect(&r.global, st)
	if bm := r.getBucket(bucket); bm != nil {
		incReconnect(bm, st)
	}
}

func incReconnect(m *EmissionMetrics, st ReconnectStatus) {
	switch st {
	case ReconnectOK:
		m.recOK.Add(1)
	case ReconnectStaleSequence:
		m.recStale.Add(1)
	case ReconnectInvalidSequence:
		m.recInvalid.Add(1)
	case ReconnectSubscriptionNotFound:
		m.recNotFound.Add(1)
	}
}

// Snapshot returns the global aggregate suitable for inclusion in status.json.
func (r *MetricsRegistry) Snapshot() map[string]interface{} { return snapshot(&r.global) }

func snapshot(m *EmissionMetrics) map[string]interface{} {
	snap := map[string]interface{}{
		"pending_entries":            m.pendingEntries.Load(),
		"subs_with_pending":          m.subsWithPending.Load(),
		"sends_total":                m.sendsTotal.Load(),
		"sends_per_sec":              m.sendsPerSec.Load(),
		"acks_total":                 m.acksTotal.Load(),
		"acks_per_sec":               m.acksPerSec.Load(),
		"reconnects_ok_total":        m.recOK.Load(),
		"reconnects_stale_total":     m.recStale.Load(),
		"reconnects_invalid_total":   m.recInvalid.Load(),
		"reconnects_not_found_total": m.recNotFound.Load(),
	}
	m.latMu.Lock()
	if m.latCount > 0 {
		snap["send_latency_ms_avg"] = float64(m.latTotalMs) / float64(m.latCount)
		snap["send_latency_ms_min"] = m.latMinMs
		snap["send_latency_ms_max"] = m.latMaxMs
		snap["send_latency_count"] = m.latCount
	} else {
		snap["send_latency_ms_avg"] = 0.0
		snap["send_latency_ms_min"] = 0
		snap["send_latency_ms_max"] = 0
		snap["send_latency_count"] = 0
	}
	m.latMu.Unlock()
	return snap
}

// BucketSnapshots returns a labeled snapshot per bucket.
func (r *MetricsRegistry) BucketSnapshots() map[string]map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]map[string]interface{}, len(r.buckets))
	for k, v := range r.buckets {
		out[k] = snapshot(v)
	}
	return out
}

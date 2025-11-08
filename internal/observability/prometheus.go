package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/sevenDatabase/SevenDB/internal/emission"
)

// customCollectors contains callbacks that return fully formatted Prometheus metric lines.
// Other packages can register lightweight metrics without introducing dependencies here.
var customCollectors []func() []string

// RegisterCustomCollector adds a collector function whose returned lines will be emitted on /metrics.
func RegisterCustomCollector(f func() []string) {
    customCollectors = append(customCollectors, f)
}

// SetupPrometheus registers a minimal Prometheus-compatible text endpoint at /metrics.
// This avoids pulling external dependencies while remaining scrape-friendly.
func SetupPrometheus(mux *http.ServeMux) {
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		// Aggregate under bucket="all"
		writeSnapshot(w, "all", emission.Metrics.Snapshot())
		// Per-bucket breakdown
		snaps := emission.Metrics.BucketSnapshots()
		// Stable iteration order for readability
		keys := make([]string, 0, len(snaps))
		for k := range snaps {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, b := range keys {
			writeSnapshot(w, b, snaps[b])
		}

		// Emit custom registered metrics
		for _, f := range customCollectors {
			if f == nil {
				continue
			}
			for _, line := range f() {
				if line == "" {
					continue
				}
				fmt.Fprintln(w, line)
			}
		}
	})
}

func writeSnapshot(w http.ResponseWriter, bucket string, snap map[string]interface{}) {
	// Helper to read numeric types
	f := func(k string) float64 {
		if v, ok := snap[k]; ok {
			switch t := v.(type) {
			case int64:
				return float64(t)
			case int:
				return float64(t)
			case float64:
				return t
			case uint64:
				return float64(t)
			case uint:
				return float64(t)
			}
		}
		return 0
	}
	// Emit lines in a compact, prometheus-compatible form
	label := fmt.Sprintf("{bucket=\"%s\"}", escapeLabel(bucket))
	fmt.Fprintf(w, "sevendb_emission_pending_entries%s %v\n", label, f("pending_entries"))
	fmt.Fprintf(w, "sevendb_emission_subs_with_pending%s %v\n", label, f("subs_with_pending"))
	fmt.Fprintf(w, "sevendb_emission_sends_total%s %v\n", label, f("sends_total"))
	fmt.Fprintf(w, "sevendb_emission_sends_per_sec%s %v\n", label, f("sends_per_sec"))
	fmt.Fprintf(w, "sevendb_emission_acks_total%s %v\n", label, f("acks_total"))
	fmt.Fprintf(w, "sevendb_emission_acks_per_sec%s %v\n", label, f("acks_per_sec"))
	fmt.Fprintf(w, "sevendb_emission_send_latency_ms_avg%s %v\n", label, f("send_latency_ms_avg"))
	fmt.Fprintf(w, "sevendb_emission_send_latency_ms_min%s %v\n", label, f("send_latency_ms_min"))
	fmt.Fprintf(w, "sevendb_emission_send_latency_ms_max%s %v\n", label, f("send_latency_ms_max"))
	fmt.Fprintf(w, "sevendb_emission_send_latency_count%s %v\n", label, f("send_latency_count"))
	// Reconnect outcomes with outcome label
	outcomes := map[string]string{
		"reconnects_ok_total":        "ok",
		"reconnects_stale_total":     "stale",
		"reconnects_invalid_total":   "invalid",
		"reconnects_not_found_total": "not_found",
	}
	for k, outcome := range outcomes {
		fmt.Fprintf(w, "sevendb_emission_reconnects_total{bucket=\"%s\",outcome=\"%s\"} %v\n", escapeLabel(bucket), outcome, f(k))
	}
}

func escapeLabel(v string) string {
	// Basic escape for quotes and backslashes
	v = strings.ReplaceAll(v, "\\", "\\\\")
	v = strings.ReplaceAll(v, "\"", "\\\"")
	return v
}

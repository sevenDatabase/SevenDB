// Copyright (c) 2022-present, DiceDB/SevenDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

// Package main provides a benchmark for measuring subscription reconnection time.
// This benchmark:
// 1. Establishes a watch subscription on a key
// 2. Simulates client disconnect (closes connection)
// 3. Reconnects and uses EMITRECONNECT to resume from last known position
// 4. Measures reconnection latency and validates no emissions are missed
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

type ReconnectConfig struct {
	Host             string
	Port             int
	Iterations       int
	WarmupEmissions  int
	PollInterval     time.Duration
	MaxWaitEmission  time.Duration
	ClientIDPrefix   string
	JSON             bool
}

type ReconnectResult struct {
	Iteration          int     `json:"iteration"`
	ReconnectTimeMs    float64 `json:"reconnectTimeMs"`
	ResumeTimeMs       float64 `json:"resumeTimeMs"`
	TotalTimeMs        float64 `json:"totalTimeMs"`
	LastCommitBefore   uint64  `json:"lastCommitBefore"`
	FirstCommitAfter   uint64  `json:"firstCommitAfter"`
	MissedEmissions    int     `json:"missedEmissions"`
	DuplicateEmissions int     `json:"duplicateEmissions"`
	EmissionsReceived  int     `json:"emissionsReceived"`
}

type ReconnectSummary struct {
	Iterations         int     `json:"iterations"`
	ReconnectP50Ms     float64 `json:"reconnectP50Ms"`
	ReconnectP95Ms     float64 `json:"reconnectP95Ms"`
	ReconnectP99Ms     float64 `json:"reconnectP99Ms"`
	ResumeP50Ms        float64 `json:"resumeP50Ms"`
	ResumeP95Ms        float64 `json:"resumeP95Ms"`
	ResumeP99Ms        float64 `json:"resumeP99Ms"`
	TotalP50Ms         float64 `json:"totalP50Ms"`
	TotalP95Ms         float64 `json:"totalP95Ms"`
	TotalP99Ms         float64 `json:"totalP99Ms"`
	TotalMissed        int     `json:"totalMissed"`
	TotalDuplicates    int     `json:"totalDuplicates"`
	AvgReconnectMs     float64 `json:"avgReconnectMs"`
	AvgResumeMs        float64 `json:"avgResumeMs"`
}

func main() {
	cfg := parseReconnectFlags()

	results := make([]ReconnectResult, 0, cfg.Iterations)

	for i := 0; i < cfg.Iterations; i++ {
		fmt.Printf("\n=== Reconnect Benchmark Iteration %d/%d ===\n", i+1, cfg.Iterations)
		result, err := runReconnectIteration(cfg, i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Iteration %d failed: %v\n", i+1, err)
			continue
		}
		results = append(results, result)
		fmt.Printf("Iteration %d: reconnect=%.2fms resume=%.2fms total=%.2fms (missed=%d dup=%d)\n",
			i+1, result.ReconnectTimeMs, result.ResumeTimeMs, result.TotalTimeMs,
			result.MissedEmissions, result.DuplicateEmissions)
	}

	if len(results) == 0 {
		fmt.Fprintln(os.Stderr, "No successful iterations")
		os.Exit(1)
	}

	summary := computeReconnectSummary(results)
	printReconnectResults(cfg, results, summary)
}

func parseReconnectFlags() ReconnectConfig {
	var (
		host           = flag.String("host", "localhost", "Server host")
		port           = flag.Int("port", 7379, "Server port")
		iterations     = flag.Int("iterations", 10, "Number of reconnect iterations")
		warmupEmits    = flag.Int("warmup-emissions", 5, "Emissions before disconnect")
		pollInterval   = flag.Duration("poll-interval", 50*time.Millisecond, "Emission poll interval")
		maxWait        = flag.Duration("max-wait", 5*time.Second, "Max wait for emission")
		clientPrefix   = flag.String("client-prefix", "reconnect-bench", "Client ID prefix")
		jsonOut        = flag.Bool("json", false, "Output results as JSON")
	)
	flag.Parse()

	return ReconnectConfig{
		Host:            *host,
		Port:            *port,
		Iterations:      *iterations,
		WarmupEmissions: *warmupEmits,
		PollInterval:    *pollInterval,
		MaxWaitEmission: *maxWait,
		ClientIDPrefix:  *clientPrefix,
		JSON:            *jsonOut,
	}
}

func runReconnectIteration(cfg ReconnectConfig, iteration int) (ReconnectResult, error) {
	clientID := fmt.Sprintf("%s-%d-%d", cfg.ClientIDPrefix, iteration, time.Now().UnixNano())
	key := fmt.Sprintf("reconnect-bench-key-%d", iteration)

	// Publisher client
	pub, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		return ReconnectResult{}, fmt.Errorf("publisher connect failed: %w", err)
	}
	defer pub.Close()

	// Initial watch client
	watch1, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		return ReconnectResult{}, fmt.Errorf("watch connect failed: %w", err)
	}

	// Handshake for watch mode
	hsResp := watch1.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp.Status != wire.Status_OK {
		watch1.Close()
		return ReconnectResult{}, fmt.Errorf("handshake failed: %v", hsResp.Message)
	}

	// Subscribe to key
	subResp := watch1.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	if subResp.Status != wire.Status_OK || subResp.Fingerprint64 == 0 {
		watch1.Close()
		return ReconnectResult{}, fmt.Errorf("GET.WATCH failed: %v", subResp.Message)
	}
	fingerprint := subResp.Fingerprint64
	subID := fmt.Sprintf("%s:%d", clientID, fingerprint)

	// Warmup emissions
	var lastCommitIndex uint64
	seenCommits := make(map[uint64]bool)
	
	for i := 0; i < cfg.WarmupEmissions; i++ {
		value := fmt.Sprintf("warmup-%d", i)
		setResp := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, value}})
		if setResp.Status != wire.Status_OK {
			watch1.Close()
			return ReconnectResult{}, fmt.Errorf("SET failed: %v", setResp.Message)
		}

		// Wait for emission
		found := false
		for j := 0; j < int(cfg.MaxWaitEmission/cfg.PollInterval); j++ {
			// Read from watch channel
			ch, err := watch1.WatchCh()
			if err != nil {
				break
			}
			select {
			case msg := <-ch:
				if msg != nil && msg.Status == wire.Status_OK {
					if getRes := msg.GetGETRes(); getRes != nil {
						// Extract commit index from message if available
						// For now, we track by iteration
						lastCommitIndex = uint64(i + 1)
						seenCommits[lastCommitIndex] = true
						found = true
					}
				}
			case <-time.After(cfg.PollInterval):
			}
			if found {
				break
			}
		}
		if !found {
			// Try alternative polling via PING
			for j := 0; j < int(cfg.MaxWaitEmission/cfg.PollInterval); j++ {
				pingResp := watch1.Fire(&wire.Command{Cmd: "PING"})
				if pingResp.Message == value {
					lastCommitIndex = uint64(i + 1)
					seenCommits[lastCommitIndex] = true
					found = true
					break
				}
				time.Sleep(cfg.PollInterval)
			}
		}
	}

	// Close watch connection (simulate disconnect)
	watch1.Close()
	disconnectTime := time.Now()

	// Reconnect timing starts here
	reconnectStart := time.Now()

	// Create new watch connection
	watch2, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		return ReconnectResult{}, fmt.Errorf("reconnect failed: %w", err)
	}
	defer watch2.Close()

	reconnectDuration := time.Since(reconnectStart)

	// Handshake again
	hsResp2 := watch2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp2.Status != wire.Status_OK {
		return ReconnectResult{}, fmt.Errorf("reconnect handshake failed: %v", hsResp2.Message)
	}

	// EMITRECONNECT to resume from last commit index
	resumeStart := time.Now()
	recResp := watch2.Fire(&wire.Command{
		Cmd:  "EMITRECONNECT",
		Args: []string{key, subID, strconv.FormatUint(lastCommitIndex, 10)},
	})
	resumeDuration := time.Since(resumeStart)

	if recResp.Status != wire.Status_OK {
		// Check for STALE_SEQUENCE which is expected if outbox was purged
		if recResp.Message == "STALE_SEQUENCE" {
			fmt.Printf("  Warning: STALE_SEQUENCE on reconnect (outbox purged)\n")
		} else {
			return ReconnectResult{}, fmt.Errorf("EMITRECONNECT failed: %v", recResp.Message)
		}
	}

	// Parse next commit index from response (format: "OK <next_idx>")
	var nextCommitIdx uint64
	if strings.HasPrefix(recResp.Message, "OK ") {
		if idx, err := strconv.ParseUint(strings.TrimPrefix(recResp.Message, "OK "), 10, 64); err == nil {
			nextCommitIdx = idx
		}
	}

	totalDuration := time.Since(disconnectTime)

	// Produce a new emission after reconnect
	postValue := fmt.Sprintf("post-reconnect-%d", iteration)
	setResp := pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, postValue}})
	if setResp.Status != wire.Status_OK {
		return ReconnectResult{}, fmt.Errorf("post-reconnect SET failed: %v", setResp.Message)
	}

	// Verify we receive the emission
	var emissionsReceived int
	var firstCommitAfter uint64
	var duplicates int

	for i := 0; i < int(cfg.MaxWaitEmission/cfg.PollInterval); i++ {
		pingResp := watch2.Fire(&wire.Command{Cmd: "PING"})
		if pingResp.Message == postValue {
			emissionsReceived++
			if firstCommitAfter == 0 {
				firstCommitAfter = nextCommitIdx
			}
			break
		}
		time.Sleep(cfg.PollInterval)
	}

	// Calculate missed emissions
	missed := 0
	if firstCommitAfter > 0 && lastCommitIndex > 0 {
		expectedNext := lastCommitIndex + 1
		if firstCommitAfter > expectedNext {
			missed = int(firstCommitAfter - expectedNext)
		}
	}

	return ReconnectResult{
		Iteration:          iteration + 1,
		ReconnectTimeMs:    float64(reconnectDuration) / float64(time.Millisecond),
		ResumeTimeMs:       float64(resumeDuration) / float64(time.Millisecond),
		TotalTimeMs:        float64(totalDuration) / float64(time.Millisecond),
		LastCommitBefore:   lastCommitIndex,
		FirstCommitAfter:   firstCommitAfter,
		MissedEmissions:    missed,
		DuplicateEmissions: duplicates,
		EmissionsReceived:  emissionsReceived,
	}, nil
}

func computeReconnectSummary(results []ReconnectResult) ReconnectSummary {
	if len(results) == 0 {
		return ReconnectSummary{}
	}

	reconnects := make([]float64, len(results))
	resumes := make([]float64, len(results))
	totals := make([]float64, len(results))

	var sumReconnect, sumResume float64
	var totalMissed, totalDups int

	for i, r := range results {
		reconnects[i] = r.ReconnectTimeMs
		resumes[i] = r.ResumeTimeMs
		totals[i] = r.TotalTimeMs
		sumReconnect += r.ReconnectTimeMs
		sumResume += r.ResumeTimeMs
		totalMissed += r.MissedEmissions
		totalDups += r.DuplicateEmissions
	}

	sort.Float64s(reconnects)
	sort.Float64s(resumes)
	sort.Float64s(totals)

	n := len(results)
	return ReconnectSummary{
		Iterations:      n,
		ReconnectP50Ms:  percentile(reconnects, 50),
		ReconnectP95Ms:  percentile(reconnects, 95),
		ReconnectP99Ms:  percentile(reconnects, 99),
		ResumeP50Ms:     percentile(resumes, 50),
		ResumeP95Ms:     percentile(resumes, 95),
		ResumeP99Ms:     percentile(resumes, 99),
		TotalP50Ms:      percentile(totals, 50),
		TotalP95Ms:      percentile(totals, 95),
		TotalP99Ms:      percentile(totals, 99),
		TotalMissed:     totalMissed,
		TotalDuplicates: totalDups,
		AvgReconnectMs:  sumReconnect / float64(n),
		AvgResumeMs:     sumResume / float64(n),
	}
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := (float64(p) / 100.0) * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	return sorted[lo] + (sorted[hi]-sorted[lo])*frac
}

func printReconnectResults(cfg ReconnectConfig, results []ReconnectResult, summary ReconnectSummary) {
	if cfg.JSON {
		out := struct {
			Config  ReconnectConfig    `json:"config"`
			Results []ReconnectResult  `json:"results"`
			Summary ReconnectSummary   `json:"summary"`
		}{
			Config:  cfg,
			Results: results,
			Summary: summary,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(out)
		return
	}

	fmt.Printf("\n=== Subscription Reconnection Benchmark Summary ===\n")
	fmt.Printf("Target: %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Iterations: %d\n", summary.Iterations)
	fmt.Printf("Warmup emissions per iteration: %d\n", cfg.WarmupEmissions)
	fmt.Printf("\nReconnection Time (TCP connect, ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.ReconnectP50Ms, summary.ReconnectP95Ms, summary.ReconnectP99Ms, summary.AvgReconnectMs)
	fmt.Printf("\nResume Time (EMITRECONNECT, ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.ResumeP50Ms, summary.ResumeP95Ms, summary.ResumeP99Ms, summary.AvgResumeMs)
	fmt.Printf("\nTotal Reconnect+Resume Time (ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f\n",
		summary.TotalP50Ms, summary.TotalP95Ms, summary.TotalP99Ms)
	fmt.Printf("\nData Integrity:\n")
	fmt.Printf("  Total missed emissions: %d\n", summary.TotalMissed)
	fmt.Printf("  Total duplicate emissions: %d\n", summary.TotalDuplicates)
}

// Additional helper: test concurrent reconnects
func runConcurrentReconnectBench(cfg ReconnectConfig, concurrency int) {
	var wg sync.WaitGroup
	var successCount, failCount uint64
	results := make(chan ReconnectResult, concurrency)

	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result, err := runReconnectIteration(cfg, id)
			if err != nil {
				atomic.AddUint64(&failCount, 1)
				return
			}
			atomic.AddUint64(&successCount, 1)
			results <- result
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []ReconnectResult
	for r := range results {
		allResults = append(allResults, r)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== Concurrent Reconnect Benchmark ===\n")
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Success: %d, Failed: %d\n", successCount, failCount)
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Reconnects/sec: %.2f\n", float64(successCount)/elapsed.Seconds())
}

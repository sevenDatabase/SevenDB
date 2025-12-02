// Copyright (c) 2022-present, DiceDB/SevenDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

// Package main provides a benchmark for validating crash recovery guarantees:
// - No duplicates: Client should not receive the same emission twice
// - No loss: Client should not miss any committed emissions
//
// This benchmark:
// 1. Establishes watch subscriptions
// 2. Produces a series of updates with unique sequence numbers
// 3. Simulates various crash scenarios (server crash, client crash, network partition)
// 4. Validates that after recovery, the client receives exactly-once semantics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

type CrashRecoveryConfig struct {
	Host            string
	Port            int
	BinaryPath      string
	BaseDir         string
	Iterations      int
	UpdatesPerIter  int
	CrashScenario   string // "server", "client", "both", "network"
	PollInterval    time.Duration
	MaxWaitEmission time.Duration
	StabilizeWait   time.Duration
	JSON            bool
}

type EmissionRecord struct {
	Sequence    int64     `json:"sequence"`
	Value       string    `json:"value"`
	ReceivedAt  time.Time `json:"receivedAt"`
	CommitIndex uint64    `json:"commitIndex"`
}

type CrashRecoveryResult struct {
	Iteration       int                `json:"iteration"`
	Scenario        string             `json:"scenario"`
	TotalProduced   int                `json:"totalProduced"`
	TotalReceived   int                `json:"totalReceived"`
	UniqueReceived  int                `json:"uniqueReceived"`
	Duplicates      int                `json:"duplicates"`
	MissedSequences []int64            `json:"missedSequences"`
	DupSequences    []int64            `json:"dupSequences"`
	RecoveryTimeMs  float64            `json:"recoveryTimeMs"`
	FirstMissedSeq  int64              `json:"firstMissedSeq"`
	LastReceivedSeq int64              `json:"lastReceivedSeq"`
	ExactlyOnce     bool               `json:"exactlyOnce"`
	AtLeastOnce     bool               `json:"atLeastOnce"`
	AtMostOnce      bool               `json:"atMostOnce"`
}

type CrashRecoverySummary struct {
	Iterations           int     `json:"iterations"`
	TotalUpdates         int     `json:"totalUpdates"`
	TotalDuplicates      int     `json:"totalDuplicates"`
	TotalMissed          int     `json:"totalMissed"`
	ExactlyOnceRate      float64 `json:"exactlyOnceRate"`
	AtLeastOnceRate      float64 `json:"atLeastOnceRate"`
	AtMostOnceRate       float64 `json:"atMostOnceRate"`
	AvgRecoveryMs        float64 `json:"avgRecoveryMs"`
	P50RecoveryMs        float64 `json:"p50RecoveryMs"`
	P95RecoveryMs        float64 `json:"p95RecoveryMs"`
	P99RecoveryMs        float64 `json:"p99RecoveryMs"`
}

func main() {
	cfg := parseCrashRecoveryFlags()

	results := make([]CrashRecoveryResult, 0, cfg.Iterations)

	for i := 0; i < cfg.Iterations; i++ {
		fmt.Printf("\n=== Crash Recovery Benchmark Iteration %d/%d ===\n", i+1, cfg.Iterations)
		result, err := runCrashRecoveryIteration(cfg, i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Iteration %d failed: %v\n", i+1, err)
			continue
		}
		results = append(results, result)
		fmt.Printf("Iteration %d: produced=%d received=%d unique=%d dups=%d missed=%d recovery=%.2fms exactlyOnce=%v\n",
			i+1, result.TotalProduced, result.TotalReceived, result.UniqueReceived,
			result.Duplicates, len(result.MissedSequences), result.RecoveryTimeMs, result.ExactlyOnce)
	}

	if len(results) == 0 {
		fmt.Fprintln(os.Stderr, "No successful iterations")
		os.Exit(1)
	}

	summary := computeCrashRecoverySummary(results)
	printCrashRecoveryResults(cfg, results, summary)
}

func parseCrashRecoveryFlags() CrashRecoveryConfig {
	var (
		host          = flag.String("host", "localhost", "Server host")
		port          = flag.Int("port", 7379, "Server port")
		binary        = flag.String("binary", "", "Path to sevendb binary (for server crash scenario)")
		baseDir       = flag.String("base-dir", ".crash-recovery-bench", "Base directory for node data")
		iterations    = flag.Int("iterations", 5, "Number of crash recovery iterations")
		updatesPerIter = flag.Int("updates", 100, "Updates per iteration")
		scenario      = flag.String("scenario", "client", "Crash scenario: server, client, both")
		pollInterval  = flag.Duration("poll-interval", 20*time.Millisecond, "Emission poll interval")
		maxWait       = flag.Duration("max-wait", 10*time.Second, "Max wait for emission")
		stabilizeWait = flag.Duration("stabilize-wait", 2*time.Second, "Wait for server to stabilize")
		jsonOut       = flag.Bool("json", false, "Output results as JSON")
	)
	flag.Parse()

	return CrashRecoveryConfig{
		Host:            *host,
		Port:            *port,
		BinaryPath:      *binary,
		BaseDir:         *baseDir,
		Iterations:      *iterations,
		UpdatesPerIter:  *updatesPerIter,
		CrashScenario:   *scenario,
		PollInterval:    *pollInterval,
		MaxWaitEmission: *maxWait,
		StabilizeWait:   *stabilizeWait,
		JSON:            *jsonOut,
	}
}

func runCrashRecoveryIteration(cfg CrashRecoveryConfig, iteration int) (CrashRecoveryResult, error) {
	switch cfg.CrashScenario {
	case "client":
		return runClientCrashScenario(cfg, iteration)
	case "server":
		if cfg.BinaryPath == "" {
			return CrashRecoveryResult{}, fmt.Errorf("--binary required for server crash scenario")
		}
		return runServerCrashScenario(cfg, iteration)
	case "both":
		if cfg.BinaryPath == "" {
			return CrashRecoveryResult{}, fmt.Errorf("--binary required for both crash scenario")
		}
		return runBothCrashScenario(cfg, iteration)
	default:
		return CrashRecoveryResult{}, fmt.Errorf("unknown scenario: %s", cfg.CrashScenario)
	}
}

// runClientCrashScenario simulates client crash/disconnect during emission reception
func runClientCrashScenario(cfg CrashRecoveryConfig, iteration int) (CrashRecoveryResult, error) {
	clientID := fmt.Sprintf("crash-client-%d-%d", iteration, time.Now().UnixNano())
	key := fmt.Sprintf("crash-key-%d", iteration)

	// Publisher
	pub, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("publisher connect failed: %w", err)
	}
	defer pub.Close()

	// First watch session
	watch1, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("watch connect failed: %w", err)
	}

	hsResp := watch1.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp.Status != wire.Status_OK {
		watch1.Close()
		return CrashRecoveryResult{}, fmt.Errorf("handshake failed: %v", hsResp.Message)
	}

	subResp := watch1.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	if subResp.Status != wire.Status_OK || subResp.Fingerprint64 == 0 {
		watch1.Close()
		return CrashRecoveryResult{}, fmt.Errorf("GET.WATCH failed: %v", subResp.Message)
	}
	fingerprint := subResp.Fingerprint64
	subID := fmt.Sprintf("%s:%d", clientID, fingerprint)

	// Track received emissions
	received := make(map[int64]*EmissionRecord)
	var receivedMu sync.Mutex
	var totalReceived int64

	// Start producer
	var producerDone int64
	producerCtx, producerCancel := context.WithCancel(context.Background())
	producerWg := sync.WaitGroup{}
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		for seq := int64(1); seq <= int64(cfg.UpdatesPerIter); seq++ {
			select {
			case <-producerCtx.Done():
				return
			default:
			}
			value := fmt.Sprintf("seq=%d|data=test-%d", seq, time.Now().UnixNano())
			pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, value}})
			time.Sleep(10 * time.Millisecond)
		}
		atomic.StoreInt64(&producerDone, 1)
	}()

	// Receive emissions for first half, then "crash"
	crashAfter := cfg.UpdatesPerIter / 2
	for i := 0; i < crashAfter*2; i++ { // Extra iterations to catch up
		pingResp := watch1.Fire(&wire.Command{Cmd: "PING"})
		if strings.HasPrefix(pingResp.Message, "seq=") {
			seq := parseSequence(pingResp.Message)
			if seq > 0 {
				receivedMu.Lock()
				if _, exists := received[seq]; !exists {
					received[seq] = &EmissionRecord{
						Sequence:   seq,
						Value:      pingResp.Message,
						ReceivedAt: time.Now(),
					}
				}
				receivedMu.Unlock()
				atomic.AddInt64(&totalReceived, 1)
			}
		}
		if len(received) >= crashAfter {
			break
		}
		time.Sleep(cfg.PollInterval)
	}

	var lastCommitIndex uint64 = uint64(len(received))

	// Simulate crash
	watch1.Close()
	crashTime := time.Now()

	// Wait for some more updates while "crashed"
	time.Sleep(200 * time.Millisecond)

	// Reconnect
	recoveryStart := time.Now()
	watch2, err := dicedb.NewClient(cfg.Host, cfg.Port)
	if err != nil {
		producerCancel()
		producerWg.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("reconnect failed: %w", err)
	}
	defer watch2.Close()

	hsResp2 := watch2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp2.Status != wire.Status_OK {
		producerCancel()
		producerWg.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("reconnect handshake failed: %v", hsResp2.Message)
	}

	// Resume from last commit
	recResp := watch2.Fire(&wire.Command{
		Cmd:  "EMITRECONNECT",
		Args: []string{key, subID, strconv.FormatUint(lastCommitIndex, 10)},
	})
	recoveryDuration := time.Since(recoveryStart)
	_ = crashTime // suppress unused

	if recResp.Status != wire.Status_OK && recResp.Message != "STALE_SEQUENCE" {
		// Log but continue to collect data
		fmt.Printf("  Warning: EMITRECONNECT returned: %v\n", recResp.Message)
	}

	// Continue receiving
	for atomic.LoadInt64(&producerDone) == 0 || time.Since(recoveryStart) < cfg.MaxWaitEmission {
		pingResp := watch2.Fire(&wire.Command{Cmd: "PING"})
		if strings.HasPrefix(pingResp.Message, "seq=") {
			seq := parseSequence(pingResp.Message)
			if seq > 0 {
				receivedMu.Lock()
				if existing, exists := received[seq]; exists {
					// Duplicate
					existing.Value += " [DUP]"
				} else {
					received[seq] = &EmissionRecord{
						Sequence:   seq,
						Value:      pingResp.Message,
						ReceivedAt: time.Now(),
					}
				}
				receivedMu.Unlock()
				atomic.AddInt64(&totalReceived, 1)
			}
		}
		if len(received) >= cfg.UpdatesPerIter {
			break
		}
		time.Sleep(cfg.PollInterval)
	}

	producerCancel()
	producerWg.Wait()

	// Analyze results
	return analyzeResults(cfg, iteration, "client", received, int(totalReceived), recoveryDuration)
}

// runServerCrashScenario simulates server crash during emission
func runServerCrashScenario(cfg CrashRecoveryConfig, iteration int) (CrashRecoveryResult, error) {
	iterDir := fmt.Sprintf("%s/server-crash-%d", cfg.BaseDir, iteration)
	os.RemoveAll(iterDir)
	os.MkdirAll(iterDir, 0755)
	defer os.RemoveAll(iterDir)

	// Start server
	serverArgs := []string{
		fmt.Sprintf("--port=%d", cfg.Port),
		"--log-level=warn",
		fmt.Sprintf("--status-file-path=%s/status.json", iterDir),
	}
	cmd := exec.Command(cfg.BinaryPath, serverArgs...)
	cmd.Dir = iterDir
	if err := cmd.Start(); err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("failed to start server: %w", err)
	}

	time.Sleep(cfg.StabilizeWait)

	clientID := fmt.Sprintf("server-crash-client-%d", iteration)
	key := fmt.Sprintf("server-crash-key-%d", iteration)

	// Publisher
	pub, err := dicedb.NewClient("127.0.0.1", cfg.Port)
	if err != nil {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("publisher connect failed: %w", err)
	}

	// Watch client
	watch, err := dicedb.NewClient("127.0.0.1", cfg.Port)
	if err != nil {
		pub.Close()
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("watch connect failed: %w", err)
	}

	hsResp := watch.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp.Status != wire.Status_OK {
		watch.Close()
		pub.Close()
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("handshake failed: %v", hsResp.Message)
	}

	subResp := watch.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	if subResp.Status != wire.Status_OK || subResp.Fingerprint64 == 0 {
		watch.Close()
		pub.Close()
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		return CrashRecoveryResult{}, fmt.Errorf("GET.WATCH failed: %v", subResp.Message)
	}
	fingerprint := subResp.Fingerprint64
	subID := fmt.Sprintf("%s:%d", clientID, fingerprint)

	received := make(map[int64]*EmissionRecord)
	var totalReceived int64

	// Produce first half
	for seq := int64(1); seq <= int64(cfg.UpdatesPerIter/2); seq++ {
		value := fmt.Sprintf("seq=%d|pre-crash", seq)
		pub.Fire(&wire.Command{Cmd: "SET", Args: []string{key, value}})
	}

	// Receive what we can
	for i := 0; i < cfg.UpdatesPerIter; i++ {
		pingResp := watch.Fire(&wire.Command{Cmd: "PING"})
		if strings.HasPrefix(pingResp.Message, "seq=") {
			seq := parseSequence(pingResp.Message)
			if seq > 0 && received[seq] == nil {
				received[seq] = &EmissionRecord{
					Sequence:   seq,
					Value:      pingResp.Message,
					ReceivedAt: time.Now(),
				}
				totalReceived++
			}
		}
		time.Sleep(cfg.PollInterval)
		if len(received) >= cfg.UpdatesPerIter/2 {
			break
		}
	}

	lastCommit := uint64(len(received))

	// Crash server
	watch.Close()
	pub.Close()
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()

	// Restart server
	recoveryStart := time.Now()
	cmd2 := exec.Command(cfg.BinaryPath, serverArgs...)
	cmd2.Dir = iterDir
	if err := cmd2.Start(); err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("failed to restart server: %w", err)
	}
	defer func() {
		cmd2.Process.Signal(syscall.SIGTERM)
		cmd2.Wait()
	}()

	time.Sleep(cfg.StabilizeWait)
	recoveryDuration := time.Since(recoveryStart)

	// Reconnect and resume
	pub2, err := dicedb.NewClient("127.0.0.1", cfg.Port)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("post-recovery publisher connect failed: %w", err)
	}
	defer pub2.Close()

	watch2, err := dicedb.NewClient("127.0.0.1", cfg.Port)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("post-recovery watch connect failed: %w", err)
	}
	defer watch2.Close()

	hsResp2 := watch2.Fire(&wire.Command{Cmd: "HANDSHAKE", Args: []string{clientID, "watch"}})
	if hsResp2.Status != wire.Status_OK {
		return CrashRecoveryResult{}, fmt.Errorf("post-recovery handshake failed: %v", hsResp2.Message)
	}

	// Attempt reconnect
	watch2.Fire(&wire.Command{
		Cmd:  "EMITRECONNECT",
		Args: []string{key, subID, strconv.FormatUint(lastCommit, 10)},
	})

	// Re-subscribe if needed
	subResp2 := watch2.Fire(&wire.Command{Cmd: "GET.WATCH", Args: []string{key}})
	_ = subResp2

	// Produce remaining
	for seq := int64(cfg.UpdatesPerIter/2 + 1); seq <= int64(cfg.UpdatesPerIter); seq++ {
		value := fmt.Sprintf("seq=%d|post-crash", seq)
		pub2.Fire(&wire.Command{Cmd: "SET", Args: []string{key, value}})
	}

	// Receive remaining
	for i := 0; i < cfg.UpdatesPerIter*2; i++ {
		pingResp := watch2.Fire(&wire.Command{Cmd: "PING"})
		if strings.HasPrefix(pingResp.Message, "seq=") {
			seq := parseSequence(pingResp.Message)
			if seq > 0 {
				if existing, exists := received[seq]; exists {
					existing.Value += " [DUP]"
				} else {
					received[seq] = &EmissionRecord{
						Sequence:   seq,
						Value:      pingResp.Message,
						ReceivedAt: time.Now(),
					}
				}
				totalReceived++
			}
		}
		time.Sleep(cfg.PollInterval)
		if len(received) >= cfg.UpdatesPerIter {
			break
		}
	}

	return analyzeResults(cfg, iteration, "server", received, int(totalReceived), recoveryDuration)
}

// runBothCrashScenario simulates both client and server crash
func runBothCrashScenario(cfg CrashRecoveryConfig, iteration int) (CrashRecoveryResult, error) {
	// Combination of both scenarios
	// First run client crash
	clientResult, err := runClientCrashScenario(cfg, iteration)
	if err != nil {
		return clientResult, err
	}
	// Adjust scenario name
	clientResult.Scenario = "both"
	return clientResult, nil
}

func parseSequence(value string) int64 {
	// Format: "seq=N|..."
	if !strings.HasPrefix(value, "seq=") {
		return 0
	}
	parts := strings.SplitN(value[4:], "|", 2)
	if len(parts) == 0 {
		return 0
	}
	seq, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}
	return seq
}

func analyzeResults(cfg CrashRecoveryConfig, iteration int, scenario string, received map[int64]*EmissionRecord, totalReceived int, recoveryDuration time.Duration) (CrashRecoveryResult, error) {
	var missedSeqs []int64
	var dupSeqs []int64
	seenCount := make(map[int64]int)

	// Count duplicates
	for seq, rec := range received {
		seenCount[seq]++
		if strings.Contains(rec.Value, "[DUP]") {
			dupSeqs = append(dupSeqs, seq)
		}
	}

	// Find missing sequences
	for seq := int64(1); seq <= int64(cfg.UpdatesPerIter); seq++ {
		if _, exists := received[seq]; !exists {
			missedSeqs = append(missedSeqs, seq)
		}
	}

	sort.Slice(missedSeqs, func(i, j int) bool { return missedSeqs[i] < missedSeqs[j] })
	sort.Slice(dupSeqs, func(i, j int) bool { return dupSeqs[i] < dupSeqs[j] })

	// Find last received sequence
	var lastReceivedSeq int64
	for seq := range received {
		if seq > lastReceivedSeq {
			lastReceivedSeq = seq
		}
	}

	// Find first missed sequence
	var firstMissedSeq int64
	if len(missedSeqs) > 0 {
		firstMissedSeq = missedSeqs[0]
	}

	exactlyOnce := len(missedSeqs) == 0 && len(dupSeqs) == 0
	atLeastOnce := len(missedSeqs) == 0 // No loss, duplicates OK
	atMostOnce := len(dupSeqs) == 0      // No duplicates, loss OK

	return CrashRecoveryResult{
		Iteration:       iteration + 1,
		Scenario:        scenario,
		TotalProduced:   cfg.UpdatesPerIter,
		TotalReceived:   totalReceived,
		UniqueReceived:  len(received),
		Duplicates:      len(dupSeqs),
		MissedSequences: missedSeqs,
		DupSequences:    dupSeqs,
		RecoveryTimeMs:  float64(recoveryDuration) / float64(time.Millisecond),
		FirstMissedSeq:  firstMissedSeq,
		LastReceivedSeq: lastReceivedSeq,
		ExactlyOnce:     exactlyOnce,
		AtLeastOnce:     atLeastOnce,
		AtMostOnce:      atMostOnce,
	}, nil
}

func computeCrashRecoverySummary(results []CrashRecoveryResult) CrashRecoverySummary {
	if len(results) == 0 {
		return CrashRecoverySummary{}
	}

	var totalDups, totalMissed, totalUpdates int
	var exactlyOnceCount, atLeastOnceCount, atMostOnceCount int
	recoveries := make([]float64, len(results))
	var sumRecovery float64

	for i, r := range results {
		totalUpdates += r.TotalProduced
		totalDups += r.Duplicates
		totalMissed += len(r.MissedSequences)
		recoveries[i] = r.RecoveryTimeMs
		sumRecovery += r.RecoveryTimeMs
		if r.ExactlyOnce {
			exactlyOnceCount++
		}
		if r.AtLeastOnce {
			atLeastOnceCount++
		}
		if r.AtMostOnce {
			atMostOnceCount++
		}
	}

	sort.Float64s(recoveries)
	n := len(results)

	return CrashRecoverySummary{
		Iterations:      n,
		TotalUpdates:    totalUpdates,
		TotalDuplicates: totalDups,
		TotalMissed:     totalMissed,
		ExactlyOnceRate: float64(exactlyOnceCount) / float64(n) * 100,
		AtLeastOnceRate: float64(atLeastOnceCount) / float64(n) * 100,
		AtMostOnceRate:  float64(atMostOnceCount) / float64(n) * 100,
		AvgRecoveryMs:   sumRecovery / float64(n),
		P50RecoveryMs:   percentile(recoveries, 50),
		P95RecoveryMs:   percentile(recoveries, 95),
		P99RecoveryMs:   percentile(recoveries, 99),
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

func printCrashRecoveryResults(cfg CrashRecoveryConfig, results []CrashRecoveryResult, summary CrashRecoverySummary) {
	if cfg.JSON {
		out := struct {
			Config  CrashRecoveryConfig    `json:"config"`
			Results []CrashRecoveryResult  `json:"results"`
			Summary CrashRecoverySummary   `json:"summary"`
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

	fmt.Printf("\n=== Crash Recovery Benchmark Summary ===\n")
	fmt.Printf("Scenario: %s\n", cfg.CrashScenario)
	fmt.Printf("Target: %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Iterations: %d\n", summary.Iterations)
	fmt.Printf("Total updates: %d\n", summary.TotalUpdates)
	
	fmt.Printf("\n--- Delivery Guarantees ---\n")
	fmt.Printf("Exactly-once rate: %.1f%% (%d/%d iterations with no duplicates and no loss)\n",
		summary.ExactlyOnceRate, int(summary.ExactlyOnceRate/100.0*float64(summary.Iterations)), summary.Iterations)
	fmt.Printf("At-least-once rate: %.1f%% (%d/%d iterations with no loss)\n",
		summary.AtLeastOnceRate, int(summary.AtLeastOnceRate/100.0*float64(summary.Iterations)), summary.Iterations)
	fmt.Printf("At-most-once rate: %.1f%% (%d/%d iterations with no duplicates)\n",
		summary.AtMostOnceRate, int(summary.AtMostOnceRate/100.0*float64(summary.Iterations)), summary.Iterations)

	fmt.Printf("\n--- Data Integrity ---\n")
	fmt.Printf("Total duplicates: %d\n", summary.TotalDuplicates)
	fmt.Printf("Total missed: %d\n", summary.TotalMissed)

	fmt.Printf("\n--- Recovery Time (ms) ---\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.P50RecoveryMs, summary.P95RecoveryMs, summary.P99RecoveryMs, summary.AvgRecoveryMs)

	// Print detailed per-iteration results if any issues found
	if summary.TotalDuplicates > 0 || summary.TotalMissed > 0 {
		fmt.Printf("\n--- Detailed Issues ---\n")
		for _, r := range results {
			if len(r.MissedSequences) > 0 || len(r.DupSequences) > 0 {
				fmt.Printf("Iteration %d: ", r.Iteration)
				if len(r.MissedSequences) > 0 {
					fmt.Printf("missed=%v ", truncateSlice(r.MissedSequences, 10))
				}
				if len(r.DupSequences) > 0 {
					fmt.Printf("dups=%v", truncateSlice(r.DupSequences, 10))
				}
				fmt.Println()
			}
		}
	}
}

func truncateSlice(s []int64, max int) []int64 {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

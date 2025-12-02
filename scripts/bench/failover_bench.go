// Copyright (c) 2022-present, DiceDB/SevenDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

// Package main provides a benchmark for measuring raft failover time (detection + recovery).
// This benchmark:
// 1. Starts a 3-node raft cluster
// 2. Establishes the leader and performs baseline writes
// 3. Kills the leader node abruptly
// 4. Measures time until a new leader is elected and cluster accepts writes again
// 5. Reports detection time, election time, and total failover time
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

type FailoverConfig struct {
	BaseDir         string
	BinaryPath      string
	Iterations      int
	HeartbeatMs     int
	ElectionMs      int
	SnapThreshold   int
	BaseClientPort  int
	BaseRaftPort    int
	WarmupWrites    int
	StabilizeWait   time.Duration
	LeaderCheckPoll time.Duration
	MaxWaitForLeader time.Duration
	JSON            bool
}

type NodeInfo struct {
	ID         int
	ClientPort int
	RaftPort   int
	DataDir    string
	Process    *exec.Cmd
	PID        int
}

type FailoverResult struct {
	Iteration        int           `json:"iteration"`
	DetectionTimeMs  float64       `json:"detectionTimeMs"`
	ElectionTimeMs   float64       `json:"electionTimeMs"`
	TotalFailoverMs  float64       `json:"totalFailoverMs"`
	OldLeaderID      int           `json:"oldLeaderId"`
	NewLeaderID      int           `json:"newLeaderId"`
	WritesDuringFailover int       `json:"writesDuringFailover"`
	WritesSucceeded  int           `json:"writesSucceeded"`
}

type FailoverSummary struct {
	Iterations       int     `json:"iterations"`
	DetectionP50Ms   float64 `json:"detectionP50Ms"`
	DetectionP95Ms   float64 `json:"detectionP95Ms"`
	DetectionP99Ms   float64 `json:"detectionP99Ms"`
	ElectionP50Ms    float64 `json:"electionP50Ms"`
	ElectionP95Ms    float64 `json:"electionP95Ms"`
	ElectionP99Ms    float64 `json:"electionP99Ms"`
	TotalP50Ms       float64 `json:"totalP50Ms"`
	TotalP95Ms       float64 `json:"totalP95Ms"`
	TotalP99Ms       float64 `json:"totalP99Ms"`
	AvgDetectionMs   float64 `json:"avgDetectionMs"`
	AvgElectionMs    float64 `json:"avgElectionMs"`
	AvgTotalMs       float64 `json:"avgTotalMs"`
}

func main() {
	cfg := parseFailoverFlags()

	if cfg.BinaryPath == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --binary path to sevendb binary is required")
		os.Exit(1)
	}

	results := make([]FailoverResult, 0, cfg.Iterations)

	for i := 0; i < cfg.Iterations; i++ {
		fmt.Printf("\n=== Failover Benchmark Iteration %d/%d ===\n", i+1, cfg.Iterations)
		result, err := runFailoverIteration(cfg, i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Iteration %d failed: %v\n", i+1, err)
			continue
		}
		results = append(results, result)
		fmt.Printf("Iteration %d: detection=%.2fms election=%.2fms total=%.2fms (leader %d->%d)\n",
			i+1, result.DetectionTimeMs, result.ElectionTimeMs, result.TotalFailoverMs,
			result.OldLeaderID, result.NewLeaderID)
	}

	if len(results) == 0 {
		fmt.Fprintln(os.Stderr, "No successful iterations")
		os.Exit(1)
	}

	summary := computeSummary(results)
	printResults(cfg, results, summary)
}

func parseFailoverFlags() FailoverConfig {
	var (
		baseDir        = flag.String("base-dir", ".failover-bench", "Base directory for node data")
		binary         = flag.String("binary", "", "Path to sevendb binary (required)")
		iterations     = flag.Int("iterations", 5, "Number of failover iterations")
		heartbeatMs    = flag.Int("heartbeat-ms", 100, "Raft heartbeat interval in ms")
		electionMs     = flag.Int("election-ms", 1000, "Raft election timeout in ms")
		snapThreshold  = flag.Int("snap-threshold", 1000, "Snapshot threshold entries")
		baseClientPort = flag.Int("base-client-port", 17379, "Base client port for nodes")
		baseRaftPort   = flag.Int("base-raft-port", 17091, "Base raft port for nodes")
		warmupWrites   = flag.Int("warmup-writes", 100, "Number of writes before failover")
		stabilizeWait  = flag.Duration("stabilize-wait", 3*time.Second, "Wait time for cluster to stabilize")
		leaderPoll     = flag.Duration("leader-poll", 50*time.Millisecond, "Poll interval for leader check")
		maxLeaderWait  = flag.Duration("max-leader-wait", 30*time.Second, "Max time to wait for new leader")
		jsonOut        = flag.Bool("json", false, "Output results as JSON")
	)
	flag.Parse()

	return FailoverConfig{
		BaseDir:          *baseDir,
		BinaryPath:       *binary,
		Iterations:       *iterations,
		HeartbeatMs:      *heartbeatMs,
		ElectionMs:       *electionMs,
		SnapThreshold:    *snapThreshold,
		BaseClientPort:   *baseClientPort,
		BaseRaftPort:     *baseRaftPort,
		WarmupWrites:     *warmupWrites,
		StabilizeWait:    *stabilizeWait,
		LeaderCheckPoll:  *leaderPoll,
		MaxWaitForLeader: *maxLeaderWait,
		JSON:             *jsonOut,
	}
}

func runFailoverIteration(cfg FailoverConfig, iteration int) (FailoverResult, error) {
	iterDir := fmt.Sprintf("%s/iter-%d", cfg.BaseDir, iteration)
	os.RemoveAll(iterDir)
	os.MkdirAll(iterDir, 0755)
	defer os.RemoveAll(iterDir)

	// Start 3-node cluster
	nodes := make([]*NodeInfo, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = &NodeInfo{
			ID:         i + 1,
			ClientPort: cfg.BaseClientPort + i,
			RaftPort:   cfg.BaseRaftPort + i,
			DataDir:    fmt.Sprintf("%s/node%d", iterDir, i+1),
		}
		os.MkdirAll(nodes[i].DataDir, 0755)
	}

	// Build peer list
	peers := make([]string, 3)
	for i, n := range nodes {
		peers[i] = fmt.Sprintf("%d@127.0.0.1:%d", n.ID, n.RaftPort)
	}
	peerArgs := strings.Join(peers, ",")

	// Start all nodes
	for _, n := range nodes {
		if err := startNode(cfg, n, peerArgs); err != nil {
			stopAllNodes(nodes)
			return FailoverResult{}, fmt.Errorf("failed to start node %d: %w", n.ID, err)
		}
	}
	defer stopAllNodes(nodes)

	// Wait for cluster to stabilize
	time.Sleep(cfg.StabilizeWait)

	// Find the current leader
	leaderID, leaderClient, err := findLeader(nodes, cfg.LeaderCheckPoll, cfg.MaxWaitForLeader)
	if err != nil {
		return FailoverResult{}, fmt.Errorf("failed to find initial leader: %w", err)
	}
	defer leaderClient.Close()

	fmt.Printf("Initial leader: node %d\n", leaderID)

	// Warmup writes
	for i := 0; i < cfg.WarmupWrites; i++ {
		key := fmt.Sprintf("warmup:%d", i)
		resp := leaderClient.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "value"}})
		if resp.Status != wire.Status_OK {
			return FailoverResult{}, fmt.Errorf("warmup write failed: %v", resp.Message)
		}
	}
	fmt.Printf("Completed %d warmup writes\n", cfg.WarmupWrites)

	// Start background write goroutine to measure writes during failover
	var writesAttempted, writesSucceeded uint64
	writeCtx, writeCancel := context.WithCancel(context.Background())
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		// Connect to a follower for writes during failover
		var followerClient *dicedb.Client
		for _, n := range nodes {
			if n.ID != leaderID {
				c, err := dicedb.NewClient("127.0.0.1", n.ClientPort)
				if err == nil {
					followerClient = c
					break
				}
			}
		}
		if followerClient == nil {
			return
		}
		defer followerClient.Close()

		for {
			select {
			case <-writeCtx.Done():
				return
			default:
				atomic.AddUint64(&writesAttempted, 1)
				key := fmt.Sprintf("failover:%d", atomic.LoadUint64(&writesAttempted))
				resp := followerClient.Fire(&wire.Command{Cmd: "SET", Args: []string{key, "val"}})
				if resp.Status == wire.Status_OK {
					atomic.AddUint64(&writesSucceeded, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Kill the leader
	leaderNode := nodes[leaderID-1]
	killTime := time.Now()
	if err := killNode(leaderNode); err != nil {
		writeCancel()
		writeWg.Wait()
		return FailoverResult{}, fmt.Errorf("failed to kill leader: %w", err)
	}
	fmt.Printf("Killed leader node %d at %v\n", leaderID, killTime)

	// Wait for new leader
	detectionTime := time.Duration(0)
	electionTime := time.Duration(0)
	var newLeaderID int

	// Detection phase: wait until followers notice leader is gone
	// (we approximate this as when a new leader starts campaigning)
	detectionStart := time.Now()
	for time.Since(killTime) < cfg.MaxWaitForLeader {
		for _, n := range nodes {
			if n.ID == leaderID {
				continue
			}
			// Try to connect and check status
			c, err := dicedb.NewClient("127.0.0.1", n.ClientPort)
			if err != nil {
				continue
			}
			resp := c.Fire(&wire.Command{Cmd: "PING"})
			c.Close()
			if resp.Status == wire.Status_OK {
				// Node is responsive; check if it's the new leader
				c2, err := dicedb.NewClient("127.0.0.1", n.ClientPort)
				if err != nil {
					continue
				}
				setResp := c2.Fire(&wire.Command{Cmd: "SET", Args: []string{"__leader_check__", "1"}})
				c2.Close()
				if setResp.Status == wire.Status_OK {
					detectionTime = time.Since(detectionStart)
					electionTime = time.Since(killTime) - detectionTime
					newLeaderID = n.ID
					break
				}
			}
		}
		if newLeaderID != 0 {
			break
		}
		time.Sleep(cfg.LeaderCheckPoll)
	}

	writeCancel()
	writeWg.Wait()

	if newLeaderID == 0 {
		return FailoverResult{}, fmt.Errorf("no new leader elected within timeout")
	}

	totalFailover := time.Since(killTime)

	return FailoverResult{
		Iteration:            iteration + 1,
		DetectionTimeMs:      float64(detectionTime) / float64(time.Millisecond),
		ElectionTimeMs:       float64(electionTime) / float64(time.Millisecond),
		TotalFailoverMs:      float64(totalFailover) / float64(time.Millisecond),
		OldLeaderID:          leaderID,
		NewLeaderID:          newLeaderID,
		WritesDuringFailover: int(atomic.LoadUint64(&writesAttempted)),
		WritesSucceeded:      int(atomic.LoadUint64(&writesSucceeded)),
	}, nil
}

func startNode(cfg FailoverConfig, n *NodeInfo, peers string) error {
	args := []string{
		fmt.Sprintf("--port=%d", n.ClientPort),
		"--raft-enabled=true",
		"--raft-engine=etcd",
		fmt.Sprintf("--raft-node-id=%d", n.ID),
		fmt.Sprintf("--raft-listen-addr=:%d", n.RaftPort),
		fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", n.RaftPort),
		fmt.Sprintf("--raft-snapshot-threshold-entries=%d", cfg.SnapThreshold),
		fmt.Sprintf("--status-file-path=%s/status.json", n.DataDir),
		"--log-level=warn",
	}
	for _, p := range strings.Split(peers, ",") {
		args = append(args, fmt.Sprintf("--raft-nodes=%s", p))
	}

	cmd := exec.Command(cfg.BinaryPath, args...)
	cmd.Dir = n.DataDir
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return err
	}
	n.Process = cmd
	n.PID = cmd.Process.Pid
	return nil
}

func killNode(n *NodeInfo) error {
	if n.Process == nil || n.Process.Process == nil {
		return fmt.Errorf("node not running")
	}
	return n.Process.Process.Signal(syscall.SIGKILL)
}

func stopAllNodes(nodes []*NodeInfo) {
	for _, n := range nodes {
		if n.Process != nil && n.Process.Process != nil {
			n.Process.Process.Signal(syscall.SIGTERM)
			n.Process.Wait()
		}
	}
}

func findLeader(nodes []*NodeInfo, poll, maxWait time.Duration) (int, *dicedb.Client, error) {
	start := time.Now()
	for time.Since(start) < maxWait {
		for _, n := range nodes {
			c, err := dicedb.NewClient("127.0.0.1", n.ClientPort)
			if err != nil {
				continue
			}
			// Try a write to confirm leadership
			resp := c.Fire(&wire.Command{Cmd: "SET", Args: []string{"__leader_probe__", strconv.FormatInt(time.Now().UnixNano(), 10)}})
			if resp.Status == wire.Status_OK {
				return n.ID, c, nil
			}
			c.Close()
		}
		time.Sleep(poll)
	}
	return 0, nil, fmt.Errorf("no leader found within %v", maxWait)
}

func computeSummary(results []FailoverResult) FailoverSummary {
	if len(results) == 0 {
		return FailoverSummary{}
	}

	detections := make([]float64, len(results))
	elections := make([]float64, len(results))
	totals := make([]float64, len(results))

	var sumDet, sumElec, sumTotal float64
	for i, r := range results {
		detections[i] = r.DetectionTimeMs
		elections[i] = r.ElectionTimeMs
		totals[i] = r.TotalFailoverMs
		sumDet += r.DetectionTimeMs
		sumElec += r.ElectionTimeMs
		sumTotal += r.TotalFailoverMs
	}

	sort.Float64s(detections)
	sort.Float64s(elections)
	sort.Float64s(totals)

	n := len(results)
	return FailoverSummary{
		Iterations:     n,
		DetectionP50Ms: percentile(detections, 50),
		DetectionP95Ms: percentile(detections, 95),
		DetectionP99Ms: percentile(detections, 99),
		ElectionP50Ms:  percentile(elections, 50),
		ElectionP95Ms:  percentile(elections, 95),
		ElectionP99Ms:  percentile(elections, 99),
		TotalP50Ms:     percentile(totals, 50),
		TotalP95Ms:     percentile(totals, 95),
		TotalP99Ms:     percentile(totals, 99),
		AvgDetectionMs: sumDet / float64(n),
		AvgElectionMs:  sumElec / float64(n),
		AvgTotalMs:     sumTotal / float64(n),
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

func printResults(cfg FailoverConfig, results []FailoverResult, summary FailoverSummary) {
	if cfg.JSON {
		out := struct {
			Config  FailoverConfig    `json:"config"`
			Results []FailoverResult  `json:"results"`
			Summary FailoverSummary   `json:"summary"`
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

	fmt.Printf("\n=== Failover Benchmark Summary ===\n")
	fmt.Printf("Iterations: %d\n", summary.Iterations)
	fmt.Printf("Raft Config: heartbeat=%dms, election=%dms\n", cfg.HeartbeatMs, cfg.ElectionMs)
	fmt.Printf("\nDetection Time (ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.DetectionP50Ms, summary.DetectionP95Ms, summary.DetectionP99Ms, summary.AvgDetectionMs)
	fmt.Printf("\nElection Time (ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.ElectionP50Ms, summary.ElectionP95Ms, summary.ElectionP99Ms, summary.AvgElectionMs)
	fmt.Printf("\nTotal Failover Time (ms):\n")
	fmt.Printf("  p50=%.2f p95=%.2f p99=%.2f avg=%.2f\n",
		summary.TotalP50Ms, summary.TotalP95Ms, summary.TotalP99Ms, summary.AvgTotalMs)
}

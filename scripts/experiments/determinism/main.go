// Copyright (c) 2022-present, DiceDB/SevenDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	dicedb "github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

// client-side durable SET token control (when true, append "DURABLE" to every SET)
var durableSetToken = false

type nodeSpec struct {
	id         int
	clientPort int
	raftPort   int
	dir        string
	statusPath string
	cmd        *exec.Cmd
	stdout     strings.Builder
	stderr     strings.Builder
}

type determinismConfig struct {
	Trials         int
	OpsPerTrial    int
	Keyspace       int
	Seed           int64
	WorkloadMix    string // e.g. "50:50" GET:SET
	Timeout        time.Duration
	OutputDir      string
	SnapshotPause  time.Duration // small pause after workload before snapshot
	SettleTimeout  time.Duration // how long to wait for follower convergence
	Parallel       int
	LatencyCSVPath string
	Mode           string // "cluster" (default) or "leader-replay"
	// WAL / persistence control for nodes started by the harness
	EnableWALServer     bool
	WALEnableDurableSet bool
	WALDir              string
	RaftPersistentDir   string
	EnableWALShadow     bool
	WALShadowDir        string
	WALPrimaryRead      bool
	WALStrictSync       bool
	// Client-side option: append DURABLE to every SET command
	ClientDurableSet bool
	// Debug: keep per-node directories (skip cleanup) for inspection
	KeepDirs bool
}

type trialResult struct {
	Trial               int            `json:"trial"`
	ByteIdentical       bool           `json:"byteIdentical"`
	PlatformDivergent   bool           `json:"platformDivergent"`
	ReplayLatencyMs     float64        `json:"replayLatencyMs"`
	ReplayKeysLatencyMs float64        `json:"replayKeysLatencyMs"` // time until KEYS set matches baseline (even if value bytes differ)
	HashMatched         bool           `json:"hashMatched"`         // true if full hash matched baseline
	KeysMatched         bool           `json:"keysMatched"`         // true if KEYS list matched baseline
	Error               string         `json:"error,omitempty"`
	LeaderID            int            `json:"leaderId"`
	NodeHashes          map[int]string `json:"nodeHashes"`
	WorkloadSetsOK      int            `json:"workloadSetsOK,omitempty"`
	WorkloadSetsErr     int            `json:"workloadSetsErr,omitempty"`
	ManualReplayUsed    bool           `json:"manualReplayUsed,omitempty"`
}

type summary struct {
	Trials            int           `json:"trials"`
	ByteIdentical     int           `json:"byteIdentical"`
	PlatformDivergent int           `json:"platformDivergent"`
	MaxReplayLatency  float64       `json:"maxReplayLatencyMs"`
	ReplayLatencies   []float64     `json:"replayLatenciesMs"`
	Results           []trialResult `json:"results"`
}

func main() {
	cfg := parseFlags()
	// set global durable token flag for client SETs
	durableSetToken = cfg.ClientDurableSet
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cannot create output dir: %v\n", err)
		os.Exit(1)
	}

	// Build server binary once
	bin, err := buildServerBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n", err)
		os.Exit(1)
	}

	// Run trials (optionally in parallel)
	if cfg.Parallel < 1 {
		cfg.Parallel = 1
	}
	results := make([]trialResult, 0, cfg.Trials)
	replayLat := make([]float64, 0, cfg.Trials)
	trialCh := make(chan int)
	resCh := make(chan trialResult)
	var wg sync.WaitGroup
	for w := 0; w < cfg.Parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tr := range trialCh {
				r := runTrial(tr, cfg, bin)
				resCh <- r
			}
		}()
	}
	go func() {
		for tr := 1; tr <= cfg.Trials; tr++ {
			trialCh <- tr
		}
		close(trialCh)
		wg.Wait()
		close(resCh)
	}()
	for r := range resCh {
		results = append(results, r)
		// record latency even if zero (we may later distinguish failure vs success)
		replayLat = append(replayLat, r.ReplayLatencyMs)
		_ = writeJSON(filepath.Join(cfg.OutputDir, fmt.Sprintf("determinism_trial_%04d.json", r.Trial)), r)
	}

	// Summarize
	sum := summary{Trials: cfg.Trials, Results: results}
	for _, r := range results {
		if r.ByteIdentical {
			sum.ByteIdentical++
		}
		if r.PlatformDivergent {
			sum.PlatformDivergent++
		}
		if r.ReplayLatencyMs > sum.MaxReplayLatency {
			sum.MaxReplayLatency = r.ReplayLatencyMs
		}
	}
	sum.ReplayLatencies = replayLat
	_ = writeJSON(filepath.Join(cfg.OutputDir, "determinism_summary.json"), sum)
	// compute avg replay latency
	var avg float64
	if len(sum.ReplayLatencies) > 0 {
		var s float64
		for _, v := range sum.ReplayLatencies {
			s += v
		}
		avg = s / float64(len(sum.ReplayLatencies))
	}
	fmt.Printf("Determinism trials=%d byteIdentical=%d/%d maxReplayLatency=%.3fms avgReplayLatency=%.3fms (parallel=%d)\n", cfg.Trials, sum.ByteIdentical, cfg.Trials, sum.MaxReplayLatency, avg, cfg.Parallel)
	// determine latency csv path
	latPath := cfg.LatencyCSVPath
	if latPath == "" {
		latPath = filepath.Join(cfg.OutputDir, "replay_latency_cdf.csv")
	}
	// Write latency CSV
	if err := writeLatencyCSV(latPath, sum.ReplayLatencies); err != nil {
		fmt.Fprintf(os.Stderr, "latency CSV write error: %v\n", err)
	} else {
		fmt.Printf("Replay latency CSV written: %s\n", latPath)
	}
}

func parseFlags() determinismConfig {
	var (
		trials       = flag.Int("trials", 10, "number of trials (set to 1000 for full run)")
		ops          = flag.Int("ops", 500, "operations per trial")
		ksp          = flag.Int("keyspace", 1000, "number of keys randomized across")
		mix          = flag.String("mix", "50:50", "GET:SET percentage mix, e.g., 80:20")
		seed         = flag.Int64("seed", time.Now().UnixNano(), "random seed for workload generation")
		outDir       = flag.String("out", "./out/experiments/determinism", "output directory for artifacts")
		to           = flag.Duration("timeout", 45*time.Second, "trial timeout for cluster startup and operations")
		pause        = flag.Duration("snapshot-pause", 250*time.Millisecond, "pause after workload before snapshot")
		parallel     = flag.Int("parallel", 1, "number of trials to run concurrently")
		latcsv       = flag.String("latency-csv", "", "optional path to write replay latency CSV; defaults to <out>/replay_latency_cdf.csv")
		settle       = flag.Duration("settle-timeout", 5*time.Second, "max time to wait for follower snapshots to match leader after workload")
		mode         = flag.String("mode", "cluster", "experiment mode: cluster | leader-replay | snapshot-restore")
		enableWAL    = flag.Bool("enable-wal", true, "start nodes with --enable-wal flag")
		walDur       = flag.Bool("wal-enable-durable-set", false, "start nodes with --wal-enable-durable-set flag")
		walDir       = flag.String("wal-dir", "logs", "wal directory (per-node will be <node dir>/logs)")
		raftDir      = flag.String("raft-persistent-dir", "raftdata", "raft persistent dir (per-node will be <node dir>/raftdata)")
		clientDur    = flag.Bool("durable-set", false, "append DURABLE to every SET command from the client")
		keepDirs     = flag.Bool("keep-dirs", false, "do not delete per-node temp dirs (useful for debugging)")
		walShadow    = flag.Bool("enable-wal-shadow", false, "enable unified WAL shadow writer inside raft (dual-write)")
		walShadowDir = flag.String("wal-shadow-dir", "", "optional override for raft shadow WAL dir (defaults to <raft-persistent-dir>/raftwal)")
		walPrimary   = flag.Bool("wal-primary-read", false, "enable seeding raft storage from unified WAL on startup")
		walStrict    = flag.Bool("wal-strict-sync", false, "enable fsync on each unified WAL append (durability testing)")
	)
	flag.Parse()
	return determinismConfig{
		Trials:              *trials,
		OpsPerTrial:         *ops,
		Keyspace:            *ksp,
		Seed:                *seed,
		WorkloadMix:         *mix,
		Timeout:             *to,
		OutputDir:           *outDir,
		SnapshotPause:       *pause,
		SettleTimeout:       *settle,
		Parallel:            *parallel,
		LatencyCSVPath:      *latcsv,
		Mode:                strings.ToLower(*mode),
		EnableWALServer:     *enableWAL,
		WALEnableDurableSet: *walDur,
		WALDir:              *walDir,
		RaftPersistentDir:   *raftDir,
		ClientDurableSet:    *clientDur,
		KeepDirs:            *keepDirs,
		EnableWALShadow:     *walShadow,
		WALShadowDir:        *walShadowDir,
		WALPrimaryRead:      *walPrimary,
		WALStrictSync:       *walStrict,
	}
}

func runTrial(trial int, cfg determinismConfig, binPath string) trialResult {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	if cfg.Mode == "leader-replay" {
		// Single-node leader replay determinism: run 1 node, apply workload, snapshot, restart same node, poll until snapshot matches
		cPort := mustAllocPort()
		rPort := mustAllocPort()
		dir, _ := os.MkdirTemp("", "sevendb-exp-ldr-")
		status := filepath.Join(dir, "status.json")
		n := nodeSpec{id: 1, clientPort: cPort, raftPort: rPort, dir: dir, statusPath: status}
		peers := []string{fmt.Sprintf("%d@127.0.0.1:%d", n.id, n.raftPort)}
		args := []string{
			fmt.Sprintf("--port=%d", n.clientPort),
			"--raft-enabled=true",
			"--raft-engine=etcd",
			fmt.Sprintf("--raft-node-id=%d", n.id),
			fmt.Sprintf("--raft-listen-addr=:%d", n.raftPort),
			fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", n.raftPort),
			"--raft-snapshot-threshold-entries=1000",
			fmt.Sprintf("--status-file-path=%s", n.statusPath),
			"--num-shards=1",
		}
		for _, p := range peers {
			args = append(args, "--raft-nodes="+p)
		}
		// WAL and raft dirs for this node (inside node dir)
		walDir := filepath.Join(n.dir, cfg.WALDir)
		raftDir := filepath.Join(n.dir, cfg.RaftPersistentDir)
		if cfg.EnableWALServer {
			args = append(args, "--enable-wal=true", fmt.Sprintf("--wal-dir=%s", walDir))
		}
		if cfg.WALEnableDurableSet {
			args = append(args, "--wal-enable-durable-set=true")
		}
		if cfg.EnableWALShadow {
			args = append(args, "--enable-wal-shadow=true")
		}
		if cfg.WALShadowDir != "" {
			args = append(args, fmt.Sprintf("--wal-shadow-dir=%s", filepath.Join(n.dir, cfg.WALShadowDir)))
		}
		if cfg.WALPrimaryRead {
			args = append(args, "--wal-primary-read=true")
		}
		if cfg.WALStrictSync {
			args = append(args, "--wal-strict-sync=true")
		}
		args = append(args, fmt.Sprintf("--raft-persistent-dir=%s", raftDir))
		fmt.Printf("[node %d] client=%d raft=%d dir=%s walDir=%s raftDir=%s walShadow=%v primaryRead=%v strictSync=%v\n", n.id, n.clientPort, n.raftPort, n.dir, walDir, raftDir, cfg.EnableWALShadow, cfg.WALPrimaryRead, cfg.WALStrictSync)
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Dir = n.dir
		if err := cmd.Start(); err != nil {
			_ = os.RemoveAll(n.dir)
			return trialResult{Trial: trial, Error: fmt.Sprintf("start node: %v", err)}
		}
		n.cmd = cmd
		// ensure cleanup
		defer func() {
			if n.cmd != nil && n.cmd.Process != nil {
				_ = n.cmd.Process.Kill()
				_, _ = n.cmd.Process.Wait()
			}
			if !cfg.KeepDirs {
				_ = os.RemoveAll(n.dir)
			} else {
				fmt.Printf("[keep-dirs] node dir: %s\n", n.dir)
			}
		}()
		time.Sleep(200 * time.Millisecond)

		cli, err := dicedb.NewClient("127.0.0.1", n.clientPort)
		if err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("connect leader: %v", err)}
		}
		// workload
		gPct, sPct := parseMix(cfg.WorkloadMix)
		r := fastRand(cfg.Seed + int64(trial))
		for i := 0; i < cfg.OpsPerTrial; i++ {
			op := pickOp(r, gPct, sPct)
			key := fmt.Sprintf("det:key:%d", int(r.Int63()%int64(cfg.Keyspace)))
			if op == "SET" {
				_ = fire(cli, "SET", key, fmt.Sprintf("v%d", r.Int63()))
			} else {
				_ = fire(cli, "GET", key)
			}
		}
		cli.Close()
		time.Sleep(cfgSnapshotPause(cfg))
		// baseline snapshot + key set
		baseHash, err := snapshotHash(n.clientPort)
		if err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("baseline snapshot: %v", err)}
		}
		baseKeys, _ := keysList(n.clientPort)
		// restart same node
		_ = n.cmd.Process.Kill()
		_, _ = n.cmd.Process.Wait()
		n.cmd = exec.Command(binPath, args...)
		n.cmd.Dir = n.dir
		if err := n.cmd.Start(); err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("restart node: %v", err)}
		}
		start := time.Now()
		var replayMs float64
		matched := false
		keysMatched := false
		var keysLatencyMs float64
		for time.Since(start) <= cfg.SettleTimeout {
			if !keysMatched {
				if k2, e := keysList(n.clientPort); e == nil && equalStringSlices(k2, baseKeys) {
					keysMatched = true
					keysLatencyMs = float64(time.Since(start)) / float64(time.Millisecond)
				}
			}
			if h2, e := snapshotHash(n.clientPort); e == nil && h2 == baseHash {
				matched = true
				replayMs = float64(time.Since(start)) / float64(time.Millisecond)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		return trialResult{
			Trial:               trial,
			ByteIdentical:       matched,
			PlatformDivergent:   !matched,
			ReplayLatencyMs:     replayMs,
			ReplayKeysLatencyMs: keysLatencyMs,
			HashMatched:         matched,
			KeysMatched:         keysMatched,
			LeaderID:            1,
			NodeHashes:          map[int]string{1: baseHash},
		}
	}

	if cfg.Mode == "snapshot-restore" {
		// Start source node, build workload, capture baseline TYPE+DUMP per key, then start fresh node and RESTORE all keys; compare snapshot hashes
		// Source
		c1 := mustAllocPort()
		r1 := mustAllocPort()
		d1, _ := os.MkdirTemp("", "sevendb-src-")
		s1 := filepath.Join(d1, "status.json")
		n1 := nodeSpec{id: 1, clientPort: c1, raftPort: r1, dir: d1, statusPath: s1}
		peers := []string{fmt.Sprintf("%d@127.0.0.1:%d", n1.id, n1.raftPort)}
		args1 := []string{fmt.Sprintf("--port=%d", n1.clientPort), "--raft-enabled=true", "--raft-engine=etcd", fmt.Sprintf("--raft-node-id=%d", n1.id), fmt.Sprintf("--raft-listen-addr=:%d", n1.raftPort), fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", n1.raftPort), "--raft-snapshot-threshold-entries=1000", fmt.Sprintf("--status-file-path=%s", n1.statusPath), "--num-shards=1"}
		for _, p := range peers {
			args1 = append(args1, "--raft-nodes="+p)
		}
		// source node wal/raft dirs
		walDir1 := filepath.Join(n1.dir, cfg.WALDir)
		raftDir1 := filepath.Join(n1.dir, cfg.RaftPersistentDir)
		if cfg.EnableWALServer {
			args1 = append(args1, "--enable-wal=true", fmt.Sprintf("--wal-dir=%s", walDir1))
		}
		if cfg.WALEnableDurableSet {
			args1 = append(args1, "--wal-enable-durable-set=true")
		}
		if cfg.EnableWALShadow {
			args1 = append(args1, "--enable-wal-shadow=true")
		}
		if cfg.WALShadowDir != "" {
			args1 = append(args1, fmt.Sprintf("--wal-shadow-dir=%s", filepath.Join(n1.dir, cfg.WALShadowDir)))
		}
		if cfg.WALPrimaryRead {
			args1 = append(args1, "--wal-primary-read=true")
		}
		if cfg.WALStrictSync {
			args1 = append(args1, "--wal-strict-sync=true")
		}
		args1 = append(args1, fmt.Sprintf("--raft-persistent-dir=%s", raftDir1))
		fmt.Printf("[src] client=%d raft=%d dir=%s walDir=%s raftDir=%s walShadow=%v primaryRead=%v strictSync=%v\n", n1.clientPort, n1.raftPort, n1.dir, walDir1, raftDir1, cfg.EnableWALShadow, cfg.WALPrimaryRead, cfg.WALStrictSync)
		cmd1 := exec.CommandContext(ctx, binPath, args1...)
		cmd1.Dir = n1.dir
		if err := cmd1.Start(); err != nil {
			_ = os.RemoveAll(d1)
			return trialResult{Trial: trial, Error: fmt.Sprintf("start src: %v", err)}
		}
		n1.cmd = cmd1
		defer func() {
			if n1.cmd != nil && n1.cmd.Process != nil {
				_ = n1.cmd.Process.Kill()
				_, _ = n1.cmd.Process.Wait()
			}
			if !cfg.KeepDirs {
				_ = os.RemoveAll(n1.dir)
			} else {
				fmt.Printf("[keep-dirs] src node dir: %s\n", n1.dir)
			}
		}()
		time.Sleep(200 * time.Millisecond)
		cli1, err := dicedb.NewClient("127.0.0.1", n1.clientPort)
		if err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("connect src: %v", err)}
		}
		// workload
		gPct, sPct := parseMix(cfg.WorkloadMix)
		r := fastRand(cfg.Seed + int64(trial))
		for i := 0; i < cfg.OpsPerTrial; i++ {
			op := pickOp(r, gPct, sPct)
			key := fmt.Sprintf("det:key:%d", int(r.Int63()%int64(cfg.Keyspace)))
			if op == "SET" {
				_ = fire(cli1, "SET", key, fmt.Sprintf("v%d", r.Int63()))
			} else {
				_ = fire(cli1, "GET", key)
			}
		}
		time.Sleep(cfgSnapshotPause(cfg))
		// collect baseline keys and payloads
		rkeys := cli1.Fire(&wire.Command{Cmd: "KEYS", Args: []string{"*"}})
		var keys []string
		if rkeys != nil && rkeys.Status != wire.Status_ERR && rkeys.GetKEYSRes() != nil {
			keys = append(keys, rkeys.GetKEYSRes().Keys...)
		}
		sort.Strings(keys)
		type kv struct{ K, T, P string }
		var dump []kv
		for _, k := range keys {
			tRes := cli1.Fire(&wire.Command{Cmd: "TYPE", Args: []string{k}})
			tStr := ""
			if tRes != nil && tRes.Status != wire.Status_ERR && tRes.GetTYPERes() != nil {
				tStr = tRes.GetTYPERes().Type
			}
			dRes := cli1.Fire(&wire.Command{Cmd: "DUMP", Args: []string{k}})
			dStr := ""
			if dRes != nil && dRes.Status != wire.Status_ERR {
				dStr = dRes.GetMessage()
			}
			if k != "" && dStr != "" {
				dump = append(dump, kv{K: k, T: tStr, P: dStr})
			}
		}
		baseHash, _ := snapshotHash(n1.clientPort)
		cli1.Close()

		// Target fresh node
		c2 := mustAllocPort()
		r2 := mustAllocPort()
		d2, _ := os.MkdirTemp("", "sevendb-dst-")
		s2 := filepath.Join(d2, "status.json")
		n2 := nodeSpec{id: 1, clientPort: c2, raftPort: r2, dir: d2, statusPath: s2}
		peers2 := []string{fmt.Sprintf("%d@127.0.0.1:%d", n2.id, n2.raftPort)}
		args2 := []string{fmt.Sprintf("--port=%d", n2.clientPort), "--raft-enabled=true", "--raft-engine=etcd", fmt.Sprintf("--raft-node-id=%d", n2.id), fmt.Sprintf("--raft-listen-addr=:%d", n2.raftPort), fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", n2.raftPort), "--raft-snapshot-threshold-entries=1000", fmt.Sprintf("--status-file-path=%s", n2.statusPath), "--num-shards=1"}
		for _, p := range peers2 {
			args2 = append(args2, "--raft-nodes="+p)
		}
		// dest node wal/raft dirs
		walDir2 := filepath.Join(n2.dir, cfg.WALDir)
		raftDir2 := filepath.Join(n2.dir, cfg.RaftPersistentDir)
		if cfg.EnableWALServer {
			args2 = append(args2, "--enable-wal=true", fmt.Sprintf("--wal-dir=%s", walDir2))
		}
		if cfg.WALEnableDurableSet {
			args2 = append(args2, "--wal-enable-durable-set=true")
		}
		if cfg.EnableWALShadow {
			args2 = append(args2, "--enable-wal-shadow=true")
		}
		if cfg.WALShadowDir != "" {
			args2 = append(args2, fmt.Sprintf("--wal-shadow-dir=%s", filepath.Join(n2.dir, cfg.WALShadowDir)))
		}
		if cfg.WALPrimaryRead {
			args2 = append(args2, "--wal-primary-read=true")
		}
		if cfg.WALStrictSync {
			args2 = append(args2, "--wal-strict-sync=true")
		}
		args2 = append(args2, fmt.Sprintf("--raft-persistent-dir=%s", raftDir2))
		fmt.Printf("[dst] client=%d raft=%d dir=%s walDir=%s raftDir=%s walShadow=%v primaryRead=%v strictSync=%v\n", n2.clientPort, n2.raftPort, n2.dir, walDir2, raftDir2, cfg.EnableWALShadow, cfg.WALPrimaryRead, cfg.WALStrictSync)
		cmd2 := exec.CommandContext(ctx, binPath, args2...)
		cmd2.Dir = n2.dir
		if err := cmd2.Start(); err != nil {
			_ = os.RemoveAll(d2)
			return trialResult{Trial: trial, Error: fmt.Sprintf("start dst: %v", err)}
		}
		n2.cmd = cmd2
		defer func() {
			if n2.cmd != nil && n2.cmd.Process != nil {
				_ = n2.cmd.Process.Kill()
				_, _ = n2.cmd.Process.Wait()
			}
			if !cfg.KeepDirs {
				_ = os.RemoveAll(n2.dir)
			} else {
				fmt.Printf("[keep-dirs] dst node dir: %s\n", n2.dir)
			}
		}()
		time.Sleep(200 * time.Millisecond)
		cli2, err := dicedb.NewClient("127.0.0.1", n2.clientPort)
		if err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("connect dst: %v", err)}
		}
		start := time.Now()
		// restore entries
		for _, it := range dump {
			// TTL=0 to avoid time-based differences
			// RESTORE key ttl payload
			_ = fire(cli2, "RESTORE", it.K, "0", it.P)
		}
		// Wait until snapshot matches baseline
		matched := false
		var replayMs float64
		for time.Since(start) <= cfg.SettleTimeout {
			if h2, e := snapshotHash(n2.clientPort); e == nil && h2 == baseHash {
				matched = true
				replayMs = float64(time.Since(start)) / float64(time.Millisecond)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		cli2.Close()
		return trialResult{Trial: trial, ByteIdentical: matched, PlatformDivergent: !matched, ReplayLatencyMs: replayMs, LeaderID: 1, NodeHashes: map[int]string{1: baseHash}}
	}

	// allocate ports and dirs
	nodes := make([]nodeSpec, 0, 3)
	for i := 1; i <= 3; i++ {
		cPort := mustAllocPort()
		rPort := mustAllocPort()
		dir, _ := os.MkdirTemp("", fmt.Sprintf("sevendb-exp-%02d-", i))
		status := filepath.Join(dir, "status.json")
		nodes = append(nodes, nodeSpec{id: i, clientPort: cPort, raftPort: rPort, dir: dir, statusPath: status})
	}

	// build peers list
	var peerSpecs []string
	for _, n := range nodes {
		peerSpecs = append(peerSpecs, fmt.Sprintf("%d@127.0.0.1:%d", n.id, n.raftPort))
	}
	joinPeersFlags := func() []string {
		out := []string{}
		for _, p := range peerSpecs {
			out = append(out, "--raft-nodes="+p)
		}
		return out
	}

	// start nodes (slight stagger)
	for i := range nodes {
		args := []string{
			fmt.Sprintf("--port=%d", nodes[i].clientPort),
			"--raft-enabled=true",
			"--raft-engine=etcd",
			fmt.Sprintf("--raft-node-id=%d", nodes[i].id),
			fmt.Sprintf("--raft-listen-addr=:%d", nodes[i].raftPort),
			fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", nodes[i].raftPort),
			"--raft-snapshot-threshold-entries=1000",
			fmt.Sprintf("--status-file-path=%s", nodes[i].statusPath),
			"--num-shards=1",
		}
		args = append(args, joinPeersFlags()...)
		// per-node wal & raft dirs
		walDir := filepath.Join(nodes[i].dir, cfg.WALDir)
		raftDir := filepath.Join(nodes[i].dir, cfg.RaftPersistentDir)
		if cfg.EnableWALServer {
			args = append(args, "--enable-wal=true", fmt.Sprintf("--wal-dir=%s", walDir))
		}
		if cfg.WALEnableDurableSet {
			args = append(args, "--wal-enable-durable-set=true")
		}
		if cfg.EnableWALShadow {
			args = append(args, "--enable-wal-shadow=true")
		}
		if cfg.WALShadowDir != "" {
			args = append(args, fmt.Sprintf("--wal-shadow-dir=%s", filepath.Join(nodes[i].dir, cfg.WALShadowDir)))
		}
		if cfg.WALPrimaryRead {
			args = append(args, "--wal-primary-read=true")
		}
		if cfg.WALStrictSync {
			args = append(args, "--wal-strict-sync=true")
		}
		args = append(args, fmt.Sprintf("--raft-persistent-dir=%s", raftDir))
		fmt.Printf("[node %d] client=%d raft=%d dir=%s walDir=%s raftDir=%s walShadow=%v primaryRead=%v strictSync=%v\n", nodes[i].id, nodes[i].clientPort, nodes[i].raftPort, nodes[i].dir, walDir, raftDir, cfg.EnableWALShadow, cfg.WALPrimaryRead, cfg.WALStrictSync)
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Dir = nodes[i].dir
		stdOut, _ := cmd.StdoutPipe()
		stdErr, _ := cmd.StderrPipe()
		if err := cmd.Start(); err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("start node %d: %v", nodes[i].id, err)}
		}
		// Attach log scanners
		var wg sync.WaitGroup
		wg.Add(2)
		go func(idx int, r io.ReadCloser) { defer wg.Done(); scanLines(r, &nodes[idx].stdout) }(i, stdOut)
		go func(idx int, r io.ReadCloser) { defer wg.Done(); scanLines(r, &nodes[idx].stderr) }(i, stdErr)
		nodes[i].cmd = cmd
		time.Sleep(150 * time.Millisecond)
	}
	defer func() {
		for _, n := range nodes {
			if n.cmd != nil && n.cmd.Process != nil {
				_ = n.cmd.Process.Kill()
				_, _ = n.cmd.Process.Wait()
			}
			if !cfg.KeepDirs {
				_ = os.RemoveAll(n.dir)
			} else {
				fmt.Printf("[keep-dirs] node%d dir: %s\n", n.id, n.dir)
			}
		}
	}()

	// wait for leader (status.json first), then verify via SET probe to ensure we talk to a writable node
	leader := -1
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for i := range nodes {
			b, err := os.ReadFile(nodes[i].statusPath)
			if err != nil {
				continue
			}
			var parsed struct {
				Nodes []struct {
					ID       int  `json:"id"`
					IsLeader bool `json:"is_leader"`
				} `json:"nodes"`
			}
			if json.Unmarshal(b, &parsed) != nil || len(parsed.Nodes) == 0 {
				continue
			}
			// find entry matching this node id
			for _, nd := range parsed.Nodes {
				if nd.ID == nodes[i].id && nd.IsLeader {
					leader = nodes[i].id
				}
			}
		}
		if leader != -1 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	// Probe fallback if status-based detection failed
	if leader == -1 {
		for i := range nodes {
			if probeWritable(nodes[i].clientPort) {
				leader = nodes[i].id
				fmt.Printf("[leader-detect] falling back to probe: node %d writable\n", leader)
				break
			}
		}
	}
	if leader == -1 {
		return trialResult{Trial: trial, Error: "no leader elected"}
	}

	// connect to leader
	lnode := nodes[leader-1]
	cli, err := dicedb.NewClient("127.0.0.1", lnode.clientPort)
	if err != nil {
		return trialResult{Trial: trial, Error: fmt.Sprintf("connect leader: %v", err)}
	}
	defer cli.Close()

	// run deterministic workload (simple mix of SET/GET)
	gPct, sPct := parseMix(cfg.WorkloadMix)
	r := fastRand(cfg.Seed + int64(trial))
	var setsOK, setsErr int
	// record workload SET commands for potential manual replay fallback if persistence missing
	type setCmd struct{ K, V string }
	var recordedSets []setCmd
	for i := 0; i < cfg.OpsPerTrial; i++ {
		op := pickOp(r, gPct, sPct)
		key := fmt.Sprintf("det:key:%d", int(r.Int63()%int64(cfg.Keyspace)))
		if op == "SET" {
			val := fmt.Sprintf("v%d", r.Int63())
			if err := fire(cli, "SET", key, val); err != nil {
				setsErr++
			} else {
				setsOK++
				recordedSets = append(recordedSets, setCmd{K: key, V: val})
			}
		} else {
			_ = fire(cli, "GET", key)
		}
	}
	fmt.Printf("[workload] SET ok=%d err=%d (ops=%d)\n", setsOK, setsErr, cfg.OpsPerTrial)
	if setsOK == 0 {
		// No successful mutations => early return with diagnostic
		return trialResult{Trial: trial, Error: "no SET succeeded", LeaderID: leader, ReplayLatencyMs: 0, NodeHashes: map[int]string{leader: ""}, WorkloadSetsOK: setsOK, WorkloadSetsErr: setsErr}
	}

	// pause for replication to settle
	time.Sleep(cfgSnapshotPause(cfg))

	// Establish leader baseline hash, then wait for all nodes to converge to it within SettleTimeout
	hashes := make(map[int]string)
	lhash, err := snapshotHash(lnode.clientPort)
	if err != nil {
		return trialResult{Trial: trial, Error: fmt.Sprintf("leader snapshot: %v", err)}
	}
	hashes[leader] = lhash
	deadline2 := time.Now().Add(cfg.SettleTimeout)
	converged := make(map[int]bool)
	converged[leader] = true
	for time.Now().Before(deadline2) {
		all := true
		for i := range nodes {
			if nodes[i].id == leader {
				continue
			}
			if converged[nodes[i].id] {
				continue
			}
			h, err := snapshotHash(nodes[i].clientPort)
			if err != nil {
				// keep trying
				all = false
				continue
			}
			hashes[nodes[i].id] = h
			if h == lhash {
				converged[nodes[i].id] = true
			} else {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	allEq := true
	for _, n := range nodes {
		if !converged[n.id] {
			// one last snapshot to record what it had
			if _, ok := hashes[n.id]; !ok {
				if h, e := snapshotHash(n.clientPort); e == nil {
					hashes[n.id] = h
				}
			}
			allEq = false
		}
	}

	// Measure replay latency by restarting the LEADER; record both hash and keys timing.
	// Capture baseline keys before killing leader (was missing earlier).
	var replayMs float64
	var replayKeysMs float64
	var hashMatched bool
	var keysMatched bool
	baseLeaderKeys, _ := keysList(lnode.clientPort)
	baseKeyCount := len(baseLeaderKeys)
	{
		v := &nodes[leader-1]
		// Baseline leader hash already captured in lhash; kill leader
		_ = v.cmd.Process.Kill()
		_, _ = v.cmd.Process.Wait()
		// restart leader with same flags (including advanced WAL ones) to force WAL replay
		args := []string{
			fmt.Sprintf("--port=%d", v.clientPort),
			"--raft-enabled=true",
			"--raft-engine=etcd",
			fmt.Sprintf("--raft-node-id=%d", v.id),
			fmt.Sprintf("--raft-listen-addr=:%d", v.raftPort),
			fmt.Sprintf("--raft-advertise-addr=127.0.0.1:%d", v.raftPort),
			"--raft-snapshot-threshold-entries=1000",
			fmt.Sprintf("--status-file-path=%s", v.statusPath),
			"--num-shards=1",
		}
		for _, p := range nodes {
			args = append(args, "--raft-nodes="+fmt.Sprintf("%d@127.0.0.1:%d", p.id, p.raftPort))
		}
		walDirV := filepath.Join(v.dir, cfg.WALDir)
		raftDirV := filepath.Join(v.dir, cfg.RaftPersistentDir)
		if cfg.EnableWALServer {
			args = append(args, "--enable-wal=true", fmt.Sprintf("--wal-dir=%s", walDirV))
		}
		if cfg.WALEnableDurableSet {
			args = append(args, "--wal-enable-durable-set=true")
		}
		if cfg.EnableWALShadow {
			args = append(args, "--enable-wal-shadow=true")
		}
		if cfg.WALShadowDir != "" {
			args = append(args, fmt.Sprintf("--wal-shadow-dir=%s", filepath.Join(v.dir, cfg.WALShadowDir)))
		}
		if cfg.WALPrimaryRead {
			args = append(args, "--wal-primary-read=true")
		}
		if cfg.WALStrictSync {
			args = append(args, "--wal-strict-sync=true")
		}
		args = append(args, fmt.Sprintf("--raft-persistent-dir=%s", raftDirV))
		start := time.Now()
		v.cmd = exec.Command(binPath, args...)
		v.cmd.Dir = v.dir
		if err := v.cmd.Start(); err != nil {
			return trialResult{Trial: trial, Error: fmt.Sprintf("restart leader: %v", err)}
		}
		defer func() { _ = v.cmd.Process.Kill(); _, _ = v.cmd.Process.Wait() }()
		// poll until hash matches previous leader snapshot (lhash)
		// base keys known: baseLeaderKeys
		for {
			if time.Since(start) > 20*time.Second {
				break
			}
			if !keysMatched {
				if k2, e := keysList(v.clientPort); e == nil {
					if equalStringSlices(k2, baseLeaderKeys) {
						keysMatched = true
						replayKeysMs = float64(time.Since(start)) / float64(time.Millisecond)
						fmt.Printf("[cluster-replay] keys matched at %.2fms (count=%d)\n", replayKeysMs, len(k2))
					} else if len(k2) == baseKeyCount && replayKeysMs == 0 { // count match fallback
						replayKeysMs = float64(time.Since(start)) / float64(time.Millisecond)
						fmt.Printf("[cluster-replay] key count matched (not order/content) at %.2fms (count=%d)\n", replayKeysMs, len(k2))
					}
				}
			}
			h, err := snapshotHash(v.clientPort)
			if err == nil && h == lhash {
				hashMatched = true
				replayMs = float64(time.Since(start)) / float64(time.Millisecond)
				fmt.Printf("[cluster-replay] hash matched at %.2fms\n", replayMs)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// ensure we at least record key latency if hash didn't match
	if replayMs == 0 && replayKeysMs > 0 {
		replayMs = replayKeysMs
	}
	tr := trialResult{
		Trial:               trial,
		ByteIdentical:       allEq,
		PlatformDivergent:   !allEq,
		ReplayLatencyMs:     replayMs,
		ReplayKeysLatencyMs: replayKeysMs,
		HashMatched:         hashMatched,
		KeysMatched:         keysMatched,
		LeaderID:            leader,
		NodeHashes:          hashes,
		WorkloadSetsOK:      setsOK,
		WorkloadSetsErr:     setsErr,
	}
	// Manual replay is now removed; persistence replay expected to hydrate state. ManualReplayUsed remains false.
	return tr
}

// snapshotHash computes a canonical hash of DB using KEYS * and DUMP per key (sorted by key)
func snapshotHash(port int) (string, error) {
	cli, err := dicedb.NewClient("127.0.0.1", port)
	if err != nil {
		return "", err
	}
	defer cli.Close()
	// KEYS *
	r := cli.Fire(&wire.Command{Cmd: "KEYS", Args: []string{"*"}})
	if r == nil || r.Status == wire.Status_ERR {
		return "", errors.New("KEYS failed")
	}
	keys := r.GetKEYSRes().Keys
	sort.Strings(keys)
	h := sha256.New()
	enc := func(s string) { _, _ = h.Write([]byte(s)); _, _ = h.Write([]byte("\n")) }
	for _, k := range keys {
		// TYPE
		tRes := cli.Fire(&wire.Command{Cmd: "TYPE", Args: []string{k}})
		tStr := "<ERRTYPE>"
		if tRes != nil && tRes.Status != wire.Status_ERR && tRes.GetTYPERes() != nil {
			tStr = tRes.GetTYPERes().Type
		}
		// DUMP
		dRes := cli.Fire(&wire.Command{Cmd: "DUMP", Args: []string{k}})
		dStr := "<ERRDUMP>"
		if dRes != nil && dRes.Status != wire.Status_ERR {
			dStr = dRes.GetMessage()
		}
		enc(k + "|" + tStr + "|" + dStr)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// keysList returns sorted KEYS * list for a node
func keysList(port int) ([]string, error) {
	cli, err := dicedb.NewClient("127.0.0.1", port)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	r := cli.Fire(&wire.Command{Cmd: "KEYS", Args: []string{"*"}})
	if r == nil || r.Status == wire.Status_ERR || r.GetKEYSRes() == nil {
		return nil, errors.New("KEYS failed")
	}
	keys := append([]string{}, r.GetKEYSRes().Keys...)
	sort.Strings(keys)
	return keys, nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// probeWritable attempts a lightweight SET/DEL cycle on a node to determine if it accepts writes
func probeWritable(port int) bool {
	cli, err := dicedb.NewClient("127.0.0.1", port)
	if err != nil {
		return false
	}
	defer cli.Close()
	k := "__probe__"
	if fire(cli, "SET", k, "v") != nil {
		return false
	}
	_ = fire(cli, "DEL", k) // ignore errors for DEL
	return true
}

// Minimal shared helpers (mirroring scripts/bench)
func pickOp(r *randSource, getPct, setPct int) string {
	if getPct+setPct <= 0 {
		return "GET"
	}
	x := int(r.Int63() % 100)
	if x < getPct {
		return "GET"
	}
	return "SET"
}

func parseMix(s string) (int, int) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 50, 50
	}
	var g, se int
	_, e1 := fmt.Sscanf(parts[0], "%d", &g)
	_, e2 := fmt.Sscanf(parts[1], "%d", &se)
	if e1 != nil || e2 != nil || g < 0 || se < 0 || g+se == 0 {
		return 50, 50
	}
	sum := g + se
	g = int(float64(g) / float64(sum) * 100.0)
	se = 100 - g
	return g, se
}

func cfgSnapshotPause(cfg determinismConfig) time.Duration {
	if cfg.SnapshotPause > 0 {
		return cfg.SnapshotPause
	}
	return 200 * time.Millisecond
}

func fire(c *dicedb.Client, cmd string, args ...string) error {
	// If this is a SET and durable token requested, append DURABLE
	up := strings.ToUpper(cmd)
	if up == "SET" && durableSetToken {
		args = append(args, "DURABLE")
	}
	wcmd := &wire.Command{Cmd: up, Args: args}
	r := c.Fire(wcmd)
	if r == nil {
		return fmt.Errorf("nil response")
	}
	if r.Status == wire.Status_ERR {
		return fmt.Errorf("ERR")
	}
	return nil
}

// ------- infra helpers ---------

func mustAllocPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func buildServerBinary() (string, error) {
	// Build root module as the sevendb server binary
	out := filepath.Join(os.TempDir(), fmt.Sprintf("sevendb-exp-%d", time.Now().UnixNano()))
	cmd := exec.Command("go", "build", "-o", out, "./")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	b, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build failed: %v\n%s", err, string(b))
	}
	return out, nil
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// writeLatencyCSV writes sorted replay latencies as a simple CSV: index,latency_ms
func writeLatencyCSV(path string, latencies []float64) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"index", "latency_ms"})
	sort.Float64s(latencies)
	for i, v := range latencies {
		_ = w.Write([]string{fmt.Sprintf("%d", i), fmt.Sprintf("%.3f", v)})
	}
	return w.Error()
}

func scanLines(r io.Reader, dst *strings.Builder) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		dst.WriteString(s.Text())
		dst.WriteByte('\n')
	}
}

// Simple fast RNG: splitmix64 style
type randSource struct{ x uint64 }

func fastRand(seed int64) *randSource { return &randSource{x: uint64(seed)} }
func (r *randSource) Int63() int64 {
	r.x += 0x9e3779b97f4a7c15
	z := r.x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return int64(z >> 1)
}

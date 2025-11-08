// Copyright (c) 2022-present, DiceDB/SevenDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

type benchConfig struct {
	Host             string
	Port             int
	Duration         time.Duration
	Warmup           time.Duration
	Conns            int
	Workers          int
	Keyspace         int
	ValueSize        int
	RatioGet         int
	RatioSet         int
	Command          string
	ReactiveProbe    bool
	ReactiveInterval time.Duration
	JSON             bool
	Seed             int64
}

type benchResults struct {
	StartTime        time.Time `json:"startTime"`
	EndTime          time.Time `json:"endTime"`
	DurationSeconds  float64   `json:"durationSeconds"`
	TotalOps         uint64    `json:"totalOps"`
	SuccessOps       uint64    `json:"successOps"`
	FailedOps        uint64    `json:"failedOps"`
	ThroughputOpsSec float64   `json:"throughputOpsSec"`
	LatencyP50Ms     float64   `json:"latencyP50Ms"`
	LatencyP95Ms     float64   `json:"latencyP95Ms"`
	LatencyP99Ms     float64   `json:"latencyP99Ms"`
	LatencyMaxMs     float64   `json:"latencyMaxMs"`
	ReactiveP50Ms    float64   `json:"reactiveP50Ms"`
	ReactiveP95Ms    float64   `json:"reactiveP95Ms"`
	ReactiveP99Ms    float64   `json:"reactiveP99Ms"`
	ReactiveMaxMs    float64   `json:"reactiveMaxMs"`
}

func main() {
	cfg := parseFlags()
	if cfg.Workers <= 0 {
		cfg.Workers = cfg.Conns
	}
	if cfg.Conns <= 0 {
		cfg.Conns = cfg.Workers
	}

	// Create clients (connections)
	clients := make([]*dicedb.Client, 0, cfg.Conns)
	for i := 0; i < cfg.Conns; i++ {
		c, err := dicedb.NewClient(cfg.Host, cfg.Port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect to %s:%d: %v\n", cfg.Host, cfg.Port, err)
			os.Exit(1)
		}
		clients = append(clients, c)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	// Keyspace and payload
	keys := make([]string, cfg.Keyspace)
	for i := 0; i < cfg.Keyspace; i++ {
		keys[i] = fmt.Sprintf("bench:key:%d", i)
	}
	value := strings.Repeat("x", cfg.ValueSize)

	// Preload a bit of data to make GET meaningful
	preloadN := min(cfg.Keyspace, cfg.Workers*10)
	for i := 0; i < preloadN; i++ {
		cli := clients[i%len(clients)]
		_ = fire(cli, "SET", keys[i], value)
	}

	// Warm-up
	if cfg.Warmup > 0 {
		warmupUntil := time.Now().Add(cfg.Warmup)
		var wgWarm sync.WaitGroup
		for w := 0; w < min(cfg.Workers, 8); w++ { // small warmup fanout
			wgWarm.Add(1)
			go func(id int) {
				defer wgWarm.Done()
				r := rand.New(rand.NewSource(cfg.Seed + int64(id)))
				for time.Now().Before(warmupUntil) {
					op := pickOp(r, cfg.RatioGet, cfg.RatioSet)
					key := keys[r.Intn(len(keys))]
					cli := clients[(id)%len(clients)]
					if op == "GET" {
						_ = fire(cli, "GET", key)
					} else {
						_ = fire(cli, "SET", key, value)
					}
				}
			}(w)
		}
		wgWarm.Wait()
	}

	var totalOps, successOps, failedOps uint64
	latencies := make(chan time.Duration, 1_000_000)
	var latWg sync.WaitGroup
	var latSlice []float64
	latWg.Add(1)
	go func() {
		defer latWg.Done()
		for d := range latencies {
			latSlice = append(latSlice, float64(d)/float64(time.Millisecond))
		}
	}()

	// Reactive probe measurements
	var reactiveLat []float64
	var reactiveMu sync.Mutex
	stopProbe := make(chan struct{})
	if cfg.ReactiveProbe {
		go func() {
			probeCli, err := dicedb.NewClient(cfg.Host, cfg.Port)
			if err != nil {
				return
			}
			defer probeCli.Close()
			ticker := time.NewTicker(cfg.ReactiveInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopProbe:
					return
				case <-ticker.C:
					t0 := time.Now()
					// Prefer PING; fallback to HELLO if PING unsupported
					err := fire(probeCli, "PING")
					if err != nil {
						// try HELLO
						_ = fire(probeCli, "HELLO")
					}
					rt := time.Since(t0)
					reactiveMu.Lock()
					reactiveLat = append(reactiveLat, float64(rt)/float64(time.Millisecond))
					reactiveMu.Unlock()
				}
			}
		}()
	}

	// Start workers
	end := time.Now().Add(cfg.Duration)
	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Use a per-worker RNG to avoid lock contention
			r := rand.New(rand.NewSource(cfg.Seed + 1000 + int64(id)))
			cli := clients[id%len(clients)]
			for time.Now().Before(end) {
				op := chooseCommand(cfg, r)
				key := keys[r.Intn(len(keys))]
				t0 := time.Now()
				var err error
				switch op {
				case "GET":
					err = fire(cli, "GET", key)
				case "SET":
					err = fire(cli, "SET", key, value)
				case "PING":
					err = fire(cli, "PING")
				default:
					err = fire(cli, op, key)
				}
				dt := time.Since(t0)
				atomic.AddUint64(&totalOps, 1)
				if err != nil {
					atomic.AddUint64(&failedOps, 1)
				} else {
					atomic.AddUint64(&successOps, 1)
				}
				latencies <- dt
			}
		}(w)
	}

	wg.Wait()
	close(latencies)
	latWg.Wait()
	close(stopProbe)

	// Summarize
	sort.Float64s(latSlice)
	var p50, p95, p99, pmax float64
	if len(latSlice) > 0 {
		p50 = percentile(latSlice, 50)
		p95 = percentile(latSlice, 95)
		p99 = percentile(latSlice, 99)
		pmax = latSlice[len(latSlice)-1]
	}

	reactiveMu.Lock()
	sort.Float64s(reactiveLat)
	var rp50, rp95, rp99, rpmax float64
	if len(reactiveLat) > 0 {
		rp50 = percentile(reactiveLat, 50)
		rp95 = percentile(reactiveLat, 95)
		rp99 = percentile(reactiveLat, 99)
		rpmax = reactiveLat[len(reactiveLat)-1]
	}
	reactiveMu.Unlock()

	dur := cfg.Duration.Seconds()
	res := benchResults{
		StartTime:        time.Now().Add(-cfg.Duration),
		EndTime:          time.Now(),
		DurationSeconds:  dur,
		TotalOps:         totalOps,
		SuccessOps:       successOps,
		FailedOps:        failedOps,
		ThroughputOpsSec: float64(successOps) / dur,
		LatencyP50Ms:     p50,
		LatencyP95Ms:     p95,
		LatencyP99Ms:     p99,
		LatencyMaxMs:     pmax,
		ReactiveP50Ms:    rp50,
		ReactiveP95Ms:    rp95,
		ReactiveP99Ms:    rp99,
		ReactiveMaxMs:    rpmax,
	}

	if cfg.JSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(res)
		return
	}

	// Human readable output
	fmt.Printf("SevenDB benchmark — %s\n", cfg.Command)
	fmt.Printf("Target: %s:%d, conns=%d, workers=%d, keyspace=%d, valueSize=%dB, mix=GET:%d/SET:%d\n",
		cfg.Host, cfg.Port, cfg.Conns, cfg.Workers, cfg.Keyspace, cfg.ValueSize, cfg.RatioGet, cfg.RatioSet)
	fmt.Printf("Warmup: %s, Duration: %s\n", cfg.Warmup, cfg.Duration)
	fmt.Printf("Ops: total=%d success=%d failed=%d\n", totalOps, successOps, failedOps)
	fmt.Printf("Throughput: %.0f ops/s\n", res.ThroughputOpsSec)
	fmt.Printf("Latency (ms): p50=%.3f p95=%.3f p99=%.3f max=%.3f\n", p50, p95, p99, pmax)
	if cfg.ReactiveProbe {
		fmt.Printf("Reactive latency (ms): p50=%.3f p95=%.3f p99=%.3f max=%.3f (interval=%s)\n", rp50, rp95, rp99, rpmax, cfg.ReactiveInterval)
	}
}

func parseFlags() benchConfig {
	var (
		host      = flag.String("host", "localhost", "server host")
		port      = flag.Int("port", 7379, "server port")
		duration  = flag.Duration("duration", 30*time.Second, "test duration")
		warmup    = flag.Duration("warmup", 5*time.Second, "warmup duration")
		conns     = flag.Int("conns", 16, "number of TCP connections")
		workers   = flag.Int("workers", 16, "number of concurrent workers (goroutines)")
		keyspace  = flag.Int("keyspace", 100_000, "number of distinct keys")
		vsize     = flag.Int("value-size", 16, "size of SET values in bytes")
		mix       = flag.String("mix", "50:50", "GET:SET percentage mix, e.g., 80:20")
		cmd       = flag.String("cmd", "GETSET", "command mix: GETSET | GET | SET | PING")
		reactive  = flag.Bool("reactive", true, "measure reactive latency with a low-frequency probe")
		rinterval = flag.Duration("reactive-interval", 100*time.Millisecond, "probe interval for reactive latency")
		jsonOut   = flag.Bool("json", false, "output results as JSON")
		seed      = flag.Int64("seed", time.Now().UnixNano(), "random seed")
	)
	flag.Parse()

	rg, rs := parseMix(*mix)
	return benchConfig{
		Host:             *host,
		Port:             *port,
		Duration:         *duration,
		Warmup:           *warmup,
		Conns:            *conns,
		Workers:          *workers,
		Keyspace:         *keyspace,
		ValueSize:        *vsize,
		RatioGet:         rg,
		RatioSet:         rs,
		Command:          strings.ToUpper(*cmd),
		ReactiveProbe:    *reactive,
		ReactiveInterval: *rinterval,
		JSON:             *jsonOut,
		Seed:             *seed,
	}
}

func parseMix(s string) (getPct, setPct int) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 50, 50
	}
	var g, se int
	_, err1 := fmt.Sscanf(parts[0], "%d", &g)
	_, err2 := fmt.Sscanf(parts[1], "%d", &se)
	if err1 != nil || err2 != nil || g < 0 || se < 0 || g+se == 0 {
		return 50, 50
	}
	// normalize to 0..100
	sum := g + se
	g = int(float64(g) / float64(sum) * 100.0)
	se = 100 - g
	return g, se
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
	hi := min(lo+1, len(sorted)-1)
	frac := rank - float64(lo)
	return sorted[lo] + (sorted[hi]-sorted[lo])*frac
}

func pickOp(r *rand.Rand, getPct, setPct int) string {
	if getPct+setPct <= 0 {
		return "GET"
	}
	x := r.Intn(100)
	if x < getPct {
		return "GET"
	}
	return "SET"
}

func chooseCommand(cfg benchConfig, r *rand.Rand) string {
	switch cfg.Command {
	case "GET":
		return "GET"
	case "SET":
		return "SET"
	case "PING":
		return "PING"
	case "GETSET":
		fallthrough
	default:
		return pickOp(r, cfg.RatioGet, cfg.RatioSet)
	}
}

func fire(c *dicedb.Client, cmd string, args ...string) error {
	// Fast-path specific helpers when available
	switch strings.ToUpper(cmd) {
	case "PING":
		resp := c.FireString("PING")
		if resp == nil {
			return fmt.Errorf("nil response")
		}
		return nil
	default:
		// Generic path via wire.Command
		wcmd := &wire.Command{Cmd: strings.ToUpper(cmd), Args: args}
		r := c.Fire(wcmd)
		if r == nil {
			return fmt.Errorf("nil response")
		}
		return nil
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

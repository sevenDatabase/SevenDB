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

// Optional operation timeout used only for reactive features to avoid hangs
var opTimeout time.Duration

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
	// Emission latency (watch) benchmark – opt-in and cautious by default
	ReactiveBench    bool
	ReactiveWatchers int
	ReactiveKeyspace int
	OpTimeout        time.Duration
	EmitMaxInflight  int
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
	// Emission (watch) latency summary
	EmitCount uint64  `json:"emitCount"`
	EmitP50Ms float64 `json:"emitP50Ms"`
	EmitP95Ms float64 `json:"emitP95Ms"`
	EmitP99Ms float64 `json:"emitP99Ms"`
	EmitMaxMs float64 `json:"emitMaxMs"`
}

func main() {
	cfg := parseFlags()
	// set global op timeout used by reactive components
	opTimeout = cfg.OpTimeout
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
					err := fireWithTimeout(probeCli, "PING")
					if err != nil {
						// try HELLO
						_ = fireWithTimeout(probeCli, "HELLO")
					}
					rt := time.Since(t0)
					reactiveMu.Lock()
					reactiveLat = append(reactiveLat, float64(rt)/float64(time.Millisecond))
					reactiveMu.Unlock()
				}
			}
		}()
	}

	// Emission (GET.WATCH) benchmark setup (opt-in and guarded)
	var (
		watcherStop   = make(chan struct{})
		watcherClients []*dicedb.Client
		emitLatCh     = make(chan float64, 100_000)
		emitLatSlice  []float64
		emitWg        sync.WaitGroup
		issueTimes   sync.Map // token(string) -> time.Time
		inflightCnt  uint64   // number of outstanding tokens to cap memory
		tokenSeq     uint64   // monotonic token sequence
		reactiveKeys []string
	)
	// collector for emission latencies
	emitWg.Add(1)
	go func() {
		defer emitWg.Done()
		for v := range emitLatCh {
			emitLatSlice = append(emitLatSlice, v)
		}
	}()

	if cfg.ReactiveBench {
		// prepare keys to watch
		ks := cfg.ReactiveKeyspace
		if ks <= 0 {
			ks = min(10_000, cfg.Keyspace)
			if ks <= 0 {
				ks = 10_000
			}
		}
		reactiveKeys = make([]string, ks)
		for i := 0; i < ks; i++ {
			reactiveKeys[i] = fmt.Sprintf("bench:watch:key:%d", i)
		}
		wcnt := cfg.ReactiveWatchers
		if wcnt <= 0 {
			wcnt = 1
		}
		watcherClients = make([]*dicedb.Client, 0, wcnt)
		// launch watchers
		for w := 0; w < wcnt; w++ {
			cli, err := createClientWithTimeout(cfg.Host, cfg.Port, opTimeout)
			if err != nil {
				continue
			}
			watcherClients = append(watcherClients, cli)
			// subscribe to a stripe of keys
			for k := w; k < len(reactiveKeys); k += wcnt {
				_ = fireWithTimeout(cli, "GET.WATCH", reactiveKeys[k])
			}
			ch, err := cli.WatchCh()
			if err != nil || ch == nil {
				continue
			}
			go func(wid int, wch <-chan *wire.Result, c *dicedb.Client) {
				for {
					select {
					case <-watcherStop:
						return
					case msg, ok := <-wch:
						if !ok || msg == nil {
							return
						}
						if msg.Status == wire.Status_ERR {
							continue
						}
						res := msg.GetGETRes()
						if res == nil || res.Value == "" {
							continue
						}
						parts := strings.SplitN(res.Value, "|", 2)
						if len(parts) == 0 || !strings.HasPrefix(parts[0], "tk=") {
							continue
						}
						if v, ok := issueTimes.LoadAndDelete(parts[0]); ok {
							t0 := v.(time.Time)
							dtMs := float64(time.Since(t0)) / float64(time.Millisecond)
							emitLatCh <- dtMs
							atomic.AddUint64(&inflightCnt, ^uint64(0)) // decrement
						}
					}
				}
			}(w, ch, cli)
		}
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
				// if emission benchmark enabled, override to SET on reactive keys
				if cfg.ReactiveBench && len(reactiveKeys) > 0 {
					op = "SET"
					key = reactiveKeys[r.Intn(len(reactiveKeys))]
				}
				t0 := time.Now()
				var err error
				switch op {
				case "GET":
					err = fire(cli, "GET", key)
				case "SET":
					if cfg.ReactiveBench {
						// cap inflight tokens to avoid unbounded memory
						if max := cfg.EmitMaxInflight; max > 0 && int(atomic.LoadUint64(&inflightCnt)) >= max {
							// skip tokenization; plain SET
							err = fireWithTimeout(cli, "SET", key, value)
							break
						}
						seq := atomic.AddUint64(&tokenSeq, 1)
						tok := fmt.Sprintf("tk=%d", seq)
						padded := value
						if len(padded)+len(tok)+1 < cfg.ValueSize {
							pad := strings.Repeat("x", cfg.ValueSize-(len(tok)+1))
							padded = pad
						}
						val := tok + "|" + padded
						issueTimes.Store(tok, t0)
						atomic.AddUint64(&inflightCnt, 1)
						err = fireWithTimeout(cli, "SET", key, val)
					} else {
						err = fire(cli, "SET", key, value)
					}
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
	close(watcherStop)
	// close watcher clients to unblock watch channels
	for _, wc := range watcherClients {
		wc.Close()
	}
	close(emitLatCh)
	emitWg.Wait()

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

	// Emission summary
	sort.Float64s(emitLatSlice)
	var ep50, ep95, ep99, emax float64
	if ln := len(emitLatSlice); ln > 0 {
		ep50 = percentile(emitLatSlice, 50)
		ep95 = percentile(emitLatSlice, 95)
		ep99 = percentile(emitLatSlice, 99)
		emax = emitLatSlice[ln-1]
	}

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
		EmitCount:        uint64(len(emitLatSlice)),
		EmitP50Ms:        ep50,
		EmitP95Ms:        ep95,
		EmitP99Ms:        ep99,
		EmitMaxMs:        emax,
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
		reactBench = flag.Bool("reactive-bench", false, "enable emission latency benchmark using GET.WATCH + SET (cautious, opt-in)")
		rwatchers  = flag.Int("reactive-watchers", 1, "number of watcher clients to subscribe to keys")
		rkeys      = flag.Int("reactive-keyspace", 10000, "number of keys to watch for emission benchmarking")
		opto       = flag.Duration("op-timeout", 5*time.Second, "operation timeout for reactive client connect/fire (0 to disable)")
		emaxflight = flag.Int("emit-max-inflight", 10000, "max in-flight tokenized SETs to cap memory; excess SETs are untracked")
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
		ReactiveBench:    *reactBench,
		ReactiveWatchers: *rwatchers,
		ReactiveKeyspace: *rkeys,
		OpTimeout:        *opto,
		EmitMaxInflight:  *emaxflight,
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

// fireWithTimeout executes a command with an optional timeout (opTimeout). If opTimeout is 0, it falls back to fire().
func fireWithTimeout(c *dicedb.Client, cmd string, args ...string) error {
	if opTimeout <= 0 {
		return fire(c, cmd, args...)
	}
	done := make(chan error, 1)
	go func() {
		done <- fire(c, cmd, args...)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(opTimeout):
		return fmt.Errorf("operation timeout after %s", opTimeout)
	}
}

// createClientWithTimeout establishes a client connection honoring opTimeout.
func createClientWithTimeout(host string, port int, timeout time.Duration) (*dicedb.Client, error) {
	if timeout <= 0 {
		return dicedb.NewClient(host, port)
	}
	type res struct {
		c   *dicedb.Client
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := dicedb.NewClient(host, port)
		ch <- res{c: c, err: err}
	}()
	select {
	case r := <-ch:
		return r.c, r.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("connect timeout after %s", timeout)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

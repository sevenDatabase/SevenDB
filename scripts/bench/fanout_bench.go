package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"os"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

func createClientWithTimeout(host string, port int, timeout time.Duration) (*dicedb.Client, error) {
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
		return nil, fmt.Errorf("connect timeout")
	}
}

type Result struct {
	N                int     `json:"n"`
	ThroughputOpsSec float64 `json:"throughputOpsSec"`
	LatencyP50Ms     float64 `json:"latencyP50Ms"`
	LatencyP95Ms     float64 `json:"latencyP95Ms"`
	LatencyP99Ms     float64 `json:"latencyP99Ms"`
	LatencyMaxMs     float64 `json:"latencyMaxMs"`
}

func main() {
	var (
		host     = flag.String("host", "localhost", "server host")
		port     = flag.Int("port", 7379, "server port")
		duration = flag.Duration("duration", 10*time.Second, "test duration per N")
		payload  = flag.Int("payload", 16, "payload size")
	)
	flag.Parse()

	scenarios := []int{1, 10, 100, 1000, 10000}
	var results []Result

	fmt.Printf("Running Fan-Out Benchmark (Host: %s:%d, Duration: %s, Payload: %d)\n", *host, *port, *duration, *payload)

	for _, n := range scenarios {
		fmt.Printf("Benchmarking N=%d watchers...\n", n)
		res := runBenchmark(*host, *port, *duration, *payload, n)
		results = append(results, res)
		fmt.Printf("  -> Throughput: %.2f ops/sec\n", res.ThroughputOpsSec)
		fmt.Printf("  -> Latency: p50=%.2fms, p95=%.2fms, p99=%.2fms\n", res.LatencyP50Ms, res.LatencyP95Ms, res.LatencyP99Ms)
		// Cool down
		time.Sleep(1 * time.Second)
	}

	// Final JSON output
	// Final JSON output
	f, err := os.Create("results.json")
	if err != nil {
		log.Fatalf("Failed to create results file: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	fmt.Println("\nWriting results to results.json")
	_ = enc.Encode(results)
}

func runBenchmark(host string, port int, duration time.Duration, payloadSize int, nWatchers int) Result {
	// 1. Setup Watchers
	watchers := make([]*dicedb.Client, nWatchers)
	readyWg := sync.WaitGroup{}
	readyWg.Add(nWatchers)
	doneWg := sync.WaitGroup{}
	doneWg.Add(nWatchers)
	latCh := make(chan float64, 100000)
	key := fmt.Sprintf("bench:fanout:%d", nWatchers)

	// Channel to collect latencies
	// latCh := make(chan float64, 100000) // moved inside loop

	// Watcher routine
	watcherFunc := func(i int) {
		defer doneWg.Done()
		cli, err := createClientWithTimeout(host, port, 5*time.Second)
		if err != nil {
			log.Printf("Watcher %d connection failed: %v", i, err)
			readyWg.Done()
			return
		}
		if i%1000 == 0 {
			log.Printf("Watcher %d connected", i)
		}
		watchers[i] = cli
		// defer cli.Close() // Handled in main loop

		// Subscribe
		wcmd := &wire.Command{Cmd: "GET.WATCH", Args: []string{key}}
		if resp := cli.Fire(wcmd); resp == nil || resp.Status == wire.Status_ERR {
			log.Printf("Watcher %d subscribe failed", i)
			readyWg.Done()
			return
		}

		ch, err := cli.WatchCh()
		if err != nil {
			log.Printf("Watcher %d watch channel failed: %v", i, err)
			readyWg.Done()
			return
		}

		readyWg.Done()

		for msg := range ch {
			if msg.Status == wire.Status_ERR {
				continue
			}
			res := msg.GetGETRes()
			if res == nil || res.Value == "" {
				continue
			}
			// Parse timestamp from payload. Format: "ts=<nanos>|..."
			parts := strings.SplitN(res.Value, "|", 2)
			if len(parts) > 0 && strings.HasPrefix(parts[0], "ts=") {
				var ts int64
				fmt.Sscanf(parts[0], "ts=%d", &ts)
				if ts > 0 {
					sent := time.Unix(0, ts)
					lat := time.Since(sent).Seconds() * 1000.0 // ms
					// Non-blocking send to avoid stalling if channel full (though it shouldn't be with enough buffer)
					select {
					case latCh <- lat:
					default:
					}
				}
			}
		}
	}

	for i := 0; i < nWatchers; i++ {
		go watcherFunc(i)
	}

	readyWg.Wait()
	// Small pause to ensure subscriptions are active
	time.Sleep(500 * time.Millisecond)

	// 2. Setup Writer
	writer, err := dicedb.NewClient(host, port)
	if err != nil {
		log.Fatalf("Writer connection failed: %v", err)
	}
	defer writer.Close()

	// 3. Run Benchmark
	end := time.Now().Add(duration)
	var ops uint64

	payloadData := strings.Repeat("x", payloadSize)

	for time.Now().Before(end) {
		ts := time.Now().UnixNano()
		val := fmt.Sprintf("ts=%d|%s", ts, payloadData)
		wcmd := &wire.Command{Cmd: "SET", Args: []string{key, val}}
		if resp := writer.Fire(wcmd); resp == nil || resp.Status == wire.Status_ERR {
			log.Printf("Writer SET failed")
		}
		ops++
		// Throttle slightly if needed, but for stress test we go fast.
		// However, with 10k watchers and 1 writer in a tight loop, we might overload.
		// Let's yield a tiny bit to allow IO threads to breathe?
		// Actually, let's just go as fast as possible synchronously.
		// Maybe sleep 1ms?
		time.Sleep(1 * time.Millisecond)
	}

	// 4. Collect Results
	// Close all watchers to stop their loops
	for _, cli := range watchers {
		if cli != nil {
			cli.Close()
		}
	}
	// Wait for all watcher goroutines to exit
	doneWg.Wait()
	close(latCh)
	var latencies []float64
	for l := range latCh {
		latencies = append(latencies, l)
	}
	sort.Float64s(latencies)

	throughput := float64(ops) / duration.Seconds() // Writer throughput
	// Fan-out throughput would be ops * nWatchers

	// Metrics
	p50 := 0.0
	p95 := 0.0
	p99 := 0.0
	max := 0.0

	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
		max = latencies[len(latencies)-1]
	}

	return Result{
		N:                nWatchers,
		ThroughputOpsSec: throughput,
		LatencyP50Ms:     p50,
		LatencyP95Ms:     p95,
		LatencyP99Ms:     p99,
		LatencyMaxMs:     max,
	}
}

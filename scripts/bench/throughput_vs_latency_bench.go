package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

type ResultPoint struct {
	TargetThroughput float64 `json:"targetOpsSec"`
	ActualThroughput float64 `json:"actualOpsSec"`
	LatencyP50Ms     float64 `json:"latencyP50Ms"`
	LatencyP95Ms     float64 `json:"latencyP95Ms"`
	LatencyP99Ms     float64 `json:"latencyP99Ms"`
	LatencyMaxMs     float64 `json:"latencyMaxMs"`
}

func main() {
	var (
		host      = flag.String("host", "localhost", "server host")
		port      = flag.Int("port", 7379, "server port")
		startRate = flag.Int("start-rate", 1000, "starting requests per second")
		endRate   = flag.Int("end-rate", 50000, "maximum requests per second")
		stepRate  = flag.Int("step-rate", 5000, "increment of requests per second per step")
		duration  = flag.Duration("step-duration", 10*time.Second, "duration of each step")
		conns     = flag.Int("conns", 50, "number of concurrent connections")
		payload   = flag.Int("payload", 64, "payload size in bytes")
		cmdName   = flag.String("cmd", "SET", "command to use (SET, GET)")
	)
	flag.Parse()

	fmt.Printf("Throughput vs Latency Benchmark\n")
	fmt.Printf("Host: %s:%d\n", *host, *port)
	fmt.Printf("Rate: %d to %d, step %d\n", *startRate, *endRate, *stepRate)
	fmt.Printf("Conns: %d, Duration per step: %s, Payload: %dB, Cmd: %s\n", *conns, *duration, *payload, *cmdName)

	var curve []ResultPoint

	// Pre-create clients
	clients := make([]*dicedb.Client, *conns)
	for i := 0; i < *conns; i++ {
		c, err := dicedb.NewClient(*host, *port)
		if err != nil {
			log.Fatalf("Failed to connect client %d: %v", i, err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	// Payload setup
	value := strings.Repeat("x", *payload)
	keyPrefix := "bench:tl:"

	for rate := *startRate; rate <= *endRate; rate += *stepRate {
		fmt.Printf("Testing Target Rate: %d ops/sec...", rate)

		res := runStep(clients, rate, *duration, keyPrefix, value, *cmdName)
		curve = append(curve, res)

		fmt.Printf(" done. Actual: %.2f ops/sec, P99 Latency: %.2f ms\n", res.ActualThroughput, res.LatencyP99Ms)

		// Cool down slightly between steps
		time.Sleep(1 * time.Second)
	}

	// Output results
	f, err := os.Create("throughput_vs_latency.json")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(curve); err != nil {
		log.Fatalf("Failed to write JSON: %v", err)
	}
	fmt.Println("\nResults saved to throughput_vs_latency.json")
}

func runStep(clients []*dicedb.Client, targetTotalRate int, duration time.Duration, keyPrefix, value, cmdName string) ResultPoint {
	var wg sync.WaitGroup
	numClients := len(clients)
	ratePerClient := float64(targetTotalRate) / float64(numClients)

	// Collect latencies
	// Using a buffered channel might be slow for high throughput, but simpler for aggregation.
	// For very high rates, per-thread slices + merge is better.
	latencyCh := make(chan float64, 10000) // Buffer

	startSignal := make(chan struct{})

	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(id int, c *dicedb.Client) {
			defer wg.Done()

			// Simple Pacing
			// Interval between requests = 1 / ratePerClient
			// To avoid drift, we track expected shipment time.

			interval := time.Duration(float64(time.Second) / ratePerClient)
			if interval < 1 {
				interval = 1 // Prevent div by zero or too small
			}

			<-startSignal // Wait for start signal

			startTime := time.Now()
			endTime := startTime.Add(duration)

			ops := 0
			for {
				now := time.Now()
				if now.After(endTime) {
					break
				}

				// Calculate when next op should ideally happen
				// Expected time = startTime + ops * interval
				targetTime := startTime.Add(time.Duration(ops+1) * interval)

				if targetTime.After(now) {
					sleepDur := targetTime.Sub(now)
					time.Sleep(sleepDur)
				}

				opStart := time.Now()

				// Execute Op
				key := fmt.Sprintf("%s%d", keyPrefix, id) // Simple key strategy
				var err error
				if cmdName == "SET" {
					wcmd := &wire.Command{Cmd: "SET", Args: []string{key, value}}
					resp := c.Fire(wcmd)
					if resp == nil || resp.Status == wire.Status_ERR {
						err = fmt.Errorf("err")
					}
				} else {
					wcmd := &wire.Command{Cmd: "GET", Args: []string{key}}
					resp := c.Fire(wcmd)
					if resp == nil || resp.Status == wire.Status_ERR {
						err = fmt.Errorf("err")
					}
				}

				lat := float64(time.Since(opStart).Microseconds()) / 1000.0 // ms

				if err == nil {
					// Non-blocking send to avoid stalling on channel full
					select {
					case latencyCh <- lat:
					default:
					}
				}

				ops++
			}
		}(i, clients[i])
	}

	// Start collector
	doneCh := make(chan struct{})
	var latencies []float64
	go func() {
		for l := range latencyCh {
			latencies = append(latencies, l)
		}
		close(doneCh)
	}()

	// Start benchmark
	stepStart := time.Now()
	close(startSignal)
	wg.Wait()
	stepDuration := time.Since(stepStart).Seconds()

	close(latencyCh)
	<-doneCh

	sort.Float64s(latencies)

	totalOps := len(latencies)
	throughput := float64(totalOps) / stepDuration

	p50, p95, p99, max := 0.0, 0.0, 0.0, 0.0
	if totalOps > 0 {
		p50 = latencies[totalOps*50/100]
		p95 = latencies[totalOps*95/100]
		p99 = latencies[totalOps*99/100]
		max = latencies[totalOps-1]
	}

	return ResultPoint{
		TargetThroughput: float64(targetTotalRate),
		ActualThroughput: throughput,
		LatencyP50Ms:     p50,
		LatencyP95Ms:     p95,
		LatencyP99Ms:     p99,
		LatencyMaxMs:     max,
	}
}

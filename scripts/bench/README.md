# SevenDB Benchmarks

This directory contains benchmarks for SevenDB covering:

1. **Throughput benchmark** (`main.go`) - Basic ops/sec, latency distribution, emission latency
2. **Failover benchmark** (`failover_bench.go`) - Raft leader failover detection and recovery time
3. **Reconnection benchmark** (`reconnect_bench.go`) - Subscription reconnection latency
4. **Crash recovery benchmark** (`crash_recovery_bench.go`) - Validates no duplicates, no loss after crashes

---

## 1. Throughput Benchmark (main.go)

This is a load generator for SevenDB that measures:

- Throughput (ops/sec)
- End-to-end request latency distribution (p50/p95/p99/max in ms)
- Reactive latency (low-frequency probe latency to reflect user-perceived responsiveness under load)

It uses the official Go client (`github.com/dicedb/dicedb-go`) and supports configurable
connections, workers, keyspace size, value size, GET/SET mix, JSON output, a reactive **probe** (periodic PING), an **opt-in emission latency benchmark** (SET → GET.WATCH delivery), and an **opt-in per-command durability mode** that appends `DURABLE` to every `SET`.

## Quick start

1. Ensure a SevenDB server is running (default is `localhost:7379`). You can start it with:

```bash
make run
```

2. Run the benchmark (defaults shown below):

```bash
# 30s, 16 conns, 16 workers, 100k keyspace, value size 16B, GET:SET=50:50
# reactive probe every 100ms (emission benchmark is OFF by default)
GOFLAGS="" go run ./scripts/bench/main.go \
  -host localhost \
  -port 7379 \
  -duration 30s \
  -warmup 5s \
  -conns 16 \
  -workers 16 \
  -keyspace 100000 \
  -value-size 16 \
  -mix 50:50 \
  -cmd GETSET \
  -reactive=true \
  -reactive-interval 100ms
```

You should see a summary like:

```
SevenDB benchmark — GETSET
Target: localhost:7379, conns=16, workers=16, keyspace=100000, valueSize=16B, mix=GET:50/SET:50
Warmup: 5s, Duration: 30s
Ops: total=123456 success=123456 failed=0
Throughput: 41234 ops/s
Latency (ms): p50=0.210 p95=0.900 p99=1.600 max=5.200
Reactive latency (ms): p50=0.250 p95=0.850 p99=1.500 max=4.900 (interval=100ms)

If run with `-durable-set`, the header will be annotated and a note added:

```

SevenDB benchmark — GETSET (DURABLE)
...
Note: SET operations were issued with DURABLE for synchronous WAL flush+fsync.

```

```

To emit machine-readable JSON:

```bash
go run ./scripts/bench/main.go -duration 15s -json
```

## Useful scenarios

- Read-heavy:

```bash
go run ./scripts/bench/main.go -duration 30s -mix 90:10
```

- Write-heavy, larger values:

```bash
go run ./scripts/bench/main.go -duration 30s -mix 10:90 -value-size 256
```

- GET only / SET only / PING only:

```bash
# GET only
go run ./scripts/bench/main.go -cmd GET -duration 20s
# SET only
go run ./scripts/bench/main.go -cmd SET -duration 20s
# PING only (baseline/reactive)
go run ./scripts/bench/main.go -cmd PING -duration 10s
```

- Increase parallelism until saturation:

```bash
# scale connections and workers
go run ./scripts/bench/main.go -duration 20s -conns 64 -workers 256 -mix 80:20
```

## Flags

- `-host` (string): server host (default `localhost`)
- `-port` (int): server port (default `7379`)
- `-duration` (duration): test duration (default `30s`)
- `-warmup` (duration): warmup duration (default `5s`)
- `-conns` (int): number of TCP connections (default `16`)
- `-workers` (int): concurrent workers/goroutines (default `16`)
- `-keyspace` (int): number of distinct keys (default `100000`)
- `-value-size` (int): SET value size (bytes, default `16`)
- `-mix` (string): GET:SET ratio, e.g. `80:20` (default `50:50`)
- `-cmd` (string): command mix: `GETSET` | `GET` | `SET` | `PING` (default `GETSET`)
- `-durable-set` (bool): append `DURABLE` to every `SET` to force synchronous WAL flush+fsync (requires server started with `--wal-enable-durable-set=true` and WAL enabled)
- `-reactive` (bool): measure reactive latency with periodic probe (default `true`)
- `-reactive-interval` (duration): probe interval (default `100ms`)
- `-reactive-bench` (bool): emission latency (SET→watch) benchmark (default `false` – enable cautiously)
- `-reactive-watchers` (int): watcher clients for emission benchmark (default `1`)
- `-reactive-keyspace` (int): number of watch keys (default `10000`)
- `-emit-max-inflight` (int): cap tracked SET tokens to bound memory (default `10000`)
- `-op-timeout` (duration): connect & operation timeout for reactive benchmark (default `5s`)
- `-json` (bool): print JSON results (default `false`)
- `-seed` (int64): random seed (default: current time)

## Notes

- Reactive probe latency uses a dedicated client sending `PING` at a fixed interval, running alongside the main load, to approximate perceived responsiveness during traffic spikes.
- Emission benchmark (when enabled) spawns watcher clients subscribing with `GET.WATCH` to a stripe of keys and measures end-to-end latency from issuing a tokenized `SET` to receiving its watch emission.
- Emission benchmark is opt-in because it adds overhead and can increase memory usage if not bounded; use `-emit-max-inflight` to cap outstanding tokenized writes.
- If `PING` is unsupported, the bench falls back to `HELLO` for the probe.
- If `PING` is unsupported, the bench falls back to `HELLO` for the probe.
- Keyspace is preloaded a bit during warmup so GETs are meaningful.
- For consistent runs, fix `-seed` and pin CPU scaling on your test box.

---

If you prefer to ship this inside `sevendb-cli` later, this tool is already self-contained and can be ported with minimal changes (keep the flags and metrics output). For now it lives under `scripts/bench` for convenience.

## Emission + DURABLE benchmark example

```bash
go run ./scripts/bench/main.go \
  -duration 60s -warmup 10s \
  -reactive-bench \
  -reactive-watchers 2 \
  -reactive-keyspace 20000 \
  -emit-max-inflight 15000 \
  -durable-set \
  -op-timeout 5s \
  -json
```

Sample (truncated) JSON fields for emission metrics (with durability flag included):

```json
{
  "emitCount": 598234,
  "emitP50Ms": 1.12,
  "emitP95Ms": 3.87,
  "emitP99Ms": 6.54,
  "emitMaxMs": 18.31,
  "durableSet": true
}
```

---

## 2. Failover Benchmark (failover_bench.go)

Measures raft leader failover time including:
- **Detection time**: Time for followers to detect leader failure
- **Election time**: Time to elect a new leader
- **Total failover time**: End-to-end unavailability window

### Prerequisites

1. Build the SevenDB binary:
```bash
make build
```

### Running the benchmark

```bash
go run ./scripts/bench/failover_bench.go \
  --binary ./sevendb \
  --iterations 5 \
  --heartbeat-ms 100 \
  --election-ms 1000 \
  --warmup-writes 100
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--binary` | (required) | Path to sevendb binary |
| `--base-dir` | `.failover-bench` | Base directory for node data |
| `--iterations` | 5 | Number of failover iterations |
| `--heartbeat-ms` | 100 | Raft heartbeat interval (ms) |
| `--election-ms` | 1000 | Raft election timeout (ms) |
| `--snap-threshold` | 1000 | Snapshot threshold entries |
| `--base-client-port` | 17379 | Base client port for nodes |
| `--base-raft-port` | 17091 | Base raft port for nodes |
| `--warmup-writes` | 100 | Number of writes before failover |
| `--stabilize-wait` | 3s | Wait time for cluster stabilization |
| `--leader-poll` | 50ms | Poll interval for leader check |
| `--max-leader-wait` | 30s | Max time to wait for new leader |
| `--json` | false | Output results as JSON |

### Sample output

```
=== Failover Benchmark Summary ===
Iterations: 5
Raft Config: heartbeat=100ms, election=1000ms

Detection Time (ms):
  p50=152.34 p95=198.45 p99=210.12 avg=165.23

Election Time (ms):
  p50=320.12 p95=450.34 p99=512.67 avg=355.89

Total Failover Time (ms):
  p50=472.46 p95=648.79 p99=722.79 avg=521.12
```

---

## 3. Subscription Reconnection Benchmark (reconnect_bench.go)

Measures subscription reconnection latency:
- **Reconnection time**: TCP connection establishment time
- **Resume time**: EMITRECONNECT command latency
- **Data integrity**: Validates no missed or duplicate emissions

### Prerequisites

1. Start a SevenDB server with emission-contract enabled:
```bash
./sevendb --raft-enabled=true --emission-contract-enabled=true
```

### Running the benchmark

```bash
go run ./scripts/bench/reconnect_bench.go \
  --host localhost \
  --port 7379 \
  --iterations 10 \
  --warmup-emissions 5
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | localhost | Server host |
| `--port` | 7379 | Server port |
| `--iterations` | 10 | Number of reconnect iterations |
| `--warmup-emissions` | 5 | Emissions before disconnect |
| `--poll-interval` | 50ms | Emission poll interval |
| `--max-wait` | 5s | Max wait for emission |
| `--client-prefix` | reconnect-bench | Client ID prefix |
| `--json` | false | Output results as JSON |

### Sample output

```
=== Subscription Reconnection Benchmark Summary ===
Target: localhost:7379
Iterations: 10
Warmup emissions per iteration: 5

Reconnection Time (TCP connect, ms):
  p50=0.85 p95=1.23 p99=1.45 avg=0.92

Resume Time (EMITRECONNECT, ms):
  p50=0.34 p95=0.56 p99=0.78 avg=0.41

Total Reconnect+Resume Time (ms):
  p50=1.19 p95=1.79 p99=2.23

Data Integrity:
  Total missed emissions: 0
  Total duplicate emissions: 0
```

---

## 4. Crash Recovery Benchmark (crash_recovery_bench.go)

Validates crash recovery guarantees:
- **Exactly-once**: No duplicates AND no loss
- **At-least-once**: No loss (duplicates OK)
- **At-most-once**: No duplicates (loss OK)

### Scenarios

| Scenario | Description |
|----------|-------------|
| `client` | Simulates client crash/disconnect during emission reception |
| `server` | Simulates server crash during emission (requires --binary) |
| `both` | Combination of client and server crash scenarios |

### Prerequisites

For client crash scenario:
```bash
./sevendb --raft-enabled=true --emission-contract-enabled=true
```

For server crash scenario (needs binary path):
```bash
make build
```

### Running the benchmark

```bash
# Client crash scenario (most common)
go run ./scripts/bench/crash_recovery_bench.go \
  --host localhost \
  --port 7379 \
  --scenario client \
  --iterations 5 \
  --updates 100

# Server crash scenario
go run ./scripts/bench/crash_recovery_bench.go \
  --binary ./sevendb \
  --scenario server \
  --iterations 5 \
  --updates 100
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | localhost | Server host |
| `--port` | 7379 | Server port |
| `--binary` | | Path to sevendb binary (required for server scenario) |
| `--base-dir` | `.crash-recovery-bench` | Base directory for node data |
| `--iterations` | 5 | Number of crash recovery iterations |
| `--updates` | 100 | Updates per iteration |
| `--scenario` | client | Crash scenario: client, server, both |
| `--poll-interval` | 20ms | Emission poll interval |
| `--max-wait` | 10s | Max wait for emission |
| `--stabilize-wait` | 2s | Wait for server to stabilize |
| `--json` | false | Output results as JSON |

### Sample output

```
=== Crash Recovery Benchmark Summary ===
Scenario: client
Target: localhost:7379
Iterations: 5
Total updates: 500

--- Delivery Guarantees ---
Exactly-once rate: 100.0% (5/5 iterations with no duplicates and no loss)
At-least-once rate: 100.0% (5/5 iterations with no loss)
At-most-once rate: 100.0% (5/5 iterations with no duplicates)

--- Data Integrity ---
Total duplicates: 0
Total missed: 0

--- Recovery Time (ms) ---
  p50=45.23 p95=78.45 p99=92.34 avg=52.67
```

---

## Running All Benchmarks

Here's a script to run all benchmarks in sequence:

```bash
#!/bin/bash
set -e

# Build
make build

# Start server for benchmarks that need a running server
./sevendb --raft-enabled=true --emission-contract-enabled=true &
SERVER_PID=$!
sleep 3

# 1. Throughput benchmark
echo "=== Running throughput benchmark ==="
go run ./scripts/bench/main.go -duration 30s -json > results_throughput.json

# 2. Reconnection benchmark  
echo "=== Running reconnection benchmark ==="
go run ./scripts/bench/reconnect_bench.go --iterations 10 --json > results_reconnect.json

# 3. Crash recovery (client scenario)
echo "=== Running crash recovery benchmark (client) ==="
go run ./scripts/bench/crash_recovery_bench.go --scenario client --iterations 5 --json > results_crash_client.json

# Stop server
kill $SERVER_PID 2>/dev/null || true

# 4. Failover benchmark (manages its own cluster)
echo "=== Running failover benchmark ==="
go run ./scripts/bench/failover_bench.go --binary ./sevendb --iterations 5 --json > results_failover.json

# 5. Crash recovery (server scenario)
echo "=== Running crash recovery benchmark (server) ==="
go run ./scripts/bench/crash_recovery_bench.go --binary ./sevendb --scenario server --iterations 5 --json > results_crash_server.json

echo "All benchmarks complete. Results saved to results_*.json"
```

---

## Original Throughput Benchmark Details

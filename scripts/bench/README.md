# SevenDB benchmark (throughput and reactive latency)

This is a small load generator for SevenDB that measures:

- Throughput (ops/sec)
- End-to-end request latency distribution (p50/p95/p99/max in ms)
- Reactive latency (low-frequency probe latency to reflect user-perceived responsiveness under load)

It uses the official Go client (`github.com/dicedb/dicedb-go`) and supports configurable
connections, workers, keyspace size, value size, GET/SET mix, and JSON output.

## Quick start

1. Ensure a SevenDB server is running (default is `localhost:7379`). You can start it with:

```bash
make run
```

2. Run the benchmark (defaults shown below):

```bash
# 30s, 16 conns, 16 workers, 100k keyspace, value size 16B, GET:SET=50:50
# reactive probe every 100ms
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
- `-reactive` (bool): measure reactive latency with periodic probe (default `true`)
- `-reactive-interval` (duration): probe interval (default `100ms`)
- `-json` (bool): print JSON results (default `false`)
- `-seed` (int64): random seed (default: current time)

## Notes

- Reactive latency uses a dedicated client sending `PING` at a fixed interval, running alongside the main load, to approximate perceived responsiveness during traffic spikes.
- If `PING` is unsupported, the bench falls back to `HELLO` for the probe.
- Keyspace is preloaded a bit during warmup so GETs are meaningful.
- For consistent runs, fix `-seed` and pin CPU scaling on your test box.

---

If you prefer to ship this inside `sevendb-cli` later, this tool is already self-contained and can be ported with minimal changes (keep the flags and metrics output). For now it lives under `scripts/bench` for convenience.

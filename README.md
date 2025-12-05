# <img src="logo.png" alt="SevenDB Logo" width="40"/> SevenDB

**SevenDB** is a **reactive database**, building on [DiceDB](https://github.com/DiceDB/dice) and extending it with a stronger foundation for **deterministic subscriptions, bucket-based sharding, and compute scalability**.

---

## Vision

SevenDB aims to be the **foundation for reactive applications at scale** — where subscriptions are as reliable as reads and as scalable as writes.

---

## Benchmarks

you can run this using `make bench` , here are the single-machine results on my ryzen-7 5700

```bash
SevenDB benchmark — GETSET
Target: localhost:7379, conns=16, workers=16, keyspace=100000, valueSize=16B, mix=GET:50/SET:50
Warmup: 5s, Duration: 30s
Ops: total=3695354 success=3695354 failed=0
Throughput: 123178 ops/s
Latency (ms): p50=0.111 p95=0.226 p99=0.349 max=15.663
Reactive latency (ms): p50=0.145 p95=0.358 p99=0.988 max=7.979 (interval=100ms)
```


---

## How to get sevenDB running

SevenDB speaks the same **RESP protocol** as Redis.
That means you can connect to it with any existing Redis client (`redis-cli`, `ioredis`, `redis-py`, etc.) and use standard commands, with extra power from **reactive subscriptions**.

### 1. Run the server

Clone and build SevenDB:

```bash
git clone https://github.com/sevenDatabase/SevenDB.git
cd sevendb

make build # builds ./sevendb executable
or
go run main.go #run like a normal go program
```

By default, the server listens on `localhost:7379`.

### 2. Connect with a SevenDB-cli

Best way to connect to sevenDB is through `SevenDB-cli`

```bash
git clone https://github.com/sevenDatabase/SevenDB-cli
cd SevenDB-cli
make build
./sevendb-cli  # ensure the sevendb server is running first
```

### 3. Basic operations

SevenDB supports familiar Redis-style commands:

```bash
> SET user:1 "Alice"
OK
> GET user:1
"Alice"
```

### 4. Reactive subscriptions

Unlike Redis, `subscriptions` in SevenDB are first-class operations.

```bash
> GET.WATCH user:1
Subscribed to key [user:1]
```

Now, when `user:1` changes:

```bash
> SET user:1 "Bob"
```

Your `subscription` immediately receives:

```bash
user:1 -> "Bob"
```

#### Enable the Emission Contract (feature flag)

Some advanced features, like durable outbox delivery and `EMITRECONNECT`, require the Emission Contract to be enabled.

- CLI flag:

  ```bash
  sevendb --emission-contract-enabled=true \
          --emission-notifier-poll-ms=5   # optional: tune notifier poll interval
  ```

- Config (`sevendb.yaml`):

  ```yaml
  emission-contract-enabled: true
  emission-notifier-poll-ms: 5  # optional
  ```

With this enabled, emissions are written to a raft-backed outbox and delivered deterministically by a per-bucket notifier.

### 5. Resuming after disconnect: EMITRECONNECT

### 5. Resuming after disconnect: EMITRECONNECT

If your client disconnects and later reconnects, you can resume emissions without gaps using `EMITRECONNECT`.

- Purpose: tell SevenDB the last commit index you fully processed for a given subscription, so the notifier can resume from the next index.
- Syntax: `EMITRECONNECT key sub_id last_commit_index`
- Returns:
  - `OK <next_index>` on success (resume from this next commit index)
  - `STALE_SEQUENCE` if the server has compacted past your index
  - `INVALID_SEQUENCE` if the provided index is invalid for this subscription
  - `SUBSCRIPTION_NOT_FOUND` if the subscription isn’t active for the key

Example (RESP/CLI style):

```bash
> EMITRECONNECT user:1 client123:987654321 42
OK 43
```

Notes:

- `sub_id` is the subscription identifier associated with your `GET.WATCH` (the client/fingerprint pair used by the server). If you’re using the official SevenDB client, it will surface this ID for reconnects.
- This feature is available when the Emission Contract is enabled; see the docs below for configuration and operational details.

## Metrics and Observability

SevenDB exposes lightweight emission metrics for visibility during development and ops.

- Per-shard/bucket breakdown: metrics are labeled by `bucket` (the internal shard ID) when the Emission Contract is enabled.
- Aggregate metrics are also exported for quick at-a-glance checks.

Options:

- Enable a Prometheus endpoint (off by default):

  ```bash
  sevendb \
    --emission-contract-enabled=true \
    --metrics-http-enabled=true \
    --metrics-http-addr=":9090"
  ```

  Then scrape `http://<host>:9090/metrics`. Example metric names:
  - `sevendb_emission_pending_entries{bucket="a"}`
  - `sevendb_emission_sends_per_sec{bucket="a"}`
  - `sevendb_emission_reconnects_total{bucket="a",outcome="ok|stale|invalid|not_found"}`

  Config (`sevendb.yaml`):

  ```yaml
  metrics-http-enabled: true
  metrics-http-addr: ":9090"
  ```

- Emit a compact log line every N seconds (off by default):

  ```bash
  sevendb --metrics-log-interval-sec=10
  ```

  You’ll see lines like:

  ```
  level=INFO msg=emission_metrics pending=0 subs=0 sends_per_sec=0 acks_per_sec=0 lat_ms_avg=0 reconnect_ok=0 reconnect_stale=0
  ```

Notes:
- Labeling per bucket enables per-shard analysis but adds minor bookkeeping overhead.
- The `/metrics` endpoint only includes SevenDB application metrics (no Go runtime by default) to keep output small and stable.

### WAL Durability (Per-Command)

SevenDB's WAL normally buffers writes and flushes/fsyncs on an interval (`--wal-buffer-sync-interval-ms`). For many workloads this amortizes IO and is sufficient. You can opt a specific `SET` write into **synchronous durability**:

1. Enable the feature flag and WAL at startup:

```bash
sevendb \
  --enable-wal=true \
  --wal-enable-durable-set=true \
  --wal-dir=logs
```

2. Issue a durable write:

```bash
SET mykey "value" DURABLE
# or
SET mykey "value" SYNC
```

Behavior:
- Without `DURABLE` / `SYNC`: SevenDB appends the command to the WAL buffer and replies `OK` (fsync happens later).
- With `DURABLE` / `SYNC`: SevenDB flushes and fsyncs the WAL segment before replying `OK`, ensuring the change is on disk (subject to filesystem semantics) when acknowledged.
- If the WAL is disabled or the feature flag is off, the tokens are ignored and the write behaves as buffered.
- On fsync failure, the server returns an `ERR wal sync failed: <reason>` instead of `OK`.

Metrics:
- `sevendb_set_durable_sync_total`: count of SETs that forced an immediate WAL sync.
- `sevendb_set_buffered_total`: count of SETs written without a durability request.

Use this for critical keys (e.g., transaction commits, idempotency markers) without globally forcing synchronous writes.

---

## Why SevenDB?

Traditional databases excel at storing and querying, but they treat _reactivity_ as an afterthought. Systems bolt on triggers, changefeeds, or pub/sub layers — often at the cost of correctness, scalability, or painful race conditions.

**SevenDB takes a different path: reactivity is core.**
We extend the excellent work of DiceDB with new primitives that make _subscriptions as fundamental as inserts and updates_.

---

## Design Plan

[**View the Design Plan PDF**](./docs/design-plan.pdf)

Additional docs:

- [WAL manifest, UWAL1, and integrity preamble](./docs/WAL_MANIFEST_AND_UWAL.md)
- [Raft testing: deterministic and multi-machine](./docs/TESTING_RAFT.md#real-world-multi-machine-testing)
- [Emission Contract architecture](./docs/src/content/docs/architecture/emission-contract.mdx)
- [Emission} Contract – operations & config](./docs/src/content/docs/emission-contract-ops.mdx)

## Benchmarks and Determinism

See docs: [Determinism: scope, harness, and how to run](./docs/DETERMINISM.md)

Quick runs:

```zsh
# Emission determinism (100-run cases)
go test ./internal/emission -run 'Determinism_Repeat100' -count=1

# WAL determinism (rotation and prune)
go test ./internal/raftwal -run 'Determinism_Repeat100' -count=1
```

Reconnect Benchmarks:

```zsh
go run ./scripts/bench/reconnect_bench.go --host localhost --port 7379 --iterations 1 --warmup-emissions 50
```

**Crash Recovery Benchmarks:**

Server Crash Scenario :
```zsh
  go run scripts/bench/crash_recovery_bench.go \ 
  --scenario=server \
  --binary=/home/blagden/Documents/sevenDB/sevendb \
  --iterations=5 \
  --updates=200
```

Client Crash Scenario:
```zsh
./sevendb --enable-wal --wal-enable-durable-set  # in another terminal
go run scripts/bench/crash_recovery_bench.go \
  --scenario=client \
  --iterations=5 \
  --updates=2

```
Failover Benchmarks:
```zsh
go run failover_bench.go -binary /home/blagden/Documents/sevenDB/sevendb -iterations 30
```

Throughput, Latency Benchmarks:
```zsh
make bench
```


## Core Concepts

### 1. Buckets

- Data is partitioned into **buckets**, each with its own Raft log, subscriptions, and notifier.
- A bucket is the atomic unit of replication, computation, and failover.(**Update**: Now Shards are the unit of replication , buckets as a raft node were too expensive)
- Subscriptions bind to the buckets that hold their data, ensuring updates are always ordered and consistent.

### 2. Compute Sharding

- Reactive computation is heavy — evaluating deltas and emitting them at scale.
- SevenDB spreads this load by **sharding compute across buckets**, with two modes:
  - **Hot mode**: every replica shadow-evaluates for instant failover.
  - **Cold mode**: only the notifier evaluates, others checkpoint for efficiency.
- This lets us trade failover speed vs CPU cost per bucket.

### 3. Deterministic Subscriptions

- `SUBSCRIBE` / `UNSUBSCRIBE` are **first-class operations**, logged like `INSERT` or `UPDATE`.
- Every subscription is replayable and deterministic, enforced by plan-hash checks.
- Failovers and replays always converge to the same state, eliminating divergence.

### 4. Durable Notifier Outbox

- Emissions aren’t ephemeral.
- Every computed delta is first written as an **outbox entry into the bucket log**, then sent.
- If a notifier crashes, the next one replays the outbox, guaranteeing _no lost updates_.
- Clients deduplicate using `(sub_id, emit_seq)` for “effective-once” delivery.

---


## What Makes Us Different from DiceDB

DiceDB gave us the foundation: a Redis-compatible, reactive engine with subscriptions.
SevenDB extends it with:

- **Buckets as the core sharding unit** (data + compute + subscriptions all scoped per bucket).
- **Epoch-aware sequencing (`emit_seq`)** for gap-free ordering across migrations.
- **Deterministic query plans** and plan-hash logging to prevent drift.
- **Notifier leases decoupled from Raft leadership**, avoiding leader hotspots.
- **Backpressure primitives** (ack/window modes, coalescing, priority queues).
- **Elasticity**: split hot buckets to scale out, merge cold ones to scale in.

In short: DiceDB rethought data reactivity. SevenDB rethinks _scalable, correct reactivity_.

---

## Status

🚧 **Early development** — expect rapid iteration.

# <img src="logo.png" alt="SevenDB Logo" width="40"/> SevenDB

**SevenDB** is a next-generation **reactive database**, building on [DiceDB](https://github.com/DiceDB/dice) and extending it with a stronger foundation for **deterministic subscriptions, bucket-based sharding, and compute scalability**.

---

## Vision

SevenDB aims to be the **foundation for reactive applications at scale** — where subscriptions are as reliable as reads and as scalable as writes. Imagine collaborative apps, trading systems, or IoT backends where every change is streamed consistently, with no hacks or bolt-ons.

We’re not just a database that can *react* — we’re building a database where reactivity, scalability and correctness is as fundamental as storage.

---

## Why SevenDB?

Traditional databases excel at storing and querying, but they treat *reactivity* as an afterthought. Systems bolt on triggers, changefeeds, or pub/sub layers — often at the cost of correctness, scalability, or painful race conditions.

**SevenDB takes a different path: reactivity is core.**
We extend the excellent work of DiceDB with new primitives that make *subscriptions as fundamental as inserts and updates*.

---

## Design Plan

[**View the Design Plan PDF**](./docs/design-plan.pdf)


## Core Concepts

### 1. Buckets
- Data is partitioned into **buckets**, each with its own Raft log, subscriptions, and notifier.
- A bucket is the atomic unit of replication, computation, and failover.
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
- If a notifier crashes, the next one replays the outbox, guaranteeing *no lost updates*.
- Clients deduplicate using `(sub_id, emit_seq)` for “effective-once” delivery.

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

In short: DiceDB rethought data reactivity. SevenDB rethinks *scalable, correct reactivity*.

---

## Status

🚧 **Early development** — expect rapid iteration.

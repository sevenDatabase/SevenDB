# Raft Testing & Deterministic Clock Guide

This document explains how the raft package tests in SevenDB are structured, how the deterministic clock works, and how to write new fast, race-stable tests without wall-clock sleeps.

## Goals

- Eliminate flakiness from timing races (leader election, heartbeats, snapshots)
- Reduce test wall time (no real `time.Sleep`) to speed up CI
- Provide clear patterns for: replication, failover, restart, pruning, WAL shadow parity, and error semantics
- Ensure WAL migration features (dual write, validator, pruning, strict sync) remain covered

## Key Concepts

### Raft Node Construction (Test Mode)
`ShardRaftNode` accepts an optional simulated clock via the unexported field `clk` injected using `RaftConfig.TestDeterministicClock`.
We expose a helper in `raft_test_helpers.go`:

```
newDeterministicNode(t, cfg, startTime) *ShardRaftNode
```

This creates a normal etcd raft node but replaces its internal tick loop scheduling with a `SimulatedClock` from `internal/harness/clock`.

### Advancing Time
Instead of sleeping, tests call:
```
advanceAll(nodes, 10*time.Millisecond)
```
This increments the simulated time for any node that has a deterministic clock; each tick triggers raft's internal heartbeat/election processing just as a real ticker would.

Choose step sizes (e.g. 5–20ms) small enough to satisfy election timeouts (configured by `HeartbeatMillis`, `ElectionTimeoutMillis`) while still running quickly.

### Leader Election Pattern
Old pattern:
```
waitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool { _,_,ok := findLeader(nodes); return ok }, "leader")
```
New deterministic pattern:
```
for i := 0; i < 500; i++ { if _,_,ok := findLeader(nodes); ok { break } ; advanceAll(nodes, 10*time.Millisecond) }
if _,_,ok := findLeader(nodes); !ok { t.Fatalf("no leader") }
```
This avoids real elapsed time and finishes as soon as leadership is established.

### Proposing & Waiting For Apply
Helper:
```
proposeOnLeader(t, nodes, bucket, payload)
```
After proposals, instead of sleeping, iterate advancing simulated time until `Status().LastAppliedIndex` reaches the target across all nodes.

### Snapshots & Pruning
Trigger snapshots by lowering `config.Config.RaftSnapshotThresholdEntries` (or adjusting per-node `snapshotThreshold` when safe in tests). Then loop advancing time until `LastSnapshotIndex > 0` or `PrunedThroughIndex >= snapshotIndex`.

### WAL Shadow & Validation
Shadow WAL (new protobuf-based log) is dual-written alongside the legacy in-memory raft storage. Tests that exercise failover or restart call:
```
n.ValidateShadowTail()
```
to assert the CRC/index parity ring matches recent writes.

### Crash / Failover Simulation
To simulate a crash of the leader without graceful shutdown:
1. Identify the leader index.
2. Replace its slot in the `nodes` slice with a placeholder `&ShardRaftNode{shardID:"_dead"}`.
3. Advance simulated time until a new leader emerges.
4. Recreate the crashed node via `newDeterministicNode` pointing to its original data directory.
5. Reattach transport and advance until catch-up.

### Restart Semantics
When restarting a node, its prior WAL shadow directory is reused so that:
- HardState is replayed (commit/term restored)
- Entries are fed back into raft `MemoryStorage` via WAL primary read (if enabled)
- Validator ring is seeded

Tests assert indices (`LastAppliedIndex`, `LastSnapshotIndex`) do not regress post-restart.

### Multi-Bucket Commit Isolation
The raft layer assigns monotonically increasing per-bucket commit indices. Tests alternate bucket IDs during proposals and validate each sequence is contiguous starting at 1 without gaps.

### NotLeader Errors
Follower proposals must return a typed `NotLeaderError` struct containing the current leader's ID (or empty while unknown). Tests assert error type + hint correctness.

## File Overview

| File | Purpose | Deterministic Integration |
|------|---------|---------------------------|
| `raft_wal_shadow_test.go` | WAL shadow parity, leader failover, restart | Converted: uses deterministic clock for multi-node failover scenario |
| `raft_cluster_test.go` | Core replication, snapshot, restart, pruning, bucket isolation, error semantics | All tests converted to deterministic advancement |
| `raft_wal_prune_test.go` | Snapshot-driven WAL segment pruning & recovery cleanup | Converted (single node leader wait + time advancement) |
| `raft_wal_primary_read_test.go` | WAL primary read seeding storage (static write then replay) | Purely file-based, does not need deterministic time |
| `raft_deterministic_test.go` | Manual mode validation (no background goroutines) | Separately exercises manual tick processing |
| `raft_test_helpers.go` | Helper utilities (closeAll, newDeterministicNode, advanceAll) | Hosts deterministic tooling |

## Writing a New Deterministic Raft Test
1. Configure global test knobs as needed:
```
config.Config = &config.DiceDBConfig{}
config.Config.RaftSnapshotThresholdEntries = 5
```
2. Create nodes with deterministic clock:
```
start := time.Unix(0,0)
for i := 1; i <= N; i++ {
  cfg := RaftConfig{ShardID: sid, NodeID: fmt.Sprintf("%d", i), Peers: peerSpecs, DataDir: dir(i), Engine: "etcd", ForwardProposals: true}
  n := newDeterministicNode(t, cfg, start)
  transport.attach(uint64(i), n); n.SetTransport(transport)
  nodes = append(nodes, n)
}
```
3. Drive election:
```
for steps:=0; steps<500; steps++ { if _,_,ok := findLeader(nodes); ok { break }; advanceAll(nodes, 10*time.Millisecond) }
```
4. Propose and confirm apply:
```
_, rIdx := proposeOnLeader(t, nodes, "b", []byte("val"))
for steps:=0; steps<300 && nodes[0].Status().LastAppliedIndex < rIdx; steps++ { advanceAll(nodes, 10*time.Millisecond) }
```
5. Assertions; prefer invariants on indices / snapshot progression over sleeps.
6. Cleanup via `closeAll(t, nodes)` to ensure WAL directory hygiene.

## Choosing Step Counts
- Election: ~ (ElectionTimeoutMillis / step) * small multiplier (we used 500 * 10ms = 5s virtual upper bound)
- Apply loops: Bound by a few hundred iterations usually sufficient; bail early once condition met.
- Snapshot/prune: 1000 iterations * 10ms (10s virtual) should be generous; adjust if thresholds increase.

Because advancement is virtual, a larger bound does not slow real time proportionally—only loop CPU + raft processing cost matters.

## Avoiding Common Pitfalls
- Do not mix real `time.Sleep` with simulated advancement inside the same logical waiting loop; it reintroduces nondeterminism.
- Always re-detect the leader just before proposing if leadership changes are expected (use `proposeOnLeader`).
- After a crash simulation, ensure placeholder node isn't used for proposals (we tag with `shardID:"_dead"`).
- When pruning tests depend on snapshots, ensure you actually cross the configured snapshot threshold; check `LastSnapshotIndex`.
- After restart, compare indices (applied, snapshot) against pre-shutdown baseline to detect regressions.

## Extending Validation
Future enhancements may include:
- Enriching WAL validator comparisons beyond (index, crc) to include namespace/bucket/opcode/sequence.
- Metrics around mismatch counts and fsync latency for durability mode comparisons.
- Helper to wait on predicate with automatic advancement (wrapper around common pattern extracted once stabilized).

## Race Detector & CI
Run with:
```
go test -race ./internal/raft -count=1
```
Deterministic tests dramatically reduce flaky race reports caused by goroutine timing windows during election and snapshotting.

## Manual Mode vs Deterministic Clock
Manual mode (`Manual: true`) disables background tick & Ready loops entirely; tests explicitly call:
```
n.ManualTick()
for n.ManualProcessReady() {}
```
Use this when you need total control or to unit-test state machine transitions in isolation. Deterministic clock mode keeps the standard background loops but replaces wall-clock tick scheduling.

## Cleanup & WAL Hygiene
`closeAll` asserts only expected segment (`seg-*.wal`) and sidecar index (`seg-*.wal.idx`) files remain. Any temporary or `.deleted` remnants cause test failure, surfacing incomplete prune or rotation cleanup.

## When Real Time Is Still Acceptable
- Pure file reconstruction tests (e.g., `raft_wal_primary_read_test.go`) where timing is irrelevant can remain simple.
- Network / integration layers not yet adapted to simulated time should isolate their sleeps behind helper abstractions if later conversion is desired.

## Adding a New WAL / Raft Scenario
1. Decide if it depends on timing (election/snapshot). If yes, use deterministic clock.
2. Compose scenario steps using proposals, crash/restart patterns, and index assertions.
3. Validate WAL parity if shadow involved (`ValidateShadowTail`).
4. Ensure final cleanup via `closeAll`.

## FAQ
**Q: Why keep a `waitUntil` helper?**
A: Some legacy tests still rely on it; new tests should prefer explicit advancement loops. We may refactor `waitUntil` to accept an optional node slice for automatic advancement.

**Q: Does advancing simulated time spin CPU heavily?**
A: Each advancement triggers raft tick processing; loop bounds are modest (hundreds) and far cheaper than multi-second real sleeps.

**Q: Can deterministic and real nodes mix?**
A: Avoid mixing in the same test; either all nodes share simulated time or all use wall clock, to retain consistent timing behavior.

---
Last updated: 2025-10-03

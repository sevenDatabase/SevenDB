# SevenDB Raft & Shadow WAL Architecture

> Status: MVP foundation with deterministic testing, dual persistence (legacy + shadow WAL), experimental primary WAL read path.

## 1. Goals & Design Principles
- Deterministic, testable replication core.
- Gradual migration to a unified, protobuf-based, richly annotated WAL without destabilizing existing raft state handling.
- Clear layering: raft state machine (etcd/raft) vs durability (legacy persistence + shadow WAL) vs transport vs application integration.
- Fast restart & validation: ability to reconstruct state from WAL envelopes and detect divergence via CRC/tuple ring.
- Safety first: snapshot before prune; prune only fully obsolete segments.

## 2. Core Types Overview
| Type | Responsibility |
|------|----------------|
| `ShardRaftNode` | Per-shard orchestration: proposals, Ready processing, snapshots, pruning, WAL dual-write. |
| `raftPersistence` | Legacy storage (entries + snapshot) bridging etcd MemoryStorage and disk. |
| Shadow WAL writer (`raftwal*`) | Segment framed append-only envelope log (CRC32 + length + proto). |
| `walValidator` | Ring buffer of last N (raftIndex, payloadCRC) tuples for parity validation. |
| `Transport` (interface) | Abstract message send; allows GRPC, noop, test memory transport. |
| `GRPCTransport` | Production multi-node networking (persistent bidirectional streams). |
| `memTransport` (tests) | In-process direct invocation of `Step` for fast deterministic tests. |
| `SimulatedClock` | Deterministic logical time source used in tests to drive election/heartbeat. |

## 3. Lifecycle & Data Flow
### 3.1 Startup Sequence
1. Construct `ShardRaftNode` with `RaftConfig`.
2. Load legacy persistence (snapshot + entries) if enabled.
3. If shadow WAL enabled, replay latest HardState envelope (may override legacy HardState if newer).
4. If `WALPrimaryRead` set (experimental) and WAL is enabled, replay WAL envelopes to seed entries/HardState.
5. Initialize validator ring (optionally seeding from WAL tail logical tuples).
6. Clean stale `*.wal.deleted` markers pre-writer open.
7. Create shadow WAL writer (segment sizing, fsync policy, forced rotation test knob).
8. Spawn `tickLoop` + `readyLoop` (unless manual mode) using real or simulated clock.

### 3.2 Proposal Path
```
client -> Propose / ProposeAndWait -> wireRecord(seq, bucket, type, payload) -> rn.Propose
```
- `ProposeAndWait` installs a waiter channel keyed by sequence for completion.
- Non-leader proposals return `NotLeaderError` including leader hint.

### 3.3 Raft Ready Processing (`readyLoop`)
For each `etcdraft.Ready`:
1. Persist via `raftPersistence` (HardState, Entries, Snapshot, MUST precede application).
2. Shadow WAL dual-write:
   - Write envelopes for new entries (NORMAL) and changed HardState (HARDSTATE) with enriched metadata.
3. Append entries to etcd storage (handled by etcd on `Advance`).
4. Apply committed entries:
   - Decode `wireRecord`, update per-bucket commit indices.
   - Increment `committedSinceSnap`.
   - Publish `CommittedRecord`.
5. Snapshot Trigger:
   - If `committedSinceSnap > snapshotThreshold`, initiate snapshot (etcd produces snapshot in subsequent Ready).
6. Pruning:
   - After compaction index > snapshot index, prune legacy entries.
   - Invoke WAL prune to remove fully obsolete segments (segment max index < prunedThroughIndex).
7. Update internal status (applied index, snapshot index, prune watermark).

### 3.4 Snapshot & Prune Interplay
- Snapshot precondition: accumulated committed entries beyond threshold.
- Compaction removes entries < snapshot index (legacy storage).
- WAL prune invoked only after pruning legacy entries to ensure WAL never drops data still needed for rebuild.
- Deleted segments are renamed (crash-safe) then unlinked; tests assert no stray `*.deleted` remains.

## 4. Elections & Timing
### 4.1 Configuration
| Parameter | Source | Meaning |
|-----------|--------|---------|
| `HeartbeatMillis` | `RaftConfig` | Interval between leader heartbeats. |
| `ElectionTimeoutMillis` | `RaftConfig` | Randomized base timeout for follower to start election; we derive ticks: `electionTicks = max( (ElectionTimeoutMillis / HeartbeatMillis), 5 )`. |
| `PreVote` | Hard-coded true | Avoid disruptive leadership churn. |
| `CheckQuorum` | Hard-coded true | Leader steps down if quorum heartbeat failed. |

### 4.2 Real-Time Tick Path
```
Ticker(time.NewTicker(tickEvery)) -> rn.Tick()
```
- `tickEvery = HeartbeatMillis`.

### 4.3 Deterministic Tick Path (Tests)
```
SimulatedClock.Advance(d) -> tickLoop polls clk.Now() -> if delta >= tickEvery -> rn.Tick()
```
- No wall sleeps dominate; minimal micro-sleep prevents CPU spin.
- Tests advance logical time with `advanceAll(nodes, duration)`.

### 4.4 Election Flow Summary
1. Followers increment election elapsed ticks; on timeout start PreVote.
2. PreVote gathers permission, transitions to Candidate on success.
3. Candidate increments term, votes for self, requests votes.
4. Majority grants -> becomes Leader; issues immediate empty append / heartbeats.
5. Heartbeats reset follower election timers; absence triggers new elections.

## 5. Transport Layer
### 5.1 `Transport` Interface
```
type Transport interface { Send(ctx context.Context, msgs []raftpb.Message) }
```
### 5.2 GRPCTransport (Production Path)
- Maintains `peerConn` per peer (ID, address, gRPC stream).
- Establishes outbound `MessageStream` (bidirectional).
- Reconnection loop with fixed sleep (future: exponential backoff, jitter).
- Serialize etcd raft messages via gogo/protobuf marshal.
- Wrap in `RaftEnvelope` (proto service defined elsewhere) and send on stream.
- Receives stream messages (currently ignored placeholder for future acks/flow control).
- Peer stats exported in `Status()`.

### 5.3 StaticPeerResolver
- Maps raft ID to address; self ID provided.
- Future: dynamic membership / service discovery plugin.

### 5.4 memTransport (Tests)
- In-process direct call to `Step`—no serialization or network latency.
- Eliminates nondeterminism in multi-node election tests.

## 6. Shadow WAL Details
### 6.1 Frame Layout
```
[CRC32(uint32)][LEN(uint32)][Envelope proto bytes]
```
### 6.2 Envelope Fields (subset)
| Field | Purpose |
|-------|---------|
| RaftIndex / RaftTerm | Position & term lineage. |
| Kind (NORMAL/HARDSTATE) | Distinguish entry vs HardState metadata. |
| AppBytes / AppCrc | Raw entry payload + integrity. |
| Bucket / Opcode / Sequence | Enriched semantics for validation and future replay. |

### 6.3 Durability Modes
| Mode | Behavior |
|------|----------|
| Buffered | Batched OS flush; fsync on rotation or periodic flush. |
| StrictSync | Fsync every append + directory fsync on rotation for strongest durability. |

### 6.4 Pruning Strategy
1. After raft compaction identifies safe index, update `prunedThroughIndex`.
2. Writer enumerates segments; any whose max raft index < watermark are renamed then deleted.
3. Validator ring shrinks implicit history window but retains last N entries.

### 6.5 Crash Safety Practices
- Two-phase segment deletion (`*.deleted` markers) prior to unlink for atomicity.
- Startup pre-clean removes any orphaned markers before writer open.

## 7. Validation & Consistency
### 7.1 Dual Read Validator
- Ring of `(raftIndex, crc32(payload))` sized by `ValidatorLastN`.
- On startup optionally seeded from WAL tail for immediate continuity checks.
- Mismatch logs warnings (non-fatal) to allow progressive hardening.

### 7.2 Proposal Waiters
- `ProposeAndWait` stores channel by sequence (etcd path) or raft index (stub path).
- On apply, waiter receives `(CommitIndex, RaftIndex)`; closed on abort/shutdown.

### 7.3 Per-Bucket Commit Indices
- Maintained in-memory map: bucket -> monotonically increasing logical commit index (dense, gapless).
- Tests assert gaplessness & independence across buckets.

## 8. Snapshot & Restart Semantics
### 8.1 Snapshot Trigger
- `committedSinceSnap` increments per committed entry.
- If exceeds `snapshotThreshold` -> snapshot requested; etcd emits snapshot in subsequent Ready.

### 8.2 Restart Path
1. Load legacy snapshot & entries into storage.
2. WAL last HardState replay may override outdated HardState (if term/commit advanced).
3. `WALPrimaryRead` (experimental) reconstructs entries + HardState solely from WAL if enabled.
4. Validator seeded; resume normal Ready processing.

### 8.3 Integrity Invariants (Tests)
- Applied index never regresses after restart.
- Snapshot index preserved.
- PrunedThroughIndex monotonic.

## 9. Deterministic Testing Strategy
| Component | Technique |
|-----------|-----------|
| Elections | Simulated clock + manual logical advance loops. |
| Leader Failover | Controlled close + new node instantiation using same datadir; advance logical time. |
| Pruning | Force segment rotation (test knob) + threshold crossing. |
| Snapshot | Low threshold config to trigger early. |
| Restart Recovery | Validate indices & snapshot continuity after re-instantiation. |

## 10. Concurrency & Safety
- `lastShadowHardState` guarded by mutex accessors.
- `Close` ordering: set closed -> close stopCh -> stop raft node -> wait -> close committedCh -> release waiters.
- Tick loop respects `stopCh` early to avoid goroutine leaks.
- Shadow WAL writes best-effort; failures logged (don’t block raft progress) during migration phase.

## 11. Limitations / Future Work
| Area | Gap | Next Step |
|------|-----|----------|
| WAL Replay Validation | No explicit gap detection | Verify contiguous indices & term monotonicity on replay. |
| HardState Envelope Indexing | Uses commit index as proxy | Introduce explicit sequence for HardState ordering. |
| Flow Control | Receiver side ignored | Implement acks / backpressure (dropped send metrics). |
| Metrics | Minimal | Expose counters: proposals, commits, snapshots, prunes, fsyncs, validator mismatches. |
| Corruption Handling | CRC only at write time | Validate CRC on replay; quarantine bad segments. |
| Primary WAL Mode | Experimental | Harden gap checks + per-bucket commit reconstruction from WAL. |
| Validator Strictness | Warn-only | Configurable enforcement (panic on mismatch in CI). |
| Dynamic Membership | Static resolver | Integrate ConfChange and resolver update path. |
| Backpressure Policy | Drop after timeout | Bounded worker pool / configurable commit channel depth. |
| Per-Bucket Durability | In-memory only | Reconstruct commit indices by scanning WAL on restart. |

## 12. Operational Guidance
- Enable shadow WAL (`EnableWALShadow`) early to accumulate history even before cutover.
- Keep `WALStrictSync` off in dev unless testing durability cost profile.
- Increase `ValidatorLastN` in staging to raise detection window (e.g., 512 or 1024).
- Delay enabling `WALPrimaryRead` until contiguous replay validation is merged.
- Monitor logs for prune or WAL write errors; promote to metrics later.

## 13. Glossary
| Term | Definition |
|------|------------|
| Envelope | Protobuf record persisted in shadow WAL describing one raft entry or HardState mutation. |
| Snapshot Threshold | Entry count beyond last snapshot required to trigger new snapshot. |
| PrunedThroughIndex | Highest raft index guaranteed durable in snapshot; earlier segments safe to drop. |
| Validator Ring | Rolling window of recent (index, crc) pairs for parity checks. |

## 14. Quick Reference (Config Flags)
```
EnableWALShadow       bool
WALShadowDir          string
WALSegmentMaxBytes    int64
WALForceRotateEvery   int (test only)
WALStrictSync         bool
WALPrimaryRead        bool (experimental)
WALDualReadValidate   bool
ValidatorLastN        int
HeartbeatMillis       int
ElectionTimeoutMillis int
DisablePersistence    bool (testing)
Manual                bool (manual control over loops)
TestDeterministicClock clock.Clock (tests)
```

---
Generated documentation reflects repository state as of current passing tests. Keep this file updated with each major replication or WAL change.

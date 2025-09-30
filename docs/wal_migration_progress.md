# WAL Migration Progress (September 30, 2025)

## Newly Implemented
* StrictSync mode (RaftConfig.WALStrictSync / raftwal.Config.StrictSync) with per-append fsync and directory sync enforcement.
* Sidecar durability refinements: cleanup of orphan `*.wal.idx.tmp`; directory+segment fsync after sidecar rewrite in strict mode.
* Pruning already integrated: segment whole-file deletion with rename + unlink crash safety.
* Experimental primary read path (RaftConfig.WALPrimaryRead): seeds in-memory raft storage from unified WAL (normal entries + last HardState) prior to starting etcd raft.
* Replay seeding logic and tests:
  - `TestStrictSyncDurability` validates persistence of frames & HardState with sidecar loss recovery.
  - `TestWALPrimaryReadSeedsStorage` asserts entries + HardState are visible when WALPrimaryRead enabled.
* Envelope enrichment: namespace, bucket, opcode, sequence fields populated for normal entries (namespace default placeholder).
* Dual-read validation (WALDualReadValidate): compares CRC of committed legacy entry with WAL ring snapshot; logs mismatches.
* Crash injection hooks in writer (after frame write, post flush/fsync, sidecar write, rotation) with accompanying tests (`TestWriterCrashInjection`).
* Sidecar loss recovery test with enriched metadata (`TestSidecarLossEnriched`).

## Outstanding For Full Cutover
1. Expand validator to also compare opcode/sequence/bucket (currently only CRC + index validated).
2. Metrics & observability: expose strict sync latency, sidecar rewrite count, validation mismatch counters.
3. Progressive rollout: gating strategy & feature flag orchestration (percentage or shard subset).
4. Optional: per-bucket sequence counters vs shard-global sequence for finer replay semantics.
5. Envelope namespace finalization (multi-tenancy model) and opcode enumeration stabilization.

## Invariants Now Held
* Any successful Append* in StrictSync mode guarantees frame durability (segment fsync) at return.
* Sidecar absence or staleness is recoverable by raw segment scan.
* Segment pruning cannot resurrect deleted data due to rename+unlink+dir fsync protocol.

## Next Suggested Steps
* Implement Envelope metadata extension and backfill population in shadow writer.
* Introduce validator extension to compare reconstructed higher-level metadata vs legacy during dual-read phase.
* Add configuration migration guide to docs (enabling StrictSync and WALPrimaryRead in test/staging).

---
Generated automatically as part of WAL migration iteration.
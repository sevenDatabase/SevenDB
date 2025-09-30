# WAL Migration Progress (September 30, 2025)

## Newly Implemented
* StrictSync mode (RaftConfig.WALStrictSync / raftwal.Config.StrictSync) with per-append fsync and directory sync enforcement.
* Sidecar durability refinements: cleanup of orphan `*.wal.idx.tmp`; directory+segment fsync after sidecar rewrite in strict mode.
* Pruning already integrated: segment whole-file deletion with rename + unlink crash safety.
* Experimental primary read path (RaftConfig.WALPrimaryRead): seeds in-memory raft storage from unified WAL (normal entries + last HardState) prior to starting etcd raft.
* Replay seeding logic and tests:
  - `TestStrictSyncDurability` validates persistence of frames & HardState with sidecar loss recovery.
  - `TestWALPrimaryReadSeedsStorage` asserts entries + HardState are visible when WALPrimaryRead enabled.

## Outstanding For Full Cutover
1. Envelope enrichment: include higher-level proposal metadata (bucket, type, sequence) to reconstruct full application semantics during replay-only startups.
2. Dual-read validation phase (legacy vs WAL) with divergence detection before disabling legacy writes.
3. Robust crash fault-injection tests spanning each step of strict ordering (between write, flush, fsync, sidecar rename, rotation).
4. Metrics & observability: expose strict sync latency and sidecar rewrite stats.
5. Config gating & progressive rollout strategy (percentage-based or shard subset enablement).

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
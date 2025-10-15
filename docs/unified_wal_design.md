# Unified WAL design addendum

This addendum refines the unified WAL plan with explicit decisions, invariants, and acceptance criteria, based on the senior review.

## Deterministic replay semantics (logical timestamps)

- Ordering contract
  - Primary ordering: raft log index (strictly increasing per shard).
  - Ties: if multiple app commands are batched into one raft entry, use a sub-sequence counter assigned at append time (0..N-1) as the second sort key.
  - Final tiebreaker for safety: raft term.
  - Wall-clock timestamps are recorded as metadata but are not used for ordering.
- Logical timestamp (LTS)
  - Define LTS = (index, subSeq, term).
  - Persist LTS with each record to allow deterministic ordering across nodes and replays.
- Acceptance
  - Given the same WAL, two replays produce identical command application order and final DB state.
  - Replay scanner sorts strictly by LTS when stitching batched operations.

## Co-fsync HardState with last entry (atomicity)

- Write group
  - Append last entry bytes, then persist HardState delta in the same segment as a paired "HS" record with the same index in its header.
  - Issue a single fsync barrier that covers both records (and directory if rotation occurred).
- Recovery rule
  - If HardState.commit > last entry index on disk after crash, truncate HS to last entry index.
  - If HardState.commit < last entry index, entries beyond commit are considered uncommitted and ignored by state apply.
- Acceptance
  - Fault injection at any instruction boundary between entry write and fsync yields a consistent state with no phantom commit.

## Replay time budget & automatic sidecar index

- Config: unifiedWAL.replayTimeBudget=duration (default: 30s).
- Behavior
  - If replay exceeds budget OR total scanned bytes > X% of WAL size, automatically build/refresh a sidecar index asynchronously.
  - Sidecar contains segment offsets for (first/last index), per-record offsets (optional sampling), and a per-segment manifest (see below).
- Acceptance
  - Cold start respecting budget switches to indexed replay in subsequent starts, reducing replay time below budget on a stable dataset.

## Per-segment validation manifest

- Manifest content (written on clean segment close, lazily rebuilt otherwise):
  - formatVersion, segmentId, firstIndex/term, lastIndex/term, recordCount
  - min/max wallTime, min/max LTS, fileLength
  - crc32c of concatenated frame headers, crc32c of payload stream (logical)
  - optional sha256 over ciphertext/plaintext (see encryption below)
- On open
  - Validate manifest against file size and quick CRCs; if mismatch, fall back to linear scan.
- Acceptance
  - Opening a healthy segment uses manifest-only validation (<5ms typical) instead of full scan.

## Follower-aware prune floor

- Prune rule
  - Prune only indices strictly below floor = min(snapshotIndex, minFollowerMatchIndex).
  - Floor must be computed per shard from raft progress; update continuously.
- Serialization
  - Prune is serialized with appends via a writer barrier (see below) to prevent interleaving.
- Acceptance
  - A deliberately slow follower can still catch up from WAL after leader pruning events.

## Full semantic reconciliation tests

- Beyond CRC parity
  - Compare final DB state, keyspaces, expirations, and observable side effects across: (a) live application path, (b) restore-from-WAL path.
  - Also compare the ordered sequence of opcodes/buckets/seq IDs derived from WAL vs. live applied sequence.
- Acceptance
  - Tests pass across random workloads, mixed op types (SET/DEL/EXPIRE/SUB/UNSUB), and snapshot boundaries.

## Mid-fsync and tail-doubling fault tests

- Mid-fsync
  - Inject failure after data write but before fsync; verify recovery does not expose partially committed HS or entries.
- Tail-doubling
  - Duplicate the last valid frame (or partial frame) at the segment tail; verify replay stops at first malformed boundary and deduplicates doubles.
- Acceptance
  - No panics; idempotent recovery; DB state consistent with pre-crash committed prefix.

## Cluster-level WAL format version enforcement

- Header
  - Each segment header carries walFormatVersion and feature flags.
- Cluster contract
  - Nodes advertise supported version via raft/gossip. The leader enforces that all voters support the active version before enabling features requiring it.
  - Joiners with incompatible versions are refused, with a clear error.
- Acceptance
  - Mixed-minor upgrades allowed when explicitly compatible; incompatible nodes fail fast with actionable messages.

## Serialize prune vs append explicitly

- Writer model
  - Single writer goroutine serializes Append, Sync, Rotate, and Prune operations with an explicit queue and barriers.
  - Prune acquires a checkpoint barrier that guarantees no in-flight append spans the prune boundary.
- Acceptance
  - No interleaving of IO causing loss or duplication; race tests show invariants hold under stress.

## Encryption and CRC interaction (future)

- Integrity strategy
  - Physical integrity: compute CRC over ciphertext to detect on-disk corruption regardless of encryption.
  - Authentication: use AEAD; per-record AEAD tag persisted in header/trailer.
  - Optional logical digest over plaintext (e.g., sha256) recorded in manifest for offline verification after decryption.
- Acceptance
  - Clear documented invariants; test vectors demonstrating corruption detection both before and after decryption.

## Metrics and observability (additions)

- Append latency histogram; fsync latency histogram; bytes written counter; replay duration; index build time; prune counts and bytes reclaimed; validation mismatches; follower match index distribution.

## Configuration (additions)

- unifiedWAL.replayTimeBudget (duration)
- unifiedWAL.autoSidecarIndex (bool)
- unifiedWAL.strictSync (bool)
- unifiedWAL.formatVersion (int/semver)

## Compatibility and rollout notes

- Versioned headers allow rolling upgrades. New features gated behind cluster capability checks.
- Dual-write and parity checks can remain enabled during rollout; once semantic reconciliation and follower-aware pruning are green in prod, retire shadow WAL.

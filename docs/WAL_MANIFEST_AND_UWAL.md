# WAL manifest, UWAL1, and integrity preamble

This document describes the unified WAL format (UWAL1), the WAL manifest and enforcement policy, the integrity preamble written to each segment, and how these pieces integrate with Raft persistence and startup seeding.

## UWAL1 envelope (implemented)

- Magic prefix: `UWAL1` (5 bytes) inside each Forge WAL `Element` payload.
- Fields (little-endian): `kind(1) | index(8) | term(8) | subSeq(4) | cmdLen(4) | cmdBytes | raftLen(4) | raftData`.
- Kinds:
  - `0` normal entry: `cmdBytes` is protobuf `wire.Command`; `raftData` is the original raft `Entry.Data`.
  - `1` hardstate: exposed via `ReplayLastHardState()`, not via `ReplayCommand`.
  - `2` manifest preamble: first record in an empty/rotated segment; carries the SHA-256 of `WAL.MANIFEST`. Ignored by replay and index scanning.
- Backward compatibility: if a payload does not start with `UWAL1`, it is treated as a legacy raw `wire.Command`.

## HardState durability (implemented)

HardState bytes are staged and persisted with the next `Sync()` barrier using a tmp+fsync+rename protocol and directory fsync, guaranteeing HS never points past the last durable WAL entry.

## WAL.MANIFEST and enforcement (implemented)

`WAL.MANIFEST` is a JSON file persisted in `wal-dir`. Fields include:
- `schemaVersion`, `format` (legacy | uwal1 | mixed), `uwalVersion` (1 for UWAL1), `enforce` (warn | strict), timestamps, `writerVersion`, and reserved `features`.

On startup, we detect the directory’s active format by scanning a small prefix of entries and enforce it against the manifest policy:
- `enforce: strict` blocks mismatches and mixed formats (unless `compat.mixedOk` is set in future revisions).
- `enforce: warn` logs mismatches and continues.

If the manifest is missing:
- When `wal-auto-create-manifest=true` (default), we detect the format and write a manifest atomically.
- When `wal-require-uwal1=true`, and the detected format is not UWAL1, startup fails.

## Integrity preamble (implemented)

Each new/rotated WAL segment begins with a manifest preamble (UWAL kind=2) whose inner payload bytes are the SHA-256 of the current `WAL.MANIFEST`. On startup, we verify the first segment’s preamble matches the manifest (strict mode fails on mismatch). Replay and `lastIndexOfSegment` ignore this preamble.

## Primary-read seeding (implemented)

At startup, when enabled, Raft storage is seeded directly from the Forge WAL using UWAL1 records (entries and last HardState). If no UWAL/HS is present, we fall back to the legacy/shadow WAL.

## Write-path policy (implemented)

When the manifest is strict and not UWAL1, UWAL appends are rejected with a clear error to prevent accidental upgrades in protected directories.

## Configuration flags

- `wal-auto-create-manifest` (default: true) – create `WAL.MANIFEST` if missing using detected format.
- `wal-require-uwal1` (default: false) – if true and no manifest, require UWAL1 and fail on legacy/mixed.
- `wal-manifest-enforce` (default: warn) – manifest enforcement policy: `warn` | `strict`.
- Existing WAL tuning (examples): `wal-dir`, `wal-rotation-mode`, `wal-max-segment-size-mb`, `wal-max-segment-rotation-time-sec`, `wal-buffer-size-mb`, `wal-buffer-sync-interval-ms`.

## Testing

Unit tests cover:
- Preamble write and hash verification on fresh directories.
- Strict-mode startup failure on preamble/manifest mismatch.
- Replay and `lastIndexOfSegment` ignoring preamble entries.

See also: `internal/raft/raft_wal_primary_read_forge_test.go` for primary-read seeding tests.

## Operational guide

This section provides practical steps for common scenarios.

### Fresh UWAL1 setup

1) Configure the WAL to use Forge and allow auto-manifest creation in warn mode (first run):

YAML (sevendb.yaml):

```yaml
wal-variant: forge
wal-dir: ./logs
wal-auto-create-manifest: true
wal-manifest-enforce: warn
```

2) Start SevenDB once to create `WAL.MANIFEST` and write the integrity preamble into the first segment.

3) Switch to strict enforcement for production:

```yaml
wal-manifest-enforce: strict
```

### Enable primary-read seeding (recommended)

Seed etcd raft storage directly from UWAL on startup; the system falls back to the legacy path if the UWAL is empty:

```yaml
raft.wal-primary-read: true
```

### Migrate from a legacy WAL directory

Option A (recommended): new UWAL1 directory
- Stop the node and point `wal-dir` to a new, empty path.
- Use the fresh UWAL1 setup steps above (warn → strict).
- Optionally keep the old directory for archival/forensics.

Option B (temporary mixed during warn)
- If you must reuse the legacy `wal-dir`, keep `wal-manifest-enforce: warn` for the first UWAL1 writes. This will produce a mixed directory (legacy + UWAL1). Do not switch to `strict` until you cut over to a pure UWAL1 directory. Note: strict mode forbids UWAL appends when the manifest is not UWAL1.

Snapshot-based cutover (cleanest)
- Take an application-level snapshot/checkpoint.
- Start with an empty UWAL1 `wal-dir` and restore from the snapshot; remaining traffic writes UWAL1 only.

### Handling strict enforcement errors at startup

Common messages and remediations:
- "wal format enforcement failed" or "manifest preamble hash mismatch" (strict):
  - Cause: `WAL.MANIFEST` content does not match the first segment’s preamble (e.g., manual edits, partial restore).
  - Fix: restore the correct manifest file, or temporarily set `wal-manifest-enforce: warn` to start, then repair by recreating the WAL directory or writing a new manifest and segments consistently.
- "wal requires UWAL1" on empty/missing manifest:
  - Cause: `wal-require-uwal1: true` but detected legacy/mixed.
  - Fix: point to a new empty `wal-dir` or migrate via snapshot.

### Write-path rejection in strict legacy manifests

When `enforce: strict` and the manifest is not UWAL1, UWAL appends are rejected. You’ll see an error similar to:

> wal manifest(strict legacy) forbids UWAL appends: migrate or relax policy

To proceed, either:
- Switch `wal-manifest-enforce: warn` temporarily (risk: mixed format), or
- Migrate to a new UWAL1 directory and leave strict enabled.

### Troubleshooting checklist

- Verify `wal-dir` exists and is writable; check for `.deleted` temp files if pruning was interrupted.
- Inspect `WAL.MANIFEST`; confirm `format`, `uwalVersion`, and `enforce` are as expected.
- If startup fails in strict mode, try warn mode to gather more logs, then repair and revert to strict.
- For slow startup on large histories, consider adding a checkpoint/restore step to bound replay time.

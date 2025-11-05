# Determinism: scope, harness, and how to run

SevenDB extends determinism beyond the Raft log to the visible behavior of emissions and WAL replay. This doc explains what we prove, how we prove it, and how to run the suite.

## What we prove

- Same Raft log → same emission transcript
  - Across crash windows: before-send and after-send-before-ack
  - Across reconnect outcomes: OK, STALE-at-compaction (snapshot boundary), INVALID
  - Across multi-replica leadership changes (3 nodes) with deterministic elections
- Same WAL bytes (modulo pruning/rotation) → same replayed logical sequence
  - Segment rotation does not change replay content or ordering
  - Pruning segments strictly below a threshold preserves deterministic replay of the tail

These are asserted over 100 runs per case to expose flakes and timing sensitivity.

## Canonicalization strategy

- Emission transcript lines are canonicalized via `internal/determinism`:
  - Stable per-sub fingerprint, event type, and an emit sequence
  - Use commit-index based sequence when exact indices are stable
  - Fall back to positional-first-N when resend counts may vary but content must match
- WAL transcript lines: one per ENTRY_NORMAL envelope formatted as `index:crc` oldest→newest.

## Emission harness highlights

Files: `internal/emission/determinism_repeat_test.go` (canonical suite), plus targeted tests.

- No background tickers or real sleeps in determinism tests
  - All loops use `TestTickOnce` and bounded iterations
  - Notifier hook resolution prefers instance hooks with a package-level fallback so tests don’t need to start goroutines
- Crash windows
  - Before-send: cancel before delivery; restart and ensure exactly one delivery
  - After-send-before-ack: crash after first send; restart and use positional canonicalization on the first two deliveries to tolerate resend count variance
- Reconnect outcomes
  - OK: resume at `next = lastAck+1`
  - STALE: resume at compaction boundary; snapshot payload at boundary (`SNAPSHOT@4`) validates deterministic reconstruction
  - INVALID: client claimed future index; resume from suggested `next` deterministically
- Multi-replica symmetry (3 nodes)
  - Deterministic elections using simulated clocks
  - Single shared collector attached to the current leader to avoid split transcripts
  - Proposals always routed to the current leader; drain to N deliveries deterministically

## WAL harness highlights

Files: `internal/raftwal/determinism_repeat_test.go`; writer in `internal/raftwal/writer.go`.

- StrictSync mode ensures durable ordering on each append (flush + fsync)
- Deterministic rotation via `ForceRotateEvery` (test-only) avoids size heuristics
- Rollover determinism: write across multiple segments, replay, and compare `index:crc` lines over 100 runs
- Prune determinism: write, `PruneThrough(threshold)`, replay the remaining tail deterministically over 100 runs
- Existing crash/sidecar tests complement the suite (CRC mismatch detection, sidecar loss recovery)

## How to run

Run focused suites (single pass):

```zsh
# Emission determinism
go test ./internal/emission -run 'Determinism_Repeat100' -count=1

# WAL determinism
go test ./internal/raftwal -run 'Determinism_Repeat100' -count=1
```

Or run all tests:

```zsh
go test ./...
```

Optional: add `-race` to surface data races in non-deterministic code paths.

## Extending coverage

- Reconnect NotFound determinism (low priority)
- Snapshot/compaction cross-boundary with larger sequences and varying snapshot payloads
- WAL: prune → reopen → append; verify continuity and replay determinism
- CI lane to run determinism group under `-race`

## Design notes

- Deterministic time: tests use simulated clocks and `TestTickOnce`; no `time.Ticker` or `time.Sleep` inside determinism loops.
- Canonicalization ensures stable, byte-for-byte equality across runs while allowing benign resend count variance.
- Where tests need to model snapshots, we encode a snapshot-like payload explicitly at the compaction boundary to validate resume semantics.

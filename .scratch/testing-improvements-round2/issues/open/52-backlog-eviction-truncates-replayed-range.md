# Backlog eviction between the PSYNC grant and the tail re-extraction silently truncates replay

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 F2 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 2 · priority 21
Area: frogdb-replication / primary replay + replica session

## Context

The backlog lower-bound check runs at PSYNC *grant* time. By the time the session re-extracts the
tail, the primary has already written `+CONTINUE` (or `+FULLRESYNC <offset>`), and
`extract_backlog` simply returns a shorter vector. The session then seeds `resume_offset` from the
last frame it *did* send and the live tail dedups against that, so the replica is permanently
missing the evicted range while its offset looks contiguous and `WAIT` converges. The window spans
the whole RocksDB checkpoint creation *and* the file transfer; at the default 10 000-entry backlog,
a multi-gigabyte checkpoint on a busy primary evicts the entire window routinely.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- The lower-bound check lives in `primary/replay.rs:198-212`
  (`Some(oldest) if req_offset >= oldest => Ok(())`), executed at grant time.
- `replica_session.rs:652` then calls `handler.replay.extract_backlog(replay_from, current)` with
  **no lower-bound re-check**; for the full-sync path `replay_from` is the `snapshot_offset`
  captured at `:438`, and `start_streaming` is only reached at `:488`.
- `ring_buffer.rs:113-117` `debug_assert!`s that the returned tail is offset-*ordered* but never
  that it starts contiguously from `start`, so the truncation is invisible even in debug builds.
- `ring_buffer.rs:106-112` filters `offset > start && offset <= end` — a wholly-evicted range
  yields an empty vector, indistinguishable from "nothing to replay".

## Options

The proposal raised an explicit `OPTIONS` block on the boundary; record the choice before starting.

- *Level 1* (`ReplicationRingBuffer` alone): cheapest, fully deterministic, but only pins the
  invariant on the data structure — it cannot prove `ReplicaSession` honours it.
- *Level 2* (`ReplicaSession` + fake stream + tiny backlog): reproduces the actual ordering, still
  deterministic. **Recommended**, together with the level-1 assertion.
- *Level 4* (server integration with `replication_split_brain_buffer_size: Some(8)` and a large
  dataset): closest to production but timing-dependent and slow. Worth one case later; not the
  primary vehicle.

## Acceptance criteria

- [ ] The chosen level(s) above are recorded on this issue before implementation starts.
- [ ] Unit assertion: `extract_backlog(start, end)` on a buffer whose `oldest_offset() > start`
      must not return a silently short tail — it either errors, or the caller re-checks (a
      `debug_assert!(tail.first().is_none_or(|f| f.0 > start))` paired with
      `oldest_offset() <= start`). **Fails today.**
- [ ] Session-level test: grant a partial sync at offset X, push enough entries to evict past X
      while the session is blocked in the checkpoint stream, then let `start_streaming` run; assert
      the replica receives every offset in `(X, current]` **or** the session aborts the resume
      (forcing a fresh full sync) rather than streaming a hole. **Fails today.**
- [ ] The contiguity invariant is asserted, not just the ordering invariant, in
      `ring_buffer.rs:113-117`.

## Test boundary

**2** — the race is between two crate-internal calls (`primary/replay.rs` grant and
`replica_session.rs:652` re-extraction); a socket adds nothing but timing nondeterminism. The
level-1 data-structure assertion is a companion, not a substitute, because it cannot prove
`ReplicaSession` honours the invariant.

## Depends on

Nothing — the technique `replica_session.rs:1083-1165` already uses (small backlog + blocking
stream) is sufficient. Related: issue 51, `.scratch/testing-improvements-round2/issues/`.

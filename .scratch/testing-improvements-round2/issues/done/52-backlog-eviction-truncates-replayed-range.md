# Backlog eviction between the PSYNC grant and the tail re-extraction silently truncates replay

Status: done
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

- [x] The chosen level(s) above are recorded on this issue before implementation starts.
- [x] Unit assertion: `extract_backlog(start, end)` on a buffer whose `oldest_offset() > start`
      must not return a silently short tail — it either errors, or the caller re-checks (a
      `debug_assert!(tail.first().is_none_or(|f| f.0 > start))` paired with
      `oldest_offset() <= start`). **Fails today.**
- [x] Session-level test: grant a partial sync at offset X, push enough entries to evict past X
      while the session is blocked in the checkpoint stream, then let `start_streaming` run; assert
      the replica receives every offset in `(X, current]` **or** the session aborts the resume
      (forcing a fresh full sync) rather than streaming a hole. **Fails today.**
- [x] The contiguity invariant is asserted, not just the ordering invariant, in
      `ring_buffer.rs:113-117`.

## Test boundary

**2** — the race is between two crate-internal calls (`primary/replay.rs` grant and
`replica_session.rs:652` re-extraction); a socket adds nothing but timing nondeterminism. The
level-1 data-structure assertion is a companion, not a substitute, because it cannot prove
`ReplicaSession` honours the invariant.

## Depends on

Nothing — the technique `replica_session.rs:1083-1165` already uses (small backlog + blocking
stream) is sufficient. Related: issue 51, `.scratch/testing-improvements-round2/issues/`.

## Resolution — the window is checked on the extraction, not only on the grant

**Levels chosen (recorded per acceptance criterion 1): level 1 + level 2**, as recommended. Level 4
deliberately skipped: the race needs the checkpoint transfer to be long enough to evict, which at
server level is a timing gamble, and the level-2 test reproduces the exact ordering deterministically
with a blocked duplex stream.

Confirmed live, exactly as read. `can_replay` checks the floor at grant time;
`start_streaming` then re-extracted with no re-check, and `extract_backlog` returned whatever had
survived. `resume_offset` was seeded from the last frame actually sent and the live tail deduped
against it, so the replica ended up permanently missing the evicted range with a contiguous-looking
offset. Pre-fix red recorded below.

Contract pinned by `FM-REPLICATION-012` in
`.scratch/hardening/specs/replication-failure-modes.md`: **a replay is contiguous from the resume
point or it does not happen.**

### The fix

`ReplicationRingBuffer::extract_backlog` returns
`Result<Vec<(u64, u16, Bytes)>, BacklogTruncated>`. The floor is re-read **under the entries lock**
— the same lock `push` holds while it evicts and raises that floor — so the window checked is the
window whose contents are returned. `BacklogTruncated { requested, floor }` carries both numbers for
the log.

Callers now decide instead of assuming:

- `PartialSyncReplay::handle_partial_sync_request` — an eviction between `can_replay` and the
  extraction degrades the grant to `ReplayDecision::FullResync(FullResyncReason::BacklogEvicted)`.
- `ReplicaSession::start_streaming` — `Err` fails the link (`io::ErrorKind::InvalidData`, logged at
  `warn`) **before** any replayed frame is written. The replica reconnects and its `PSYNC` is
  answered `+FULLRESYNC`, because the grant-time floor check fails too.

Two ranges are served without a window, both required to avoid breaking working configurations:

- `start >= end` — an empty range has nothing to truncate. This is the caught-up replica and, more
  importantly, the **fresh-primary full sync**: a node that has never had a replica stays unarmed
  (`PrimaryReplicationHandler::new` only arms from a nonzero recovered offset), so its first sync
  asks for `(0, 0]`.
- `backlog.enabled = false` — nothing is buffered by configuration, every reconnect already
  full-resyncs (`FullResyncReason::Disabled`), and the full-sync handoff has no replay to make.
  Without this escape, replication with the backlog disabled would drop every link.

### Divergence on acceptance criterion 4

The issue asks for a `debug_assert!` pairing `tail.first()` with `oldest_offset()`. Implemented as a
**runtime** check that returns `Err` instead: it is on in release builds, and it lets the caller
abandon the resume rather than only screaming in debug. The cheap `tail.first()` ordering assert is
kept alongside it.

Byte-level contiguity *within* the tail (`offset - len == previous offset`) was considered and
rejected: it is equivalent to the floor check (eviction raises the floor to exactly the end offset of
the entry it dropped), and it would force every synthetic `rb.push(10, .., "cmd1")` in the test suite
to carry realistic payload lengths — a trap for future tests, for no extra coverage.

### Tests

Level 1, `frogdb-server/crates/replication/src/primary/tests.rs`, tagged `// FM-REPLICATION-012`:

- `an_evicted_resume_point_is_refused_not_truncated` — a cap-3 buffer that evicted its first entry
  refuses a resume below the floor and reports the floor; `floor == start` is still servable.
  **Failed pre-fix** (returned the surviving 3 entries).
- `a_closed_window_refuses_every_extraction` — a `reset` window refuses every non-empty range and
  still serves the empty one. **Failed pre-fix.**

Level 2, `frogdb-server/crates/replication/src/replica_session.rs`, same tag — both use a tiny
duplex to park the session in the exact gap, and both **failed pre-fix** (they streamed the
surviving suffix and the session returned `Ok`):

- `a_resume_evicted_after_the_grant_is_abandoned_not_truncated` — the session is parked writing
  `+CONTINUE` while the resume point is evicted; the link then fails with `InvalidData`, no frame
  reaches the wire, and the session is unregistered.
- `a_full_sync_whose_handoff_window_is_evicted_abandons_the_link` — the same with the whole dataset
  transfer as the window, which is the production shape (a checkpoint cut plus a file transfer).

`an_idle_backlog_is_freed_once_its_ttl_elapses` (FM-REPLICATION-009) was strengthened in passing: a
freed window now asserts `Err(BacklogTruncated { floor: None })` rather than an empty tail.

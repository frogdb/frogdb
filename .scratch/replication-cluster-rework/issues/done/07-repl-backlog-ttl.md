# 07 — the backlog has no TTL: a replica-less primary buffers writes forever

Status: done

## Resolution — an idle clock on the 1 Hz maintenance tick (2026-08-02)

Built as described, with one naming deviation: the knob is **`repl-backlog-ttl`**
(`replication.backlog-ttl-secs` in the config file, `ReplBacklogTtl`), not
`replication-backlog-ttl-secs` — it matches the Redis name exactly, like the rest of the
replication params. Default 3600 (Redis's), `0` disables, `mutable` so a `CONFIG SET` retunes an
idle window that is already running. Golden config snapshot 118 → 119 (appended last, so the first
118 rows stay byte-identical); mutable-param count 73 → 74.

The clock lives in `BacklogTtl { secs: AtomicU64, idle_since: Mutex<Option<Instant>> }` next to
`PartialSyncReplay` (`primary/replay.rs`). It is an **idle** clock, not a countdown from arming:
`due()` clears its start whenever the replica count is non-zero or the TTL is `0`, sets it on the
first tick that finds zero replicas, and fires — clearing itself in the same breath, so it cannot
re-fire once per tick forever. `expire_backlog_if_idle` refuses to start the clock while nothing is
armed, and on fire calls exactly `reset_backlog()`, the same disarm both ends of a primary stint
already use, so the floor goes down with the buffer and no `+CONTINUE` can be granted over the hole.
No offset and no replication id moves. The tick is the server's 1 Hz maintenance task
(`server/subsystems.rs`, `BACKLOG_TTL_TICK`), matching Redis freeing the backlog from
`replicationCron` on the same cadence.

`SplitBrainBufferConfig` was renamed `BacklogConfig` — it now carries `ttl_secs` alongside the size
knobs and is no longer only about split brain.

Spec: FM-REPLICATION-009 (`specs/replication.md`), outcome variant
`FullResyncReason::BacklogEvicted`. Tests (all `primary/replay.rs`):
`an_idle_backlog_is_freed_once_its_ttl_elapses`, `a_replica_reconnecting_before_the_ttl_still_resumes`,
`a_connected_replica_never_starts_the_ttl_clock`, `a_ttl_of_zero_never_frees_the_backlog`,
`a_freed_backlog_full_resyncs_the_next_psync`,
`freeing_the_backlog_moves_no_offset_and_no_replication_id`,
`an_unarmed_backlog_never_starts_the_ttl_clock`,
`the_ttl_fires_once_per_idle_window_not_once_per_tick`.

## What to build

`PartialSyncReplay::new` arms the backlog floor whenever the recovered offset is non-zero, and
`begin_primary_stint` arms it at the promotion boundary. Once armed, every write is pushed into the
ring buffer for the rest of the process's life — whether or not any replica has ever connected, and
whether or not one ever will. A restarted standalone primary that once had a replica (so it
recovered a non-zero offset) pays ring-buffer memory and push cost on the write path forever, for
resume history nobody can use.

Redis bounds exactly this with **`repl-backlog-ttl`** (default 3600s): the backlog is freed after
that many seconds with zero connected replicas, and `master_repl_offset` keeps advancing without
it. Valkey inherits the same knob. Freeing it is safe because a resume request below the floor
already degrades to a full resync — the TTL only decides how long the cheap path stays available.

Proposed analogue:

- Config parameter `replication-backlog-ttl-secs` (0 = never free), plumbed like the other live
  mutable replication params (golden-config entry, `CONFIG SET`-able).
- Track "last moment the replica count fell to zero"; when the TTL elapses with the count still
  zero, `reset_backlog()` — which already exists (`ReplicationRingBuffer::reset`, used at both ends
  of a primary stint) and correctly disarms the floor, so a later `PSYNC` gets `+FULLRESYNC`
  instead of a `+CONTINUE` over history that was dropped.
- The offsets themselves must not move: freeing the backlog is not a stint change and must not
  touch the replid, `second_repl_offset`, or the live/applied heads.

## Acceptance criteria

- [x] `replication-backlog-ttl-secs` exists, defaults to a Redis-comparable 3600, 0 disables
- [x] With zero replicas for longer than the TTL, the ring buffer is emptied and the floor disarmed
- [x] A replica connecting after the free gets `+FULLRESYNC`, never a `+CONTINUE` over a hole
- [x] A replica reconnecting *before* the TTL still gets its `+CONTINUE`
- [x] The timer resets whenever a replica is connected; a replica connected the whole time never
      triggers a free
- [x] Freeing the backlog does not change the replication id or any offset

## Source

Adversarial review of `promotion-replid-psync.md` — the cost side of finding LOW-7 (2026-07-28).
LOW-7 itself (arming the floor at a recovered non-zero offset) was upheld as correct: the floor is
what stops a restarted node from claiming resume history its empty buffer does not hold. This issue
covers the price that decision charges when no replica ever arrives.

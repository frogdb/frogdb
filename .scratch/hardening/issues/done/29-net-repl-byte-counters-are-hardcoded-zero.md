# `total_net_repl_input_bytes` / `total_net_repl_output_bytes` are hardcoded `0`

Status: done
Type: bug (observability accuracy)
Severity: likelihood 3/3 (every `INFO stats` on every node reports both), consequence 1/3 (an
operator sizing replication bandwidth reads two zeros; unlike the backlog geometry, `0` here is not
self-contradictory, merely absent-shaped-like-data) — score 4
Area: replication / INFO stats

## Problem

Both fields are the literal `0` in **both** `INFO stats` renderers, with no source behind either:

- `frogdb-server/crates/server/src/info/sections.rs:343-344` (connection-level)
- `frogdb-server/crates/server/src/commands/info.rs:358-359` (shard-local, what a script sees)

Filed by FM-REPLICATION-058's audit of the replication-owned `stats` fields, which named them as
"issue 20's position: a hardcoded literal shaped like data". Closing issue 20 (FM-REPLICATION-059)
did **not** reach them, and the difference is the whole point: the backlog geometry had a live source
inside the process and only needed publishing, whereas these two have no counter anywhere. Nothing
counts replication bytes today.

`0` on an idle standalone node is correct, which is what makes this milder than issue 20 — but on a
primary that has streamed gigabytes it is a confident wrong number, and an operator comparing it
against `total_net_input_bytes` concludes replication costs nothing.

## What would have to exist

Redis counts these at the connection layer: every byte read from / written to a link whose client is
flagged as a replica or as this node's master. FrogDB has no equivalent accounting point:

- **Output** would have to cover the primary's broadcast fan-out *and* the full-sync payload, which
  take different paths (frame lane vs. `CheckpointStreamCodec`). Summing only the frame lane would
  undercount every resync by the size of the dataset — worse than `0`, because it looks plausible.
- **Input** on a replica is the applied stream plus the received checkpoint, from the ingest loop.
- The counters are lifetime totals and must be zeroed by `CONFIG RESETSTAT` (FM-REPLICATION-058) once
  they are real, and reported identically by both renderers (FM-REPLICATION-050's rule).

## Open question

Two shapes, and the cheap one is a trap:

1. **Count at the offset coordinator.** `master_repl_offset` already counts RESP command-stream bytes,
   so `total_net_repl_output_bytes` could be derived from it almost free — but that is bytes
   *stamped*, not bytes *sent*, it counts once regardless of how many replicas received them, and it
   excludes the full-sync payload entirely. It would be a differently-named copy of an existing
   field, which is the FM-REPLICATION-050 mistake ("a quantity wearing another's name").
2. **Count at the socket.** An `AtomicU64` pair incremented where replication bytes actually cross
   the wire (frame writer, checkpoint sender, replica ingest reader), published on the tracker like
   the sync counters and the backlog handle. Honest, and the only shape that can cover both lanes;
   costs a relaxed atomic add per write on the replication path.

Interim option, if neither is scheduled: **drop the two fields** rather than print a constant. Per
`feedback_observability_accuracy`, missing data beats misleading data — but dropping a field Redis
emits is client-visible and belongs in the deviations table, so it is a decision, not a cleanup.

## Tests that should exist

- `repl_output_bytes_grow_when_a_write_is_forwarded`
- `repl_output_bytes_include_the_full_sync_payload` — the undercount trap above.
- `repl_input_bytes_grow_on_the_replica_as_it_applies`
- `both_info_renderers_report_the_same_repl_byte_counters`
- `config_resetstat_zeroes_the_repl_byte_counters`

## Spec impact

A new row (the counters themselves), and FM-REPLICATION-058's audit paragraph loses its "not in this
row's position" caveat once they become resettable statistics.

## Resolution

Took option 2 from the open question above (count at the socket), not the offset-coordinator shortcut
or the drop-the-fields fallback. `NetByteCounters` (`frogdb-server/crates/replication/src/net_bytes.rs`)
is an `AtomicU64` pair — `output`, `input` — with `record_output`/`record_input` methods that no-op on
`bytes == 0` and add with `Ordering::Relaxed`, matching the existing `SyncCounters` pattern. It lives on
`ReplicationTrackerImpl` next to the sync counters and the backlog handle, vended via
`net_bytes_handle()` (fully `pub`, since it is recorded from `frogdb-server` call sites as well as from
inside `frogdb-replication`) and read back as a `NetByteCountersSnapshot` via `net_bytes()`.

**Per-field disposition** (both real now, neither hardcoded, neither dropped):

- `total_net_repl_output_bytes` — real, incremented at every point a primary actually writes
  replication bytes to a socket: the live frame-forwarding lane (`forward_frame`, only on
  `Forward::Continue`, never on a dropped/broken link), the backlog-replay tail a resuming replica
  reads (a separate write path from the live lane inside `start_streaming`, and the one place this
  fix caught an undercount mid-implementation — it was originally uninstrumented), and the full-sync
  payload, recorded once as `FullSyncMetadata.rdb_size` after `write_metadata` succeeds in both
  `stream_checkpoint` and `stream_live_dataset`. This directly avoids the "count at the offset
  coordinator" trap named in the issue: `master_repl_offset` counts bytes *stamped*, once, regardless
  of replica fan-out, and excludes the full-sync payload entirely — a differently-named copy of an
  existing field, not real output accounting.
- `total_net_repl_input_bytes` — real, incremented at every point a replica actually receives
  replication bytes: the full-sync payload, recorded as the checksum-verified `rdb_size` after
  `receive_snapshot`/`receive_checkpoint`'s checksum (or `stager.commit`) succeeds — never before
  verification — and the frame stream, a `buf.len()` before/after diff around `drain_frames`'s decode
  loop in `replica/streaming.rs`, recorded at both the channel-closed-early and loop-completion exit
  points so a mid-stream disconnect still counts what was actually decoded.

Both counters are published on the tracker shared by both `INFO stats` renderers
(`info/sections.rs`'s `StatsSection` and `commands/info.rs`'s `build_stats_info`) through one
`net_byte_fields()` list, in Redis's original field position (`total_net_output_bytes` →
`total_net_repl_{input,output}_bytes` → `instantaneous_input_kbps`), so the two renderers cannot
report a different pair for the same tracker state. `CONFIG RESETSTAT` zeroes both via
`tracker.reset_net_bytes()`, alongside `reset_sync_counters()`, without touching the replica registry,
offset, or lag history. Both boot-time (`replication_init.rs`) and runtime `REPLICAOF`/failover-driven
demotion (`role_manager.rs`'s `RealReplicaStreamer` + `cluster_init.rs`) wire the replica handler to the
same tracker-owned handle, so a node counts real bytes in every role-transition path, not just the
happy boot path.

All five named tests exist and pass: `repl_output_bytes_grow_when_a_write_is_forwarded`,
`repl_output_bytes_include_the_full_sync_payload`, `repl_input_bytes_grow_on_the_replica_as_it_applies`
(all in `frogdb-replication`), `both_info_renderers_report_the_same_repl_byte_counters`,
`config_resetstat_zeroes_the_repl_byte_counters` (in `frogdb-server`), plus companion tests
(`net_bytes_handle_is_shared_with_the_tracker_snapshot`,
`resetting_the_net_bytes_leaves_the_replica_registry_and_offset_alone`,
`both_info_renderers_report_zeros_after_the_repl_byte_counters_are_reset`).

Spec: new row `FM-REPLICATION-063` added to
`specs/replication.md`; FM-REPLICATION-058's stats-field audit
paragraph updated to say these two fields are real, resettable counters instead of "not in this row's
position." `just lint-failure-modes` passes (247 failure modes, 1226 test references, 1226 tags).

Verification: `just check`, scoped `cargo clippy -p frogdb-replication --tests -- -D warnings` and
`cargo clippy -p frogdb-server --tests -- -D warnings` (both clean, zero warnings), full
`just test frogdb-replication` (378/378) and `just test frogdb-server` (1957 passed, 4 skipped, 0
failed) all green. `just mutants-diff frogdb-replication` run against the touched locked crate; see
the session's final report for the result.

**Pre-existing, unrelated gap noted but not fixed** (out of scope for this issue, flagged per
CLAUDE.md): the spec file's "Scope, part N" prose paragraphs near the top stop at "part six" (through
row 057); rows 058-062 already weren't reflected there before this change, and row 063 isn't either.

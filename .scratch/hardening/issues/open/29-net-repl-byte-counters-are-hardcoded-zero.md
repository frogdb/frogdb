# `total_net_repl_input_bytes` / `total_net_repl_output_bytes` are hardcoded `0`

Status: needs-triage
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

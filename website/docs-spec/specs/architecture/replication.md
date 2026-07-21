# Spec: architecture/replication.md
Status: update (substantial correction — several central mechanisms are documented wrong)
Audiences: A3 (architecture-curious), A5 (contributors)

Goal: The reader understands how FrogDB replicates a dataset from a primary to
its replicas: the replication identity model (replication ID + secondary ID
across promotion), the PSYNC handshake, how a new replica gets a full copy
(RocksDB checkpoint transfer with integrity verification), how ongoing changes
stream (logical command replication over a bounded broadcast channel), what
happens to a lagging replica (disconnect → full resync, not unbounded
buffering), and how write quorum (`WAIT`, `min-replicas-to-write`) and
split-brain fencing actually behave. The reader leaves able to map every claim
to a source file, and able to tell which behaviors are Redis-compatible versus
FrogDB-specific.

Not in scope:
- Operator setup, `REPLICAOF`/`ROLE` usage, failover runbooks, WAL retention
  tuning — that is Operations → Replication (link, do not duplicate).
- Cluster topology consensus (Raft) — that is architecture/clustering. This page
  covers the data-plane PSYNC/checkpoint mechanism that the cluster reuses.
- The persistence/WAL engine internals — architecture/persistence owns RocksDB
  and durability modes; link for the checkpoint source.

Sources of truth (author MUST read and re-verify every claim below against
current source before writing — the snapshot here is a starting point, not
authority):
- `frogdb-server/crates/replication/src/state.rs` — replication ID generation,
  secondary ID on promotion, partial-sync eligibility.
- `frogdb-server/crates/replication/src/replica_session.rs` — primary side:
  FULLRESYNC (`handle_full`), checkpoint streaming (`stream_checkpoint`),
  `+FULLRESYNC`/`+CONTINUE` responses, per-session frame channel, write timeout.
- `frogdb-server/crates/replication/src/replica/connection.rs` — replica side:
  handshake sequence (REPLCONF/PSYNC), checkpoint receive + checksum verify.
- `frogdb-server/crates/replication/src/replica/streaming.rs` — replica decode +
  re-execute of streamed commands.
- `frogdb-server/crates/replication/src/primary/mod.rs` — `broadcast_command`,
  the bounded broadcast channel, offset counter.
- `frogdb-server/crates/replication/src/primary/ring_buffer.rs` — split-brain
  divergent-write ring buffer (NOT the replication backlog).
- `frogdb-server/crates/replication/src/frame.rs` — `ReplicationFrame`,
  `serialize_command_to_resp`.
- `frogdb-server/crates/replication/src/fullsync.rs` — SHA256 helpers, checkpoint
  metadata wire format.
- `frogdb-server/crates/replication/src/tracker.rs` — `wait_for_acks` (used by
  `WAIT`).
- `frogdb-server/crates/replication/src/split_brain_log.rs` — divergent-write
  audit log.
- `frogdb-server/crates/server/src/replication_quorum.rs` — `ReplicationQuorumChecker`
  (CLUSTERDOWN write fencing).
- `frogdb-server/crates/config/src/replication.rs` — `min_replicas_to_write`,
  `min_replicas_timeout_ms`, `self_fence_on_replica_loss` (default values).
- `frogdb-server/crates/server/src/commands/info.rs` — `INFO replication` fields
  (the real observability surface).

Existing content: current `architecture/replication.md`. Mine the framing and the
diagrams, but the following claims on it are FACTUALLY WRONG and MUST be corrected
(each verified against source, cite the finding in the drift note, not in prose):

1. **"Concurrent FULLRESYNC → single shared checkpoint amortized to all
   replicas."** FALSE. `handle_full` creates a *per-replica* checkpoint directory
   (`fullsync_<replica_id>`); there is no shared/amortized checkpoint. Remove this
   claim or replace with the real per-replica behavior.
2. **Checkpoint format: "SHA256 of all files", "`$40` checksum footer".** WRONG on
   two counts. The checksum is SHA256 but computed as a *hash of hashes* — SHA256
   over the concatenation of `(filename, per-file-SHA256)` pairs — and is 32 bytes
   / 64 hex chars, transported inside a colon-delimited metadata blob
   (`<rdb_size>:<checksum_hex>:<replication_id>:<replication_offset>`), not a fixed
   `$40` frame. Correct the framing description against `stream_checkpoint` /
   `fullsync.rs`; the real header is `$FROGDB_CHECKPOINT`, a file count, then
   per-file `name`/`size`/bytes.
3. **"WAL streaming uses RocksDB `GetUpdatesSince()`."** FALSE — no such call
   exists in the repo. Live replication is *logical command replication*: each
   write is serialized to RESP (`serialize_command_to_resp`) into a
   `ReplicationFrame`, broadcast over a tokio broadcast channel, and re-executed
   on replicas (`replica/streaming.rs`). Rewrite the "WAL Streaming" section to
   describe command replication, not physical WAL/SST shipping. (Note the
   architectural implication for A3: replicas re-execute commands, so
   nondeterministic commands must be rewritten to deterministic form on the
   primary before broadcast — verify how the primary handles this and document it
   if present.)
4. **"TCP backpressure, no buffer limits."** FALSE. The broadcast channel is
   bounded (verify capacity — 10000 frames at spec time); a replica that lags past
   the buffer receives `RecvError::Lagged` and is **disconnected**, forcing a
   FULLRESYNC — this is the opposite of "slow replicas just slow the primary
   down." There is also a per-session bounded frame channel and an optional
   per-write `write_timeout_ms` that drops a stalled replica. Rewrite the
   "TCP Backpressure Design" section to describe bounded-buffer + disconnect-on-lag
   honestly, and drop the "deliberate departure from Redis / no full-sync loop"
   claim (FrogDB in fact resyncs a lagged replica, like Redis).
5. **Synchronous replication `execute_with_sync_replication`, `-NOREPL`,
   `replica_ack_timeout`.** NOT IMPLEMENTED as written. There is no write-path
   code that returns `-NOREPL`; `min_replicas_to_write` is config only. The real
   surfaces are: the **`WAIT`** command (uses `tracker::wait_for_acks`, returns an
   integer ack count, never errors on timeout) and the separate
   `ReplicationQuorumChecker`, which fences writes with **`-CLUSTERDOWN`** (not
   `-NOREPL`) when quorum/replica-loss fencing is armed. Rewrite this section
   around `WAIT` + `-CLUSTERDOWN` fencing + config `min_replicas_to_write` /
   `min_replicas_timeout_ms`. Remove the fabricated Rust snippet and the `-NOREPL`
   error entirely unless a grep proves the string exists.
6. **`NodeRole { Primary, Replica, Standalone }`.** The `NodeRole` enum lives in
   the **cluster** crate and has only `Primary` and `Replica` (no `Standalone`).
   The replication crate's own state types are `Phase` (in `replica_session.rs`)
   and a replica `ConnectionState`. Do not present a `Standalone` variant; if the
   page needs a standalone concept, describe it as "no replication configured,"
   not an enum variant.
7. **Replication metrics table (`last_write_seq`, `pending_sync_writes`,
   `norepl_errors`, `replica_ack_wait_time`).** These named metrics do not exist.
   The real observability surface is the `INFO replication` section
   (`connected_slaves`, `master_repl_offset`, per-slave lines, etc.). Replace the
   table with the actual INFO fields, or link to Operations → Replication /
   Reference → Metrics (S3).

Claims that are CORRECT and can stay (still re-verify): replication ID is 20
random bytes → 40 hex (`generate_replication_id`, `REPLICATION_ID_LEN = 40`);
secondary ID is captured on promotion (`new_replication_id()` sets
`secondary_id`/`secondary_offset`, the latter an `i64` with `-1` sentinel);
full sync uses a RocksDB checkpoint (with an RDB fallback when no RocksStore);
handshake sends `REPLCONF listening-port`, `REPLCONF capa …`, `REPLCONF
frogdb-version`, then `PSYNC`; primary answers `+FULLRESYNC <id> <offset>` or
`+CONTINUE <id>`. Note the handshake has **no AUTH step** in the code path — do
not document one unless verified. Note the capabilities line is sent as
`capa eof capa psync2` (two `capa` tokens) — document the actual bytes.

Structure (H2/H3 outline):

## Overview
- One paragraph: primary accepts writes; replicas get an initial full copy then a
  continuous stream of replicated commands; roles and offsets are tracked per
  connection. Cross-link Operations → Replication for setup. State plainly that
  live replication is *logical command replication*, and initial sync is a
  *RocksDB checkpoint transfer* — the two mechanisms are different.

## Replication identity
- Replication ID: 20 random bytes → 40 hex, generated at startup / on promotion.
  Secondary ID + secondary offset: what promotion captures and why it lets a
  primary's former replicas re-attach via partial sync. Reference
  `can_partial_sync`. Keep the promotion diagram from the current page (it is
  correct in spirit); fix any field names to `secondary_id`/`secondary_offset`.

## PSYNC handshake
- The real REPLCONF/PSYNC sequence and the exact capability tokens. `+FULLRESYNC`
  vs `+CONTINUE` and what each triggers. No AUTH step. One sequence diagram,
  corrected to match source.

## Full synchronization (checkpoint transfer)
- When it happens (new replica, or lag past the buffer). RocksDB checkpoint
  created per-replica (not shared), streamed as `$FROGDB_CHECKPOINT` + file count
  + per-file entries + colon-delimited metadata trailer. Integrity: per-file
  SHA256 rolled into a hash-of-hashes checksum, verified on the replica; mismatch
  → retry. RDB fallback path when there is no RocksStore. Correct the framing
  block wholesale.

## Command streaming (steady state)
- Logical command replication: write → RESP-serialized `ReplicationFrame` →
  bounded broadcast channel → replica decode + re-execute. The offset counter and
  how it maps to `master_repl_offset`. Determinism requirement (verify + document
  how the primary ensures replicated commands are deterministic).

## Backpressure and lag
- Honest model: bounded broadcast buffer (state the capacity from source),
  per-session bounded frame channel, optional `write_timeout_ms`. A replica that
  falls behind the buffer is **disconnected and must full-resync**. Explicitly
  retract the old "no buffer limits / avoids the full-sync loop" framing.

## Write quorum and fencing
- `WAIT numreplicas timeout` → `wait_for_acks`, returns the ack count, never
  errors. `min-replicas-to-write` / `min-replicas-timeout-ms` config semantics
  (verify what actually enforces them). `ReplicationQuorumChecker` fencing →
  `-CLUSTERDOWN` when armed and no fresh streaming replica. No `-NOREPL`.

## Split-brain handling
- After a partition heals, a demoted former-primary's divergent writes (captured
  in `primary/ring_buffer.rs`) are discarded and recorded to a
  `split_brain_discarded_<ts>.log` audit file (`split_brain_log.rs`:
  `SplitBrainLogHeader` fields + RESP bytes of discarded ops). Clarify this is a
  post-mortem audit artifact, not a hot-path fence. Cross-link
  architecture/consistency for the guarantee-level discussion.

## Observability
- The `INFO replication` fields that expose replication state; link Reference →
  Metrics (S3) and Operations → Replication. No invented metric names.

Generated data: none embedded. Version/toolchain strings (if any) via
`versions.json` (S6). Metric names, if referenced, must come from the Metrics
reference (S3), not be hand-typed.

Drift guards:
- S7 code-path check must cover every `crates/replication/...` and
  `crates/server/src/replication_quorum.rs` path cited here.
- No hardcoded buffer capacities in prose unless traceable to a named const; if a
  capacity is stated (e.g. broadcast channel size), cite the source line in a
  comment for reviewers and re-verify on each edit — these are `channel(N)`
  literals, not config, so they drift silently.
- Every error code named (`-CLUSTERDOWN`, `-READONLY`) must exist in source and
  match the glossary error table; `-NOREPL` must NOT appear unless grep proves it.
- The determinism-of-replicated-commands claim must be re-verified against the
  primary broadcast path on each edit; it is the correctness linchpin of logical
  replication.

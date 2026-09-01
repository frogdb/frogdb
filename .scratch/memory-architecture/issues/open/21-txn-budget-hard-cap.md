# 21: transaction buffering budget and hard cap

Status: ready-for-agent
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R14
Area: frogdb-txn + frogdb-persistence + frogdb-replication-runtime + frogdb-memory
Phase: 6 — polish. **Locked-area work: spec-first discipline applies.**

## Why

A `MULTI` client can queue commands forever; EXEC stages a write batch proportional to the
queued work; a replica applying a primary's txn buffers the pending group until commit. Today
only the replica side is capped (`replica_txn_max_commands` / `replica_txn_max_bytes`, wired
at `role_manager.rs:738` and `server/subsystems.rs:500`) — the client-facing side is
open-ended, and `FM-PERSISTENCE-001` currently rows this as an explicit *non-guarantee*
(unbounded txn staging acknowledged, not bounded). R14's ruling replaces the non-guarantee
with a bound: all txn buffering charges `Subsystem::TxnBuffering` (the variant already exists
with exactly this doc comment, `frogdb-server/crates/memory/src/budget.rs:37–39`, filed by
[issue 05](../)), with a configurable hard cap, default ~512MB, that **aborts the
transaction before EXEC applies anything**.

Redis has no equivalent (a big MULTI can OOM the server; `proto-max-bulk-len` caps single
args only) — this is a documented Redis deviation of the "deviation is an improvement" kind.

## What to build

### 1. One budget, three charge sites

Open `Subsystem::TxnBuffering` per core (`Disposition::Shed` — the shed action is "abort this
txn", never block the shard). Charge sites:

- **Queued MULTI commands** (frogdb-txn: `push_queued_command` path, `txn/src/lib.rs` /
  `state.rs`): charge per queued command's retained bytes at queue time. Refusal → the queue
  enters the error state Redis uses for queue-time failures (EXEC then aborts with EXECABORT),
  plus a clear `-OOM transaction buffer limit` style error on the offending QUEUE reply.
  Precedent for the charge shape: the tracking table's **one long-lived grow/shrink Charge
  per structure** from issue 05 (not per-entry charges) — same pattern here: one `Charge` per
  in-flight txn, `grow()` per queued command, released on EXEC/DISCARD/abort.
- **Staged write batches** (frogdb-persistence: the WriteBatch staged between EXEC validation
  and apply): `grow()` the same txn's charge as the batch builds. Refusal mid-staging →
  abort before any apply; the all-or-nothing property is exactly why the cap must trip
  *before* the first apply, which is the FM row's forcing shape.
- **Replica-side pending groups** (frogdb-replication-runtime): the existing
  `replica_txn_max_commands/bytes` caps generalize onto the same budget — keep the config
  knobs as the per-txn ceiling, but the bytes now also charge `TxnBuffering` so the broker
  breakdown sees them. Replica-side refusal semantics are already specified (spec-first:
  check `specs/replication.md`'s existing rows before touching behavior — the *observable*
  behavior must not change, only the accounting seam).

### 2. Config

`txn-buffer-limit <bytes>` (default 512MB, 0 = the old unbounded behavior **not offered** —
minimum enforced floor instead, since "unbounded" is the bug). Runtime-adjustable via CONFIG
SET (`Budget::set_limit` exists). Per-core limit vs global: per-core (broker is per-core);
document that the effective server-wide bound is limit × cores.

### 3. Spec-first sequencing (locked areas)

`frogdb-txn` (gate 0.90) and `frogdb-persistence` (0.85) are locked; so is replication.
Order of work:

1. Rewrite `FM-PERSISTENCE-001` from non-guarantee to bound: "txn staging memory is charged
   and capped; a txn exceeding the cap aborts with no partial apply", with forcing tests
   named. Add the mirror row in `specs/txn.md` for queue-time refusal (EXECABORT semantics).
2. Failing tests (in the owning crates — mutation gates need in-crate forcing tests).
3. Implementation.
4. `just lint-spec` green; `just mutants-diff frogdb-txn` / `frogdb-persistence` before push.

### 4. Deviation documentation

Website: document the cap, the error string, and the Redis difference on the existing
deviations page.

## Acceptance criteria

- [ ] All three sites charge one `TxnBuffering` charge per txn; broker breakdown shows txn
      bytes under a large MULTI.
- [ ] Queue-time refusal: MULTI exceeding the cap gets the OOM-style error at queue time and
      EXECABORT at EXEC; nothing applied.
- [ ] Staging-time refusal: a txn whose staged batch exceeds the cap aborts with **zero**
      applied writes (forcing test drives the cap between validation and apply).
- [ ] Replica-side observable behavior unchanged (existing replication tests green); bytes
      visible in the breakdown.
- [ ] Spec rows updated + `just lint-spec` green; mutation diff run on touched locked crates.
- [ ] Redis-deviation doc updated.
- [ ] DISCARD/RESET/connection-drop release the charge (test each path — leaked charges are
      the classic bug here; `Charge` releases on drop, so tie it to txn state lifetime).

## Test boundary

Level 2 in-crate for the charge lifecycle and refusal paths (mutation-gate requirement).
Level 3 socket test for the client-visible error strings and EXECABORT flow. Turmoil for the
replica-side accounting if any behavior seam moves (it should not).

## Spec rows at R15

This issue *writes* the rows (FM-PERSISTENCE-001 replacement + txn queue-refusal row);
[issue 22](../) locks `specs/memory.md` which cross-references them.

## Out of scope

Capping single-command argument size (separate `proto-max-bulk-len` concern), WAL channel
accounting (`Subsystem::WalChannel`, separate), scripting/function atomicity buffers,
changing replica cap defaults.

## Depends on

[Issue 05](../) broker (done). Independent of phases 4–5; sequenced in phase 6 because
it is spec-heavy and benefits from the budget seam being battle-tested by
[issue 18](../) first.

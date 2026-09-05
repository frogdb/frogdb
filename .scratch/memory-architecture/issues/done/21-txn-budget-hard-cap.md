# 21: transaction buffering budget and hard cap

Status: done
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
## Resolution

Landed 2026-09-04 on `mem-arch-integration` (picks `bf3d177f`, `ad89f853`, 2 commits).

What shipped: one `Subsystem::TxnBuffering` budget per shard (`Disposition::Shed`, default
512 MiB, floor 1 MiB) minted in `init_infrastructure`, adopted by the shard worker and handed
to the replica streamer. Three charge sites: queue-time in `frogdb-txn` (`TransactionState`
holds one `Charge` per open `MULTI`, grown by each command's `retained_bytes`; refusal answers
`-OOM transaction buffer limit exceeded`, `EXEC` then `-EXECABORT`, FM-TXN-054); shard
pre-apply in `frogdb-core` `execute_transaction` (charge after every admission gate, before the
apply loop; a refusal applies nothing — FM-PERSISTENCE-001's "unbounded staging" non-guarantee
rewritten into FM-PERSISTENCE-062); replica pending groups in `frogdb-replication`
(FM-REPLICATION-045). The connection releases its queue-time charge when the batch is handed
to its shard, so the bytes are counted once. Config `memory.txn-buffer-limit` (mutable;
`CONFIG SET` reaches every shard's budget). Website deviation note, spec mirrors,
`spec-gen.py` AREAS entry for `memory`.

Review: round 0 (1 Critical, 3 Important, 8 Minor) — the Critical was a double charge across
`EXEC` on single-shard nodes (connection charge held through the shard round trip, shard
charging the same refcount-shared bytes on the same budget; hidden by the 4-shard harness).
Fix round 1: charge released before the target-shard round trip, single-shard socket test
(proven to fail without the fix), breakdown/metrics-row tests, spec rows corrected. Re-review
r1: all findings addressed, no new Critical/Important (one new Minor, carried below). Gates: full `frogdb-server` suite 2145/2145, workspace clippy,
lint-spec (318 rows / 1781 refs), lint-gates, spec-gen/docs-gen checks, mutants-diff on
frogdb-txn (7: 3 caught, 4 unviable) and frogdb-replication (7: 6 caught, 1 unviable).

Deviations from the brief, for human sign-off:

- **Shard-side charge site is `frogdb-core`, not `frogdb-persistence`** — the staged batch
  lives in `frogdb-core/src/shard/execution.rs`. FM-PERSISTENCE-062's forcing tests therefore
  sit outside the `frogdb-persistence` mutation gate (recorded at the row). This is the
  established convention — 69 `// FM-PERSISTENCE-` tags already live in `frogdb-core`,
  including FM-PERSISTENCE-001's own shard-side tests — so the gap is systemic to the
  persistence area's crate boundary, not specific to this row. Options: extend the persistence
  gate to `frogdb-core/src/shard/{execution,persistence}.rs`, or re-home the shard-side rows.
- **Replica refusal takes the ceiling-breach path** (group abandoned, history ended, link
  through full resync). The brief said "observable behavior must not change"; PRD R14 puts
  replica pending groups explicitly under the hard cap, and a group the replica cannot hold
  has only that path. Client `MULTI` pressure on a core can now trigger it — the budget is
  per core and shared with connections homed there. Accepted with the spec row
  (FM-REPLICATION-045) stating it; flagging for explicit sign-off.

Known gaps carried (follow-up material): a `MULTI` whose commands are all deferred
(connection-level / server-wide) with no live watches makes no shard round trip, so `EXEC`
releases the queue-time charge and nothing re-charges the bytes while the deferred commands
run (bounded: one in-flight `EXEC` per connection, bytes were charged at queue time);
`frogdb_memory_budget_refusals_total{subsystem="txn_buffering"}` is not asserted end to end;
`queued_errors` strings on a poisoned `MULTI` are
uncharged (pre-existing in kind); `charge_transaction_buffer` repeats registry/action lookups
under `wal-failure-policy = rollback` (cost only, non-default); `ReplicaTxnBound::with_budgets`
rebuilds the bound (order-dependent in a builder chain, safe at current call sites);
`retained_bytes` counts argument bytes as owned, so a shared read-buffer slice is over-counted.

# A single-key write to a MIGRATING slot is `+OK`'d, then discarded

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/04 F1 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 3 · priority 20
Area: frogdb-server / cluster slot migration

## Context

`check_migrating_multikey` gates the TRYAGAIN/ASK logic on `keys.len() >= 2`, so a single-key
command never reaches it; the only ASK conversion runs *after* execution and only on a nil reply,
which a `SET` never produces. The source therefore serves the write locally and answers `+OK`, and
`SETSLOT NODE` then clears the source's copy — the acknowledged write is silently discarded. This
is the *normal* window of every slot migration: for each key, the interval between `MIGRATE`-ing it
and `SETSLOT NODE`. Redis `-ASK`s here regardless of key count and regardless of the command being
a write.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `frogdb-server/crates/server/src/connection/guards.rs:750` — `check_migrating_multikey` returns
  `None` when `keys.len() < 2`, so a single-key command never reaches the TRYAGAIN/ASK logic.
- `connection/dispatch.rs:630-680` — stage order is `ClusterSlotValidation` → `MigratingTryAgain`
  (multi-key only) → `Execute` → `migrating_ask_for_nil` at `:677`, i.e. the ASK conversion runs
  *post*-execution.
- `guards.rs:152` — `migrating_ask_for_nil` only fires on a nil response, and a `SET` returns
  `+OK`, not nil.
- `slot_migration/routing.rs:143` — the source's decision is `LocalServeMigrating` = serve locally.
- Redis `getNodeByQuery` returns `-ASK` whenever `missing_keys > 0 && existing_keys == 0`,
  regardless of key count and regardless of the command being a write.
- Completion clears the source's copy (`integration_cluster.rs:8614`,
  `test_slot_ownership_transfer_clears_source_keys`).
- **Why the existing test passes anyway**: `test_e2e_migration_concurrent_writes`
  (`integration_cluster.rs:5817-5942`) writes to `key2` only while it is *still present* on the
  source, so it never enters this window.

## What to fix

1. Move the migrating check ahead of `Execute` for single-key commands too, gated on a
   key-presence probe — which is exactly what the batch path already does at `guards.rs:910`.
2. Match Redis's rule: `-ASK` when `missing_keys > 0 && existing_keys == 0`, independent of key
   count and of read/write.
3. Keep `migrating_ask_for_nil` only as a backstop, or delete it if the pre-execution gate
   subsumes it.

## Acceptance criteria

- [ ] New cluster test: two-node cluster, open a migration for slot S, `MIGRATE` key K to the
      target, then from a client on the source run `SET K v2` — assert the source replies
      `-ASK <slot> <target-addr>`, **not** `+OK`. **Fails today.**
- [ ] After completing with `SETSLOT NODE`, `GET K` on the target returns the *last acknowledged*
      write — i.e. no acknowledged value is ever lost.
- [ ] `DEL K` and `INCR K` variants prove the rule is not `SET`-specific.
- [ ] The read variant stays covered by the existing
      `test_expired_key_during_migration_returns_ask`; the new test stays write-focused.

## Test boundary

**5** — the behaviour is the interaction of two live nodes' cluster state with a real redirect over
RESP; nothing below level 5 can produce a real MIGRATING slot with a partially-migrated keyspace.
The existing `test_e2e_migration_concurrent_writes` already does 90% of the setup.

## Depends on

Nothing — the cluster harness this needs already exists.

## Re-triage 2026-08-06

**Verdict: fixed**

This is the same defect the hardening campaign tracked as issue 40 / issue 31 §"Bug X", fixed by
**9128d086** *"fix(cluster): probe key presence on the migrating source at every arity"* (plus
clippy follow-up **fa699be2**). That commit did exactly what "What to fix" asks: `route_migrating_source`
(`server/src/slot_migration/routing.rs`) turns an open migration off an owned slot into a probe
instruction, `KeyPresence::migrating_source_reply` is the single presence→reply policy shared by the
per-command stage and `validate_queued_batch`, `check_migrating_multikey` became
`check_migrating_source` with **no arity gate** (so `guards.rs:750` no longer exists in that shape),
the stage was renamed `MigratingTryAgain` → `MigratingSourceProbe` and hoisted ahead of
`ConnectionCommand`, and the post-execution `migrating_ask_for_nil` hack was retired outright. The
contract is now
[FM-CLUSTER-028](../../../hardening/specs/cluster-failure-modes.md#fm-cluster-028--the-migrating-source-serves-what-it-still-holds-and-asks-what-has-moved)
(rewritten by the same commit — it previously documented the buggy behaviour), forced by 15 tests
including the end-to-end `test_single_key_write_on_migrating_source_asks_and_never_orphans`
(`server/tests/cluster_migration.rs:4130`), which covers criteria 1-2 verbatim (`SET` on a migrated
key → `-ASK <slot> <target>`, `GET` → `-ASK`, `EVAL` → `-ASK`, source `DBSIZE == 0` before *and*
after `SETSLOT NODE`, target keeps the migrated value). Criterion 3's `DEL`/`INCR` variants are not
spelled out by name, but the rule is now arity- and command-agnostic by construction, so `SET`-
specificity is structurally impossible; criterion 4's read variant is covered by the same test's
`GET` assertion.

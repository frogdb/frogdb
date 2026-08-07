# BCAST client-side-caching trackers are never invalidated on lazy expiry

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/02 F1 + proposals/02 F3 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 3 · priority 20 (02/F1); severity 3 · likelihood 4 · effort 1 · priority 16 (02/F3)
Area: frogdb-core / shard tracking + client-side caching

## Context

The two lazy-expiry drains in `ShardWorker` call the **default-mode-only** invalidation entry
point, not the both-modes seam that every other write path uses. A BCAST client caches `k`, `k`
expires lazily on the next touch, the client is never invalidated, and it serves the stale value
indefinitely — the exact failure mode client-side caching exists to prevent. BCAST plus TTLs is an
ordinary configuration and lazy expiry is the *default* reclamation path for any key the active
cycle does not sweep first. The companion finding (02/F3) explains why this escaped review:
`has_tracking_clients()` and `has_any_tracking_clients()` are provably the same function, so the
guard at the call site reads as mode-scoped and is not.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix — though the consolidation re-check corroborates the shape: `shard/types.rs:479
invalidate_keys_all_modes` has 37 tests and 16/18 regions, i.e. it is well tested *and* the
lazy-expiry callsite calls the other function.

## Evidence

- `crates/core/src/shard/worker.rs:732-733` and `:758-759` call
  `self.tracking.invalidate_keys(&[key.as_ref()], 0)` — the **default-mode-only** entry point
  (`shard/types.rs:471`, delegates to `tracking_table` only).
- The both-modes seam `invalidate_keys_all_modes` (`shard/types.rs:479`, documented as "The single
  seam every write path uses so the two modes can never drift apart again") is used by
  `post_execution.rs:686` and reached by active expiry via `run_internal_removal_effects`
  (`event_loop.rs:247`) — but **not** by these two lazy-expiry drains.
- A BCAST client's keys live in `broadcast_table`, never in `tracking_table`, so `invalidate_keys`
  sends nothing.
- **Why the existing test passes anyway**: the round-1 regression
  `integration_client.rs:1307 regression_lazy_expiry_invalidates_tracked_key` uses default mode and
  therefore passes.
- Camouflage (02/F3): `shard/types.rs:433` `has_tracking_clients() =
  !invalidation_registry.is_empty()`; `:439` `has_any_tracking_clients() = has_tracking_clients()
  || !broadcast_table.is_empty()`. But `register_broadcast` (`:451`) calls
  `invalidation_registry.register(conn_id, conn)` *before* `broadcast_table.register(...)` — so a
  BCAST client always makes the registry non-empty. The second disjunct at `:440` is unreachable
  and the two predicates are the same function. Consequences: (a) `post_execution.rs:672`'s
  early-return guard is fine by accident; (b) `invalidate_keys_all_modes`'s inner
  `if self.has_tracking_clients()` (`:483`) is always-true dead branching; (c) `worker.rs:732`
  reads as "only if someone is tracking by key" and is actually "if anyone is tracking at all".

## What to fix

1. Change `worker.rs:732` and `:758` to call `invalidate_keys_all_modes`.
2. Rename `has_tracking_clients` → `has_default_mode_tracking_clients`, implemented as
   `!tracking_table.is_empty()`, so a future copy-paste of the lazy-expiry drain is a
   compile-visible mistake; delete or fix the now-unreachable disjunct at `:440` and the always-true
   inner guard at `:483`.
3. Sweep for any other call site of the default-mode-only entry point.

## Acceptance criteria

- [ ] New server integration test beside the existing default-mode regression in
      `integration_client.rs`: `CLIENT TRACKING on BCAST PREFIX k` on a RESP3 connection;
      `SET k v PX 50`; wait past expiry; touch `k` from a second connection to force the lazy
      drain; assert an `invalidate` push carrying `k` arrives within a bounded wait.
      **Fails today.**
- [ ] Mirror case for the `emptied` branch (`worker.rs:758` — hash-field death emptying the
      container).
- [ ] Unit test on `ShardTracking` asserts `register_broadcast` alone makes **both** predicates
      true and `unregister` makes both false — documenting the equivalence and forcing the rename.
- [ ] After the rename, no call site reads `has_tracking_clients()` expecting mode scoping.

## Test boundary

**4** (server integration, `TestServer` + RESP3) for the invalidation assertion — the defect is a
*call-site* choice inside `ShardWorker`, so a unit test on `ShardTracking` cannot catch it, and the
shard driver has no seam to register a `TrackedConnection`. **1** for the predicate-equivalence
unit test (pure predicate algebra). Level 3 becomes available once the driver gains that seam.

## Depends on

Infrastructure I1 (`shard_driver` harness extension — specifically `drive_register_tracking`,
mirroring `drive_capture_keyspace`) — issue 01, `.scratch/testing-improvements-round2/issues/`.
With it, the invalidation assertion moves from level 4 to level 3.

## Re-triage 2026-08-06

**Verdict: still-valid**

Confirmed live; every claim reproduces, only line numbers moved. Both lazy-expiry drains still call
the default-mode-only entry point: `crates/core/src/shard/worker.rs:770-771` (whole-key `purged`
branch, issue cited `:732-733`) and `:796-797` (hash-`emptied` branch, issue cited `:758-759`), each
guarded by `has_tracking_clients()`. `ShardTracking::invalidate_keys` is now `shard/types.rs:480-483`
(cited `:471`) and still delegates to `tracking_table` alone; the both-modes seam
`invalidate_keys_all_modes` is `types.rs:488-499` (cited `:479`) and is reached only from
`post_execution.rs:686`, which the *active* sweep hits via `run_internal_removal_effects`
(`post_execution.rs:561-603` → `run_write_effects`) — the lazy drains bypass it entirely. The
camouflage finding (02/F3) is intact: `types.rs:442-444` `has_tracking_clients() =
!invalidation_registry.is_empty()`, `:448-450` `has_any_tracking_clients() = has_tracking_clients()
|| !broadcast_table.is_empty()`, and `register_broadcast` (`:458-466`) still calls
`invalidation_registry.register` before `broadcast_table.register`, so the second disjunct at `:449`
is unreachable and the inner guard at `:492` is always true. The only lazy-expiry regression is
still default-mode (`crates/server/tests/integration_client.rs:1309
regression_lazy_expiry_invalidates_tracked_key`, cited `:1307`); the BCAST tests at `:1418-1600` all
exercise ordinary writes, none an expiry. Core is not one of the six locked areas, so no FM row
covers this. Distinct from issue 22 (expiry not checked *before reads* — a store-level liveness
question) and from issue 66 (tracking-table LRU ordering, a memory bound); both remain open and
neither subsumes this call-site choice.

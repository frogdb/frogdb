# allow_cross_slot_standalone must not relax MULTI/EXEC cross-shard atomicity — unguarded by test

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: transactions

## Context

No test pins that enabling standalone cross-slot single-key-op support does NOT extend to MULTI/EXEC transactions. `fold_transaction_keys` (`frogdb-server/crates/server/src/connection/state.rs:859-868`) and `TransactionTarget::resolve` (`state.rs:42-47`) unconditionally map a shard-spanning transaction to `Err(crossslot())` — they never consult the config flag. This is a load-bearing architectural guarantee: VLL gives cross-shard multi-key *ops* execution atomicity, but that is deliberately withheld from transactions, which have no cross-shard rollback story. The only standalone cross-shard-MULTI CROSSSLOT test today is the WATCH-fold control case (`frogdb-server/crates/server/tests/integration_transactions.rs:360-380`); there is no test that enables the config flag and proves MULTI over cross-shard keys still CROSSSLOTs, and no plain-key (no WATCH) cross-shard MULTI CROSSSLOT test for the default config either.

The config field is `allow_cross_slot_standalone: bool` (`frogdb-server/crates/config/src/server.rs:38-41`, default via `default_allow_cross_slot_standalone()`), confirmed as the real field name — the audit's original writeup flagged this name as unverified; it is correct as written. It is threaded through `ClusterNodeConfig`/test harness as `allow_cross_slot_standalone` (`frogdb-server/crates/test-harness/src/server.rs:142,182,369`) and surfaces at the connection layer as `allow_cross_slot` (`frogdb-server/crates/server/src/connection/deps.rs:172`, `connection.rs:131`, wired from `subsystems.rs:449`).

Expected behavior (least-surprise, arch doc): cross-shard MULTI returns CROSSSLOT even when standalone cross-slot single ops are enabled via `allow_cross_slot_standalone=true`. Without a test, a future refactor that threads the config into `fold_transaction_keys` would silently permit a non-atomic, non-rollback cross-shard transaction — a correctness/data-integrity regression with no failing test to catch it.

## What to build

- Integration test: start standalone (multi-shard, e.g. 4 shards) with `allow_cross_slot_standalone: true`; run `MULTI; SET {a}...; SET {b}...` with keys on different shards; assert EXEC (or queue time, matching actual behavior) returns CROSSSLOT.
- Mirror test with `allow_cross_slot_standalone: false` (should already CROSSSLOT — confirms baseline).
- Add a plain-key (no WATCH) cross-shard MULTI CROSSSLOT test under default config — currently the only coverage is via the WATCH-fold control case.

## Acceptance criteria

- [ ] Test proves `allow_cross_slot_standalone=true` does not relax MULTI/EXEC cross-shard CROSSSLOT rejection.
- [ ] Test proves the `false` (default) case also CROSSSLOTs (regression baseline).
- [ ] Plain-key, non-WATCH cross-shard MULTI CROSSSLOT test exists (currently missing — only WATCH-fold path is covered).
- [ ] Tests fail if `fold_transaction_keys`/`TransactionTarget::resolve` were changed to consult `allow_cross_slot_standalone` and permit cross-shard transactions.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/src/connection/state.rs:42-47` (`TransactionTarget::resolve`)
- `frogdb-server/crates/server/src/connection/state.rs:859-868` (`fold_transaction_keys`)
- `frogdb-server/crates/server/tests/integration_transactions.rs:360-380` (existing WATCH-fold CROSSSLOT control case)
- `frogdb-server/crates/config/src/server.rs:38-41,91,117` (`allow_cross_slot_standalone` config field — name confirmed correct)
- `frogdb-server/crates/server/src/connection/deps.rs:172`, `connection.rs:131`, `server/subsystems.rs:449` (config threading to connection-layer `allow_cross_slot`)

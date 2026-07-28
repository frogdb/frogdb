# allow_cross_slot_standalone must not relax MULTI/EXEC cross-shard atomicity — unguarded by test

Status: done
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

## Resolution

Resolved 2026-07-23 by **pinning** the invariant with tests — no bug found; the guarantee holds by construction.

**Actual config field name.** Confirmed as `allow_cross_slot_standalone: bool`
(`frogdb-server/crates/config/src/server.rs:38-41`, default `false` via
`default_allow_cross_slot_standalone()`). The audit's guess was correct as written; the earlier
"may be off" warning was unfounded.

**Observed behavior (verified).** The MULTI co-location rule lives entirely in
`TxnSlotAccumulator` (`state.rs`): `fold_transaction_keys` → `add_keys` → `fold_shard` promotes a
shard-spanning transaction to `TransactionTarget::Multi`, and `TransactionTarget::resolve` maps
`Multi → Err(redirect::crossslot())` at EXEC. Neither path takes `allow_cross_slot_standalone` (or
any config) as input — the flag is threaded only to the single-op scatter-gather layer, never to
the transaction fold. So cross-shard MULTI CROSSSLOTs at EXEC regardless of the flag. Confirmed
live: a cross-shard MSET scatter-succeeds with the flag on, while a MULTI over the same two shards
still CROSSSLOTs and lands zero writes (no partial commit).

**Fix-or-pin outcome: PIN.** Invariant already held; added regression tests in
`frogdb-server/crates/server/tests/integration_transactions.rs`:
- `test_multi_cross_shard_plain_keys_crossslot_default_config` — plain-key, no-WATCH cross-shard
  MULTI under default config CROSSSLOTs (fills the previously-missing plain-key coverage; only the
  WATCH-fold control case existed).
- `test_multi_cross_shard_crossslot_with_allow_cross_slot_standalone` — flag **on**: single MSET
  scatters OK, but cross-shard MULTI still CROSSSLOTs and seed values survive unchanged.
- `test_multi_cross_shard_crossslot_with_flag_disabled` — flag **off** (explicit) baseline mirror.
- `test_multi_single_shard_commits_with_allow_cross_slot_standalone` — boundary: hash-tag-colocated
  (same-slot, distinct keys) MULTI commits normally with the flag on.

Added a `cross_shard_key_pair` helper (mirrors the `integration_client.rs` one) to pick two
plain keys on different internal shards.

**Test evidence.** `just test frogdb-server` (matching the 4 tests): `4 passed, 1807 skipped`.
Load-bearing check: temporarily mutating `TransactionTarget::resolve` to permit `Multi` (return
`Single(shards[0])`) failed all 3 cross-shard tests (EXEC returned `[OK, OK]` instead of
CROSSSLOT) while the single-shard boundary test still passed; mutation reverted. This satisfies
acceptance criterion 4 (tests fail if the fold/resolve path were changed to consult the flag).

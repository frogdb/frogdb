# Tiered-storage (warm tier) spilled keys have no real spill→restart→recover integration test

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 3/3 (score 3)
Area: persistence / tiered storage (area D)

## Context

`recovery.rs:145` implements `recover_warm_shard_into`, and
`operations/persistence.md:102` documents hot-over-warm precedence on recovery. However, no test
anywhere reopens a `RocksStore` after keys have genuinely spilled to the warm tier — the closest
existing coverage, `core/tests/tiered_storage.rs:226-232`, only asserts the warm CF is empty
after a `clear()` call. There is no integration-level tiered-storage test at all (`grep
spill/warm_tier` in `server/tests` returns nothing). A spilled key's value exists *only* in the
warm CF (per `test_memory_accounting_warm_keys:243-254`), so if restart recovery of the warm tier
were broken, the failure mode is silent data loss on restart — not a crash, just keys that don't
come back.

The initial audit report scored this higher, but the adversarial verification pass downgraded it:
`recover_warm_shard_into` **is** exercised at the unit level (`recovery.rs:408`), including
hot-wins-over-warm and warm-expiry-pruning cases, and startup does wire warm recovery. The
remaining true gap is narrower than "unproven logic" — it's specifically the missing
integration/e2e seam that drives a *real* spill (not a hand-constructed warm CF) through an
actual server restart.

Verdict (adversarial pass): ADJUSTED L1/C3 — `recover_warm_shard_into` unit logic is sound;
scope is the missing real-spill-through-restart integration test.

## What to build

An integration test that: forces several keys of different types to spill to the warm tier via
real memory-pressure/spill triggering (not a hand-constructed warm CF), restarts the server, and
asserts each spilled key is recovered correctly — value, `KeyType`, expiry, and any
unspillable-type edge cases. Also cover a spilled key with an already-past TTL surviving the
restart correctly (i.e., expiring, not resurrecting).

## Acceptance criteria

- [x] Integration test spills real keys of multiple types (not a hand-built warm CF) via the
      actual spill trigger path, then restarts the server.
- [x] Post-restart assertions cover value, `KeyType`, and expiry correctness for each spilled key.
- [x] Test covers the hot+warm dual-presence resolution case (same key present in both tiers
      pre-restart resolves to hot) surviving a restart.
- [x] Test covers a spilled key whose TTL has already elapsed by the time of restart — asserts it
      does not resurrect.
- [x] Existing unit-level coverage (`recovery.rs:408`, `core/tests/tiered_storage.rs:226-232`)
      left intact; this adds the integration seam, not a replacement.

## Blocked by

None - can start immediately

## References

- `core/src/persistence/recovery.rs:145,408`
- `core/tests/tiered_storage.rs:226-232,243-254`
- `operations/persistence.md:102`
- `.scratch/testing-improvements/audit/D-persistence.md` (`tiered-storage-spilled-keys-no-restart-recovery-test`, D#4)
- `.scratch/testing-improvements/audit/verdicts-D.md`

## Resolution

Added the missing real-spill-through-restart integration seam plus a deterministic
warm-vs-hot recovery-path unit test. Split across two levels because which tier a key
recovers *from* is timing-dependent at the server level (WAL flush vs. spill ordering),
while the warm/hot/stale/expired recovery-path classification must be asserted
deterministically.

### Harness knobs (`test-harness/src/server.rs`)

`TestServerConfig` gained `maxmemory: Option<u64>`, `maxmemory_policy: Option<String>`,
and `tiered_storage_enabled: bool` (with matching `Clone` and `start_with_config`
wiring). These drive `config.memory.maxmemory` / `maxmemory_policy` and the startup-only
`config.tiered_storage.enabled`, so a test can force genuine eviction-spill under memory
pressure. Spill trigger path: `tiered-lru` policy + small `maxmemory` + `tiered_storage.enabled`
+ persistence → `check_memory_for_write` evicts-by-spill on writes once over the limit.

### Server integration tests (`server/tests/integration_persistence.rs`)

- `test_tiered_spill_survives_restart`: writes typed keys (str/list/hash/set/zset) + a
  TTL'd string + 200 ~4KiB filler strings, asserts `INFO tiered` reports `tiered_spills>0`
  and `tiered_warm_keys>0` (proves a *real* spill happened), restarts, then asserts every
  value, `TYPE`, `PTTL`, and `DBSIZE` recovered intact from the warm/hot tiers.
- `test_tiered_spilled_key_past_ttl_does_not_resurrect`: a spilled key with a short TTL
  that elapses during the shutdown window must not come back on restart; a no-TTL survivor
  spilled alongside it must.

### Core recovery-path test (`core/tests/tiered_storage.rs`)

`test_real_spill_recovers_across_reopen` drives real `store.spill_key` to build genuine
spilled artifacts (warm-only string + sorted set), a dual-presence key (warm CF + primary
CF via `rocks.put`), a warm key with an already-elapsed expiry, and a hot-only key; then
`recover_all_shards` + `set_warm_store` (mirroring the server's `spawn_shard_workers`
wiring) and asserts `warm_keys_loaded==2`, `warm_keys_stale==1`,
`keys_expired_skipped==1`, dual-presence resolves to the hot value, the expired warm key
is gone, and `store.len()==4`.

Note: the recovered store returned by `recover_all_shards` has an *unconfigured* warm
tier; the warm-tier handle is wired in later by the server via `set_warm_store`. The core
test reproduces that ordering explicitly so warm values are actually readable post-recovery.

### Verification

- `just test frogdb-server test_tiered` → 2 passed.
- `just test frogdb-core test_real_spill_recovers_across_reopen` → 1 passed.
- `just lint` on affected crates + `just fmt` clean.

Existing unit-level coverage left intact.

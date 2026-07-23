# Tiered-storage (warm tier) spilled keys have no real spill→restart→recover integration test

Status: needs-triage
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

- [ ] Integration test spills real keys of multiple types (not a hand-built warm CF) via the
      actual spill trigger path, then restarts the server.
- [ ] Post-restart assertions cover value, `KeyType`, and expiry correctness for each spilled key.
- [ ] Test covers the hot+warm dual-presence resolution case (same key present in both tiers
      pre-restart resolves to hot) surviving a restart.
- [ ] Test covers a spilled key whose TTL has already elapsed by the time of restart — asserts it
      does not resurrect.
- [ ] Existing unit-level coverage (`recovery.rs:408`, `core/tests/tiered_storage.rs:226-232`)
      left intact; this adds the integration seam, not a replacement.

## Blocked by

None - can start immediately

## References

- `core/src/persistence/recovery.rs:145,408`
- `core/tests/tiered_storage.rs:226-232,243-254`
- `operations/persistence.md:102`
- `.scratch/testing-improvements/audit/D-persistence.md` (`tiered-storage-spilled-keys-no-restart-recovery-test`, D#4)
- `.scratch/testing-improvements/audit/verdicts-D.md`

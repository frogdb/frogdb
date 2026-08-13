# 27 — nothing but its own tests watches the replication byte counters

Status: done

## Parent

Found by [issue 15](../open/15-retro-validation-gate.md)'s retro-validation gate, revert (e) — the
gate-time pick from the LOCKED replication failure-mode spec. [PRD](../../PRD.md) §6 exit
criterion 8.

## The pick

Revert (e) is drawn rather than chosen, so the fifth sample cannot be a defect the layers were
built around. The draw was fixed before any candidate was inspected: the branch point
`origin/main` `2a81867e`, low six bits (`0x7e` → 62), indexed 0-based into
FM-REPLICATION-001..064 → **FM-REPLICATION-063**, "`total_net_repl_*_bytes` count bytes that
actually crossed the wire" (`specs/replication.md`; originating
bug `.scratch/hardening/issues/done/29-net-repl-byte-counters-are-hardcoded-zero.md`, fixed by
`17b9b552`).

## What the gate found

The inverse patch is two lines: `NetByteCounters::record_output` and `record_input`
(`frogdb-server/crates/replication/src/net_bytes.rs`) become no-ops, which is exactly the pre-fix
shape — both `INFO stats` fields sit at the literal `0` while real payloads and frames cross the
wire.

Twelve tests failed, and every one of them is excluded by issue 15's rules. Eight are named forcing
tests of FM-REPLICATION-063 itself. The other four —
`net_bytes::tests::{recording_output_moves_only_output, recording_input_moves_only_input,
records_accumulate_across_calls, reset_zeroes_both_counters_and_counting_resumes_from_zero}` —
shipped in the fix commit `17b9b552`, so they are the fix's own regression tests. Everything else
was green: `frogdb-replication-runtime` 45/45, the seeded sweep 32/32, `frogdb-server` replication
integration 236/236.

**Verdict: MISS**, and the reason is structural rather than accidental. The counters are
observability, and every layer in this campaign is built on `ReplicationView`, a projection of
replication *bookkeeping* — offsets, ids, phases, gate state, registry entries. A byte tally is not
in it, so no catalog invariant, property or model can state the violated fact. The seeded sweep
reads `INFO` on every node in every observation round and never looks at these two fields.

## Resolution (2026-08-11)

Closed by the layer built for [issue 25](25-no-layer-sees-what-a-full-resync-payload-contains.md),
which is the natural home for it: `a_full_resync_carries_writes_still_sitting_in_the_batch_window`
(`frogdb-server/crates/server/tests/simulation/full_sync_payload.rs`) ships a real full-sync payload
between two real servers over a simulated network, so at the end of that run the primary has
demonstrably sent bytes and the replica has demonstrably received them. The scenario now asserts
both: `total_net_repl_output_bytes > 0` on the primary and `total_net_repl_input_bytes > 0` on the
replica, naming FM-REPLICATION-063 at the assertion.

This is deliberately a *liveness* claim and not an exact-byte one. An exact figure would only
restate what the fix's own unit tests already pin, and it would break every time a frame's encoding
changes; "a node that just transferred a payload reports a non-zero tally" is the fact that
distinguishes a real counter from a hardcoded zero, which is what the row exists to rule out.

Re-run under the inverse patch turns the assertion red; on a clean tree the scenario passes. Command:

```
just concurrency-turmoil full_sync_payload
```

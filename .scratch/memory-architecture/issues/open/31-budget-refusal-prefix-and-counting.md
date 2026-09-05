# 31: budget refusal reply prefix, and refusal counting polish

Status: needs-triage
Type: AFK
Origin: whole-branch review 2026-09-05
Area: frogdb-server + frogdb-persistence + frogdb-memory
Phase: 6 — polish. Follows the lock; **locked-area work: spec-first discipline applies.**

## Why

Two memory-budget refusals reach a client, and they do not agree on how a budget refusal
looks; and one refusal *counter* counts a condition where it means to count an event.

**Prefix drift.** `frogdb-server/crates/server/src/net_charge.rs:38` replies

```
ERR <what> reply dropped: network output memory budget exceeded
```

while the txn budget replies `-OOM transaction buffer limit exceeded`
([issue 21](../done/21-txn-budget-hard-cap.md)). Same class of event — a memory budget shed the
work — two different error codes. A client library that special-cases memory pressure sees one
of them and not the other, and which one it sees depends on which budget tripped.

Redis's precedent is `-OOM` for `maxmemory` refusals, and FrogDB already follows it for the
`maxmemory` verdict (`CommandError::OutOfMemory`) and for the txn cap. `net_charge.rs` is the
outlier. Picking `-OOM` there makes budget refusals one recognisable class; picking `ERR`
everywhere would too, but would move the txn cap and the `maxmemory` verdict off the Redis
string, which is a compatibility cost for no gain.

Whichever way it is ruled, the deviation belongs on the compat page: Redis has no
network-output or transaction-buffer budget, so any code we send for them is a FrogDB-only
reply that a client may encounter.

**Refusal counting.** `RocksMemory::refresh` re-counts a refusal on every 10 s tick while
pinned cache usage sits above capacity, so
`frogdb_memory_budget_refusals_total{persistence}` climbs monotonically for a single steady
condition. The other subsystems' refusal counters count *events* — a request that was shed. An
operator alerting on `rate(frogdb_memory_budget_refusals_total[5m]) > 0` gets a firing alert
for as long as the condition lasts rather than one per excursion, and cannot tell a steadily
over-capacity cache from a cache being hammered.

The whole-branch fix round left the behaviour alone and recorded the tick shape at
FM-PERSISTENCE-061, cross-referencing this issue. This issue is where the counting actually
changes.

## What to build

1. **Rule the prefix.** Pick one code for budget refusals — the reviewer's recommendation is
   `-OOM`, matching Redis's `maxmemory` precedent and FrogDB's own txn cap — and apply it to
   `net_charge.rs::shed_error`. Update the socket tests that assert the string.
2. **Document the deviation** on the website compat page: the two FrogDB-only budgets, their
   error code, and the fact that Redis has no equivalent.
3. **Count refusals per excursion.** `RocksMemory::refresh` counts once on the transition into
   over-capacity, not once per tick, and re-arms when usage falls back under. Update
   FM-PERSISTENCE-061's Observable to state the per-excursion shape and drop the operator note
   the fix round added.

## Acceptance criteria

- [ ] One error code across every memory-budget refusal; ruled and recorded in the spec.
- [ ] Compat page documents the FrogDB-only budgets and their reply code.
- [ ] `frogdb_memory_budget_refusals_total{persistence}` increments once per excursion; forcing
      test holds usage over capacity across several ticks and asserts the counter moved by one.
- [ ] FM-PERSISTENCE-061 states the per-excursion shape; `just lint-spec` green.
- [ ] `just mutants-diff frogdb-persistence` before push.

## Files likely touched

- `frogdb-server/crates/server/src/net_charge.rs`
- `frogdb-server/crates/persistence/src/` (`RocksMemory::refresh`)
- `specs/persistence.md` (FM-PERSISTENCE-061)
- `website/` compat/deviations page
- socket tests asserting the network-output shed string

## Out of scope

The uncharged hand-off window between EXEC and the shard gate — that is
[issue 29](29-txn-charge-through-shard-channel.md). The `memory_size` capacity-vs-len ruling —
[issue 30](30-memory-size-capacity-not-len.md). Changing any budget's limit, default or
disposition. The `command_admission.rs` OOM string: the whole-branch fix round verified it was
byte-identical to `frogdb_types::error`'s `OutOfMemory` and DRY'd it there, so nothing is
outstanding.

## Depends on

Nothing.

## Blocks

Nothing.

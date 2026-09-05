# 24: burn down the budget-growth allowlist

Status: needs-triage
Type: AFK
Origin: [issue 22](22-lock-memory-spec.md) audit, 2026-09-05 — the locked
[specs/memory.md](../../../../specs/memory.md) records this as an open item
Area: frogdb-server (replication, persistence/wal, fullsync) + scripts/budget-growth.py
Phase: 5 — network memory

## Why

The load-bearing claim of the memory area is *a structure that cannot charge cannot grow*.
The chokepoint for it exists — the `lint-budget-growth` seam lint
([agents/seam-lints.md](../../../../agents/seam-lints.md), `scripts/budget-growth.py`) —
and it ratchets. What it does not yet do is bind:

```
OK: 20 budgeted growth site(s); 83 unconverted site(s) pinned in 32 file(s)
```

Four of the seven declared `Subsystem` variants hold a live budget today
(`network_output`, `client_tracking`, `txn_buffering`, `persistence`). The other three —
`replication_backlog`, `wal_channel`, `fullsync_staging` — appear in the operator's
breakdown and are charged by nobody, which is worse than absent: a gauge that reads zero
because nothing reports is indistinguishable from a subsystem holding nothing.

Because of that, the lock of `specs/memory.md` deliberately writes **no** row claiming any
buffer is bounded that does not name its own `Budget`. Issue 22's audit found that the
draft's "every buffer is bounded and budget-charged" group described a plan, not behaviour,
and dropped it to an open item. This issue is how it comes back as rows.

## What to build

1. **Convert the three unowned subsystems.** Each of `replication_backlog`, `wal_channel`
   and `fullsync_staging` gets a `Budget` opened by the shard broker, a limit with a
   configuration key, and a stated disposition — shed or backpressure, never neither and
   never both. Backlog and WAL channel are backpressure candidates (the producer can be made
   to wait); full-sync staging is the interesting one, and it is entangled with
   [issue 25](25-snapshot-handle-bounded-full-sync.md).
2. **Burn the allowlist down by file, ratcheting as you go.** 83 sites in 32 files. The lint
   already refuses a net increase; each conversion removes its pin in the same commit.
3. **Row each converted buffer** in `specs/memory.md` under the spec-first discipline: the
   row names the `Budget`, its configuration key, and its behaviour at the limit, and
   arrives with its forcing test. Do not write one row for "every buffer" — the claim is
   only as good as the allowlist being empty.
4. **Retire the allowlist** when it reaches zero, and only then write the row that says the
   seam is total.

## Acceptance criteria

- `scripts/budget-growth.py` reports zero unconverted sites, and the allowlist mechanism is
  removed rather than left empty.
- Every `Subsystem` variant either holds a live budget or is deleted.
- `specs/memory.md` carries a row per converted buffer, each with forcing tests, and the
  "every buffer is bounded" open item is removed from the Open items section.

## Out of scope

The keyspace itself. Keyspace bytes are governed by the `maxmemory` verdict
(FM-MEMORY-004), not by a `Budget`, and that split is deliberate — see ADR-0006 §2.

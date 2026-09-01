# 05: per-core memory broker, the `Budget` handle, and the buffer-growth seam lint

Status: done
Type: AFK
Origin: memory-architecture PRD ruling R8 + R15
Area: new broker crate + frogdb-core (tracking) + `scripts/` seam lint
Phase: 2 (observability first) — [PRD.md](../../PRD.md) R8, and the lint half of R15

## Why

R8's claim is one sentence and the whole area rests on it: **a structure that cannot charge
cannot grow.** Today it is not true anywhere — every non-keyspace buffer either carries its own
private cap with its own private counter, or carries nothing at all. This issue builds the
machinery, gives it a chokepoint, and proves it on one real subsystem. It does not convert
everything; conversion is a ratchet, which is what the lint is for.

The chokepoint discipline is [adr/0006](../../../../adr/0006-memory-architecture-seams.md) §2,
and the vocabulary the lint enforces — Budget, charge, shed, backpressure, ceiling vs budget —
is defined once in [`specs/memory.md`](../../../../specs/memory.md) "Invariant vocabulary". Use
those words; do not invent parallel ones.

## What to build

### 1. Broker skeleton, one per core

Owns every `Budget` on its core and the core's allocator reading. Deliberately minimal in this
issue: it hands out budgets, tracks their charges, and reports a per-subsystem breakdown. It
does **not** yet drive eviction or answer `maxmemory` — those are later phases, and building
them now guesses at the table design.

The broker's ground truth is the sampled per-arena figure from [issue 03](../), which is an
**upper bound** (thread-cache residue) refreshed at 10–100 Hz, not a per-command exact reading.
The error direction is the safe one — an upper bound refuses slightly early — but the broker's
API must not pretend otherwise: no method returns "live bytes".

### 2. `Budget` handle

- `charge(n) -> Result<Charge, Refused>` — asked **before** the bytes exist. The returned
  charge is held until released; a `Refused` is a refusal the caller handles at that seam.
  There is no "charge anyway and log a warning" path, because that is what today's unbounded
  buffers already do.
- `release` on drop, or explicitly. Prefer drop: a subsystem that has to remember to release
  will eventually not.
- A declared disposition — **shed** or **backpressure** — recorded at construction, because
  [`specs/memory.md`](../../../../specs/memory.md) requires every buffer to state which it does
  and forbids neither.
- A stable subsystem name, so the per-subsystem breakdown an operator sees is not derived from
  a type name that will be refactored.

### 3. The seam lint

Following [`agents/seam-lints.md`](../../../../agents/seam-lints.md) "Adding a new rule", all
six parts:

1. **Invariant** — growth of a non-keyspace buffer goes through a `Budget` charge.
2. **Chokepoint** — the `Budget` handle from part 2. (This is why the lint cannot precede it: a
   lint without a chokepoint is a nag.)
3. **Mechanical predicate** — the growth sites a violation looks like, over `rg` output.
4. **Escape hatch** — one of the two shipped idioms, never an in-code `#[allow]`: a
   count-pinned per-file allowlist (`scripts/clock-seam.py`'s shape) checked in **both**
   directions, so a converted buffer must leave the list and a new violation in a listed file
   fails like a new file would.
5. **Ratchet** — lands with every unconverted buffer pinned, so the rule ships before the
   cleanup does.
6. **Wiring** — `scripts/<rule>.py` with a PEP-723 header and no dependencies, reusing
   `scripts/_rustscan.py`'s `cfg_test_spans()`/`is_test_path()` helpers rather than re-copying
   them; a `just lint-<rule>` recipe; membership in `just lint`; and — since it is compile-free
   — membership in `just lint-gates`, or it inherits the family's original hole of being
   convention rather than enforcement. Add the row to `agents/seam-lints.md`'s table.

### 4. One subsystem wired, as proof

**The client tracking table** (`frogdb-server/crates/core/src/tracking.rs`) is the recommended
proof: it is the bug class R8 exists to kill (`lru_order` grew outside accounting —
[issue 66](../../../testing-improvements-round2/issues/)), it already has a memory-estimate
function to reconcile a charge against, it is per-connection state with an obvious shed
disposition, and `frogdb-core` is **not** a locked area, so the conversion is an ordinary
change.

The replication backlog ring (`frogdb-server/crates/replication/src/primary/ring_buffer.rs`) is
the alternative and is in some ways the better demonstration — it already has `max_bytes` and a
`current_bytes` counter, so the conversion is exactly "a private cap becomes a broker-issued
budget" — but `frogdb-replication` is a **locked** crate (gate 0.85) whose caps are rowed in
[`specs/replication.md`](../../../../specs/replication.md), so that conversion is spec-first
and carries a mutation-gate obligation. Take it only if the tracking table turns out not to
exercise the API. Either way: **one** subsystem. The point is to prove the seam, not to convert
the server.

## Acceptance criteria

- [ ] A broker exists per core, hands out named `Budget` handles, and exposes a per-subsystem
      charge breakdown (metric and/or `INFO`).
- [ ] `Budget::charge` refuses when over limit; a unit test forces both a successful charge and
      a refusal, and asserts the charge is released on drop.
- [ ] Each `Budget` declares shed or backpressure, and a test asserts the declared disposition
      is what the wired subsystem actually does at its limit.
- [ ] The broker's arena-derived figure is documented and named as a sampled upper bound; no
      API on it returns a value described as live or exact bytes.
- [ ] The tracking table (or the backlog ring, with the spec-first work done) charges before it
      grows; a test drives it past its limit and asserts the declared disposition and the
      resulting metric, not just that memory stopped growing.
- [ ] `just lint-<rule>` exists, is in `just lint` **and** `just lint-gates`, runs in well under
      a second, and has a row in `agents/seam-lints.md`.
- [ ] The lint's allowlist is checked in both directions: a test (or a fixture under
      `scripts/tests/`) pins that removing a converted entry fails and that adding a violation
      to an allowlisted file fails.
- [ ] The lint fails on a deliberately introduced unbudgeted growth site and passes on the tree
      as landed.
- [ ] `just lint-gates` and `just scratch-check` clean; `just test frogdb-server` green.

## Test boundary

Level 1/2 for the broker and `Budget` — pure accounting with no engine involvement, and driving
it through a socket would be strictly worse. Level 2 for the wired subsystem's limit behavior.
The lint gets its own dependency-free assert script under `scripts/tests/` if its scanning is
more than a regex, per the seam-lint doc's rule about parsers that the whole rule rests on.

## Out of scope

`maxmemory` verdicts, eviction, the RocksDB budget (issue 06), converting a second subsystem,
and network output-buffer classes. Each of those is a later phase and each would pull design
decisions this issue is not equipped to make.

## Depends on

[Issue 02](../) — a per-core broker needs cores to be per-core. [Issue 03](../) supplies the
arena figure the broker reads; the broker can land with a stubbed reading and pick it up when
03 lands, so 03 is a soft dependency, but the "sampled upper bound" wording is required either
way.

## Resolution

Landed on main (4 commits, reviewed and cherry-picked from the implementing agent's branch).
New `frogdb-memory` crate: `Budget` (Arc-shared, relaxed-CAS charge loop, saturating
give-back, peak/refusal counters, live `set_limit`), `Charge` (`#[must_use]`,
grow/shrink/release, releases on drop, `open_charge()` zero-byte form for grow-in-place),
`Subsystem` (7 stable names), `Disposition::{Shed,Backpressure}` declared at construction,
`Refused`. `MemoryBroker` is per-core; an unopened subsystem's budget has limit 0, so a
structure that has not been converted cannot grow by default. Arena seam: `ArenaSampler`
trait + `NoArenaReading` stub — issue 03's plug-in point is one site in
`ShardWorkerBuilder`; "sampled upper bound" naming throughout, no API returns "live bytes".

Proof subsystem: the client tracking table. One long-lived grow/shrink-in-place `Charge`
(not per-entry charges) makes `memory_usage()` O(1) and equal to the broker breakdown by
construction; drift is guarded by test-only `recomputed_memory_usage()` + a reconciliation
test covering every mutation path. Disposition shed: a refused charge evicts the oldest
tracked key (invalidating its clients) and retries; an empty table that still cannot fit
declines the read. Both observable via drained counters →
`frogdb_tracking_table_budget_{shed_keys,declines}_total`; broker breakdown →
`frogdb_memory_budget_{charged,limit}_bytes` + `_refusals_total` (5 new metrics, grafana
regenerated). `ShardTracking::new(&Budget)` replaces `Default` — an unbudgeted table is
unrepresentable.

Seam lint: `just lint-budget-growth` (`scripts/budget-growth.py`, struct-level attribution
via enclosing `impl`, keyspace out of scope per ADR-0006 §3), 99 unconverted sites pinned
by file+count in a both-directions ratchet; in `lint-gates` (nineteenth compile-free gate) +
row in `agents/seam-lints.md`; scanner unit-tested (`scripts/tests/test_budget_growth.py`,
13 checks, `just test-budget-growth`).

Flagged for human review: the one-long-lived-Charge structure call (vs per-entry charges) —
see the reconciliation test if revisiting. Verification: `just test frogdb-server` green
except one unrelated contention flake (passes in isolation); lint-gates clean; budget-growth
gate fails on a deliberately introduced unbudgeted site.

For issue 06: `broker.open(Subsystem::Persistence, limit, Disposition::Backpressure)`;
`Budget` clones cheaply into a RocksDB `WriteBufferManager` shim mapping callbacks onto
`grow`/`shrink`; `Refused` is the backpressure signal; metrics come free via the breakdown.
Global-vs-per-shard RocksDB budget is 06's call — `Budget` supports both (limit lives in
the shared Arc).

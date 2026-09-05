# 12: segment-integrated 2Q eviction

Status: done
Type: AFK
Origin: drafted 2026-09-01 from the landed
[spike report](../../spike-report-table.md) §4 — [PRD.md](../../PRD.md) R9
Area: `frogdb-table` (issue 11's crate) + frogdb-core eviction driver
Phase: 3 — keyspace table

## Why

R9 rules segment-granularity 2Q eviction with **no per-key LRU field**. The spike proved
the space claim: the whole eviction state is 22 B per segment header (0.045 B/entry at
measured occupancy) plus 24 B of reserved headroom, against 4–16 B/entry for any per-key
scheme a bare two-word slot would otherwise need. The header fields are allocated and
named — `q_state`, `victim_cursor`, `q_prev`/`q_next` (u32 segment indices), `hits`,
`misses`, `last_touch`, `bytes_charged` — this issue implements the logic.

## What to build

### 1. 2Q over segments

- Queues are intrusive lists through `q_prev`/`q_next` (segment indices, never
  pointers): **A1in** (new segments), **A1out** (ghost — membership only), **Am**
  (frequently hit). Promotion A1in → Am driven by the `hits`/`misses` counters;
  `last_touch` is the coarse clock tick (through the clock seam — the clock-seam lint
  applies).
- Victim selection: tail of A1in, else tail of Am; inside the victim segment, evict
  starting at `victim_cursor` (rotating bucket index, O(1) resume), oldest-touch
  granularity is the segment, not the key — that coarseness is R9's explicit trade.
- Research check before building: compare against Dragonfly's segment-eviction heuristics
  and Redis's sampled-LRU/LFU (`maxmemory-policy`) for the policy-surface mapping —
  which `maxmemory-policy` values map onto 2Q-over-segments, and which are documented
  deviations.

### 2. Driver and invariants

Eviction runs only under the broker's `maxmemory` verdict pressure (the phase-2 budget
seam), and each eviction:

- evicts only from the configured policy's candidate set (volatile-* policies restrict
  to segments containing TTL'd keys — decide and document how segment-granularity
  handles the volatile/all split; a segment mixing both is the design question here),
- emits the keyspace notification and the eviction metric (through the typed metric
  handles — seam lint applies),
- charges/uncharges via `bytes_charged`,
- **terminates**: when nothing is evictable the OOM verdict stands (no livelock) —
  forcing test required.

### 3. Spec rows

This issue writes the eviction FM rows the [issue 22](../) audit expects (eviction only
under verdict pressure; candidate-set confinement; event+metric per eviction;
termination), each with its forcing test in the owning crate, tagged per lint-spec
convention. The rows land in `specs/memory.md` (DRAFT — no lock yet).

## Acceptance criteria

- [ ] 2Q state machine unit-forced: A1in admission, ghost demotion, Am promotion via
      hit/miss counters, victim rotation resume.
- [ ] Header stays 64 B; the 24 reserved bytes are not exceeded (spike's assertion test
      ported and kept).
- [ ] Under verdict pressure with an unevictable keyspace, writes get the OOM error,
      reads still served, no livelock.
- [ ] Keyspace event + metric per eviction; eviction visible in the budget breakdown.
- [ ] Redis `maxmemory-policy` surface mapped: supported values behave, unsupported
      values documented as deviations.
- [ ] `just lint-spec` green with the new rows.

## Test boundary

Level 1: state-machine unit tests + property test (random hit/miss sequences never
corrupt queue links; every segment is in exactly one queue or none). Level 2: e2e
maxmemory pressure tests through the server. Turmoil: eviction under sim determinism
(seeded clock ticks).

## Out of scope

Changing the header layout or growing past the 24 reserved bytes (back to the PRD if it
doesn't fit), per-key eviction metadata of any kind (ruled out by R9), tiering/flags
semantics (`flags` bits stay reserved), the swap-out decision (issue 11's gate).

## Depends on

[Issue 11](../) — the production table crate and its header. Blocked until 11 lands.

## Resolution

Landed 2026-09-05 on `mem-arch-integration` (picks `03ca4c104`..`338d16acf`, 20
commits). **Default store backend unchanged (griddle)** — this issue binds 2Q through the
`Keyspace` seam; the sampled path is bit-for-bit what it was, and issue 11's swap gate stays
ready-for-human.

What shipped: segment-granularity 2Q in `frogdb-table` (`src/evict.rs`, new) — three intrusive
lists (A1in / A1out / Am) linked by `u32` segment indices in the segment header (`q_state`,
`q_prev`, `q_next`, `hits`/`misses` as `Cell<u32>` so `get` keeps `&self`, `last_touch` u16
epoch, `victim_cursor`): 28 header bytes per 16 KiB segment, ~0.034 B/key, 36 B still reserved
(pinned by `segment_capacity_and_r9_reservation`). `Table::cold_candidates` walks A1in then Am
tail→head, reconciling (promote at most once per epoch) or nominating from the stored cursor,
one lap per call; termination is a `2n + 2` step bound asserted in code, and a frozen epoch
freezes promotion but never nomination. A1out is a membership set of emptied segments (not the
paper's ghost cache — vacuous at segment granularity), re-admitted on the next insert.
Confinement for `volatile-*` is per slot inside a mixed segment. `bytes_charged` dropped: the
table sees only key bytes, the eviction metric already reports exact freed bytes, and keyspace
bytes belong to the arena (ADR-0006 §3) — the freed memory surfaces on the next arena sample,
not as a budget decrement. Seam: `Keyspace::cold_candidates` (griddle answers `None`),
`Store::eviction_candidates` (default `None`), `EvictionPolicy::candidate_source()` →
`Cold | Sampled`; `ShardWorker::choose_victim` is one chooser for the delete and spill paths
(`tiered-*` spills exactly the key `allkeys-*` would delete). Policy map on the table backend:
`*-lru`/`*-lfu` → 2Q; `*-random`/`volatile-ttl` stay sampling (documented on the compat page
with the Redis/Dragonfly comparison; `maxmemory-policy` doc in `config/src/memory.rs`). A
refusal is paid once: `Table::generation` counts every mutation that can change a walk's
answer (insert/remove/clear/`get_mut` and the walk's own queue moves), and `TableKeyspace`
memoizes an *inert* empty walk keyed by `(volatile_only, generation)`. Spec rows FM-MEMORY-004
(verdict pressure only), 005 (candidate-set confinement), 006 (event + counters + replicated
removal), 007 (frees or reports, never spins; inert-walk memo). `scripts/spec-lint.py` lets
memory rows name `frogdb-table` tests. `frogdb-server` gains the `table-keyspace` passthrough
feature and `tests/integration_eviction_2q.rs`; fuzz `table_ops` gains a model-checked `Evict`
op.

**Executor seam change (outside the brief, on controller ruling).** The feature build did not
compile: `ShardExecutor::launch` took a built `Send` future, the shard body holds
`&ShardWorker` across awaits, so the bound demanded `ShardWorker: Sync`, which the
`Send`-only table cannot give (`*mut u8` from issue 11 — fatal alone; `Cell` from this
issue). Inherited from #11, which never built the server with the feature. Fixed at the seam:
`launch` takes a `Send` *constructor* (`frogdb_net::shard_body`) and builds the future on the
shard thread (real executor: inside its `std::thread` before `block_on`; sim executor:
`spawn_local`, sound because turmoil drives host software inside a `LocalSet`). No `unsafe
impl Sync`; all 11 `launch` call sites updated (`net` tests, `acceptor.rs`, `shards.rs`).

Review: round 0 (0 Critical, 4 Important, 8 Minor) — `colder`/`warmer` label inverted in the
walk and mirrored into FM-MEMORY-007; O(segments) re-walk on every refused write against an
unevictable keyspace; an undeclared proptest regression seed; the budget-breakdown clause
unaddressed (ruled satisfied by construction — keyspace is not a `Budget` subsystem). Fix r1
(7 commits): all addressed; the new fuzz assertion caught the first memo as unsound (a
promoting walk withholds a segment, so a repeat can legitimately produce) → only inert walks
are cached, forced by `a_walk_that_promotes_a_segment_moves_the_generation`. Re-review r1:
all findings addressed, no new Critical/Important; two Minors it uncovered (a `want == 0` memo hole at the seam, and the O(1)-per-refusal claim's silence about command-path reads bumping the generation) fixed in r2 (1 commit), diff verified by the controller. Gates: `frogdb-table` 85/85, `frogdb-core` 1061/1061 (1063 with the
feature), `frogdb-server --features table-keyspace` eviction/maxmemory/oom set 11/11 incl.
both 2Q e2e, full `just test frogdb-server` 2151/2151, turmoil sim suite 159/159 (proves
`spawn_local`), workspace + feature clippy clean, lint-spec, lint-gates, miri 60, fuzz
`table_ops` 103 954 + 5 318 runs clean.

Deviations, for human sign-off:

- **Within-segment victim order is cursor rotation, not recency** — R9's segment-granularity
  trade, matching Dragonfly's bargain; a workload with one hot key per segment will see it
  evicted. Benchmark against the sampled path before the backend becomes the default.
- **No turmoil/sim eviction coverage** — `shard-harness` has no `maxmemory` hooks; reported
  as a gap per the brief, not built.
- **`hits`/`misses` never decay** (reset only at reconcile) — promotion signal is
  lifetime-cumulative; documented in `evict.rs`.
- **`just lint-turmoil` is red, pre-existing** (`acceptor.rs:90` `shard_placement` dead
  under the turmoil cfg, from `4817d3203`); untouched here.

Follow-ups to file: (1) sim-harness `maxmemory` hooks so eviction gets deterministic
coverage; (2) benchmark 2Q vs sampled LRU on the feel workloads before the swap decision;
(3) `spec-lint` now compiles `frogdb-table`'s test binary on every run — fine today, watch it
on CI.

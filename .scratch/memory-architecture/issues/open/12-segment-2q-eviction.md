# 12: segment-integrated 2Q eviction

Status: ready-for-agent
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

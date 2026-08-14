# 37: Promotion validity bound — offset-unknown candidates rank last, staleness bounded logically

Status: ready-for-agent

## Origin

Distsys-review MAJ-9 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **accept with logical bound** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

Automatic promotion has no replica-validity bound. The scoring formula
(`priority * 100_000 + (max_offset - replica_offset) * 1_000`) assumes a known offset,
and FM-CLUSTER-056 *explicitly* keeps a candidate whose offset could not be determined
in the candidate set. Consequences: a replica partitioned for an hour — or one whose
offset probe timed out — can win the election and be promoted, discarding writes a
healthier replica holds. With an undetermined offset there is no meaningful term to
score; eligibility is decided by the formula's blind spot.

Comparisons: Redis disqualifies via `cluster-replica-validity-factor` (disconnection
time vs node timeout); Raft's voting restriction (§5.4.1) is the principled, clock-free
form — a candidate whose log is not at least as up-to-date as the voter's cannot win.

**Ruled shape**: the Redis validity factor is wall-clock; the campaign principle bans
wall-clock gating of state transitions. FrogDB bounds staleness **logically**:

- Offset-unknown candidates rank **strictly last** — promotable only when no
  known-offset candidate exists (availability floor: a solo survivor is still
  promotable; the cluster never trades a known-good candidate for a blind one).
- The optional staleness disqualifier is expressed in **offset lag** (bytes/entries
  behind `max_offset`), consistent with issue 17's byte-cap and issue 26's
  offset-parity barrier — never in disconnection seconds.

## What to build (spec-first; cluster locked, gate 0.80)

1. Spec rows first:
   - Amend FM-CLUSTER-056: offset-unknown candidates form a last-resort tier, not
     peers of scored candidates.
   - New FM row: "a replica whose offset is unknown is never promoted while a
     known-offset candidate exists" — NOT observable: an offset-unknown winner when a
     scored candidate was eligible.
   - TR row for the lag bound if adopted: candidates more than N bytes/entries behind
     `max_offset` are disqualified from *automatic* promotion (manual/forced failover
     unaffected — issue 19's forced-failover semantics own that path); N is a
     config param, live-mutable, default justified in the row.
2. Code: candidate-set construction separates tiers (scored / last-resort); election
   only reaches the last-resort tier when the scored tier is empty; lag disqualifier
   applied in tier-1 construction.
3. Forcing tests:
   - Offset-unknown + known-offset candidates present → known-offset wins regardless
     of priority ordering that would otherwise favor the unknown.
   - Only offset-unknown candidates → promotion still proceeds (availability floor).
   - Candidate beyond the lag bound → excluded from automatic promotion; still
     promotable via forced failover.

## Cross-references

- [Issue 26](26-planned-failover-gets-a-drain-and-offset-parity-barrier.md): same
  offset-comparison machinery on the planned path; keep vocabulary consistent.
- [Issue 19](19-a-forced-failover-promotes-a-node-that-inherits-nothing.md): forced
  path deliberately bypasses validity — the TR row must scope the bound to automatic
  promotion only.

## Acceptance criteria

- [ ] FM-CLUSTER-056 amended; new FM/TR rows; `just lint-spec` green
- [ ] Two-tier candidate set in code; lag bound live-mutable
- [ ] Forcing tests fail pre-fix, pass post-fix
- [ ] `just mutants-diff` on frogdb-cluster (locked, 0.80) triaged

## Blocked by

None — can start immediately.

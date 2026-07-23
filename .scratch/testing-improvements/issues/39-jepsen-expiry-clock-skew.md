# Jepsen expiry workload never runs under clock skew

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: Jepsen harness (area G)

## Context

`expiry.clj` is wall-clock based (`System/currentTimeMillis`, 2s tolerance; lines 30, 107-155,
366) and only runs under `none`/`kill`/`rapid-kill` nemeses (`run.py:114-138`). The `clock-skew`
nemesis exists in the harness but is bound only to the `elle-rw-register` workload under
`raft-extended` — expiry is never combined with clock skew, despite expiry being exactly the kind
of feature most sensitive to clock divergence between nodes (a skewed node might expire keys
early or late relative to peers). Additionally, the checker itself computes expected expiry from
wall-clock time rather than server-authoritative TTL/PTTL, so even if clock-skew were wired in,
the checker would need rework to correctly interpret results under skew (a skewed checker host
clock would misjudge correct server behavior as a bug, or vice versa).

Verdict (adversarial pass): CONFIRMED L2/C2 (`currentTimeMillis` occurrence confirmed ×6; checker
needs server-authoritative time to be meaningful under skew).

## What to build

1. New `expiry-clock-skew` TestDefinition in `run.py` combining the expiry workload with the
   clock-skew nemesis.
2. Rework the expiry checker to derive expected-expiry from server-reported TTL/PTTL rather than
   the Jepsen client's own wall clock, so it remains meaningful when node clocks diverge from the
   checker host.

## Acceptance criteria

- [ ] `run.py` has an `expiry-clock-skew` (or equivalently named) TestDefinition combining
      `expiry.clj` with the `clock-skew` nemesis.
- [ ] `expiry.clj` checker logic derives expected expiry from server-reported TTL/PTTL rather
      than raw `System/currentTimeMillis` comparisons, so skew between the Jepsen control node
      and DB nodes doesn't produce false positives/negatives.
- [ ] New TestDefinition runs clean against current FrogDB (establishing baseline) and is wired
      into the suite selection used by CI/nightly runs once issue `10-jepsen-nightly-ci` lands.

## Blocked by

None - can start immediately

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/expiry.clj:30,107-155,366`
- `testing/jepsen/frogdb/run.py:114-138`
- `.scratch/testing-improvements/audit/G-jepsen-harness.md` (`expiry-never-clock-skewed`)
- `.scratch/testing-improvements/audit/verdicts-G.md`

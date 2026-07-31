# Jepsen expiry workload never runs under clock skew

Status: done
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

## Resolution

Status: **done**.

Both acceptance criteria met; new test establishes a clean baseline against current FrogDB.

### 1. `expiry-clock-skew` TestDefinition (`run.py`)

Added `expiry-clock-skew` = expiry workload + `clock-skew` nemesis, single-node,
60s time-limit (two full skew/reset cycles of the 15s-interval generator), suites
`("crash", "all")` — grouped with the other expiry fault variants. No `core.clj`
change was needed: `clock-skew` was already an accepted nemesis and `expiry` an
already-registered workload.

Container-capability note: the clock-skew nemesis skews via `date -s` where
`CAP_SYS_TIME` is granted, else falls back to a `/tmp/faketime` offset file
(`nemesis.clj`). The single-node compose grants neither — deliberately, since
`date -s` in a non-time-namespaced Docker container would move the **host** clock.
So on this topology the nemesis perturbs the control-vs-server relationship as an
offset rather than a true server-clock jump (the same posture as the existing
raft-extended `clock-skew` test, whose raft compose also lacks `CAP_SYS_TIME`).
That offset case is exactly what the reworked checker is now robust to. Truly
skewing the server clock's *rate* would require a time-namespaced container or an
`LD_PRELOAD` libfaketime build — out of scope here and noted as a possible future
hardening.

### 2. Server-time-authoritative checker (`expiry.clj`)

The old checker predicted each key's expiry instant from the Jepsen client's own
wall clock (`System/currentTimeMillis` at SET + the client-chosen TTL) and compared
it to the client wall-clock time of each read — unsound the moment a DB node's clock
diverges from the control node's. Rewritten to be server-time authoritative:

- **All timing/ordering comes from Jepsen's per-op `:time`** (a single control-node
  monotonic clock, unaffected by DB-node skew). Invoke/complete times are paired per
  process; no client wall-clock timestamps are recorded in `invoke!` anymore.
- **All correctness values come from the server** (GET alive/dead, TTL/PTTL). The
  `:ttl` op now also issues PTTL, giving a millisecond-precision remaining-lifetime
  anchor.
- **Premature-expiry / zombie-key** are now derived by anchoring on the server's own
  PTTL reply and projecting it forward with *elapsed control-node time* — a duration,
  invariant under a constant clock offset between control node and server. Conservative
  anchor edges + full tolerance + any-mutation-in-span suppression avoid false positives.
- **Unconditional safety invariants** (hold under *any* clock behaviour): no
  resurrection (a key observed dead — read nil, TTL -2, or DELETE — only becomes alive
  again via a SET) and persisted-key durability (a persisted key must not spontaneously
  expire). Plus a TTL/PTTL configured-maximum sanity check.

### Bug found + fixed during validation

The first baseline smoke run went **red with 24 `:premature-expiry` false positives** —
a bug in the new checker, not FrogDB. The premature check paired every PTTL anchor with
every read, including reads that occurred *before* the anchor was measured (e.g. a read
at t=1148ms flagged against a PTTL measured at t=20373ms). A PTTL says nothing about a
read in its own past. Fixed by requiring the read to happen strictly after the anchor
(`(> (:it r) (:ct a))`); the zombie branch was already inherently forward-ordered. This
is exactly the false-positive class the issue warned a skew-unsound checker would produce.

### Smoke results (clean single-node, `frogdb:latest`, `--no-build`)

| Test | Verdict | Violations | reads / ttl-checks / keys | Notes |
|------|---------|-----------|---------------------------|-------|
| `expiry` (baseline) | `:valid? true` | 0 | 80 / 43 / 20 | post-fix |
| `expiry-clock-skew` | `:valid? true` | 0 | 152 / 67 / 20 | nemesis fired (6 skew/reset log lines) |

`lein check` passes; `run.py list` shows the new test.

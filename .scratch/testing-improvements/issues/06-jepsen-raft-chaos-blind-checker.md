# Jepsen: raft-chaos (harshest nemesis) backed by a checker blind to wrong/dropped values

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 3/3, consequence 3/3 (score 9)
Area: jepsen

## Context

`key_routing.clj:333` defines `:valid? (empty? redirect-failures)`, where `redirect-failures` is
populated only from `:too-many-redirects` errors (line 331). This means the checker only detects
excessive redirect churn — it has no visibility into the actual key/value data at all. A `GET` that
returns the wrong value, or a write that is silently dropped, is invisible to this checker and the
run reports `:valid? true`.

This matters because `key_routing` is the workload backing `raft-chaos`
(`run.py:246-254`), the harness's harshest composed nemesis, as well as `key-routing-kill` and
`key-routing-partition`. The single most aggressive fault-injection scenario in the entire Jepsen
suite is currently validated by a checker that cannot detect data loss or corruption — only
excessive redirects. Real value loss or wrong-value reads under `raft-chaos` would pass green today.

## What to build

- Switch `raft-chaos` (and the other `key-routing-*` test definitions) to use a workload with a
  real data-correctness checker — `elle-rw-register` or `elle-list-append` (per the
  `cross_slot.clj:382` conservation-checker pattern already used elsewhere) — instead of
  `key_routing`.
- Alternatively (if `key_routing`'s cluster-routing-specific coverage is still wanted alongside
  data correctness), add key -> last-acked-write tracking to `key_routing.clj` itself and have
  `:valid?` cross-check final reads against it, not just redirect-failure count.

## Acceptance criteria

- [ ] `raft-chaos` (and `key-routing-kill`/`key-routing-partition`) run against a workload whose
      checker can detect wrong-value reads and dropped writes, not just redirect-failure counts
- [ ] If `key_routing.clj` is kept, its `:valid?` incorporates last-acked-write tracking, not just
      `(empty? redirect-failures)`
- [ ] A deliberately-introduced data-loss/wrong-value bug (manual smoke test) is caught by the
      updated checker where it previously passed

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/key_routing.clj:331,333` — `redirect-failures`,
  data-blind `:valid?`
- `testing/jepsen/run.py:246-254` — raft-chaos composed nemesis, backed by key_routing
- `testing/jepsen/frogdb/src/jepsen/frogdb/cross_slot.clj:382` — conservation-checker pattern to
  reuse
- Source: `.scratch/testing-improvements/audit/G-jepsen-harness.md` `key-routing-checker-blind-to-data`,
  `.scratch/testing-improvements/audit/verdicts-G.md` (same, "key-routing-partition not in run.py; core claim
  stands — backs raft-chaos + key-routing-kill")

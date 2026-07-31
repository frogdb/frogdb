# Jepsen: raft-chaos (harshest nemesis) backed by a checker blind to wrong/dropped values

Status: done
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

## Resolution

Done 2026-07-23. Chose the issue's **alternative** — add value-tracking to `key_routing.clj`
itself — over switching `raft-chaos` to an Elle workload, and here is why: `raft-chaos` exists to
stress cluster **routing** under the harshest faults. Both Elle workloads (`elle-list-append`,
`elle-rw-register`) co-locate every key to a single `{elle}` hash slot — i.e. one node — which
would collapse the multi-slot routing coverage that is `raft-chaos`'s unique value. The issue
explicitly offers the in-place alternative "if `key_routing`'s cluster-routing-specific coverage is
still wanted alongside data correctness." It is, so the checker was made data-aware while keeping
un-tagged keys spread across every shard. Elle's anomaly detection remains exercised by the
`raft-extended` suite (`elle-rw-register`).

### What changed
- `key_routing.clj` generator: reads/writes now draw from a bounded, multi-slot key pool
  (`kr-0..kr-99`) so reads actually target previously-written keys; written values come from a
  per-run monotonic counter, so every value is **globally unique** (a value under the wrong key =
  routing corruption; a never-written value = fabrication — both detectable). A `final-generator`
  reads every pool key back post-recovery as a distinct `:final-read` op.
- `key_routing.clj` checker now computes and gates on real data, not just redirect counts:
  - **Value-correctness (ALWAYS gates `:valid?`):** any successful non-nil read returning a value
    never written to that key fails. Timing- and topology-independent — closes the "wrong value
    passes green" hole under every schedule including `raft-chaos`.
  - **Routing (ALWAYS gates):** no `:too-many-redirects`.
  - **Durability (conditional gate):** every acked write must be readable in the post-recovery
    `:final-read` phase. Asserted *soundly* on final reads only (they run after every write
    completed + after nemesis heal, so a mid-run nil read of a not-yet-written key is not
    miscounted as loss). It gates `:valid?` **only when no process was killed**: the raft-cluster
    shards are single-owner with **no per-shard replication** (confirmed in
    `docker-compose.raft-cluster.yml` — n1/n2/n3 are independent masters; Raft governs only
    metadata/membership), so a SIGKILL can lose recent un-fsynced acked writes by design
    (Redis-style async persistence) — the same reasoning the `raft-cluster-membership` nemesis
    already encodes. Under kills, lost-write counts are surfaced but do not fail the run; without
    kills, a lost acked write is a hard failure.
- `run.py` raft-chaos comment updated; `test-catalog.md` "what it tests" columns updated.
- New `test/jepsen/frogdb/key_routing_test.clj`: 8 deterministic checker tests / 27 assertions
  (acceptance criterion 3) — proves it catches lost writes (no kill) and wrong values, treats a
  post-kill loss as informational, treats an early nil-before-write correctly (no false positive),
  and never excuses corruption.

### Run evidence (Docker, clean topology, `--time-limit 90`, reused image)
- **First run exposed a false-positive in my own initial checker** (durability asserted on *all*
  reads, ignoring order): `:valid? false`, `:lost-write-count 29` — but the flagged keys (e.g.
  `kr-1`) had been written and read back correctly; the "loss" was early reads of not-yet-written
  pool keys. Notably **no `:kill` fired that run** (only partitions/pauses/slow-net/disk), so it
  could not be genuine single-master loss. Fixed by scoping durability to `:final-read` +
  kill-gating (above).
- **After the fix, `raft-chaos` PASSES legitimately** (`store/frogdb-key-routing-raft-cluster-docker-cluster/`):
  `:valid? true`, `:values-correct? true`, `:no-lost-writes? true`, `:durability-enforced? true`,
  `:process-killed? false`, `:final-reads 100`, `:written-keys 92`, `:total-writes 116`,
  `:wrong-value-count 0` — under a real fault mix (asymmetric + leader-isolated partitions, pauses,
  add-latency, disk-readonly). All 92 acknowledged writes survived and read back with correct
  values; routing stayed correct under chaos. The checker is now **sighted** and validating data
  rather than blind.
- No genuine FrogDB data-loss bug was surfaced (routing + durability held under the non-kill chaos
  this run sampled). The `raft-chaos` test stays in the default `raft`/`all` suites — it is not a
  known-red test.

### Acceptance criteria
- [x] `raft-chaos` (+ `key-routing-kill`) run against a checker that detects wrong-value reads and
      dropped writes, not just redirect counts.
- [x] `key_routing.clj` kept; `:valid?` now incorporates value + last-acked-write tracking.
- [x] A deliberately-introduced data-loss / wrong-value bug is caught — proven deterministically by
      `key_routing_test.clj` (lost write with no kill → fail; wrong value → fail), where the old
      redirect-only checker passed.

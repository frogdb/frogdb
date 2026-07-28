# Register linearizability workload has thin fault coverage; :all and nemesis-pause are dead/unreachable

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 3/3 (score 6)
Area: jepsen

## Context

`register.clj` implements a Knossos-checked linearizable CAS-register workload (lines 161-165) — the harness's canonical single-key linearizability check. It currently only runs paired with `register/none` and `register/crash|kill` on a single node (`run.py`). It never runs against the replication or raft topologies, and never under clock, disk, network, memory, or pause faults — the extended-fault suite swaps register out for Elle specifically to avoid Knossos OOM at scale (`run.py:256-258`), leaving no linearizability check under any fault beyond crash/kill on a single node.

Two related dead spots compound the gap: the `:all` composed nemesis (`nemesis.clj:1251`) has no `TestDefinition` referencing it at all, and `nemesis-pause` is defined with `suites=()` in `run.py:143` — meaning it exists in the harness's option space but is unreachable through any suite grouping.

## What to build

- Add `register+pause` `TestDefinition`: register workload + pause nemesis (bounded time-limit to keep Knossos checking tractable).
- Add `register+partition` `TestDefinition`: register workload + partition nemesis (bounded time-limit, single-node or small replication topology to bound state space).
- Fix `nemesis-pause`'s `suites=()` in `run.py:143` so it's reachable via at least one suite, or remove it if genuinely unused.
- Either give `:all` (`nemesis.clj:1251`) a real `TestDefinition`, or delete the dead composed-nemesis definition if it's not meant to be used standalone.

## Acceptance criteria

- [ ] `register+pause` and `register+partition` `TestDefinition`s exist in `run.py`, bounded in time/state-space to avoid Knossos OOM.
- [ ] `nemesis-pause` is reachable through at least one suite grouping (fixed `suites=` list), or is removed along with dead references if not needed.
- [ ] `:all` composed nemesis (`nemesis.clj:1251`) either has a real `TestDefinition` or is deleted.
- [ ] New register+fault runs execute successfully (or with a triaged, understood failure) in a manual harness invocation before merging.

## Blocked by

None - can start immediately.

## References

- `testing/jepsen/frogdb/src/jepsen/frogdb/register.clj:161-165` (Knossos linearizable CAS-register check)
- `testing/jepsen/frogdb/run.py:256-258` (extended-fault suite swaps register for Elle citing Knossos OOM)
- `testing/jepsen/frogdb/run.py:143` (`nemesis-pause`, `suites=()` — unreachable)
- `testing/jepsen/frogdb/src/jepsen/frogdb/nemesis.clj:1251` (`:all` composed nemesis, no `TestDefinition`)

## Resolution

Done. New register-under-fault coverage added, the two dead spots wired, and — the headline
finding — a latent harness bug root-caused and fixed while validating the partition variant.

### Coverage added (`run.py`)

- `register-pause` — register + `pause` nemesis, single node, 60s. (This also resolves the
  dead `nemesis-pause` entry: it was renamed to `register-pause` and given a real
  `suites=("crash","all","register-fault")` list, so it is now reachable.)
- `register-all` — register + the composed single-node `:all` nemesis (kill+pause), 60s,
  `suites=("crash","all","register-fault")`. This wires the previously-dead `:all` nemesis
  (`nemesis.clj:1251`) rather than deleting it — it is a legitimate composed fault worth
  exercising against the linearizable register.
- `register-partition` — register + `partition` nemesis on the 3-node replication topology,
  30s, `--concurrency 2`, in its own `register-fault` suite (kept out of the default suites
  so a heavier replication-topology Knossos run never joins a batched suite).

To make `register-partition` meaningful and valid, two supporting changes:

- `core.clj` — the multi-node client node list now honors an explicit `--node`/`:nodes` opt
  (falling back to `["n1" "n2" "n3"]`). This pins the client to the primary `n1`. A partition
  on a single node is a no-op, and an *unpinned* 3-node register would be red-by-construction
  (async replicas serve stale reads → non-linearizable by design), so pinning asserts the real
  property: the primary keeps serving a linearizable single-key register even when isolated
  from all replicas.
- `run.py` `TestDefinition` gained `replication_flag`, `client_nodes`, and `concurrency`
  fields, threaded into both `run_test` (CLI flags) and `generate_batch_edn` (`:replication`,
  `:nodes`, `:concurrency`).

### Headline finding — harness bug: Carmine RESP errors were escaping uncaught

Validating `register-partition` first produced repeated Knossos `:valid? :unknown`
out-of-memory results. Root cause was **not** a linearizability violation and **not** merely
search-space size — it was a real bug in the register client's error handling:

- `client.clj`'s `with-error-handling` used slingshot's `try+`. slingshot special-cases
  `clojure.lang.ExceptionInfo` (to unwrap its own `throw+` payloads), so a **plain** `ex-info`
  thrown by Carmine — which is *every* RESP error reply, including `CLUSTERDOWN` — slips past
  both the `(catch clojure.lang.ExceptionInfo ...)` clause *and* the trailing
  `(catch Exception ...)` and escapes uncaught. Verified in isolation: `try+` lets the ex-info
  through; plain `try` catches it.
- Consequence: under the partition nemesis, every write the isolated primary rejected with
  `CLUSTERDOWN` crashed the Jepsen worker and Jepsen recorded the op as indeterminate `:info`.
  Hundreds of pending `:info` ops made the Knossos `:linear` search explode into OOM. This bug
  affected *all* workloads using `with-error-handling` under any fault that elicits a RESP
  error reply, not just this new test.
- Fix: `with-error-handling` now uses plain `try` (the invoke path throws only plain
  Java/Carmine exceptions — no slingshot conditions — so `try+` bought nothing), and its
  `ExceptionInfo` branch classifies `CLUSTERDOWN`/`READONLY`/`MASTERDOWN` rejections as `:fail`
  (definitively did-not-apply) by inspecting the Carmine `:prefix` keyword and the full cause
  chain. Rejected writes are now a clean no-op for the checker.

### Validation (clean topology, moderate limits; `:valid?` per run)

- `register-pause`  — PASS, `:valid? true` (395 ok / 0 info).
- `register-all`    — PASS, `:valid? true` (399 ok / 0 info; nemesis confirmed firing).
- `register-partition` — PASS, `:valid? true` (57 ok / 24 fail / **0 info**), 0 worker
  crashes, 0:46. The 24 `:fail` are the `CLUSTERDOWN` write rejections now correctly
  classified. No linearizability violation was found — FrogDB keeps the primary's single-key
  register linearizable while isolated from its replicas, and correctly rejects writes it
  cannot make durable (quorum safety).

### Acceptance criteria

- [x] `register-pause` and `register-partition` `TestDefinition`s exist, bounded in
  time/state-space (60s single-node; 30s + `--concurrency 2` for the replication partition).
- [x] `nemesis-pause` made reachable (renamed `register-pause`, real `suites=`).
- [x] `:all` composed nemesis wired via `register-all` (kept, not deleted).
- [x] New register+fault runs execute successfully in manual harness invocation (all three
  PASS; store artifacts captured under `store/`, gitignored).

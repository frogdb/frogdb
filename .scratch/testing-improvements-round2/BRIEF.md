# Round 2 testing audit — shared brief

**Read this fully before starting.** You are one of ~15 parallel agents auditing test
coverage/quality for the core FrogDB database ahead of a production-readiness push. Each
agent owns one crate/region. Your job is **investigation and a written proposal**, not
implementation.

## Hard constraints

- **Do NOT modify any source or test file.** Read-only investigation. The single file you
  write is your proposal doc (path given in your dispatch prompt).
- **Do NOT run the test suite or any build.** Compiles are expensive and other agents are
  running concurrently. Static reading + the pre-computed coverage data below is enough.
  `rg`/`grep`/`jq`/`python3` are fine.
- Work only inside `/Users/nathan/workspace/workspace-3` (this is a worktree; do not touch
  the main checkout).
- Ignore: `frogctl`, `frogdb-operator`, `website`, `ops/*`, codegen crates
  (`*-derive`, `docs-gen`, `helm-gen`, `dashboard-gen`, `deb-gen`). Those are out of scope
  for everyone.

## Repo shape

Workspace crates live under `frogdb-server/crates/`. FrogDB is a Redis/Valkey-compatible
database: RESP2/3 protocol, sharded single-writer-per-shard core, standalone +
primary/replica replication + Raft-backed cluster mode, RDB/AOF/WAL persistence, Lua
scripting, tantivy-backed FT.* search, ACL, tiered storage.

Existing test surfaces (know these before proposing a boundary):

| surface | where | what it is |
|---|---|---|
| unit tests | `#[cfg(test)] mod tests` inline in `src/**` | most crates have only this |
| `frogdb-core` integration | `crates/core/tests/*.rs` | incl. `shard_driver/` harness: builds a real `ShardWorker` via `ShardWorkerBuilder` + `frogdb_commands::register_all`, drives commands/ticks through feature-gated `drive*` seams (`shard-driver`, `fake-wal` features). **No socket, no connection layer.** Also `proptest_*`, `concurrency.rs`, `tiered_storage.rs` |
| server integration | `crates/server/tests/integration_*.rs` (59 files) | boots a real server via `frogdb-test-harness` (`TestServer::start_standalone/start_primary/start_replica/start_with_tls/...`) and talks RESP over a socket |
| cluster integration | `frogdb-test-harness::cluster_harness` / `cluster_helpers` | multi-node Raft cluster in-process |
| redis parity | `crates/redis-regression/tests/` (99 files, ~63k LOC) | behavioural parity vs real Redis |
| model/chaos | `crates/testing/` | history/checker/conservation/fault-injection/partition/pubsub-oracle/quiescence/workload + Jepsen-style models, turmoil, shuttle |
| jepsen / fuzz / load | `testing/jepsen`, `testing/fuzz`, `testing/load` (repo root) | |
| property | `proptest` used in `core/tests/proptest_*.rs`, `server/tests/property_tests.rs`, `proptest_commands.rs` | |

Build recipes are in `Justfile` (do not run them).

## Coverage data (fresh, 2026-07-28 — use it, don't regenerate it)

- Summary report: `.scratch/testing-improvements/audit/coverage-depth-2026-07-28.md`
- Methodology: `docs/agents/coverage-depth.md` — read the "Reading the classes" table.
- Machine-readable: `target/llvm-cov/depth/depth.json` (34 MB), `tests.json` (38 MB),
  `target/llvm-cov/lcov.info` (9 MB). **Never `Read` these whole** — query with
  `python3 -c` / `jq --stream` and filter to your crate first.
- Line coverage is already high workspace-wide (85.0%), so *percentage is not the signal*.
  The signal is the depth classes: `untested` (14849 fns), `single-test` (6475),
  `monoculture` (4325), `hot-but-shallow` (13). A `single-test`/`monoculture` function on a
  durability or consistency path is a bigger finding than an uncovered getter.
- Neither tier proves a test **asserts** anything. Explicitly look for assertion-free or
  assertion-weak tests (executes the path, checks nothing / checks only "no error"), and
  for tests that assert on internal state where they should assert on observable behaviour.

Suggested query starting point (adapt):

```bash
python3 - <<'EOF'
import json
d = json.load(open('target/llvm-cov/depth/depth.json'))
print(type(d), list(d)[:20] if isinstance(d, dict) else len(d))
EOF
```

Inspect the schema first, then filter functions whose `location`/file starts with your
crate path, and rank by class + region count.

## Scoring rubric (this is the deliverable's backbone)

Score every gap on three axes, 1–5 integers:

- **Severity (weight 3)** — consequence if this code misbehaves in production for a
  critical system. 5 = silent data loss / durability violation / consistency violation /
  auth bypass / cross-tenant leak. 4 = crash-loop, unavailability, corruption caught at
  restart. 3 = wrong answer on a data path a user would notice. 2 = wrong metrics,
  wrong INFO field, degraded perf. 1 = cosmetic.
- **Likelihood (weight 2)** — chance a real FrogDB operator hits it in production. 5 =
  normal operation on default config. 4 = common config/ordinary failure (restart,
  failover, network blip). 3 = plausible ops event (rolling upgrade, slot migration). 2 =
  rare combination. 1 = adversarial/contrived.
- **Effort (weight -1)** — how hard the *test* is to build. 1 = plain unit test. 2 = crate
  integration test. 3 = server integration test with existing harness. 4 = new harness /
  multi-node / fault injection needed. 5 = new infrastructure (deterministic sim, new
  fault primitive, model checker).

**Priority = 3·Severity + 2·Likelihood − Effort.** Report it. High-effort/low-impact work
must be explicitly deprioritised — say so rather than padding the list. It is better to
return 12 sharp findings than 40 mediocre ones.

## Test-design guidance (the user cares about this a lot)

For every proposed test, name the **abstraction boundary** and justify it in one line.

> Write the test at the **highest level that still directly exercises the behaviour**,
> without adding *unnecessary* layers. Higher-level tests survive refactors; unnecessary
> layers make tests slow, flaky, and indirect.

Concretely, prefer in this order — pick the first that genuinely covers the behaviour:

1. **Pure unit test** on the function/type — for pure algorithmic edge cases (encoding,
   parsing, geohash boundaries, listpack→hashtable conversion thresholds, glob matching).
2. **Crate-level API test** — the crate's public surface, no server.
3. **`shard_driver` harness** (`core/tests/shard_driver/`) — real command dispatch, real
   shard worker, real WAL seam, no socket/connection/routing. Use for command semantics +
   engine interactions.
4. **Server integration over RESP** (`test-harness::TestServer`) — only when the behaviour
   genuinely involves the connection, RESP encoding, blocking clients, auth, config
   reload, or process lifecycle.
5. **Multi-node harness / cluster harness / turmoil / jepsen** — only for replication,
   failover, partitions, slot migration, distributed consistency.

Anti-pattern to call out if you find it: geohash-style single-shard command edge cases
tested through a full redis client + connection + routing flow.

**When the right boundary is genuinely ambiguous, do not silently pick one** — present it
as an explicit `OPTIONS:` block on that finding with 2–3 candidate boundaries, the
trade-off for each, and your recommendation. The main agent will surface these to the user
as decisions.

Also flag, where relevant:
- gaps better closed by **property/fuzz/model** tests than example tests, and why;
- places where an existing test should be **moved down** a level (over-abstracted, slow,
  brittle) or **moved up** (unit test asserting internals that a refactor would break);
- **missing negative/error-path coverage** (bad arity, wrong type, OOM/limit, truncated
  input, partial write) — these are usually cheap and high-severity;
- **concurrency/interleaving** gaps where shuttle/loom/turmoil would be the right tool.

## Do not duplicate round 1

A prior audit (`.scratch/testing-improvements/`) produced 60 issues + follow-ups 61–66,
**all implemented and merged**. Read `.scratch/testing-improvements/issues/` filenames (and
open any that look adjacent to your area) before writing anything. If your finding overlaps
one, either drop it or state precisely what residue round 1 left uncovered. Round 1 was
heavily replication/cluster/jepsen-focused; per-crate command-level and unit-level depth was
largely *not* covered.

## Deliverable format

Write exactly one markdown file at the path given in your dispatch prompt:

```markdown
# <Area> — testing gap audit (round 2)

## Scope
Paths audited, LOC, current coverage %, depth-class breakdown for this area.

## Summary
3–6 sentences: the shape of the risk in this area. What kind of bug would escape today?

## Existing test inventory
Table: surface → what it covers → notable strengths → notable blind spots.

## Findings

### F<N>: <one-line title>
- **Severity** N — <why, one line: the production consequence>
- **Likelihood** N — <why>
- **Effort** N — <why>
- **Priority** <3S+2L-E>
- **Evidence**: `path/file.rs:line` — <what you actually read that proves the gap; cite
  coverage class where relevant, e.g. "`single-test`, only reached by X">
- **Proposed test**: <what it asserts — concrete, e.g. "after N, restart, assert key K has
  value V and TTL within ±Xms">
- **Boundary**: <level 1–5 from the list> — <one-line justification>
- **OPTIONS** (only when ambiguous): <2–3 boundaries, trade-offs, recommendation>

## Deprioritised
Gaps you found but judged not worth the effort — one line each with the reason. (Prove you
looked; prevent re-litigation later.)

## Cross-area notes
Anything that belongs to another agent's crate, or that needs shared infrastructure.
```

Findings sorted by Priority descending. Aim for 8–20 findings; quality over count.

## Return value

Your final message is consumed by the coordinating agent, not a human. Return:
1. the absolute path of the file you wrote,
2. a compact table of your findings (id, title, S/L/E, priority, boundary),
3. your top 3 in one line each,
4. any cross-area or shared-infrastructure needs,
5. any `OPTIONS` blocks you raised (these become user decisions).

Keep the return under ~150 lines.

# Spec: compatibility/testing-methodology.mdx
Status: rewrite + move (from architecture/testing.md)
Audiences: A2, A5 (primary A2 — this is skeptic-facing evidence, not just a
contributor guide)

Goal: The reader understands the testing pyramid *as it actually exists in the
repo* and can trust that FrogDB's compatibility and correctness claims are
backed by layered, automated testing: unit/integration → Shuttle randomized
concurrency → Turmoil deterministic network simulation → Jepsen (replication +
Raft-cluster topologies) → cargo-fuzz targets → ported Redis regression suites
→ history-based consistency checking. A skeptic (A2) leaves convinced the
methodology is real and specific; a contributor (A5) leaves knowing which
Justfile recipe runs each layer. No aspirational or fictional entries.

Not in scope:
- Per-test-suite exclusion detail — that is [Test suite
  results](/compatibility/test-suite/). Link to it as the concrete output of
  the "ported Redis regression suites" layer.
- The command surface — that is [Command matrix](/compatibility/command-matrix/).
- Full contributor onboarding / dev-environment setup. Keep "how to run tests"
  brief (Justfile recipe names only); deeper contributor content stays in
  Architecture. This page is evidence-first, not a tutorial.
- Consistency-model theory beyond a short, accurate framing (linearizability
  for single-key, serializability for transactions). No aspirational guarantees.

CRITICAL removals / corrections (the whole reason this is a rewrite, not a
move — every one is verified against source):
- **Remove the Loom layer entirely.** The current `architecture/testing.md`
  lists "Loom Tests (Exhaustive Concurrency)" as pyramid tier and section 8.
  There is **no `loom` dependency anywhere** in the workspace (verified: no
  `loom` in any `Cargo.toml`; no `use loom` / `loom::` in any `.rs` outside
  false-positive `bloom` matches). The Loom tier, its section, and its pyramid
  box must all be deleted. PLAN §4 mandates this.
- **Do NOT claim tests run as live/real TCL.** The upstream Redis TCL runner
  (`testing/redis-compat/`) was removed; the regression suite is Rust-native
  ports (`*_tcl.rs` `#[tokio::test]` functions), zero `.tcl` files exist in the
  repo (verified in `frogdb-server/crates/redis-regression/src/lib.rs`). The
  original task framing "run both as real TCL and Rust-native" is FALSE — write
  only "Rust-native ports of the upstream Redis TCL suites."
- **Do not hardcode the fuzz-target count.** The count is 33 `[[bin]]` targets
  today (verified: `testing/fuzz/fuzz_targets/` has 33 `.rs` files and
  `testing/fuzz/Cargo.toml` has 33 `[[bin]]` entries), and it must not be
  hardcoded in prose. Describe the fuzz layer qualitatively (what surfaces are
  fuzzed) and, if a number is wanted, generate it or point to `just fuzz-list`. No
  literal count in committed prose (PLAN §6, §7).
- **DST → name it Turmoil.** The current pyramid box says "DST" (deterministic
  simulation testing) generically; the actual implementation is
  [Turmoil](https://github.com/tokio-rs/turmoil) network simulation (verified:
  `turmoil` feature + `dep:turmoil` in `frogdb-server/crates/server`; tests at
  `frogdb-server/crates/server/tests/simulation.rs` run under
  `--features turmoil`). Name it Turmoil.
- Cut the generic "Best Practices Do/Don't" and "Benchmarks" sections from the
  old contributor page — off-audience here. (Benchmarks are cut sitewide per
  PLAN §4; do not add latency claims.)

Sources of truth (author must verify each layer against these before writing):
- `website/src/content/docs/architecture/testing.md` — the page being rewritten
  and moved. Mine structure, but re-verify every claim; it contains the stale
  Loom/DST/TCL claims above.
- **Shuttle**: features `shuttle` in `frogdb-server/crates/types/Cargo.toml`,
  `crates/core/Cargo.toml`, `crates/server/Cargo.toml`
  (`dep:shuttle`, optional). Randomized concurrency permutation (AWS Labs
  Shuttle). Run via `just concurrency` (nextest `-p frogdb-core --features
  shuttle -E 'test(/concurrency/)'`).
- **Turmoil**: feature `turmoil` in `crates/server/Cargo.toml` (and passthrough
  in `crates/config`). Tests: `crates/server/tests/simulation.rs`,
  `crates/server/tests/common/sim_harness.rs`. Run via `just concurrency`
  (second command: `-p frogdb-server --features turmoil -E
  'test(/simulation/)'`).
- **Jepsen**: harness at `testing/jepsen/`. Topologies driven by
  `testing/jepsen/run.py` and compose files at
  `testing/jepsen/frogdb/docker-compose.replication.yml` and
  `testing/jepsen/frogdb/docker-compose.raft-cluster.yml` (VERIFY the exact
  paths — they are under `testing/jepsen/frogdb/`, NOT directly under
  `testing/jepsen/`; the top-level `testing/jepsen/docker-compose.yml` is the
  control-node compose). Justfile recipes: `just jepsen`, `just jepsen-suite`
  (suites: all, single, crash, replication, raft, raft-extended), `just
  jepsen-up <topology>`, `just jepsen-summary`, `just jepsen-list`. Also see
  the `jepsen-testing` skill and `testing/jepsen/README.md`.
- **cargo-fuzz**: `testing/fuzz/` — 33 targets (do not hardcode). Categories
  span protocol/parse (resp_parse, resp_pipeline, rpc_parse, repl_frame_*),
  data-structure ops (skiplist_ops, bitfield_ops, geo_ops, vectorset_ops,
  json_doc_ops, json_path), search/filter (search_expr_*, filter_expr_*,
  aggregate_*, ft_create_parse), scripting (script_parse, function_restore),
  serialization/decompress (deserialize, hll_deserialize, ts_decompress),
  cluster (cluster_bus_postcard, cluster_snapshot_json), glob matching, stream
  IDs, ACL parse. Recipes: `just fuzz <target>`, `just fuzz-all`,
  `just fuzz-list`.
- **Redis regression suites**: `frogdb-server/crates/redis-regression/`
  (~98 port files). Rust-native ports of upstream TCL; per-file `## Intentional
  exclusions` doc-comments feed the Test suite results page. Link there.
- **History-based consistency checker**:
  `frogdb-server/crates/testing/` — `src/history.rs`, `src/models.rs`,
  `src/checker.rs`, `src/lib.rs`. Records operation histories and checks them
  against consistency models (the Porcupine/Elle-style linearizability idea).
  VERIFY what `checker.rs`/`models.rs` actually check before describing the
  guarantee; keep the framing to what the code does.
- `Justfile` — recipe names only (verified): `just test`, `just test <crate>
  <pattern>`, `just concurrency` (Shuttle + Turmoil), `just test-all`,
  `just fuzz` / `fuzz-all` / `fuzz-list`, `just jepsen*`. Note: tests run via
  `cargo nextest`.

Existing content: `architecture/testing.md` (moved out of Architecture per
PLAN §4/§5; update the Architecture sidebar to drop it and add a redirect or
just repoint internal links). The old `TestServer` struct snippet and the
"integration-tests-first" philosophy can be trimmed to a sentence — this page
is skeptic-facing, not a contributor deep-dive.

Structure (H2/H3 outline):
- Intro (no heading): one paragraph — FrogDB's correctness rests on layered
  automated testing, every layer described below exists in the repo and runs in
  CI; states the pyramid runs bottom-up from unit tests to distributed Jepsen.
- `## The testing pyramid` — an ASCII (or simple) diagram, corrected: Unit →
  Integration → Shuttle (randomized concurrency) → Turmoil (network
  simulation) → Jepsen (distributed correctness). **No Loom box, no generic
  DST box.** Fuzzing and regression suites are cross-cutting; show them
  alongside rather than pretending a strict linear order if that's more honest.
- `## Unit & integration tests` — one short paragraph each. Integration = real
  TCP, black-box RESP contract + white-box internal-state assertions; `cargo
  nextest`. Mention property-based (`proptest`) briefly if still used (VERIFY it
  is still a dependency before naming it).
- `## Randomized concurrency (Shuttle)` — what Shuttle does (randomized
  interleaving exploration, AWS Labs), what it's used for (cross-shard/
  multi-client concurrency), how to run (`just concurrency`).
- `## Network simulation (Turmoil)` — deterministic simulated network for
  replication/cluster scenarios; `simulation.rs`; run via `just concurrency`.
- `## Distributed correctness (Jepsen)` — replication and Raft-cluster
  topologies; fault injection; the `run.py` harness and compose files; suites
  and `just jepsen*` recipes; link to `jepsen-testing` skill/README. This is
  the strongest skeptic evidence — give it room.
- `## Fuzzing (cargo-fuzz)` — coverage-guided fuzzing of parsers and
  serializers; enumerate the *categories* of surfaces fuzzed (not a count);
  `just fuzz-all` / `fuzz-list`.
- `## Ported Redis regression suites` — Rust-native ports of the upstream Redis
  TCL suites; run every commit; intentional exclusions documented in source and
  surfaced on the Test suite results page (link). Explicitly: not a live TCL
  runner.
- `## History-based consistency checking` — the `frogdb-server/crates/testing`
  checker; what consistency properties it verifies (framed to match
  `checker.rs`); short, accurate note on linearizability (single-key) vs
  serializability (transactions) — no aspirational guarantees.
- `## Running the tests` — a short table/list of Justfile recipe names only
  (`just test`, `just concurrency`, `just fuzz-all`, `just jepsen-suite ...`).
  Keep brief; deeper contributor detail lives in Architecture.
- `## See also` — LinkCards: Test suite results, Command matrix, Architecture
  (consistency / replication / clustering pages).

Generated data: none directly. Optionally reference S6/`versions.json` for the
targeted Redis version if the regression-suite paragraph names one (prefer
linking to Test suite results, which already renders it). If a fuzz-target
count is ever shown, it must be generated, not hardcoded — but omission is
preferred.

Drift guards:
- No hardcoded counts (fuzz targets, test files, crates) — PLAN §6/§7. Describe
  categories; link to generated pages for numbers.
- S7 (code-path check) must cover every `testing/...`,
  `frogdb-server/crates/...`, and Justfile path this page cites, so a moved or
  renamed harness path fails CI. List these paths so the S7 script picks them
  up: `testing/jepsen/frogdb/docker-compose.replication.yml`,
  `testing/jepsen/frogdb/docker-compose.raft-cluster.yml`, `testing/fuzz/`,
  `frogdb-server/crates/redis-regression/`, `frogdb-server/crates/testing/`,
  `frogdb-server/crates/server/tests/simulation.rs`.
- Reviewer checklist: assert no "loom", no "real TCL", no latency numbers, and
  that every named recipe exists in the `Justfile` before publish.

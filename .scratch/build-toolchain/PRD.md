# build-toolchain — PRD

Goal: every path that ships or gates FrogDB stays green — cross-compilation, CI toolchain
installs, generated artifacts, and the doc checks that guard them. Session of 2026-09-04
opened this from the Knossos/Jepsen thread (PR #93) after finding `main` red on seven jobs
and `just cross-build` broken.

Follow-ups tracked here: main CI red (issue 02), cross-build usearch/zig (issue 01), and the
detection gap behind 01 — no CI job runs `just cross-build` and `.mise.toml` floats `zig` —
filed as issue 08 once 01 landed.

## Decisions

- D1: parent record lives in `.scratch/build-toolchain/` (scope broadened from
  "build/cross-compilation/release-path" to include CI toolchain and generated-artifact
  checks); integration branch `build-toolchain/impl` branched from
  `origin/issue-cross-build-usearch` (= `origin/main` + issue 01) so the tracker dir exists
  from the first commit and issue 01 ships in the same PR.
- D2: the Unit Tests regressions (cluster handoff/migration/finalization tests) are filed as
  `.scratch/memory-architecture/issues/33` — a regression from that directory's issue 18 —
  and investigated first (reproduce, bisect, root-cause), with the spec-first fix issue
  carved from the investigation report rather than guessed now.
- D3: `just cross-build` / `cross-build-arm` are fixed at the recipe level with target-scoped
  C++ flags — `CXXFLAGS_x86_64_unknown_linux_gnu="-x c++ -mevex512"` and
  `CXXFLAGS_aarch64_unknown_linux_gnu="-x c++"` on the `cargo zigbuild` invocations — not in
  `.cargo/config.toml` (the Docker builder's older clang has no `-mevex512`) and not by
  upgrading `usearch` (2.26.2's numkong fails worse under zig). Rationale in issue 01.
- D4: the CI mise step gets `MISE_DISABLE_TOOLS=rust` as step `env` (generator
  `mise_setup_step()` plus the hand-written testbox workflow) so the `cargo:` backend falls
  back to the runner's PATH cargo, and the `MISE_VERSION` pin from issue 02 is dropped. Not:
  pinning mise ≤ 2026.8.10, removing `rust` from `.mise.toml`, or adding `rust` to
  `install_args`. Rationale in issue 03.
- D5: the issue 33 regression is fixed spec-first in `frogdb-cluster` as a derived budget —
  `max(50 ms, k × heartbeat_interval)`, exposed as cluster config, with the prepare/drain waits
  polling until that budget inside an unchanged `HANDOFF_BARRIER_MS` (FM-CLUSTER-104) — the
  CockroachDB lease-duration / FoundationDB knob shape, not a measured constant. nextest
  `cluster.max-threads` stays 2; no testbox confirmation; issue 34 (jemalloc arenas in test
  binaries) stays separate. Carved as memory-architecture/35, sequenced after 23.
- D6: the `quint_conformance` CI failures are a cold-cache download race on quint's Rust
  evaluator, fixed by a serial warm-up step in the `unit-tests` job before `cargo nextest run`
  (issue 09), not by a `~/.quint` cache or timeout bumps.
- D7: the six genuinely heavy unit tests from issue 06 part B (hash_tcl fuzz ×2, scan_tcl
  write-load, bf_false_positive_rate, telemetry metrics_usage, core scan_stress) get per-test
  nextest overrides at `30s × 3` in `.config/nextest.toml`, in the existing "legitimately heavy,
  not flaky" style — not smaller test inputs (changes what the tests prove) and not a lower CI
  `test-threads` (slows every job). Carved as issue 10.
- D8: issue 06 part C (the two runner-only failures, `test_broadcast_lag_disconnect_and_resync`
  and shard-harness `regression_gap4_second_watcher_aborts`) is decided on evidence, not carved
  now: the `workflow_dispatch` run of `test.yml` that verifies issue 10 doubles as the probe. A
  recurrence carves an investigation issue (11); no recurrence closes 06 with C recorded as a
  one-off. Local mode has no Linux box, so a speculative investigation issue would only block.

# build-toolchain — PRD

Goal: every path that ships or gates FrogDB stays green — cross-compilation, CI toolchain
installs, generated artifacts, and the doc checks that guard them. Session of 2026-09-04
opened this from the Knossos/Jepsen thread (PR #93) after finding `main` red on seven jobs
and `just cross-build` broken.

Follow-ups tracked here: main CI red (issue 02), cross-build usearch/zig (issue 01), and a
CI smoke job for the shipping build path (to be filed once 01 has a fix direction).

## Decisions

- D1: parent record lives in `.scratch/build-toolchain/` (scope broadened from
  "build/cross-compilation/release-path" to include CI toolchain and generated-artifact
  checks); integration branch `build-toolchain/impl` branched from
  `origin/issue-cross-build-usearch` (= `origin/main` + issue 01) so the tracker dir exists
  from the first commit and issue 01 ships in the same PR.
- D2: the Unit Tests regressions (cluster handoff/migration/finalization tests) are filed as
  `.scratch/memory-architecture/issues/24` — a regression from that directory's issue 18 —
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
- D5: the issue 24 regression is fixed spec-first in `frogdb-cluster` as a derived budget —
  `max(50 ms, k × heartbeat_interval)`, exposed as cluster config, with the prepare/drain waits
  polling until that budget inside an unchanged `HANDOFF_BARRIER_MS` (FM-CLUSTER-104) — the
  CockroachDB lease-duration / FoundationDB knob shape, not a measured constant. nextest
  `cluster.max-threads` stays 2; no testbox confirmation; issue 25 (jemalloc arenas in test
  binaries) stays separate. Carved as memory-architecture/26, sequenced after 23.

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

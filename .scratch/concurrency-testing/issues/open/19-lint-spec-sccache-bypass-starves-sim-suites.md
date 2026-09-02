# 19 — `just lint-spec` bypasses sccache; concurrent full recompile starves sim suites

Status: ready-for-agent
Type: bug
Origin: memory-architecture drain session (2026-09-01) — running `just lint-spec` while
turmoil sim suites were executing produced phantom 120s nextest timeouts (fresh test
binaries starved of CPU/disk by an uncached full workspace recompile).

## What happened

`lint-spec` (Justfile:381) runs `RUSTC_WRAPPER="" ./scripts/spec-lint.py` — it explicitly
disables the sccache wrapper the rest of the Justfile sets (Justfile:31). The result is a
full uncached compile of everything spec-lint touches, every run. When that compile shares
the machine with a running sim suite, the sim's per-test wall-clock budget blows and
nextest kills healthy tests at their `terminate-after` limit — failures that look like
product hangs but are pure CPU starvation.

Second-order cost: even run alone, `lint-spec` recompiles work sccache already has, and
its artifacts don't populate the shared cache for later builds.

## What to do (human ruling 2026-09-01: investigate + fix, not just document)

1. Find why `lint-spec` sets `RUSTC_WRAPPER=""`. Likely a stale workaround (sccache once
   choked on whatever `spec-lint.py` passes to cargo — figure out the original failure,
   check whether current sccache still has it). `loop-cost.py` (Justfile:321) and the fuzz
   recipe (Justfile:1165) carry the same bypass — check whether the same reason applies or
   they were cargo-culted.
2. If sccache now works: drop the bypass from `lint-spec` (and any other recipe where the
   reason doesn't hold), verify `just lint-spec` green twice (cold + warm) and that the
   second run hits the cache (`sccache -s` delta).
3. If sccache genuinely can't wrap it: keep the bypass but serialize the recipe instead —
   e.g. wrap the compile step in `flock` on a repo-local lockfile shared with the sim-suite
   recipes, or at minimum document the "never run concurrently with sim suites" rule in the
   Justfile recipe comment *and* the repo CLAUDE.md long-running-commands section.

## Acceptance criteria

- [ ] Root cause of the `RUSTC_WRAPPER=""` bypass identified and written down (commit
      message or recipe comment).
- [ ] Either the bypass is removed and `just lint-spec` passes cold+warm with cache hits,
      or the serialize guard/doc rule is in place.
- [ ] `loop-cost.py` / fuzz recipes checked for the same stale workaround (fix or justify
      in place).

## Files likely touched

- Justfile
- scripts/spec-lint.py
- CLAUDE.md (only if the doc-rule branch is taken)

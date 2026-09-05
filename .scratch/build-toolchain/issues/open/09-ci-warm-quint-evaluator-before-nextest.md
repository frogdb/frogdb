# 09 — CI: warm quint's Rust evaluator once before `cargo nextest run` in the unit-tests job

Status: ready-for-agent
Type: AFK
Size: S
Origin: carved from issue 06 part A (CI runs 33941010778, 33942554391)

## Parent

`.scratch/build-toolchain/PRD.md`

## What to build

The `unit-tests` job in `.github/workflows/test.yml` (generated from
`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`, job `unit-tests`, steps
list ending in `run_step(name="Run unit tests", run="cargo nextest run --all")`) gets one new
step immediately before `Run unit tests`:

```python
run_step(
    name="Warm quint evaluator",
    run="quint run specs/quint/replication_feed_gate.qnt --max-steps 0 --max-samples 1",
),
```

with a short comment above it (in `test.py`, not the YAML) saying why: the first `quint run` on a
runner downloads quint's Rust evaluator into `~/.quint/rust-evaluator-<version>/`, and the
`frogdb-replication::quint_conformance` tests each shell out to `quint run` in parallel under
nextest, racing that download (`Error: EEXIST: file already exists, open
'/home/runner/.quint/rust-evaluator-v0.6.0/quint_evaluator-x86_64-unknown-linux-gnu.tar.gz'`
plus 15 s timeouts queued behind it — build-toolchain issue 06 / 09). One serial run warms the
cache; the tests then find it.

The command mirrors the harness's own per-run invocation
(`frogdb-server/crates/replication/tests/quint_conformance.rs`, `run_quint(&["run", <spec>,
"--init", …, "--max-steps", "0", "--max-samples", "1"])`) so it exercises exactly the evaluator
the tests need. `quint` is already on PATH in that job via
`mise_setup_step(install_args=MISE_JUST_NEXTEST_QUINT)`.

Regenerate `test.yml` with `just workflow-gen`. Nothing else changes: no nextest config, no test
code, no cache step, no change to the `quint` job or the testbox workflow.

## Acceptance criteria

- [ ] `test.py` has the step, placed immediately before `Run unit tests`, with the comment naming
      issue 06/09 and the EEXIST race
- [ ] `.github/workflows/test.yml` regenerated; `just workflow-gen --check` green
- [ ] cold-cache proof, recorded in the report: `HOME=$(mktemp -d) quint run
      specs/quint/replication_feed_gate.qnt --max-steps 0 --max-samples 1` exits 0 and leaves
      `$HOME/.quint/rust-evaluator-*/` populated (run with the temp HOME captured in a variable
      so the check reads the same directory)
- [ ] `cargo nextest run -p frogdb-replication --test quint_conformance` green locally
      (unchanged code path; proves nothing regressed)
- [ ] the full gate green

## Files likely touched

- `.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py`
- `.github/workflows/test.yml` (regenerated)

## Blocked by

None.

## Decisions

D6

# `just lint-spec` is enforced nowhere — put it in CI

Status: ready-for-agent
Type: mechanism (enforcement gap)
Severity: likelihood 3/3 (campaign 2 already found two mis-tagged rows, c2-09, and a
34% weak-tag rate — consistent with a lint nobody is forced to run), consequence 2/3
(spec ↔ test agreement, the campaign's one mechanical link between a row and its witness,
silently rots) — score 6
Area: campaign mechanism / CI

## Problem

The spec ↔ test agreement lint is the only thing tying an `FM-<AREA>-NNN` row to a real test.
It runs:

- in `just lint` — which lefthook's `rust-clippy` job **skips under `CLAUDECODE=1`**
  (`lefthook.yml`: "agents must run `just lint` before pushing"), i.e. for every agent commit;
- in CI — **nowhere**. `test.yml`'s `lint` job runs `cargo clippy` directly, not `just lint`;
  the `seam-gates` job runs `just lint-gates` (compile-free family; `lint-spec` is excluded
  because it builds test binaries) and `just test-spec-lint`, which is the lint script's *own
  fixture suite*, not the lint over the repo.

So the campaign's spec-first discipline is an honor system: an agent that never runs `just lint`
can land a row naming a test that does not exist, or a tag naming a row that does not.

## Design (ruled 2026-09-02)

**Trailing step in the existing `Unit Tests` job** (`workflow_gen/workflows/test.py`), after
`cargo nextest run --all`:

```
- name: Spec ↔ test agreement
  run: just lint-spec
```

- The job already has every default test binary built, so `cargo nextest list` over
  `NEXTEST_CRATES` is seconds. The `turmoil` feature variant (`NEXTEST_FEATURE_VARIANTS`) is one
  extra build against the warm `rust-cache` (shared-key `stable`) — accepted; the alternative
  (artifact plumbing across the turmoil job) is not worth it.
- Widen the job's `if:` from `rust == 'true'` to `rust == 'true' || specs == 'true'` — a
  spec-only edit is exactly when the lint matters. The `specs` path filter already exists in the
  `changes` job.
- Separate step name so a failure is attributable even though the job shows as `Unit Tests`.
- A separate `spec-lint` job (clean attribution, but a full test-binary rebuild on its own
  runner) was considered and rejected on cost.

## Not in scope

- The lefthook `CLAUDECODE` skip stays; the fix is CI, not making agents wait 10 minutes per
  commit.
- Header/manifest validation runs compile-free in `lint-gates` (issue 13), not here.

## Forcing test

`just workflow-gen --check` passes after the change; a deliberately broken tag (`// FM-TXN-999`
on any test) pushed to a branch turns the `Unit Tests` job red at the new step, and the
step name appears in the failure. Remove the tag before merging.

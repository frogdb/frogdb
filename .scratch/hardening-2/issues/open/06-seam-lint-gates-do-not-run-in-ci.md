# The 11 seam-lint gates run neither in CI nor on agent commits

Status: ready-for-agent
Type: bug (process / CI wiring)
Severity: likelihood 3/3 (every commit), consequence 3/3 (eleven invariants that the repo believes
are enforced are not) — score 9
Area: CI, lefthook, Justfile

## Problem

FrogDB has eleven chokepoint/seam gates: `lint-info-seam`, `lint-redirect-seam`,
`lint-pubsub-confirmation-seam`, `lint-failover-atomicity`, `lint-metrics-chokepoint`,
`lint-format-float`, `lint-clock-seam`, `lint-failure-modes`, `lint-no-typed-unwrap`,
`lint-keyspace-notify-routing`, `lint-script-gate`.

Where they actually run:

- **CI: none of them.** The `lint` job is a bare `cargo clippy --all-targets -- -D warnings`
  (`.github/workflows/workflow_gen/src/workflow_gen/workflows/test.py:158-159`, rendered at
  `.github/workflows/test.yml:117`).
- **Agent commits: three of them.** Eight run only through `just lint`, and lefthook's
  `rust-clippy` job is skipped entirely when `CLAUDECODE=1` (`lefthook.yml:34-37`). Only
  `no-typed-unwrap`, `keyspace-notify-routing` and `script-gate` are wired to lefthook directly.
- **Human commits:** the eight fire only if the developer runs `just lint` by hand.

So the guarantees these gates encode are push-discipline convention
(`docs/agents/hardening-campaign.md:26-29`), not enforcement — and the majority of commits in this
repo are agent commits, which is exactly the population the skip excludes.

There is also no `docs/agents/` page describing the seam-lint family, so the convention is
invisible to a new agent reading the docs.

## Fix

1. Add a `just lint-gates` recipe containing only the compile-free gates — everything except
   `lint-failure-modes` (compiles) and the turmoil lints. Target: sub-second.
2. Wire `lint-gates` into lefthook pre-commit **unconditionally**. The `CLAUDECODE=1` skip exists
   because clippy compiles and is slow (`lefthook.yml:27-32`); that rationale does not apply to a
   set of greps.
3. Add a `seam-gates` CI job in `workflow_gen/src/workflow_gen/workflows/test.py` running
   `just lint-gates`, and list it in the required-jobs array (mirroring
   `.github/workflows/test.yml:474-483`). Regenerate the workflows.
4. Add `docs/agents/seam-lints.md` — the family, the two suppression idioms (count-pinned
   allowlist, named-gap warn), and how to add a rule. Link it from `CLAUDE.md`'s agent-skills
   section next to coverage-depth / issue-tracker / triage-labels.

## Verification

Introduce a deliberate violation of each gate on a scratch branch and confirm both the pre-commit
hook and the CI job fail. Then revert.

## Comments

Found by the campaign-2 chokepoint-lint survey, 2026-08-07. This blocks every other W1 item: new
rules inherit the same hole, so this lands first.

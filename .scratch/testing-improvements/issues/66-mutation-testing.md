# 66 — Mutation testing (cargo-mutants) over the low-diversity modules

Status: needs-triage

## Why

Neither line coverage nor the new coverage-depth metrics prove that any test *asserts*
on a behavior. A test can execute a function 500 times and check nothing — it will show
up as a hot, well-covered, high-diversity function in every report we produce today:

- `just coverage` / `coverage-nightly.yml`: reports the line as covered.
- `just coverage-depth` T1 (exec counts): reports the line as hot.
- `just coverage-depth` T2 (test diversity): reports the function as reached by many
  distinct tests across many suites.

All three measure *reachability*. Mutation score is the only metric that measures
*sensitivity*: mutate the code, and if the suite still passes, no test was actually
checking that behavior. It is the gap that closes the argument.

Mutation testing was deliberately kept out of the coverage-depth work
(`docs/agents/coverage-depth.md`) because `cargo-mutants` reruns the test suite once per
surviving mutant, which is a fundamentally different cost class from a single
instrumented run.

## Proposal

Scope `cargo-mutants` to the modules coverage-depth surfaces as low-diversity, rather
than the whole workspace:

1. Run `just coverage-depth` and take `target/llvm-cov/depth/depth.json`.
2. Select the files carrying the most `single-test` / `monoculture` / `hot-but-shallow`
   functions — these are exactly the places where a passing suite is least likely to be
   an actual guarantee.
3. Run `cargo mutants --file <selected>` against those, one module at a time, with a
   per-mutant timeout derived from the module's normal test time.
4. Record surviving mutants as issues; a survivor is a concrete, reproducible "this
   behavior is unasserted" claim, not a heuristic.

Ordering matters: running mutants over a `well-covered` module mostly confirms what the
suite already guarantees, while running it over a `monoculture` module is where the
survivors are expected to be.

## Open questions for triage

- Tool selection: `cargo-mutants` is the obvious candidate (workspace-aware, no nightly
  requirement). Confirm it copes with the crate graph size and with the tests that spin
  up multi-node clusters — those should almost certainly be excluded from the mutant
  test command via a nextest filter, since their runtime dominates.
- Where does it run? Per `CLAUDE.md`, anything this heavy belongs on a Blacksmith
  testbox, not the laptop; likely a periodic (not per-PR) job.
- Baseline first: mutation score is only useful as a trend. Establish a baseline on two
  or three modules before deciding whether to widen scope.
- Add `"cargo:cargo-mutants"` to `.mise.toml` if this is adopted.

## References

- `docs/agents/coverage-depth.md` — the tiers this issue extends, and why they stop short
- `.scratch/testing-improvements/audit/coverage-depth-<date>.md` — the ranked
  low-diversity lists that would seed the module selection
- `.scratch/testing-improvements/audit/coverage-summary.md` — the 2026-07-22 line-coverage
  baseline

## Comments

Filed 2026-07-28 as the explicit follow-up recorded in the coverage-depth plan. The plan
text said "17 is next"; the directory is actually at 65, so this is 66.

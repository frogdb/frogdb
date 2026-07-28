# Coverage depth: exec counts + test diversity

`just coverage` answers **"is this line covered"**. `just coverage-depth` answers
**"how well tested is this codepath"**, which is a different question.

- Tooling: [`scripts/coverage-depth.py`](../../scripts/coverage-depth.py),
  [`scripts/cov-runner.sh`](../../scripts/cov-runner.sh)
- Recipes: `just coverage-depth [crate] [pattern]`, `just coverage-calibrate <crate>`
- Outputs: `target/llvm-cov/depth/index.html`, `target/llvm-cov/depth/depth.json`,
  `.scratch/testing-improvements/audit/coverage-depth-<date>.md`

The existing `just coverage` / `just coverage-lcov` recipes are untouched and keep
working; this is a parallel pipeline with its own `CARGO_TARGET_DIR`
(`target/covdepth`), so it never invalidates the normal build cache.

## Why a percentage is not enough

The repo already computes execution counts and throws them away: `just coverage-lcov`
emits `DA:<line>,<count>` records, and the only consumer (`coverage-nightly.yml`)
sums `LH`/`LF` into a single number. Every count is discarded.

Restoring the counts is necessary but not sufficient, because **summed exec count
measures hotness, not test quality**:

- a line inside a hot loop touched by *one* test scores 1,000,000
- a line touched once by *fifty different* tests scores 50

The second is far better tested. So the tool reports two signals:

| Tier | What it measures | How it is produced |
|---|---|---|
| **T1** | exec counts, region coverage, cold lines | one aggregate `llvm-cov export` over the whole suite |
| **T2** | per-function **test diversity** — how many *distinct tests* enter a function, and which | per-test profiles, joined by mangled symbol name against T1's `functions[]` |

## How T2 is affordable

nextest forks one process per test. That means a per-test coverage profile costs
nothing at runtime — only a different `LLVM_PROFILE_FILE` per process, which
`scripts/cov-runner.sh` sets as a cargo target-runner. Under plain `cargo test` an
entire binary's tests share one process and cannot be separated at all.

The expensive operation is `llvm-cov export`, which re-parses the binary's coverage
map on every call; the test binaries here are 100–127 MB debug objects, so a per-test
export would take hours. T2 avoids it entirely: it reads function entry counters
straight out of each per-test profile with `llvm-profdata show` (~3.5 ms) and joins
them to the single aggregate export.

Measured on `frogdb-types` (364 tests, `just coverage-calibrate frogdb-types`):

| step | cost |
|---|---|
| per-test sparse `.profdata` | ~9 KiB mean (raw `.profraw` would be 5–15 MB) |
| `llvm-profdata show` per profile | ~3.5 ms |
| aggregate `llvm-profdata merge` | seconds, once |
| `llvm-cov export` | seconds, **once**, regardless of test count |
| profile-name → export-name join | 100 % hit rate |

**Line-level diversity is out of scope** — that is the tier that would need one export
per test. T2 is function-granular.

## Reading the classes

Each function gets exactly one class, assigned in this priority order:

| Class | Rule | Why it matters |
|---|---|---|
| `untested` | `test_count == 0` | no test reaches it at all |
| `single-test` | `test_count == 1` | one test is the entire safety net; deleting it silently removes all coverage |
| `monoculture` | one suite, `test_count > 1` | several tests, but all from one angle of attack — 100 % line coverage can hide this |
| `hot-but-shallow` | high `exec_total`, few tests | **the class this tool exists for**: both the coverage percentage and raw exec counts report these as healthy |
| `well-covered` | `>= 5` tests across `>= 2` suites | — |
| `covered` | anything else | middling breadth |

A "suite" is `<test-binary>::<top-level test module>`, so `integration_cluster::…`
tests and `frogdb_core` unit tests count as different suites.

**Cold lines** (`count == 1`) are reported separately: executed exactly once across the
entire suite, which is almost always an incidental touch on the way to something else
rather than a tested path.

Thresholds are tunable: `--well-covered-tests`, `--hot-tests`, `--hot-exec-floor` on
`coverage-depth.py report`. The `hot-but-shallow` exec floor defaults to the greater of
1000 and the 90th percentile of exec counts, and the value actually used is printed in
every report.

## Limitations

- **No branch coverage.** Rust is pinned to stable 1.92.0; `-Z coverage-options=branch`
  and MC/DC are nightly-only. Reports show **region** coverage instead, which is finer
  than line coverage but is not branch coverage. Do not read `regions %` as branch %.
- **Function granularity for diversity.** The HTML gutter shows the per-line exec count
  (T1, exact) next to the test count of the *enclosing function* (T2). Two lines in the
  same function always show the same test count.
- **Neither tier proves a test asserts anything.** A test can execute a function 500
  times and check nothing. Closing that gap needs mutation testing — tracked as
  [issue 66](../../.scratch/testing-improvements/issues/66-mutation-testing.md).
- **HTML embeds a bounded number of sources.** The page is self-contained (no CDN, opens
  offline), so it embeds source only for the highest-signal files; the count is stated on
  the page and `depth.json` always holds the complete, untruncated data.
- **Local-only.** Nothing here is wired into `coverage-nightly.yml`. Revisit once the
  full-suite timings are known.

## Operational notes

- Run it **in the foreground**. `CLAUDE.md` steers heavy compute to a testbox, but this
  pipeline is deliberately local; foreground also avoids the documented macOS
  background-QoS disk-I/O throttle.
- `coverage-depth.py run` serially pre-validates every freshly built test binary
  (`<bin> --list`, one at a time) before starting nextest. ~40 new instrumented binaries
  hitting `exec` simultaneously is the documented `syspolicyd` wedge trigger.
- A test killed by nextest's slow-timeout dies before its own profile merge. The runner
  writes its manifest entry *before* running the child, and the orchestrator sweeps
  leftover `.profraw` files afterwards, so those tests are recovered rather than lost.
- **Expect a few timing-sensitive tests to fail under instrumentation.** The instrumented
  build is slower, so tests with hard wall-clock budgets can miss them — the 2026-07-28 run
  lost `integration_replication::test_broadcast_lag_disconnect_and_resync::case_2_with_persistence`
  on a `WAIT 1 3000` that returned 0, and it passes in 2.6 s uninstrumented. A test failure
  does not invalidate the profiles: the pipeline continues past a non-zero nextest exit and
  reports on whatever ran. Confirm any failure by rerunning it with plain `just test` before
  treating it as a real bug.
- `integration_cluster::` tests have `retries = 2`, so one test can produce up to three
  profiles; profile filenames carry the pid to keep them from clobbering each other, and
  the report de-duplicates by test name.

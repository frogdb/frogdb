"""Fuzzing workflow definition.

The 34 fuzz targets under `testing/fuzz/fuzz_targets/` are FrogDB's only coverage of
the raw byte-boundary parsers/decoders (RESP, cluster-bus postcard, replication frame
codecs, RDB `RESTORE` payloads, JSON/search/aggregate expression parsers, HLL/TS
deserialisers, …) against adversarial input. Before this workflow the harness existed
but was inert (issue 40, `.scratch/testing-improvements/issues/`): `fuzz.yml` was
`workflow_dispatch`-only, ran each target for a fixed `-max_total_time=10`, and
`testing/fuzz/.gitignore` excluded `corpus/` — so every invocation started from an empty
corpus and nothing a prior session learned was ever retained. Ten seconds from cold with
no seed corpus barely clears the parser's front door; the targets were effectively never
meaningfully fuzzed.

Design (mirrors the repo's other long-running tiers, `concurrency_nightly.py` /
`jepsen_nightly.py`, single Blacksmith job, cron + `workflow_dispatch`):

* **Scheduled campaign** (`fuzz-campaign`, cron + manual): runs every target for a
  multi-minute budget (`FUZZ_SECONDS`, default 180s, overridable via dispatch input) and
  accumulates the libFuzzer corpus so each run builds on the last.
* **Corpus persistence via `actions/cache`** rather than committing the corpus to git
  (`.gitignore` keeps `corpus/`/`artifacts/` out of the tree). The campaign key embeds
  `github.run_id` (always unique → the post-job save always writes a fresh entry) with a
  `fuzz-corpus-` `restore-keys` prefix (restores the newest prior corpus). Caches created
  on the default branch are readable by PR branches, which is what lets the PR job below
  replay the corpus the nightly campaign grew.
* **PR corpus-replay** (`corpus-replay`, `pull_request`): a fast regression gate that
  *restores* the persisted corpus (restore-only — no per-PR cache writes) and replays every
  entry against each target with libFuzzer `-runs=0` (execute the corpus once, no
  mutation). It catches a parser regression against any previously-found input without
  paying the campaign's time budget on every PR. A cold PR (empty corpus) degrades to a
  no-op per target rather than failing.
* **Crash artifacts**: cargo-fuzz writes crashing/oom/timeout reproducers to
  `testing/fuzz/artifacts/<target>/`; both jobs upload that tree `if: failure()` so a red
  run ships its reproducers. Each job loops over all targets accumulating a nonzero exit
  (like the Jepsen suite loop) so one crash does not mask crashes in later targets.

Toolchain: nightly Rust (`dtolnay/rust-toolchain@nightly`) + `cargo install cargo-fuzz`,
same as before — cargo-fuzz/libFuzzer require nightly and are orthogonal to the pinned
stable toolchain in `.mise.toml`, so this workflow does not use the mise step.
"""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.helpers import (
    cache_step,
    cargo_cache_step,
    checkout_step,
    libclang_step,
    omap,
    rust_nightly_toolchain_step,
    script,
    self_hosted_env_step,
    upload_artifact_step,
)
from workflow_gen.schema import (
    Job,
    PullRequestTrigger,
    ScheduleTrigger,
    Step,
    Trigger,
    Workflow,
)

RUNS_ON = "blacksmith-4vcpu-ubuntu-2404"

# Off-the-hour and overnight-US, distinct from the other nightly crons (jepsen 37 5 /
# 37 6, concurrency 14 3) so the campaigns don't contend for Blacksmith minutes.
NIGHTLY_CRON = "41 2 * * *"

# Default per-target fuzzing budget in seconds for the scheduled campaign. 180s x 34
# targets ~= 100min of pure fuzzing; with the instrumented build (amortised by the
# rust-cache) the job lands comfortably inside the 360min ceiling. Overridable per manual
# dispatch via the `duration` input. Materially larger than the old fixed 10s.
DEFAULT_FUZZ_SECONDS = "180"

FUZZ_DIR = "testing/fuzz"
CORPUS_DIR = f"{FUZZ_DIR}/corpus"
ARTIFACTS_DIR = f"{FUZZ_DIR}/artifacts"

# Corpus cache: unique save key per run (run_id + attempt) so the post-job save always
# writes; the prefix restore-key pulls the newest prior corpus so it grows across runs.
CORPUS_KEY = "fuzz-corpus-${{ github.run_id }}-${{ github.run_attempt }}"
CORPUS_RESTORE_PREFIX = "fuzz-corpus-"


def _duration_input() -> CommentedMap:
    inp = CommentedMap()
    inp["description"] = (
        "Per-target fuzzing budget in seconds for the scheduled campaign "
        f"(default {DEFAULT_FUZZ_SECONDS})."
    )
    inp["required"] = False
    inp["default"] = SQ(DEFAULT_FUZZ_SECONDS)
    inp["type"] = "string"
    return inp


def _campaign_run_step() -> Step:
    """Fuzz every target for the configured budget, accumulating a nonzero exit.

    The duration reaches the script as the `FUZZ_SECONDS` env var (not inline
    `${{ }}`) so the untrusted dispatch input stays out of the command text — the
    standard script-injection mitigation. On a schedule event the input is empty, so
    the `|| default` expression supplies the default. Each target runs even if an
    earlier one crashed, so a single crash doesn't hide crashes in later targets and
    every reproducer lands in the artifacts tree.
    """
    return Step(
        name="Run fuzz campaign",
        env=omap(
            FUZZ_SECONDS=f"${{{{ github.event.inputs.duration || '{DEFAULT_FUZZ_SECONDS}' }}}}",
        ),
        run=script(
            f"""\
            set -uo pipefail
            targets=$(cargo +nightly fuzz list --fuzz-dir {FUZZ_DIR})
            rc=0
            for target in $targets; do
              echo "::group::Fuzzing $target for ${{FUZZ_SECONDS}}s"
              if cargo +nightly fuzz run "$target" --fuzz-dir {FUZZ_DIR} \\
                   -- -max_total_time="${{FUZZ_SECONDS}}"; then
                echo "$target: OK"
              else
                echo "$target: CRASH (reproducer in {ARTIFACTS_DIR}/$target)"
                rc=1
              fi
              echo "::endgroup::"
            done
            exit $rc
            """
        ),
    )


def _replay_run_step() -> Step:
    """Replay the persisted corpus against every target (fast PR regression gate).

    `-runs=0` makes libFuzzer execute each corpus file once and exit without mutating,
    so this pays only build + replay time, not a fuzzing budget. Targets with no
    persisted corpus (cold cache) are skipped rather than failed. Exit is accumulated
    so one regression doesn't mask others.
    """
    return Step(
        name="Replay persisted corpus",
        run=script(
            f"""\
            set -uo pipefail
            targets=$(cargo +nightly fuzz list --fuzz-dir {FUZZ_DIR})
            rc=0
            for target in $targets; do
              corpus_dir="{CORPUS_DIR}/$target"
              if [ ! -d "$corpus_dir" ] || [ -z "$(ls -A "$corpus_dir" 2>/dev/null)" ]; then
                echo "$target: no persisted corpus, skipping"
                continue
              fi
              echo "::group::Replaying corpus for $target"
              if cargo +nightly fuzz run "$target" --fuzz-dir {FUZZ_DIR} -- -runs=0; then
                echo "$target: OK"
              else
                echo "$target: REGRESSION (reproducer in {ARTIFACTS_DIR}/$target)"
                rc=1
              fi
              echo "::endgroup::"
            done
            exit $rc
            """
        ),
    )


def _crash_artifact_step() -> Step:
    return upload_artifact_step(
        name="fuzz-artifacts",
        path=ARTIFACTS_DIR,
        if_="failure()",
        # This step only runs after a crash/regression, so an empty upload would mean
        # cargo-fuzz wrote the reproducer somewhere other than the expected path — fail
        # loudly rather than the default green "nothing to upload".
        if_no_files_found="error",
    )


def fuzz_workflow() -> Workflow:
    w = Workflow(
        name="Fuzz",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            pull_request=PullRequestTrigger(branches=["main"]),
            workflow_dispatch_inputs=CommentedMap(duration=_duration_input()),
        ),
        env=omap(CARGO_TERM_COLOR="always"),
    )

    # Scheduled (or manually dispatched) multi-minute campaign that grows the corpus.
    w.job(
        "fuzz-campaign",
        Job(
            name="Fuzz Campaign",
            runs_on=RUNS_ON,
            if_="github.event_name != 'pull_request'",
            timeout_minutes=360,
            steps=[
                checkout_step(),
                self_hosted_env_step(),
                rust_nightly_toolchain_step(),
                libclang_step(),
                Step(name="Install cargo-fuzz", run="cargo install cargo-fuzz"),
                cargo_cache_step(shared_key="fuzz"),
                cache_step(
                    name="Persist fuzz corpus",
                    path=CORPUS_DIR,
                    key=CORPUS_KEY,
                    restore_keys=CORPUS_RESTORE_PREFIX,
                ),
                _campaign_run_step(),
                _crash_artifact_step(),
            ],
        ),
    )

    # Fast PR gate: restore the corpus (read-only) and replay it, no fuzzing.
    w.job(
        "corpus-replay",
        Job(
            name="Corpus Replay",
            runs_on=RUNS_ON,
            if_="github.event_name == 'pull_request'",
            timeout_minutes=60,
            steps=[
                checkout_step(),
                self_hosted_env_step(),
                rust_nightly_toolchain_step(),
                libclang_step(),
                Step(name="Install cargo-fuzz", run="cargo install cargo-fuzz"),
                cargo_cache_step(shared_key="fuzz"),
                cache_step(
                    name="Restore fuzz corpus",
                    path=CORPUS_DIR,
                    key="fuzz-corpus-replay-${{ github.run_id }}",
                    restore_keys=CORPUS_RESTORE_PREFIX,
                    restore_only=True,
                ),
                _replay_run_step(),
                _crash_artifact_step(),
            ],
        ),
    )

    return w

"""Nightly Jepsen distributed-correctness workflow definition.

The Jepsen harness (testing/jepsen/run.py + the Clojure workloads under
testing/jepsen/frogdb/) is the only place FrogDB's replication, Raft-cluster,
slot-migration, and failover behaviour is checked for linearizability /
serializability / durability under real fault injection. Before this workflow it
had *no* CI automation (issue 10, `.scratch/testing-improvements/issues/`) — a
distributed-systems regression was only ever caught when someone ran the suite by
hand. This closes that gap once the checker fixes it depended on (tasks 04-08:
slot-migration + raft-chaos + key-routing checkers now assert data, orphaned
cluster/failover workloads wired) had landed, so the signal is real rather than
false-green.

Design, mirroring `concurrency_nightly.py` (the repo's other long-running on-demand
tier):

* Single GitHub-hosted job, `workflow_dispatch` only — the nightly and weekly crons
  were removed; a core run is ~1.5-2h of runner time and nothing gates on it.
* Build the debug Docker image **once** via `just docker-build-debug`
  (Dockerfile.builder, in-Docker Rust build — needs no host Rust/zig toolchain).
  We deliberately do NOT go through `just jepsen-suite`, which forces
  `run.py --build` in its default `cross`/zigbuild mode (the path issue 08's agent
  flagged as macOS-broken and redundant here); instead each suite runs with
  `--no-build` against the image built by the dedicated build step.
* The Jepsen *control* node (Leiningen/JVM) runs on the host and talks to the
  dockerised DB nodes via `--docker`, so the runner needs `lein` on PATH (it is
  not a mise tool — see Brewfile/shell.nix) plus the mise-managed `java`.
* Two suite tiers in one workflow, selected by the `suites` dispatch input: empty
  runs the core topologies, the literal `full` adds the heavier extended/fault
  suites, and any other value is passed through as an explicit suite list.
* `store/` (histories, logs, results.edn, analysis) uploaded as an artifact on
  failure for offline triage; `just jepsen-summary` written to the job summary.
"""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.helpers import (
    checkout_step,
    mise_setup_step,
    omap,
    script,
    upload_artifact_step,
)
from workflow_gen.schema import Job, Step, Trigger, Workflow

# Only the tools the harness needs on the host: `just` (recipes), `uv` (run.py is a
# uv script), and `java` (Leiningen's runtime). No Rust toolchain — the server is
# compiled inside Docker by Dockerfile.builder — so this stays a lean install.
MISE_TOOLS = "just uv java"

# GitHub-hosted standard runner: free and unmetered on public repos, x86 (matching
# production and the rest of CI). The whole run is one sequential build-then-suite
# loop, so extra cores buy little.
RUNS_ON = "ubuntu-latest"

# Core suites: cover every `all`-suite test (single ∪ crash ∪ replication ∪ raft
# == the `all` suite) including the issue's required `raft` + `replication`.
CORE_SUITES = "single crash replication raft"
# The `full` keyword adds the topology-specific extended/fault-only suites that are
# intentionally kept out of the per-PR/`all` tiers for runtime: raft-extended
# (clock/disk/slow-net/memory nemeses on elle-rw-register), replication-extended
# (clock-skew/slow-network on the replication topology), register-fault (Knossos
# register under pause/all + the pinned replication register-partition).
FULL_SUITES = f"{CORE_SUITES} raft-extended replication-extended register-fault"
# Sentinel accepted in the `suites` input as shorthand for FULL_SUITES.
FULL_KEYWORD = "full"

STORE_DIR = "testing/jepsen/frogdb/store"


def _suites_input() -> CommentedMap:
    inp = CommentedMap()
    inp["description"] = (
        "Space-separated run.py suites to run (e.g. 'raft replication'), or "
        f"'{FULL_KEYWORD}' for core plus the extended/fault suites. Empty = core suites."
    )
    inp["required"] = False
    inp["default"] = SQ("")
    inp["type"] = "string"
    return inp


def _install_lein_step() -> Step:
    """Install Leiningen on the runner if it is not already present.

    Jepsen's control node runs `lein` on the host (run.py shells out to it), but
    Leiningen has no mise plugin (it lives in Brewfile/shell.nix for local dev) and
    is not guaranteed to be baked into the runner image. The `stable` bootstrap
    script is JDK-version-agnostic and self-installs its uberjar on first `version`;
    `java` is already on PATH from the mise step. Guarded so a pre-baked lein is a
    no-op. `$HOME/.local/bin` is exported to `$GITHUB_PATH` for later steps.
    """
    return Step(
        name="Install Leiningen",
        run=script(
            """\
            if command -v lein >/dev/null 2>&1; then
              echo "lein already present: $(command -v lein)"
              exit 0
            fi
            mkdir -p "$HOME/.local/bin"
            curl -fsSL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \\
              -o "$HOME/.local/bin/lein"
            chmod +x "$HOME/.local/bin/lein"
            echo "$HOME/.local/bin" >> "$GITHUB_PATH"
            export PATH="$HOME/.local/bin:$PATH"
            lein version
            """
        ),
    )


def _run_suites_step() -> Step:
    """Run the selected Jepsen suites, continuing past a failing suite.

    Each suite is its own `run.py` invocation (own topology up/down via `--teardown`)
    so a red verdict in one topology still lets the rest run and report — the whole
    point of a nightly signal. run.py exits nonzero on any FAIL/UNKNOWN verdict; we
    accumulate that and fail the step at the end so the job goes red while every suite
    still executes. Suite selection comes from env vars (not inline `${{ }}`) to keep
    the untrusted dispatch input out of the command text.
    """
    return Step(
        name="Run Jepsen suites",
        env=omap(
            SUITES_INPUT="${{ github.event.inputs.suites }}",
            FULL_KEYWORD=FULL_KEYWORD,
            CORE_SUITES=CORE_SUITES,
            FULL_SUITES=FULL_SUITES,
        ),
        run=script(
            """\
            set -uo pipefail
            if [ "${SUITES_INPUT}" = "${FULL_KEYWORD}" ]; then
              suites="${FULL_SUITES}"
            elif [ -n "${SUITES_INPUT}" ]; then
              suites="${SUITES_INPUT}"
            else
              suites="${CORE_SUITES}"
            fi
            echo "Running Jepsen suites: ${suites}"

            rc_total=0
            for suite in ${suites}; do
              echo "::group::Jepsen suite: ${suite}"
              # --no-build: the image was built once by the previous step; never let
              # run.py fall back to its host cross/zigbuild path here.
              if uv run testing/jepsen/run.py run \\
                   --suite "${suite}" --no-build --teardown --no-color; then
                echo "suite ${suite}: OK"
              else
                rc=$?
                echo "suite ${suite}: FAILED (run.py exit ${rc})"
                rc_total=1
              fi
              echo "::endgroup::"
            done
            exit ${rc_total}
            """
        ),
    )


def _summary_step() -> Step:
    """Emit the pass/fail table to the log and the GitHub step summary (always)."""
    return Step(
        name="Jepsen summary",
        if_="always()",
        run=script(
            """\
            just jepsen-summary | tee /tmp/jepsen-summary.txt || true
            {
              echo '### Jepsen nightly results'
              echo '```'
              cat /tmp/jepsen-summary.txt 2>/dev/null || echo '(no summary produced)'
              echo '```'
            } >> "$GITHUB_STEP_SUMMARY"
            """
        ),
    )


def jepsen_nightly_workflow() -> Workflow:
    w = Workflow(
        name="Jepsen Nightly",
        # Manual dispatch only: the nightly and weekly crons are deliberately off.
        # A core run is ~1.5-2h of runner time; dispatch it when the distributed
        # layer actually changes.
        on=Trigger(workflow_dispatch_inputs=CommentedMap(suites=_suites_input())),
    )

    w.job(
        "jepsen-nightly",
        Job(
            name="Nightly Jepsen Distributed-Correctness Suites",
            runs_on=RUNS_ON,
            # A core run (single+crash+replication+raft) takes ~1.5-2h wall:
            # the in-Docker debug build (~20-30m on this box) + the sum of suite
            # time-limits (run.py `TESTS`) plus per-topology bring-up and checker
            # time. The `full` suite set adds ~20-30m of extended/fault suites. 360m
            # matches concurrency-nightly and leaves generous margin for CI variance.
            timeout_minutes=360,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_TOOLS),
                _install_lein_step(),
                Step(
                    name="Build debug Docker image",
                    run=script("just docker-build-debug\n"),
                ),
                _run_suites_step(),
                _summary_step(),
                upload_artifact_step(
                    name="jepsen-store",
                    path=STORE_DIR,
                    if_="failure()",
                    # store/ may legitimately be empty if the failure was in the build
                    # step (before any test wrote results), so keep the default "warn"
                    # rather than "error" — we don't want a build failure to also emit a
                    # confusing missing-artifact error.
                ),
            ],
        ),
    )

    return w

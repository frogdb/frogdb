"""Fuzz workflow definition (manual trigger only)."""

from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    libclang_step,
    omap,
    run_step,
    rust_nightly_toolchain_step,
    script,
)
from workflow_gen.schema import Job, Trigger, Workflow


def fuzz_workflow() -> Workflow:
    w = Workflow(
        name="Fuzz",
        on=Trigger(),
        env=omap(CARGO_TERM_COLOR="always"),
    )

    w.jobs["fuzz-regression"] = Job(
        name="Fuzz Regression",
        steps=[
            checkout_step(),
            rust_nightly_toolchain_step(),
            libclang_step(),
            run_step(name="Install cargo-fuzz", run="cargo install cargo-fuzz"),
            cargo_cache_step(shared_key="fuzz"),
            run_step(
                name="Run fuzz targets",
                run=script("""\
                    targets=$(cargo +nightly fuzz list --fuzz-dir testing/fuzz 2>/dev/null)
                    for target in $targets; do
                      echo "=== Fuzzing $target for 10s ==="
                      cargo +nightly fuzz run "$target" --fuzz-dir testing/fuzz -- -max_total_time=10
                    done
                """),
            ),
        ],
    )

    return w

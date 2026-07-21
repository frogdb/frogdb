"""Deploy docs workflow definition."""

from workflow_gen.constants import (
    DEPLOY_PAGES,
    UPLOAD_PAGES_ARTIFACT,
)
from workflow_gen.helpers import (
    cargo_cache_step,
    checkout_step,
    libclang_step,
    mise_setup_step,
    omap,
    run_step,
    rust_toolchain_step,
)
from workflow_gen.schema import (
    Concurrency,
    Environment,
    Job,
    Permissions,
    PushTrigger,
    Step,
    Trigger,
    Workflow,
)

# `just docs-build` regenerates the config-reference, compat-exclusions, and
# command-matrix JSON (via docs-gen, compat-gen, matrix-gen) before running
# the Astro build, so this job needs the same toolchain those generators need
# in CI (see docs-gen-check/compat-gen-check/matrix-gen-check in test.py) —
# Rust (+ libclang, for docs-gen's frogdb-server dependency), Python/uv (for
# the compat-gen.py/matrix-gen.py scripts), and just — on top of node/bun for
# the site build itself.
MISE_INSTALL_ARGS = "node bun python uv just"


def deploy_docs_workflow() -> Workflow:
    w = Workflow(
        name="Deploy docs to GitHub Pages",
        on=Trigger(
            push=PushTrigger(branches=["main"], paths=["website/**"]),
            push_first=True,
        ),
        permissions=Permissions(contents="read", pages="write", id_token="write"),
        concurrency=Concurrency(group="pages", cancel_in_progress=True),
    )

    w.jobs["build"] = Job(
        steps=[
            checkout_step(),
            mise_setup_step(install_args=MISE_INSTALL_ARGS),
            rust_toolchain_step(),
            libclang_step(),
            cargo_cache_step(shared_key="stable"),
            run_step(name="Install site dependencies", run="just docs-install"),
            run_step(name="Build documentation site", run="just docs-build"),
            Step(uses=UPLOAD_PAGES_ARTIFACT, with_=omap(path="website/dist")),
        ],
    )

    w.jobs["deploy"] = Job(
        needs="build",
        environment=Environment(
            name="github-pages",
            url="${{ steps.deployment.outputs.page_url }}",
        ),
        steps=[
            Step(id="deployment", uses=DEPLOY_PAGES),
        ],
    )

    return w

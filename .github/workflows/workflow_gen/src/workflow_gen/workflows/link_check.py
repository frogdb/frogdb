"""Link check workflow definition."""

from workflow_gen.constants import LYCHEE
from workflow_gen.helpers import checkout_step, mise_setup_step, omap
from workflow_gen.schema import (
    Concurrency,
    Defaults,
    DefaultsRun,
    Job,
    Step,
    Trigger,
    Workflow,
)


def link_check_workflow() -> Workflow:
    w = Workflow(
        name="Link check",
        # CI is manual-dispatch-only during the hardening campaign.
        on=Trigger(),
        concurrency=Concurrency(
            group="link-check-${{ github.ref }}",
            cancel_in_progress=True,
        ),
    )

    w.jobs["link-check"] = Job(
        name="Check links",
        defaults=Defaults(run=DefaultsRun(working_directory="website")),
        steps=[
            checkout_step(),
            mise_setup_step(install_args="node bun"),
            Step(name="Install dependencies", run="bun install"),
            Step(name="Build site", run="bun run build"),
            Step(
                name="Check links",
                uses=LYCHEE,
                with_=omap(
                    args="--config lychee.toml --root-dir website/dist website/dist/",
                    **{"fail": "true"},
                ),
            ),
        ],
    )

    return w

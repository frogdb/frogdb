"""Link check workflow definition."""

from workflow_gen.constants import CHECKOUT, LYCHEE, SETUP_BUN, SETUP_NODE
from workflow_gen.helpers import ensure_path, omap
from workflow_gen.schema import (
    Defaults,
    DefaultsRun,
    Job,
    PullRequestTrigger,
    PushTrigger,
    Step,
    Trigger,
    Workflow,
)


def link_check_workflow() -> Workflow:
    paths = ["website/**", "docs/**"]

    w = Workflow(
        name="Link check",
        on=Trigger(
            push=PushTrigger(branches=["main"], paths=paths),
            pull_request=PullRequestTrigger(branches=["main"], paths=paths),
        ),
    )

    w.jobs["link-check"] = Job(
        name="Check links",
        defaults=Defaults(run=DefaultsRun(working_directory="website")),
        steps=[
            Step(uses=CHECKOUT),
            Step(uses=SETUP_NODE, with_=omap(**{"node-version-file": ensure_path("website/.nvmrc")})),
            Step(uses=SETUP_BUN),
            Step(name="Install dependencies", run="bun install --frozen-lockfile"),
            Step(name="Build site", run="bun run build"),
            Step(
                name="Check links",
                uses=LYCHEE,
                with_=omap(
                    args="--config ../lychee.toml dist/",
                    **{"fail": "true"},
                ),
            ),
        ],
    )

    return w

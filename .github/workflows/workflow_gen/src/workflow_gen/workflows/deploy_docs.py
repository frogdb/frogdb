"""Deploy docs workflow definition."""

from workflow_gen.constants import (
    CHECKOUT,
    DEPLOY_PAGES,
    SETUP_BUN,
    SETUP_NODE,
    UPLOAD_PAGES_ARTIFACT,
)
from workflow_gen.helpers import ensure_path, omap
from workflow_gen.schema import (
    Concurrency,
    Defaults,
    DefaultsRun,
    Environment,
    Job,
    Permissions,
    PushTrigger,
    Step,
    Trigger,
    Workflow,
)


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
        defaults=Defaults(run=DefaultsRun(working_directory="website")),
        steps=[
            Step(uses=CHECKOUT),
            Step(
                uses=SETUP_NODE, with_=omap(**{"node-version-file": ensure_path("website/.nvmrc")})
            ),
            Step(uses=SETUP_BUN),
            Step(run="bun install --frozen-lockfile"),
            Step(run="bun run build"),
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

"""Deploy docs workflow definition."""

from workflow_gen.constants import (
    DEPLOY_PAGES,
    UPLOAD_PAGES_ARTIFACT,
)
from workflow_gen.helpers import checkout_step, mise_setup_step, omap
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
            checkout_step(),
            mise_setup_step(install_args="node bun"),
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

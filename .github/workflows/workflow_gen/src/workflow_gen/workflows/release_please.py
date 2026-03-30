"""Release Please workflow definition."""

from workflow_gen.constants import RELEASE_PLEASE
from workflow_gen.helpers import omap
from workflow_gen.schema import (
    Job,
    Permissions,
    PushTrigger,
    Step,
    Trigger,
    Workflow,
)


def release_please_workflow() -> Workflow:
    w = Workflow(
        name="Release Please",
        on=Trigger(
            workflow_dispatch=False,
            push=PushTrigger(branches=["main"]),
            push_first=True,
        ),
    )

    w.jobs["release-please"] = Job(
        name="Release Please",
        permissions=Permissions(contents="write", pull_requests="write"),
        steps=[
            Step(
                name="Run release-please",
                uses=RELEASE_PLEASE,
                with_=omap(
                    **{
                        "config-file": "release-please-config.json",
                        "manifest-file": ".release-please-manifest.json",
                    }
                ),
            ),
        ],
    )

    return w

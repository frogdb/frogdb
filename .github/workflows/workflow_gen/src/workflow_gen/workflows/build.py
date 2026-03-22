"""Build workflow definition."""

from workflow_gen.helpers import (
    checkout_step,
    docker_build_push_step,
    docker_env,
    docker_login_ghcr_step,
    docker_metadata_step,
    setup_buildx_step,
    setup_qemu_step,
)
from workflow_gen.schema import Job, Permissions, PushTrigger, Trigger, Workflow


def build_workflow() -> Workflow:
    w = Workflow(
        name="Build",
        on=Trigger(push=PushTrigger(branches=["main"])),
        env=docker_env(),
    )

    w.jobs["docker"] = Job(
        name="Docker Build",
        permissions=Permissions(contents="read", packages="write"),
        steps=[
            checkout_step(),
            setup_qemu_step(),
            setup_buildx_step(),
            docker_login_ghcr_step(),
            docker_metadata_step(
                tags=[
                    ("ref", {"event": "branch"}),
                    ("sha", {"prefix": ""}),
                    ("raw", {"value": "dev", "enable": "{{is_default_branch}}"}),
                ]
            ),
            docker_build_push_step(
                push="${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}",
                cache=True,
            ),
        ],
    )

    return w

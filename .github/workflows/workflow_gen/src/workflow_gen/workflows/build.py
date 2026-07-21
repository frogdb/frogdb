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
from workflow_gen.schema import Job, Permissions, Trigger, Workflow


def build_workflow() -> Workflow:
    w = Workflow(
        name="Build",
        # Manual trigger only: auto-building on every main merge burns paid
        # Blacksmith minutes the pre-production project doesn't need.
        on=Trigger(),
        env=docker_env(),
    )

    w.jobs["docker"] = Job(
        name="Docker Build",
        runs_on="blacksmith-4vcpu-ubuntu-2404",
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
                push="${{ github.event.workflow_run.conclusion == 'success' }}",
                cache=True,
            ),
        ],
    )

    return w

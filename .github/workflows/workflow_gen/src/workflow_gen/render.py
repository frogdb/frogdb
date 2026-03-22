"""Workflow rendering to YAML."""

from io import StringIO

from ruamel.yaml import YAML

from workflow_gen.schema import Workflow
from workflow_gen.workflows.build import build_workflow
from workflow_gen.workflows.deploy_docs import deploy_docs_workflow
from workflow_gen.workflows.release import release_workflow
from workflow_gen.workflows.test import test_workflow

HEADER = """\
# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: .github/workflows/workflow_gen/
# Regenerate with: just workflow-gen
# =============================================================================

"""

WORKFLOWS = {
    "test.yml": test_workflow,
    "build.yml": build_workflow,
    "release.yml": release_workflow,
    "deploy-docs.yml": deploy_docs_workflow,
}


def render(workflow: Workflow) -> str:
    """Render a workflow to YAML string with header."""
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.width = 4096
    stream = StringIO()
    yaml.dump(workflow.to_yaml(), stream)
    body = stream.getvalue()
    # Quote 'on' key for GitHub Actions compatibility (YAML 1.1 treats on as boolean)
    body = body.replace("\non:\n", "\n'on':\n")
    return HEADER + body

"""Workflow rendering to YAML."""

from io import StringIO

from ruamel.yaml import YAML

from workflow_gen.schema import Workflow
from workflow_gen.workflows.build import build_workflow
from workflow_gen.workflows.cluster_model_nightly import cluster_model_nightly_workflow
from workflow_gen.workflows.cluster_nightly import cluster_nightly_workflow
from workflow_gen.workflows.cluster_quint_quarantine_nightly import (
    cluster_quint_quarantine_nightly_workflow,
)
from workflow_gen.workflows.concurrency_nightly import concurrency_nightly_workflow
from workflow_gen.workflows.coverage_nightly import coverage_nightly_workflow
from workflow_gen.workflows.deploy_docs import deploy_docs_workflow
from workflow_gen.workflows.fuzz import fuzz_workflow
from workflow_gen.workflows.jepsen_nightly import jepsen_nightly_workflow
from workflow_gen.workflows.link_check import link_check_workflow
from workflow_gen.workflows.mutants_weekly import mutants_weekly_workflow
from workflow_gen.workflows.quint_verify import quint_verify_workflow
from workflow_gen.workflows.regression_nightly import regression_nightly_workflow
from workflow_gen.workflows.release import release_workflow
from workflow_gen.workflows.release_please import release_please_workflow
from workflow_gen.workflows.replication_model_nightly import (
    replication_model_nightly_workflow,
)
from workflow_gen.workflows.replication_nightly import replication_nightly_workflow
from workflow_gen.workflows.replication_seeds_nightly import (
    replication_seeds_nightly_workflow,
)
from workflow_gen.workflows.test import test_workflow

HEADER = """\
# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: .github/workflows/workflow_gen/
# Regenerate with: just workflow-gen
# =============================================================================

"""

# Hand-written workflow files that are allowed to exist in the workflows directory.
# The check command will fail if any .yml file exists that is neither generated nor listed here,
# and will also fail if a file listed here is missing.
MANUAL_WORKFLOWS: set[str] = {
    # Blacksmith testbox hydration workflow (dispatched by `blacksmith testbox warmup`,
    # see scripts/testbox-warmup.sh); maintained by hand alongside the CLI-generated skill.
    "test-unit-tests-testbox.yml",
}

WORKFLOWS = {
    "test.yml": test_workflow,
    "fuzz.yml": fuzz_workflow,
    "concurrency-nightly.yml": concurrency_nightly_workflow,
    "cluster-nightly.yml": cluster_nightly_workflow,
    "cluster-model-nightly.yml": cluster_model_nightly_workflow,
    "cluster-quint-quarantine-nightly.yml": cluster_quint_quarantine_nightly_workflow,
    "quint-verify-nightly.yml": quint_verify_workflow,
    "replication-model-nightly.yml": replication_model_nightly_workflow,
    "replication-nightly.yml": replication_nightly_workflow,
    "replication-seeds-nightly.yml": replication_seeds_nightly_workflow,
    "coverage-nightly.yml": coverage_nightly_workflow,
    "jepsen-nightly.yml": jepsen_nightly_workflow,
    "regression-nightly.yml": regression_nightly_workflow,
    "mutants-weekly.yml": mutants_weekly_workflow,
    "build.yml": build_workflow,
    "release.yml": release_workflow,
    "release-please.yml": release_please_workflow,
    "deploy-docs.yml": deploy_docs_workflow,
    "link-check.yml": link_check_workflow,
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

"""Dataclasses representing GitHub Actions workflow structure."""

from dataclasses import dataclass, field

from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ


@dataclass
class PushTrigger:
    branches: list[str]
    tags: list[str] | None = None
    paths: list[str] | None = None

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        if self.branches:
            m["branches"] = CommentedSeq(self.branches)
        if self.tags is not None:
            m["tags"] = CommentedSeq(self.tags)
        if self.paths is not None:
            m["paths"] = CommentedSeq([SQ(p) for p in self.paths])
        return m


@dataclass
class PullRequestTrigger:
    branches: list[str]
    paths: list[str] | None = None

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["branches"] = CommentedSeq(self.branches)
        if self.paths is not None:
            m["paths"] = CommentedSeq([SQ(p) for p in self.paths])
        return m


@dataclass
class WorkflowRunTrigger:
    workflows: list[str]
    types: list[str]
    branches: list[str]

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["workflows"] = CommentedSeq(self.workflows)
        m["types"] = CommentedSeq(self.types)
        m["branches"] = CommentedSeq(self.branches)
        return m


@dataclass
class Trigger:
    workflow_dispatch: bool = True
    push: PushTrigger | None = None
    push_first: bool = False
    pull_request: PullRequestTrigger | None = None
    workflow_run: WorkflowRunTrigger | None = None

    def to_yaml(self) -> CommentedMap:
        on = CommentedMap()
        if self.push_first and self.push is not None:
            on["push"] = self.push.to_yaml()
        if self.workflow_dispatch:
            wd = CommentedMap()
            wd.fa.set_flow_style()
            on["workflow_dispatch"] = wd
        if not self.push_first and self.push is not None:
            on["push"] = self.push.to_yaml()
        if self.pull_request is not None:
            on["pull_request"] = self.pull_request.to_yaml()
        if self.workflow_run is not None:
            on["workflow_run"] = self.workflow_run.to_yaml()
        return on


@dataclass
class Permissions:
    contents: str | None = None
    packages: str | None = None
    pages: str | None = None
    pull_requests: str | None = None
    id_token: str | None = None

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        if self.contents is not None:
            m["contents"] = self.contents
        if self.packages is not None:
            m["packages"] = self.packages
        if self.pages is not None:
            m["pages"] = self.pages
        if self.pull_requests is not None:
            m["pull-requests"] = self.pull_requests
        if self.id_token is not None:
            m["id-token"] = self.id_token
        return m


@dataclass
class Concurrency:
    group: str
    cancel_in_progress: bool

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["group"] = self.group
        m["cancel-in-progress"] = self.cancel_in_progress
        return m


@dataclass
class DefaultsRun:
    working_directory: str

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["working-directory"] = self.working_directory
        return m


@dataclass
class Defaults:
    run: DefaultsRun

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["run"] = self.run.to_yaml()
        return m


@dataclass
class Environment:
    name: str
    url: str

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["name"] = self.name
        m["url"] = self.url
        return m


@dataclass
class MatrixInclude:
    includes: list[CommentedMap]

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["include"] = CommentedSeq(self.includes)
        return m


@dataclass
class Strategy:
    matrix: MatrixInclude
    fail_fast: bool = False

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["matrix"] = self.matrix.to_yaml()
        m["fail-fast"] = self.fail_fast
        return m


@dataclass
class Step:
    name: str | None = None
    id: str | None = None
    uses: str | None = None
    run: str | None = None
    with_: CommentedMap | None = None
    if_: str | None = None

    def __post_init__(self) -> None:
        if self.uses is not None and self.run is not None:
            raise ValueError("Step cannot have both 'uses' and 'run'")
        if self.uses is None and self.run is None:
            raise ValueError("Step must have either 'uses' or 'run'")

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        if self.id is not None:
            m["id"] = self.id
        if self.name is not None:
            m["name"] = self.name
        if self.if_ is not None:
            m["if"] = self.if_
        if self.uses is not None:
            m["uses"] = self.uses
        if self.run is not None:
            m["run"] = self.run
        if self.with_ is not None:
            m["with"] = self.with_
        return m


@dataclass
class Job:
    name: str | None = None
    runs_on: str = "ubuntu-latest"
    steps: list[Step] = field(default_factory=list)
    permissions: Permissions | None = None
    strategy: Strategy | None = None
    needs: list[str] | str | None = None
    if_: str | None = None
    defaults: Defaults | None = None
    environment: Environment | None = None
    outputs: CommentedMap | None = None

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        if self.needs is not None:
            if isinstance(self.needs, list):
                m["needs"] = CommentedSeq(self.needs)
            else:
                m["needs"] = self.needs
        if self.if_ is not None:
            m["if"] = self.if_
        if self.name is not None:
            m["name"] = self.name
        m["runs-on"] = self.runs_on
        if self.permissions is not None:
            m["permissions"] = self.permissions.to_yaml()
        if self.strategy is not None:
            m["strategy"] = self.strategy.to_yaml()
        if self.defaults is not None:
            m["defaults"] = self.defaults.to_yaml()
        if self.environment is not None:
            m["environment"] = self.environment.to_yaml()
        if self.outputs is not None:
            m["outputs"] = self.outputs
        m["steps"] = CommentedSeq([s.to_yaml() for s in self.steps])
        return m


@dataclass
class Workflow:
    name: str
    on: Trigger
    env: CommentedMap | None = None
    permissions: Permissions | None = None
    concurrency: Concurrency | None = None
    jobs: dict[str, Job] = field(default_factory=dict)

    def job(self, key: str, job: Job) -> str:
        """Register a job and return its key for use in needs lists."""
        self.jobs[key] = job
        return key

    def to_yaml(self) -> CommentedMap:
        m = CommentedMap()
        m["name"] = self.name
        if self.env is not None:
            m["env"] = self.env
        m["on"] = self.on.to_yaml()
        if self.permissions is not None:
            m["permissions"] = self.permissions.to_yaml()
        if self.concurrency is not None:
            m["concurrency"] = self.concurrency.to_yaml()
        jobs_map = CommentedMap()
        for k, v in self.jobs.items():
            jobs_map[k] = v.to_yaml()
        m["jobs"] = jobs_map
        return m

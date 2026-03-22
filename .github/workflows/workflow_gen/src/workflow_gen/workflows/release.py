"""Release workflow definition."""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import GH_RELEASE, HELM_REPO_URL
from workflow_gen.helpers import (
    MACOS_TARGETS,
    cargo_cache_step,
    checkout_step,
    docker_build_push_step,
    docker_env,
    docker_login_ghcr_step,
    docker_metadata_step,
    download_all_artifacts_step,
    ensure_path,
    run_step,
    rust_toolchain_step,
    script,
    setup_buildx_step,
    setup_helm_step,
    setup_qemu_step,
    upload_artifact_step,
)
from workflow_gen.schema import (
    Job,
    MatrixInclude,
    Permissions,
    Step,
    Strategy,
    Trigger,
    Workflow,
)


def release_workflow() -> Workflow:
    w = Workflow(
        name="Release",
        on=Trigger(),
        env=docker_env(),
    )

    w.jobs["docker"] = Job(
        name="Docker Release & Linux Binaries",
        permissions=Permissions(contents="read", packages="write"),
        steps=[
            checkout_step(),
            setup_qemu_step(),
            setup_buildx_step(),
            docker_login_ghcr_step(),
            docker_metadata_step(
                tags=[
                    ("semver", {"pattern": "{{version}}"}),
                    ("semver", {"pattern": "{{major}}.{{minor}}"}),
                    ("semver", {"pattern": "{{major}}"}),
                ]
            ),
            docker_build_push_step(cache=False),
            run_step(
                name="Extract Linux binaries from image",
                run=script("""\
                    IMAGE=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ steps.meta.outputs.version }}
                    for arch in amd64 arm64; do
                      docker pull --platform "linux/${arch}" "${IMAGE}"
                      CONTAINER=$(docker create --platform "linux/${arch}" "${IMAGE}")
                      docker cp "${CONTAINER}":/usr/local/bin/frogdb-server ./frogdb-server
                      docker rm "${CONTAINER}"
                      tar -czvf frogdb-${{ github.ref_name }}-linux-"${arch}".tar.gz frogdb-server
                      rm frogdb-server
                    done"""),
            ),
            upload_artifact_step(
                name="frogdb-linux-amd64",
                path="frogdb-${{ github.ref_name }}-linux-amd64.tar.gz",
            ),
            upload_artifact_step(
                name="frogdb-linux-arm64",
                path="frogdb-${{ github.ref_name }}-linux-arm64.tar.gz",
            ),
        ],
    )

    w.jobs["build-macos"] = Job(
        name="Build macOS Binaries (${{ matrix.target }})",
        runs_on="${{ matrix.os }}",
        strategy=Strategy(matrix=MatrixInclude(list(MACOS_TARGETS))),
        steps=[
            checkout_step(),
            rust_toolchain_step(targets="${{ matrix.target }}"),
            cargo_cache_step(shared_key="release-${{ matrix.target }}"),
            run_step(
                name="Build",
                run="cargo build --release --target ${{ matrix.target }} --bin frogdb-server",
            ),
            run_step(
                name="Create release archive",
                run=script("""\
                    cd target/${{ matrix.target }}/release
                    tar -czvf ../../../frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz frogdb-server${{ matrix.ext }}"""),
            ),
            upload_artifact_step(
                name="frogdb-${{ matrix.target }}",
                path="frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz",
            ),
        ],
    )

    w.jobs["helm"] = Job(
        needs=["docker"],
        name="Publish Helm Chart",
        permissions=Permissions(contents="write", pages="write"),
        steps=[
            checkout_step(fetch_depth=SQ("0")),
            setup_helm_step(),
            run_step(
                name="Package Helm chart",
                run=script(f"""\
                    helm package {ensure_path('frogdb-server/ops/deploy/helm/frogdb')} --destination .helm-packages
                    helm repo index .helm-packages --url {HELM_REPO_URL}"""),
            ),
            checkout_step(name="Checkout gh-pages branch", ref="gh-pages", path="gh-pages"),
            run_step(
                name="Update Helm repository",
                run=script(f"""\
                    mkdir -p gh-pages/helm
                    cp .helm-packages/*.tgz gh-pages/helm/
                    # Merge index files
                    if [ -f gh-pages/helm/index.yaml ]; then
                      helm repo index gh-pages/helm --merge gh-pages/helm/index.yaml --url {HELM_REPO_URL}
                    else
                      helm repo index gh-pages/helm --url {HELM_REPO_URL}
                    fi"""),
            ),
            run_step(
                name="Commit and push",
                run=script("""\
                    cd gh-pages
                    git config user.name "GitHub Actions"
                    git config user.email "actions@github.com"
                    git add helm/
                    git commit -m "Release Helm chart ${{ github.ref_name }}" || echo "No changes"
                    git push"""),
            ),
        ],
    )

    gh_with = CommentedMap()
    gh_with["draft"] = SQ("false")
    gh_with["prerelease"] = (
        "${{ contains(github.ref, 'alpha')"
        " || contains(github.ref, 'beta')"
        " || contains(github.ref, 'rc') }}"
    )
    gh_with["generate_release_notes"] = SQ("true")
    gh_with["files"] = "artifacts/frogdb-*/*.tar.gz"

    w.jobs["github-release"] = Job(
        needs=["docker", "build-macos"],
        name="GitHub Release",
        permissions=Permissions(contents="write"),
        steps=[
            checkout_step(),
            download_all_artifacts_step(),
            Step(name="Create GitHub Release", uses=GH_RELEASE, with_=gh_with),
        ],
    )

    return w

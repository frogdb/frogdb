"""Release workflow definition."""

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import LiteralScalarString
from ruamel.yaml.scalarstring import SingleQuotedScalarString as SQ

from workflow_gen.constants import (
    ATTEST_BUILD_PROVENANCE,
    COSIGN_INSTALLER,
    GH_RELEASE,
    HELM_REPO_URL,
    IMPORT_GPG,
    SETUP_GO,
)
from workflow_gen.helpers import (
    DOCKERHUB_IMAGE,
    MACOS_TARGETS,
    cargo_cache_step,
    checkout_step,
    docker_build_push_step,
    docker_env,
    docker_login_dockerhub_step,
    docker_login_ghcr_step,
    docker_metadata_step,
    download_all_artifacts_step,
    ensure_path,
    mise_setup_step,
    omap,
    run_step,
    rust_toolchain_step,
    script,
    setup_buildx_step,
    setup_qemu_step,
    upload_artifact_step,
)
from workflow_gen.schema import (
    Job,
    MatrixInclude,
    Permissions,
    PushTrigger,
    Step,
    Strategy,
    Trigger,
    Workflow,
)


def release_workflow() -> Workflow:
    w = Workflow(
        name="Release",
        on=Trigger(
            push=PushTrigger(branches=[], tags=["v*"]),
            push_first=True,
        ),
        env=docker_env(),
    )

    w.jobs["docker"] = Job(
        name="Docker Release & Linux Binaries",
        environment="release",
        permissions=Permissions(contents="read", packages="write"),
        steps=[
            checkout_step(),
            setup_qemu_step(),
            setup_buildx_step(),
            docker_login_ghcr_step(),
            docker_login_dockerhub_step(),
            docker_metadata_step(
                tags=[
                    ("semver", {"pattern": "{{version}}"}),
                    ("semver", {"pattern": "{{major}}.{{minor}}"}),
                    ("semver", {"pattern": "{{major}}"}),
                    ("raw", {"value": "latest"}),
                ],
                extra_images=[f"docker.io/{DOCKERHUB_IMAGE}"],
            ),
            docker_build_push_step(cache=False),
            run_step(
                name="Extract Linux binaries from image",
                run=script("""\
                    IMAGE="${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ steps.meta.outputs.version }}"
                    BINARIES=(frogdb-server frog frogdb-admin)
                    for arch in amd64 arm64; do
                      docker pull --platform "linux/${arch}" "${IMAGE}"
                      CONTAINER=$(docker create --platform "linux/${arch}" "${IMAGE}")
                      for bin in "${BINARIES[@]}"; do
                        docker cp "${CONTAINER}:/usr/local/bin/${bin}" "./${bin}"
                      done
                      docker rm "${CONTAINER}"
                      tar -czvf "frogdb-${{ github.ref_name }}-linux-${arch}.tar.gz" "${BINARIES[@]}"
                      rm "${BINARIES[@]}"
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
                run="cargo build --release --target ${{ matrix.target }} --bin frogdb-server --bin frogctl --bin frogdb-admin",
            ),
            run_step(
                name="Create release archive",
                run=script("""\
                    cd target/${{ matrix.target }}/release
                    tar -czvf ../../../frogdb-${{ github.ref_name }}-${{ matrix.target }}.tar.gz frogdb-server${{ matrix.ext }} frogctl${{ matrix.ext }} frogdb-admin${{ matrix.ext }}"""),
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
            mise_setup_step(install_args="helm"),
            run_step(
                name="Package Helm chart",
                run=script(f"""\
                    helm package {ensure_path("frogdb-server/ops/deploy/helm/frogdb")} --destination .helm-packages
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

    w.jobs["github-release"] = Job(
        needs=["docker", "build-macos", "deb"],
        name="GitHub Release",
        permissions=Permissions(contents="write", id_token="write", attestations="write"),
        steps=[
            checkout_step(),
            download_all_artifacts_step(),
            run_step(
                name="Copy Grafana dashboard",
                run=script("""\
                    mkdir -p artifacts/grafana
                    cp frogdb-server/ops/grafana/frogdb-overview.json artifacts/grafana/"""),
            ),
            Step(name="Install cosign", uses=COSIGN_INSTALLER),
            run_step(
                name="Generate checksums",
                run=script("""\
                    cd artifacts
                    sha256sum frogdb-*/*.tar.gz frogdb-deb-*/*.deb > ../sha256sums.txt
                    mv ../sha256sums.txt ."""),
            ),
            run_step(
                name="Sign artifacts with cosign",
                run=script("""\
                    cd artifacts
                    for f in frogdb-*/*.tar.gz frogdb-deb-*/*.deb; do
                      cosign sign-blob --yes --bundle "${f}.bundle" "${f}"
                    done
                    cosign sign-blob --yes --bundle sha256sums.txt.bundle sha256sums.txt"""),
            ),
            Step(
                name="Attest build provenance",
                uses=ATTEST_BUILD_PROVENANCE,
                with_=omap(
                    **{
                        "subject-path": LiteralScalarString(
                            "artifacts/frogdb-*/*.tar.gz\nartifacts/frogdb-deb-*/*.deb"
                        ),
                    }
                ),
            ),
            Step(
                name="Create GitHub Release",
                uses=GH_RELEASE,
                with_=_gh_release_with(),
            ),
        ],
    )

    w.jobs["homebrew"] = Job(
        needs=["github-release"],
        name="Update Homebrew Tap",
        environment="release",
        permissions=Permissions(contents="read"),
        steps=[
            checkout_step(),
            download_all_artifacts_step(),
            run_step(
                name="Compute SHA256 and update formula",
                run=script("""\
                    VERSION="${GITHUB_REF_NAME#v}"

                    SHA_LINUX_AMD64=$(sha256sum artifacts/frogdb-linux-amd64/*.tar.gz | awk '{print $1}')
                    SHA_LINUX_ARM64=$(sha256sum artifacts/frogdb-linux-arm64/*.tar.gz | awk '{print $1}')
                    SHA_MACOS_X86=$(sha256sum artifacts/frogdb-x86_64-apple-darwin/*.tar.gz | awk '{print $1}')
                    SHA_MACOS_ARM=$(sha256sum artifacts/frogdb-aarch64-apple-darwin/*.tar.gz | awk '{print $1}')

                    sed -e "s/{{version}}/${VERSION}/g" \\
                        -e "s/{{sha256_linux_amd64}}/${SHA_LINUX_AMD64}/g" \\
                        -e "s/{{sha256_linux_arm64}}/${SHA_LINUX_ARM64}/g" \\
                        -e "s/{{sha256_macos_x86_64}}/${SHA_MACOS_X86}/g" \\
                        -e "s/{{sha256_macos_aarch64}}/${SHA_MACOS_ARM}/g" \\
                        .github/homebrew/frogdb.rb.template > frogdb.rb"""),
            ),
            run_step(
                name="Push formula to Homebrew tap",
                run=script("""\
                    git clone https://x-access-token:${{ secrets.HOMEBREW_TAP_TOKEN }}@github.com/${{ github.repository_owner }}/homebrew-tap.git tap
                    mkdir -p tap/Formula
                    cp frogdb.rb tap/Formula/frogdb.rb
                    cd tap
                    git config user.name "GitHub Actions"
                    git config user.email "actions@github.com"
                    git add Formula/frogdb.rb
                    git commit -m "Update frogdb to ${GITHUB_REF_NAME}" || echo "No changes"
                    git push"""),
            ),
        ],
    )

    w.jobs["deb"] = Job(
        needs=["docker"],
        name="Build Debian Packages",
        permissions=Permissions(contents="read"),
        steps=[
            checkout_step(),
            download_all_artifacts_step(),
            Step(
                name="Set up Go",
                uses=SETUP_GO,
                with_=omap(
                    **{"go-version": "stable"},
                ),
            ),
            run_step(
                name="Install nfpm",
                run="go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest",
            ),
            run_step(
                name="Build .deb packages",
                run=script(
                    """\
                    VERSION="${GITHUB_REF_NAME#v}"
                    DEB_DIR="""
                    + ensure_path("frogdb-server/ops/deploy/deb")
                    + """

                    for arch in amd64 arm64; do
                      WORK_DIR=$(mktemp -d)

                      # Extract binaries from tarball
                      tar -xzf artifacts/frogdb-linux-${arch}/*.tar.gz -C "$WORK_DIR"

                      # Copy packaging files alongside binaries
                      cp "$DEB_DIR"/frogdb.toml "$WORK_DIR"/
                      cp "$DEB_DIR"/frogdb-server.service "$WORK_DIR"/
                      cp "$DEB_DIR"/postinstall.sh "$WORK_DIR"/
                      cp "$DEB_DIR"/postremove.sh "$WORK_DIR"/
                      cp "$DEB_DIR"/frogdb.logrotate "$WORK_DIR"/

                      # Build .deb
                      cd "$WORK_DIR"
                      ARCH=$arch nfpm package \\
                        --config "${GITHUB_WORKSPACE}/$DEB_DIR/nfpm.yaml" \\
                        --packager deb \\
                        --target "${GITHUB_WORKSPACE}/frogdb-server_${VERSION}_${arch}.deb"
                      cd "$GITHUB_WORKSPACE"
                      rm -rf "$WORK_DIR"
                    done"""
                ),
            ),
            upload_artifact_step(
                name="frogdb-deb-amd64",
                path="frogdb-server_*_amd64.deb",
            ),
            upload_artifact_step(
                name="frogdb-deb-arm64",
                path="frogdb-server_*_arm64.deb",
            ),
        ],
    )

    w.jobs["apt"] = Job(
        needs=["deb"],
        name="Publish APT Repository",
        environment="release",
        permissions=Permissions(contents="write", pages="write"),
        steps=[
            checkout_step(),
            download_all_artifacts_step(),
            Step(
                name="Import GPG key",
                uses=IMPORT_GPG,
                with_=omap(
                    gpg_private_key="${{ secrets.DEB_SIGNING_KEY }}",
                ),
            ),
            checkout_step(name="Checkout gh-pages branch", ref="gh-pages", path="gh-pages"),
            run_step(
                name="Update APT repository",
                run=script("""\
                    mkdir -p gh-pages/apt/pool

                    # Copy .deb packages
                    cp artifacts/frogdb-deb-*/*.deb gh-pages/apt/pool/

                    cd gh-pages/apt

                    # Generate Packages index
                    dpkg-scanpackages pool/ > Packages
                    gzip -k Packages

                    # Generate Release file
                    apt-ftparchive release . > Release

                    # Sign Release file
                    gpg --armor --detach-sign -o Release.gpg Release
                    gpg --armor --clearsign -o InRelease Release

                    # Export public key for users
                    gpg --armor --export "${{ secrets.DEB_SIGNING_KEY_ID }}" > signing-key.gpg"""),
            ),
            run_step(
                name="Commit and push",
                run=script("""\
                    cd gh-pages
                    git config user.name "GitHub Actions"
                    git config user.email "actions@github.com"
                    git add apt/
                    git commit -m "Release APT packages ${{ github.ref_name }}" || echo "No changes"
                    git push"""),
            ),
        ],
    )

    return w


def _gh_release_with() -> CommentedMap:
    """Build the 'with' map for the GitHub Release step."""
    m = CommentedMap()
    m["draft"] = SQ("false")
    m["prerelease"] = (
        "${{ contains(github.ref, 'alpha')"
        " || contains(github.ref, 'beta')"
        " || contains(github.ref, 'rc') }}"
    )
    m["generate_release_notes"] = SQ("true")
    m["files"] = LiteralScalarString(
        "artifacts/frogdb-*/*.tar.gz\n"
        "artifacts/frogdb-*/*.tar.gz.bundle\n"
        "artifacts/frogdb-deb-*/*.deb\n"
        "artifacts/frogdb-deb-*/*.deb.bundle\n"
        "artifacts/sha256sums.txt\n"
        "artifacts/sha256sums.txt.bundle\n"
        "artifacts/grafana/frogdb-overview.json"
    )
    return m

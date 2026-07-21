# Spec: getting-started/installation.mdx
Status: rewrite
Audiences: A1 (primary — evaluator who wants a running binary fast), A4 (test-deployment operator who will keep it running), A5 (contributor building from source).

Goal: The reader ends with a running `frogdb-server` binary obtained through one of the install paths that actually exists in this repo, and a one-line check that it is up. Every version number and dependency string on the page is sourced from a repo file, never hand-typed into prose.

Not in scope:
- Homebrew / macOS package manager install. CUT it. The current page shows `brew install frogdb/tap/frogdb` under a "coming soon" Aside; PLAN §6 forbids coming-soon content and there is no tap in this repo.
- Configuration, first commands, client setup — those belong to Quickstart (link to it at the end).
- Production/systemd hardening, replication, cluster bootstrap — Operations section.
- Any published-artifact instructions (registry `docker pull`, GitHub Releases download) that cannot be verified as actually published for the current pre-release state — see Sources of truth and the VERIFY item below.

Sources of truth (author MUST read before writing):
- `rust-toolchain.toml` — pins the Rust toolchain. Currently `channel = "1.92.0"`. The current page's "Rust 1.75+" is WRONG. Do NOT hand-copy 1.92.0 either; the number must come from `versions.json` (S6). Until S6 exists, mark the toolchain string VERIFY-BEFORE-WRITING and reference `rust-toolchain.toml` as the source.
- `frogdb-server/docker/Dockerfile` — the runtime image. Note it is `FROM debian:bookworm-slim` and `COPY`s a *pre-built cross-compiled* binary (`target/x86_64-unknown-linux-gnu/release/frogdb-server`); it does not build from source. Runtime deps installed: libssl3, ca-certificates, redis-tools, etc. Default `CMD ["frogdb-server"]`, `EXPOSE 6379`, `VOLUME /data`, env vars `FROGDB_SERVER__BIND/PORT`, `FROGDB_PERSISTENCE__DATA_DIR`, `FROGDB_LOGGING__LEVEL`.
- `Justfile` — recipes `docker-cross-build` (`just docker-cross-build` → cross-compiles then `docker build -f frogdb-server/docker/Dockerfile -t frogdb:latest .`), `docker-build-prod`, `cross-build`, `cross-install`. Use these for the "build the image yourself" path.
- `frogdb-server/ops/deb/deb-gen/src/main.rs` + `frogdb-server/ops/deploy/deb/nfpm.yaml` — the Debian package definition. Install paths (from `nfpm.yaml` `contents`): binary `/usr/bin/frogdb-server`, plus `/usr/bin/frog` (frogctl) and `/usr/bin/frogdb-admin`; config `/etc/frogdb/frogdb.toml`; systemd unit `/usr/lib/systemd/system/frogdb-server.service`; data dir `/var/lib/frogdb` (data at `/var/lib/frogdb/data`); logs `/var/log/frogdb`. The current page's "installs to `/usr/bin/frogdb-server` with data at `/var/lib/frogdb/`" is roughly right but omits the two extra CLIs and the exact config/data subpaths — correct it.
- `.github/workflows/release.yml` — the release automation: on a version tag it (a) builds+pushes multi-arch images to `ghcr.io/nathanjordan/frogdb` and `docker.io/frogdb/frogdb` (repo owner `nathanjordan`, resolved in operations/deployment.md), (b) builds `.deb` packages via `nfpm` and attaches them (cosign-signed) to the GitHub Release, (c) uploads per-target `.tar.gz` binary bundles. This tells you the *intended* published artifact names — but see the VERIFY item: the workflow existing is not proof a release has been published.
- `frogdb-server/crates/server/src/main.rs` — the binary. Confirms `frogdb-server --version` works (`#[command(author, version, ...)]`) and default port 6379.

Existing content: Replace `website/src/content/docs/getting-started/installation.mdx`. Keep the Tabs-per-method shape and the "Test" check. Drop the macOS tab entirely; fix the source-build toolchain string; correct the deb details.

Structure:

- **Intro (1–2 sentences)**: FrogDB ships as a single `frogdb-server` binary. State that it is pre-release software (link nothing new; a plain sentence). List the install paths that follow.

- **VERIFY-BEFORE-WRITING — which paths are actually documentable.** Before writing the Docker-pull and Debian-download tabs as working commands, confirm a published release/tag exists:
  - If a tag has been pushed and `release.yml` has run, then `docker pull frogdb/frogdb:<tag>` (Docker Hub) / `ghcr.io/nathanjordan/frogdb` and the GitHub Release `.deb`/tarball assets are real — document them with the concrete published tag from `versions.json`, not `:latest` guessed blindly.
  - If NO release has been published yet (likely, given "unreleased, pre-production"), do NOT present `docker pull frogdb/frogdb:latest` or "download the .deb from GitHub Releases" as if they work. Instead document: (1) build the image locally via `just docker-cross-build` (produces `frogdb:latest` locally), and (2) build from source. Optionally describe the deb/registry paths as "on tagged releases" with an honest status note. This choice is the single most important correctness decision on the page — resolve it against the real release/tag state, do not assume.

- **## Docker** (Tab or H2). Two sub-cases depending on the VERIFY outcome:
  - Published image (only if verified): `docker run -d --name frogdb -p 6379:6379 -v frogdb-data:/data frogdb/frogdb:<tag>`; note env-var config (`FROGDB_SERVER__BIND`, `FROGDB_PERSISTENCE__DATA_DIR`) and that the image already contains `redis-cli` (redis-tools). Use the exact image name from `release.yml` (`docker.io/frogdb/frogdb`), not an invented one.
  - Build-it-yourself (always documentable): `just docker-cross-build` (requires zigbuild; `just cross-install` first) → tags `frogdb:latest` locally → same `docker run`. Point to the Dockerfile. Keep a Docker Compose snippet (mine the current page's — it is accurate: ports, `frogdb-data` volume, env vars, `redis-cli PING` healthcheck) but use whichever image name matches the case.

- **## Debian / Ubuntu** (Tab or H2). Install via the `.deb`: `sudo dpkg -i frogdb-server_*.deb` then `sudo systemctl enable --now frogdb-server`. State the real installed layout from `nfpm.yaml` (binary `/usr/bin/frogdb-server`, plus `frog` and `frogdb-admin`; config `/etc/frogdb/frogdb.toml`; unit `/usr/lib/systemd/system/frogdb-server.service`; data `/var/lib/frogdb`; logs `/var/log/frogdb`). VERIFY the `.deb` source: it is a GitHub Release asset (per `release.yml`) — only tell the reader to download it if a release exists; otherwise document building it locally with `just deb-gen` + `nfpm pkg` and mark clearly.

- **## Build from source** (Tab or H2). Always valid.
  - Toolchain: reference `rust-toolchain.toml` for the exact Rust version (rustup auto-installs the pinned toolchain when you build in the repo). Do NOT write a hardcoded "Rust 1.x+" string; source it from `versions.json` (S6) or say "the version pinned in `rust-toolchain.toml`".
  - System deps: RocksDB and compression libs. Keep the per-OS dependency lists (macOS `brew install rocksdb snappy lz4 zstd`; Debian/Ubuntu `librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev build-essential`; Arch equivalents) but VERIFY these against `Brewfile` / `shell.nix` / build docs — the project builds RocksDB via `librocksdb-sys` and there is a known DYLD/libclang note in the repo. Prefer citing the repo's own dependency files over re-listing packages that could drift.
  - Build: `git clone https://github.com/nathanjordan/frogdb` (repo owner `nathanjordan`, resolved in operations/deployment.md), `cd frogdb`, `cargo build --release`; binary at `target/release/frogdb-server`. Prefer mentioning `just` recipes where they exist rather than raw cargo, per CLAUDE.md.

- **## Verify the install**: `frogdb-server --version` (confirms the binary) and `redis-cli -p 6379 PING` → `PONG` (confirms it accepts connections). Note `redis-cli` comes from `redis-tools` and is bundled in the Docker image.

- **## Next steps**: LinkCard to Quickstart (first commands + config) and to Operations → Configuration. Do not duplicate config content here.

Generated data:
- `versions.json` (S6) — supplies Rust toolchain version, workspace/release version (for the concrete Docker tag and `.deb` filename), and the targeted Redis version. This page MUST NOT hardcode any of these; every version string is a token filled from `versions.json`. Until S6 lands, mark each such string VERIFY-BEFORE-WRITING and name its repo source (`rust-toolchain.toml`, `Cargo.toml`, `release.yml`).

Drift guards:
- All version strings come from `versions.json` (S6); a bump to `rust-toolchain.toml`/`Cargo.toml` regenerates them, so prose cannot go stale (this directly fixes the current "Rust 1.75+" drift).
- Install layout (paths, extra CLIs) is copied from `nfpm.yaml`, the same source `deb-gen` uses; a `just deb-gen --check` job guards that file.
- Only artifacts the repo actually produces are documented; the VERIFY-BEFORE-WRITING gate prevents documenting a registry pull or Release download that has not been published for the current pre-release.
- Link checker (`just docs-link-check`) validates the Quickstart/Configuration links.

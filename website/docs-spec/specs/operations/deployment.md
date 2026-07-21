# Spec: operations/deployment.md

Status: rewrite (merge of current `deployment.md` + `kubernetes.mdx`; correct the
wrong registry path and hand-drifted systemd unit)
Audiences: A4 (primary), A1

Goal: The reader can deploy FrogDB three ways — Docker (from the published
image), the Debian package with its systemd unit, or the Helm chart on Kubernetes
— using the REAL published artifacts, and gets one honest sentence on the
Kubernetes operator's maturity (not a "planned features" list). `kubernetes.mdx`
is cut; its accurate parts fold in here.

Not in scope:
- Per-parameter config — that is [Configuration](/operations/configuration/) and
  Reference → Configuration reference; show minimal snippets only.
- Cluster bootstrap topology — that is [Clustering](/operations/clustering/);
  link.
- TLS/ACL setup — that is [Security](/operations/security/); link.
- A "production configuration" section that implies production-readiness — FrogDB
  is pre-release (PLAN §1). Reframe as "example configuration," no production
  tuning claims.

Sources of truth (read before writing):
- `frogdb-server/docker/Dockerfile.builder` — the PRODUCTION image (alpine 3.21,
  non-root `frogdb` user, builds `frogdb-server`/`frogctl`/`frogdb-admin`, EXPOSE
  6379, VOLUME `/data`, `FROGDB_PERSISTENCE__DATA_DIR=/data`, redis-cli
  healthcheck). `Dockerfile` (bookworm) is the dev/Jepsen image — do NOT document
  it as the deploy image.
- `.github/workflows/release.yml` — the real publish targets/tags (fires only on
  `v*` tags). Registries: `docker.io/frogdb/frogdb` and
  `ghcr.io/nathanjordan/frogdb`. Helm repo published to
  `https://nathanjordan.github.io/frogdb/helm`.
- `frogdb-server/ops/deploy/deb/frogdb-server.service` — the REAL generated
  systemd unit (`ExecStart=/usr/bin/frogdb-server --config
  /etc/frogdb/frogdb.toml`, `User=frogdb`, hardening directives, NO `ExecReload`).
  And `frogdb-server/ops/deb/deb-gen/` + `nfpm.yaml` for package layout.
- `frogdb-server/ops/deploy/helm/frogdb/` — the generated chart:
  `statefulset.yaml`, `service.yaml`, `service-headless.yaml`, `configmap.yaml`,
  `servicemonitor.yaml`, `pdb.yaml`, `grafana-dashboard-configmap.yaml`,
  `values.yaml`, `Chart.yaml`.
- `frogdb-operator/src/controller.rs`, `crd.rs`, `Cargo.toml`, `deploy/` — for the
  honest operator status.
- `frogdb-server/crates/server/src/observability_server.rs` — confirms
  `/health/live`, `/health/ready`, `/healthz`, `/readyz` on the metrics port
  (validates HTTP probes).

Existing content: `operations/deployment.md` (rewrite) + `operations/kubernetes.mdx`
(cut; fold honest Helm/operator status here). Both contain hand-written artifacts
that have drifted from the generated ones — replace with the real artifacts.

## Verified facts (authoritative — correct the current docs)

**Docker.**
- Published on `v*` tags to `docker.io/frogdb/frogdb` AND
  **`ghcr.io/nathanjordan/frogdb`** (the current docs' `ghcr.io/frogdb/frogdb` is
  WRONG). Tags: `latest`, `<major>`, `<major>.<minor>`, and the full semver.
- There is **no `dev`/`<branch>`/`<short-sha>` main-branch image** — the release
  workflow only fires on tags. DELETE the "development builds from main" claims;
  they are aspirational/false.
- The production image is `Dockerfile.builder` (alpine, non-root `frogdb`, VOLUME
  `/data`, EXPOSE 6379, redis-cli healthcheck, `FROGDB_PERSISTENCE__DATA_DIR=/data`).
  The `docker run`/compose examples are fine but should reflect the real
  data-dir/env and can keep the metrics port mapping.

**Debian package + systemd.** Built in CI by **nfpm** (not cargo-deb) from the
generated artifacts under `ops/deploy/deb/`. Do NOT hand-write a systemd unit —
point at / reproduce the generated one:
- Binary at `/usr/bin/frogdb-server` (docs say `/usr/local/bin` — WRONG). Also
  ships `frogctl` and `frogdb-admin`.
- Unit: `User=frogdb`/`Group=frogdb`, `Type=simple`, `Restart=on-failure`,
  `LimitNOFILE=65535`, `LimitMEMLOCK=infinity`, hardening
  (`NoNewPrivileges`/`ProtectSystem=strict`/`ProtectHome`/`PrivateTmp`/
  `ProtectKernelTunables`/`ProtectControlGroups`, `ReadWritePaths=/var/lib/frogdb
  /var/log/frogdb`). **No `ExecReload`** — do not show `kill -HUP` (SIGHUP
  handling is UNVERIFIED).
- Package creates the `frogdb` system user (`/usr/sbin/nologin`), dirs
  `/var/lib/frogdb/{data,snapshots,cluster}` and `/var/log/frogdb`, config
  `/etc/frogdb/frogdb.toml` (0640 root:frogdb, marked `config|noreplace`), and
  `systemctl enable --now`s the service. Default config binds `127.0.0.1:6379`,
  data `/var/lib/frogdb/data`, metrics `127.0.0.1:9090`.
- The section should mostly say "install the package; it lays down this unit and
  these paths" rather than instruct the reader to create users/units by hand.

**Helm chart.** Real and published (Chart name `frogdb`, version/appVersion from
`crates/server/src/config/`). Templates include a working `servicemonitor.yaml`
(`monitoring.coreos.com/v1`, gated on `serviceMonitor.enabled &&
frogdb.metrics.enabled`) and `pdb.yaml` (`policy/v1`, gated on
`podDisruptionBudget.enabled`, default `minAvailable: 1`). Key values:
`image.repository: ghcr.io/nathanjordan/frogdb`, `replicaCount: 1`,
`persistence.enabled: true` (`10Gi`, `/data`), hardened securityContext.
- NOTE the probe mismatch to state honestly: the shipped chart's StatefulSet uses
  **`redis-cli ping` exec probes**, while the server ALSO serves HTTP
  `/health/live` + `/health/ready` on the metrics port. Both are valid; document
  the HTTP endpoints as the K8s-probe option and note the chart currently uses
  exec probes. Don't present a hand-written StatefulSet as canonical — point at
  the chart.

**Operator — honest one-liner (use this, NOT a planned-features list):**
> The `frogdb-operator` is a working Rust controller for a single `FrogDB` CRD
> (`frogdb.io/v1alpha1`) that reconciles a ConfigMap, Services, a StatefulSet, and
> a PodDisruptionBudget with status conditions and rolling-upgrade detection, and
> is unit/integration-tested against the real server — but it is unpublished (no
> image, not in release CI, `publish = false`, a standalone workspace) and should
> be treated as experimental.
Corrections to fold in: there is **no `FrogDBCluster` CRD** (cluster mode is
`spec.mode: "cluster"` on the single `FrogDB` kind) — `kubernetes.mdx`'s promise
of a `FrogDBCluster` is false. The operator's ServiceMonitor builder exists but is
NOT reconciled (dormant). Backup-to-S3/GCS is genuinely unimplemented — the only
item that may be called "not yet available."

**OS tuning.** The current `ulimit`/`somaxconn` section is generic and fine to
keep, but trim any implication of production tuning; keep it short.

Structure (H2/H3 — one line each):

- Intro: three supported ways to run FrogDB (Docker, Debian package + systemd,
  Helm on Kubernetes), plus a note that the operator is experimental. One line:
  FrogDB is pre-release; these are for lab/staging, not production.
- **## Docker** — pull from `docker.io/frogdb/frogdb` (or
  `ghcr.io/nathanjordan/frogdb`); tag scheme; `docker run` with `-p 6379`, `-v
  …:/data`; a compose example; note the built-in redis-cli healthcheck. Correct
  registry + tag facts.
- **## Debian package (systemd)** — `apt install` the `.deb`; describe what the
  package lays down (paths, user, unit, config) and the real `systemctl
  enable --now` flow. Show / link the generated unit; do not hand-author one.
- **## Kubernetes (Helm)** — add the Helm repo
  (`nathanjordan.github.io/frogdb/helm`), `helm install`, the notable values
  (persistence, resources, `serviceMonitor.enabled`, `podDisruptionBudget`).
  Mention ServiceMonitor + PDB ship in-chart. Note HTTP health endpoints for
  probes and the current exec-probe default.
- **### Kubernetes operator (experimental)** — the honest one-liner above; the
  single `FrogDB` CRD; link to the operator source/examples; explicitly not
  published. This replaces the entire "Planned Features" section.
- **## OS tuning** — short: file descriptors, TCP backlog.
- **## Example configuration** — a minimal, clearly-labeled *example* (not
  "production") config; link Configuration for the full surface.
- **## Verify** — `redis-cli ping`, `curl /metrics`.
- **## See also** — Configuration, Clustering, Security, Observability.

Generated data:
- The deb and Helm artifacts are themselves generated by `deb-gen`/`helm-gen` from
  `crates/config` — the page should link the reader to those artifacts rather than
  reproduce large YAML that will drift. Version strings come from `versions.json`
  (S6), never hardcoded.
- Links: Reference → Configuration reference, Reference → frogdb-server CLI.

Drift guards:
- `just generate-check` (runs `helm-gen --check`, `deb-gen --check`, etc.) fails
  CI if the committed deb/Helm artifacts drift from `crates/config` — so the page
  should reference the generated files, keeping it correct by construction.
- No hardcoded version numbers in prose (S6 `versions.json`); the `0.1.0` in
  Chart.yaml/nfpm is itself generated.
- S7 code-path check for cited `frogdb-server/...` / `frogdb-operator/...` paths.
- Registry paths and tag scheme trace to `release.yml`; if that workflow changes,
  the page's Docker/Helm facts must be re-verified (call this out for reviewers).
- Content policy: honest operator status only (no planned-features list); no
  "production" framing for pre-release software.

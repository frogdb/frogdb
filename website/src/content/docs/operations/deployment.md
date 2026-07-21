---
title: "Deployment"
description: "Deploy FrogDB with the published Docker image, the Debian package, or the Helm chart."
sidebar:
  order: 1
---
FrogDB is distributed three ways: a Docker image, a Debian package with a systemd unit,
and a Helm chart for Kubernetes. A separate, experimental operator can also manage FrogDB
on Kubernetes; its current maturity is described honestly below rather than as a roadmap.
FrogDB is pre-release software — these are suitable for lab and staging environments, not
yet a production deployment target.

## Docker

Releases publish multi-arch (`linux/amd64`, `linux/arm64`) images to two registries on
every `v*` tag:

- `docker.io/frogdb/frogdb`
- `ghcr.io/nathanjordan/frogdb`

Both carry the same tags: `latest`, `<major>`, `<major>.<minor>`, and the full semantic
version. There is no rolling `main`-branch image — the release workflow
(`.github/workflows/release.yml`) only runs on tag pushes, so every published tag
corresponds to a tagged release.

The image (built from `frogdb-server/docker/Dockerfile.builder`) runs as a non-root
`frogdb` user, exposes port `6379`, and declares `/data` as a volume:

```bash
docker run -d --name frogdb \
  -p 6379:6379 \
  -v frogdb-data:/data \
  frogdb/frogdb:latest
```

To reach the HTTP metrics/health endpoint from outside the container, bind it to all
interfaces explicitly — it defaults to loopback-only:

```bash
docker run -d --name frogdb \
  -p 6379:6379 -p 9090:9090 \
  -v frogdb-data:/data \
  -v $(pwd)/frogdb.toml:/etc/frogdb/frogdb.toml:ro \
  -e FROGDB_METRICS__BIND=0.0.0.0 \
  frogdb/frogdb:latest --config /etc/frogdb/frogdb.toml
```

Docker Compose:

```yaml
services:
  frogdb:
    image: frogdb/frogdb:latest
    ports:
      - "6379:6379"
      - "9090:9090"
    volumes:
      - frogdb-data:/data
      - ./frogdb.toml:/etc/frogdb/frogdb.toml:ro
    environment:
      - FROGDB_METRICS__BIND=0.0.0.0
    command: ["frogdb-server", "--config", "/etc/frogdb/frogdb.toml"]
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "6379", "ping"]
      interval: 10s
      retries: 3

volumes:
  frogdb-data:
```

The image ships its own `HEALTHCHECK` (`redis-cli ping` against the configured port), so
the healthcheck block above is optional if you rely on Docker's built-in check instead.

## Debian package (systemd)

The `.deb` package is built in CI by [nfpm](https://nfpm.goreleaser.com/) from the
generated packaging manifest under `frogdb-server/ops/deploy/deb/`. Install it and it lays
down a complete deployment — there's no need to hand-create the user, directories, or
systemd unit yourself:

```bash
sudo apt install ./frogdb-server_<version>_<arch>.deb
```

The package:

- Installs `frogdb-server` at `/usr/bin/frogdb-server`, plus the `frogdb-admin` and
  `frogctl` CLIs. (The packaging manifest currently lists the third binary under the name
  `frog`; that mismatch with the compiled binary's actual name, `frogctl`, is being fixed
  separately.)
- Creates a system user `frogdb` (`/usr/sbin/nologin`, no login shell).
- Creates `/var/lib/frogdb/{data,snapshots,cluster}` and `/var/log/frogdb`, owned by
  `frogdb:frogdb`.
- Installs a config file at `/etc/frogdb/frogdb.toml` (mode `0640`, owned `root:frogdb`,
  marked so `apt` won't overwrite local edits on upgrade) that binds to `127.0.0.1:6379`
  with metrics on `127.0.0.1:9090` by default.
- Installs the systemd unit at `/usr/lib/systemd/system/frogdb-server.service` and enables
  and starts it.

The generated unit runs as the unprivileged `frogdb` user with systemd hardening
(`ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `NoNewPrivileges`, restricted to
writing `/var/lib/frogdb` and `/var/log/frogdb`). It does not define `ExecReload`, so
`systemctl reload` is not supported — use `systemctl restart` to pick up configuration
changes:

```bash
sudo systemctl status frogdb-server
sudo systemctl restart frogdb-server
sudo journalctl -u frogdb-server -f
```

## Kubernetes (Helm)

FrogDB publishes a Helm chart from the `deb`/`docker` release outputs to
`https://nathanjordan.github.io/frogdb/helm`:

```bash
helm repo add frogdb https://nathanjordan.github.io/frogdb/helm
helm repo update
helm install my-frogdb frogdb/frogdb
```

The chart deploys a `StatefulSet` with persistence enabled by default (a `10Gi`
`PersistentVolumeClaim` per pod) and a hardened pod `securityContext` (non-root,
read-only root filesystem, all capabilities dropped). Notable values:

| Value | Default | Purpose |
|---|---|---|
| `replicaCount` | `1` | Pod count |
| `persistence.enabled` / `persistence.size` | `true` / `10Gi` | Data volume |
| `resources` | `2` CPU / `4Gi` limits | Container resource limits/requests |
| `serviceMonitor.enabled` | `false` | Emit a Prometheus Operator `ServiceMonitor` |
| `podDisruptionBudget.enabled` | `true` (`minAvailable: 1`) | Emit a `PodDisruptionBudget` |

Both the `ServiceMonitor` and `PodDisruptionBudget` templates ship in the chart and only
need their `enabled` flag set. The server also serves HTTP `/health/live` and
`/health/ready` endpoints on the metrics port (see
`frogdb-server/crates/server/src/observability_server.rs`), suitable for `httpGet` liveness
and readiness probes — but the chart's shipped `StatefulSet` currently uses `redis-cli
ping` exec probes instead. Either works; if you override the probes to use the HTTP
endpoints, point them at the metrics port.

### Kubernetes operator (experimental)

`frogdb-operator` is a working Rust controller for a single `FrogDB` custom resource
(`frogdb.io/v1alpha1`) that reconciles a `ConfigMap`, `Service`s, a `StatefulSet`, and a
`PodDisruptionBudget`, reports status conditions, and detects rolling upgrades — and it is
unit- and integration-tested against the real server. It is not published, however: it has
no released container image, is not part of release CI, and its crate is marked
`publish = false` in its own standalone Cargo workspace. Treat it as experimental.

A `FrogDB` resource's `spec.mode` field selects `"standalone"` or `"cluster"` — there is no
separate `FrogDBCluster` kind. The operator also has an unused `ServiceMonitor` builder
(present in source, not yet wired into reconciliation) and does not implement backing up
data to S3 or GCS.

## OS tuning

```bash
# File descriptors
echo "* soft nofile 65535" >> /etc/security/limits.conf
echo "* hard nofile 65535" >> /etc/security/limits.conf

# TCP backlog
echo "net.core.somaxconn = 65535" >> /etc/sysctl.conf
sysctl -p
```

## Example configuration

A minimal example — see [Configuration](/operations/configuration/) and
[Configuration reference](/reference/configuration/) for the complete parameter surface,
and [Persistence](/operations/persistence/) for what these durability settings mean:

```toml
[server]
bind = "0.0.0.0"
port = 6379

[persistence]
enabled = true
data-dir = "/var/lib/frogdb/data"
durability-mode = "periodic"
sync-interval-ms = 1000

[memory]
maxmemory-policy = "allkeys-lru"

[security]
requirepass = "change-me"

[metrics]
enabled = true
port = 9090
```

## Verify

```bash
redis-cli -p 6379 ping             # PONG
curl http://localhost:9090/metrics # Prometheus metrics
curl http://localhost:9090/health/ready
```

## See also

- [Configuration](/operations/configuration/) — the four configuration surfaces in detail.
- [Clustering](/operations/clustering/) — multi-node cluster bootstrap.
- [Security](/operations/security/) — TLS and ACL setup.
- [Observability](/operations/observability/) — metrics, tracing, and the `/status` endpoints.

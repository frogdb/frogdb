# Spec: operations/observability.md

Status: rewrite (MERGE of current `monitoring.md` + `observability.mdx`; PLAN §4)
Audiences: A4 (primary), A3

Goal: The reader can scrape Prometheus `/metrics`, configure OTLP export and
distributed tracing, use the HTTP health/status endpoints, read the relevant
`INFO` sections, configure structured logging, and — if they build with the
`usdt-probes` feature — attach DTrace/bpftrace to the USDT probes. The metrics
catalog itself lives in Reference → Metrics and is linked, not duplicated.

Not in scope:
- The full metric catalog (name/type/labels/help) — Reference → Metrics
  (`metrics.json`, S3). Link only.
- Slowlog / LATENCY / hotkeys / debug UI / diagnostic bundles — those move to
  [Diagnostics](/operations/diagnostics/) (merge of performance + debug-ui).
- Alerting dashboards as a product — keep alert-threshold guidance minimal and
  qualitative (no SLA claims).

Sources of truth (read before writing):
- `frogdb-server/crates/telemetry/src/http_handlers.rs` and
  `frogdb-server/crates/server/src/observability_server.rs` (~218–268) — the real
  HTTP routes, auth middleware, and 200/503 semantics.
- `frogdb-server/crates/telemetry/src/health.rs` — `/health/ready` starts
  not-ready until `set_ready()`.
- `frogdb-server/crates/config/src/metrics.rs` — `[metrics]` incl. OTLP keys.
- `frogdb-server/crates/config/src/distributed_tracing.rs` — `[tracing]` keys.
- `frogdb-server/crates/config/src/logging.rs` — `[logging]` + `[logging.rotation]`.
- `frogdb-server/crates/core/src/probes.rs` — the 12 USDT probes + feature gate.
- `frogdb-server/crates/server/src/commands/info.rs` (~34–153) — real `INFO`
  sections.

Existing content: `monitoring.md` + `observability.mdx` (merge). Keep the accurate
Prometheus/OTLP/tracing/health/logging/probe content; DELETE fabricated items (see
below); DE-DUPLICATE the overlapping metrics/health sections.

## Verified facts (authoritative)

**HTTP endpoints** (observability server, default port 9090):
- Unauthenticated: `GET /metrics` (Prometheus text `version=0.0.4`, always 200),
  `GET /health/live` (200 alive / 503 after shutdown), `GET /health/ready` (200
  ready / **503 until `set_ready()`** — starts not-ready, correct for readiness
  gating). `GET /status/json` (200; JSON body carries healthy/degraded/unhealthy;
  503 only if no collector) is owned by [Diagnostics](/operations/diagnostics/) —
  mention it here only as an available endpoint and link there. Aliases `/healthz`
  and `/readyz` exist.
- Bearer-protected (only when `http.token` is set — otherwise open): `/debug/*`,
  `/admin/*`. Note: HTTP server binds `127.0.0.1` by default, so no-token means
  loopback-only access, not network exposure — intended for trusted internal
  use. (Detail lives in Diagnostics/Clustering; mention here for
  the health-vs-protected boundary.)

**Metrics / OTLP** (`[metrics]`): `enabled`, `bind`, `port` (9090),
`otlp-enabled` (false), `otlp-endpoint` (`http://localhost:4317`),
`otlp-interval-secs` (15). Note the OTLP endpoint key lives under `[metrics]` for
metric export and separately under `[tracing]` for traces — the current pages
conflate them; keep them distinct.

**Tracing** (`[tracing]`): `enabled`, `otlp-endpoint`, `sampling-rate`,
`service-name`, and the granularity toggles `scatter-gather-spans`, `shard-spans`,
`persistence-spans`, plus `recent-traces-max` (feeds the debug UI recent-traces
view). The span-hierarchy and semantic-convention descriptions in the current
`observability.mdx` are fine to keep (verify the attribute names against source if
retained).

**USDT/DTrace probes — REAL** (`core/src/probes.rs`, feature `usdt-probes`,
provider `frogdb`, `usdt 0.6`). The 12 probes are exactly as the current page
lists: `command__start`, `command__done`, `shard__message__sent`,
`shard__message__received`, `key__expired`, `key__evicted`, `memory__pressure`,
`wal__write`, `scatter__start`, `scatter__done`, `pubsub__publish`,
`connection__accept`. Off by default; compiles to no-ops without the feature. Keep
this section — it is accurate. (One nit: verify the dtrace probe-name form
`command__start` vs `command-start` in the example commands against the provider
before shipping.)

**INFO sections** (`info.rs`): real default sections are `server`, `clients`,
`memory`, `persistence`, `stats`, `replication`, `cpu`, `keyspace`. Under
`all`/`everything`: `commandstats`, `errorstats`, `latencystats`,
`latency_baseline`, `tiered`, `keysizes`.
- **`INFO hotshards` and `INFO frogdb` DO NOT EXIST** — the current
  `monitoring.md` INFO example lists both. DELETE them from the INFO section list.
  (`frogdb` exists only as a field inside `/status/json`.)

**Logging** (`[logging]`): `level`, `format` (`json`/`pretty`), `output`
(`stdout`/`stderr`/`none`), `per-request-spans`, `file-path`, and
`[logging.rotation]` (`max-size-mb`, `frequency`, `max-files`; rotation needs
`file-path`). `loglevel` is runtime-mutable (`CONFIG SET loglevel debug`).
- **DELETE the "~13% CPU overhead" claim** for `per-request-spans` — it has no
  source (doc-only). The feature is real and runtime-toggleable; the number is
  fabricated (content policy §6). Describe the cost qualitatively ("adds
  per-request span overhead") or omit.

Structure (H2/H3 — one line each):

- Intro: FrogDB exposes Prometheus metrics, OTLP export, distributed tracing,
  health/status HTTP endpoints, structured logging, and optional USDT probes.
  Link Reference → Metrics for the catalog; link Diagnostics for slowlog/latency
  tooling.
- **## Prometheus metrics** — `GET /metrics` on port 9090; `[metrics]` config;
  link to Reference → Metrics (do not list metrics). Merge the two current
  Prometheus sections into one.
- **## OTLP export** — `[metrics]` OTLP keys; export interval; point at any
  OTLP-compatible collector.
- **## Distributed tracing** — `[tracing]` keys incl. granularity toggles;
  keep span-hierarchy + semantic-conventions if verified; note recent-traces feed
  the debug UI.
- **## Health and status endpoints** — the table: `/health/live`, `/health/ready`
  (503-until-ready — call this out for readiness probes), plus `/healthz`/`/readyz`
  aliases and `PING`. Mention `/status/json` briefly and link Diagnostics (its
  canonical home) rather than documenting it. K8s probe snippet (HTTP on 9090).
- **## INFO sections** — the REAL section list (no `hotshards`/`frogdb`); note
  FrogDB extras exposed under `all` (`tiered`, `keysizes`, `latency_baseline`,
  etc.). Point to redis.io for base-section field meanings.
- **## Logging** — `[logging]` + rotation; runtime `CONFIG SET loglevel`; the
  qualitative per-request-spans note (no percentage).
- **## USDT / DTrace probes** — build with `--features usdt-probes`; the 12
  probes; dtrace/bpftrace examples. Keep, it's accurate.
- **## Key metrics to watch** — keep a SHORT qualitative list (memory ratio,
  command p99, persistence errors, rejected connections) as PromQL pointers, but
  frame thresholds as examples, not SLAs. Verify each metric name against the
  catalog.
- **## See also** — Reference → Metrics, Diagnostics, Configuration, Clustering.

Generated data:
- Reference → Metrics (`metrics.json`, work-item **S3**) is the authoritative
  metric catalog — this page links to it and must not duplicate metric names
  (any metric cited inline must be validated against S3 output).
- Links to Reference → Configuration reference for `[metrics]`/`[tracing]`/
  `[logging]`.

Drift guards:
- `just docs-gen-check` keeps the linked config keys honest.
- S3 (`metrics-gen`) is the single source for metric names; the "key metrics"
  list should be checked against it (ideally generated).
- S7 code-path check for cited `crates/telemetry/...`, `crates/core/src/probes.rs`.
- Content policy: no fabricated overhead percentages (the ~13% claim);
  no nonexistent INFO sections; no metric names absent from the catalog.

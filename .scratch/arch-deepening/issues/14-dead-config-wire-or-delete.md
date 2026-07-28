# 14 — Dead config sections: wire or delete

Status: done (landed 2026-07-22, branch workspace-3)

Resolution — user-approved disposition (wire 7 / delete 11):
- WIRED + promoted immutable (`GOLDEN_SNAPSHOT` 104→111, `ImmutableParamId` 58→65):
  `metrics-otlp-enabled`/`-endpoint`/`-interval-secs` (OtlpRecorder + CompositeRecorder at startup,
  identity gauges included in OTLP-only mode), `json-max-depth`/`json-max-size` (via
  `CommandContext.json_limits`; enforced at ALL ingest+growth paths incl. JSON.MERGE/ARRAPPEND/
  ARRINSERT/STRAPPEND and the scripting gate), `repl-ack-interval-ms` (replica ACK tick, both boot
  and demotion paths), `tls-ciphersuites` (filtered aws-lc-rs provider, server+client builders,
  hard error on unknown names / no-usable-suite).
- DELETED: `metrics.bind` (superseded by `http`), whole `vll` section + runtime `VllConfig` mirror
  in `vll/src/types.rs` + `VllTimeoutOrderingValidator` (validated only dead config),
  `hotshards.*` + plumb chain (collector KEPT — feature filed as issue 17),
  `replication.fullsync-timeout-secs`/`fullsync-max-memory-mb` (mechanisms don't exist).
- Downstream fallout fixed: website configuration.mdx (dangling ConfigTable = build break),
  helm-gen configmap (`metrics.bind` would fail deny_unknown_fields on boot; also fixed
  pre-existing snake_case drift in the stale operator copy), replication/vll/concurrency/
  diagnostics docs. `website/docs-spec/` audit records left as historical snapshots.
- The 20 immutable propagation-wiring candidates: user chose LEAVE IMMUTABLE — list below stays
  as the tracked backlog for a future mutability round.
- Review extras: `refresh_key` cross-source reconcile (delete-first) fixing a stale-doc regression
  + pre-existing hash-into-json-index pollution; UFCS for chokepoint lint in init.rs tests.
- `metrics.enabled` determined NOT dead: live http-mapped registry param (`metrics-enabled`).

## What to build

Round-9 triage decided to **skip** wire-or-delete work for this round (config sections that parse
successfully from TOML but whose values the server never reads at runtime — "dead config": CONFIG
GET would report a number the server silently ignores, which is the same misleading-observability
failure mode called out in issue 11/12 for the status command). This issue exists so the work isn't
lost — a later round should either wire each section to a real runtime consumer (making it eligible
for CONFIG GET/SET promotion) or delete it outright.

The list below is the **14 downgraded-justify** fields from issue `13-01`'s propagation-truth gate
(Pass 2b), plus the **4 metrics OTLP/bind fields** downgraded in Pass 2a — all still carrying
`#[param(skip)] // skip: config not yet consumed by server` today. Each was re-verified directly
against the config crate for this issue (grep + read, 2026-07-21); all 18 fields across 6 sections
are confirmed still unwired.

### Verified dead-config sections

| Section.field(s) | File | Verification |
| --- | --- | --- |
| `metrics.bind` | `frogdb-server/crates/config/src/metrics.rs:19-23` | Only read by `validators/network.rs:122` for bind-overlap validation. No listener binds it — the metrics HTTP endpoint is superseded by the `http` section (`StaticConfig` maps `metrics-enabled`/`metrics-port` to `config.http.*`). |
| `metrics.otlp-enabled` / `otlp-endpoint` / `otlp-interval-secs` | `frogdb-server/crates/config/src/metrics.rs:30-47` | `OtlpRecorder::new` (`frogdb-server/crates/telemetry/src/otlp.rs:28`) is called only from telemetry's own unit tests (`otel_unit_tests.rs`) and its own doctest, never from production startup code. `ops/helm/helm-gen` reads these fields only to *render* Helm chart YAML (config generation, not runtime consumption). |
| `vll.max-queue-depth`, `lock-acquisition-timeout-ms`, `per-shard-lock-timeout-ms`, `timeout-check-interval-ms`, `max-continuation-lock-ms` | `frogdb-server/crates/config/src/vll.rs:15-38` | Zero references to `VllConfig` or `config.vll` anywhere in `frogdb-server/crates/server`. The runtime `frogdb_vll` crate (`frogdb-server/crates/vll/src/{lib,types}.rs`) uses compile-time constants; the config-crate `VllConfig` struct is never passed to it. |
| `json.max-depth`, `json.max-size` | `frogdb-server/crates/config/src/json.rs:21-29` | `JsonConfigExt::to_limits()` (`frogdb-server/crates/server/src/config/mod.rs:112-116`) has zero callers (`grep -rn "to_limits()"` → no hits). Handlers hardcode `JsonLimits::default()` instead (`frogdb-server/crates/commands/src/json/mod.rs:53`, `frogdb-server/crates/types/src/json.rs:149`). |
| `hotshards.hot-threshold-percent`, `warm-threshold-percent`, `default-period-secs` | `frogdb-server/crates/config/src/status.rs:98-111` | Flow into `HotShardConfig` (`server/src/server/subsystems.rs:449` via `to_collector_config()`), but the consuming field is named `_hotshards_config` (leading underscore = compiler-flagged unused) in `connection.rs:164`. The hot-shard collector is never built in production. |
| `replication.ack-interval-ms`, `fullsync-timeout-secs`, `fullsync-max-memory-mb` | `frogdb-server/crates/config/src/replication.rs:47-62` | Zero references anywhere outside `replication.rs` itself (`grep -rn` across `frogdb-server` for all three field names returns only their own declarations). |
| `tls.ciphersuites` | `frogdb-server/crates/config/src/tls.rs:74-77` | Zero references anywhere outside `tls.rs` itself. `TlsManager` has a `reload` path but it has no runtime caller (per 13-01's follow-up note), so even the fields that *are* consumed (protocols, client-cert mode) aren't live-reloadable, and `ciphersuites` isn't consumed at all. |

All six sections still carry the exact `// skip: config not yet consumed by server` comment in
today's tree (`grep -rn "skip: config not yet consumed" frogdb-server/crates/config/src/` — 18
lines across `json.rs`, `metrics.rs`, `replication.rs`, `status.rs`, `tls.rs`, `vll.rs`).

### Disposition options per section (to be decided by whoever picks this up)

For each section: either (a) wire it to a real runtime consumer and promote the fields through the
normal `#[param]`/`#[param(mutable)]` path (with a propagation-truth check per 13-01's gate), or (b)
delete the dead fields/section from the config schema (a breaking TOML change, acceptable per this
project's pre-production stance). Do not leave a third option of promoting to CONFIG GET while still
unread — that reintroduces the "misleading observability data" problem this round's audit avoided.

## Propagation-wiring candidates (carried over from issue `13-01`)

`13-01`'s Pass 2b also downgraded 20 fields to **immutable** (not dead — startup-consumed into a
`Duration`/struct/bool with no shared handle a `CONFIG SET` could reach, so GET is honest but SET
would be a no-op). These are a distinct, shallower class of follow-up: adding a shared-atomic/handle
seam would make them live-mutable, matching their Redis MODIFIABLE analogues. Restating 13-01's
candidate list here so both classes of "config wiring debt" are tracked from one issue:

- `tls-ciphersuites` + `tls-client-cert-file`/`tls-client-key-file` — needs a wired
  `TlsManager::reload` with a runtime caller (currently `reload` exists but nothing invokes it) plus
  a cert watcher.
- `replica-priority`, `cluster-auto-failover` — needs an atomic in `FailureDetectorConfig` and a
  handle injected into `ConfigManager`.
- The four `status.*` thresholds (`status-memory-warning-percent`,
  `status-connection-warning-percent`, `status-durability-lag-warning-ms`,
  `status-durability-lag-critical-ms`) — needs `StatusCollector` to read through a shared handle
  rather than a startup snapshot.
- Re-wiring the inert `vll.*`/`json.*` sections to their runtime consumers (see the dead-config table
  above) is a *prerequisite* for those specific fields before they could be exposed as CONFIG params
  at all — i.e. those two sections straddle both lists: currently dead, and only reachable via the
  propagation-wiring work once a consumer exists.

## Acceptance criteria

- [ ] Each of the 6 dead-config sections (`metrics` OTLP/bind, `vll`, `json`, `hotshards`,
      `replication` ack/fullsync, `tls.ciphersuites`) has a decision: wired-and-promoted, or
      deleted from the schema.
- [ ] Any section wired to a live consumer goes through the same propagation-truth gate as `13-01`
      Pass 2b (a `CONFIG SET` must demonstrably change runtime behavior before being marked mutable).
- [ ] Any section deleted removes the dead struct fields, their `#[param(skip)]` comments, and
      updates the golden snapshot / TOML docs accordingly (breaking change is acceptable per
      project conventions).
- [ ] The propagation-wiring candidates (20 immutable params carried over from 13-01) are
      either wired to a live handle and promoted to mutable, or explicitly left immutable with
      the reasoning re-confirmed.

## Blocked by

None. Independent of `13-02`/`13-03`. Builds on the completed `13-01` audit.

## Source

Round-9 triage: wire-or-delete work explicitly deferred out of this round's scope. Filed 2026-07-21
to track it, carrying forward the dead-config list and propagation-wiring candidates surfaced by
`13-01`'s Pass 2a/2b (`.scratch/arch-deepening/issues/13-01-config-param-skip-audit.md`).

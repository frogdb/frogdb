# 13-01 — Promote-or-justify audit of the 123 `#[param(skip)]` config fields

Status: ready-for-human

> **Pass 2b complete (2026-07-21).** The 35 Pass-1 promote-mutable rows were run
> through the **propagation-truth gate** (a param may become CONFIG SET-able only
> if a SET actually changes runtime behavior). Tracing each value's consumer
> found that FrogDB copies these config values into their subsystems **once at
> startup** — the runtime `Arc<RwLock<RuntimeConfig>>` is not shared with any
> subsystem, and the `Propagation` enum only routes to shards for
> eviction/histograms — so essentially none had an existing writable-live seam.
> Outcome: **1 promoted-mutable, 20 downgraded-immutable, 14 downgraded-justify.**
>
> - **1 promoted-mutable** — `acl.log-max-len` → `acllog-max-len` (Redis
>   analogue, MODIFIABLE). Sole survivor: the ACL log length is re-read on every
>   append and the consuming `AclLog` is already reachable from `ConfigManager`
>   via the injected `Arc<AclManager>`, so making it live was a few lines
>   (`AclLog.max_len` `usize`→`AtomicUsize` + `set_max_len`/`max_len`). CONFIG SET
>   now genuinely retunes the live log-trim bound.
> - **20 downgraded-immutable** (CONFIG GET-only, startup value is honest to
>   report): persistence `compaction-rate-limit-mb`/`batch-size-threshold-kb`;
>   `snapshot-interval-secs`; replication `replication-lag-threshold-bytes`/
>   `-secs`, `self-fence-on-replica-loss`, `replica-freshness-timeout-ms`;
>   cluster `cluster-auto-failover`, `cluster-self-fence-on-quorum-loss`,
>   `replica-priority`; tls `tls-cluster-migration`, `tls-client-cert-file`,
>   `tls-client-key-file`, `tls-handshake-timeout-ms`; `tracing-sampling-rate`;
>   status `status-memory-warning-percent`, `status-connection-warning-percent`,
>   `status-durability-lag-warning-ms`, `status-durability-lag-critical-ms`;
>   `latency-bands-enabled`. Each copies its value out of `Config` at construction
>   (into a `Duration`/`tokio::interval`/struct/bool), with no shared handle a SET
>   could reach. Implemented the 2a way (ImmutableParamId + readonly_param_meta +
>   StaticConfig field).
> - **14 downgraded-justify** — **dead config**, consumed nowhere at runtime, so
>   even CONFIG GET would report values the server ignores: replication
>   `ack-interval-ms`/`fullsync-timeout-secs`/`fullsync-max-memory-mb`;
>   `tls.ciphersuites`; all five `vll.*` timeouts (the config-crate `VllConfig` is
>   never wired to the runtime `vll::VllConfig`, which uses compile-time consts);
>   `json.max-depth`/`max-size` (`to_limits()` has zero callers; handlers hardcode
>   `JsonLimits::default()`); all three `hotshards.*` (flow to an unread
>   `_hotshards_config`; the collector is never built in production). All keep
>   `#[param(skip)]` + `// skip: config not yet consumed by server`.
>
> Registry grew **83 → 104** (21 rows appended after Pass-2a's 83; first 83
> unchanged). `MutableParamId` **45 → 46**, `ImmutableParamId` **38 → 58**; golden
> snapshot re-captured to 104; `id_counts_are_stable`/`test_golden_snapshot_row_count`
> updated deliberately. New tests: `test_config_get_promoted_immutable_params_pass2b`
> (GET value + SET-rejected for all 20), `test_config_set_acllog_max_len_roundtrip`
> (SET→GET→live trim behavior), `test_rewrite_config_acllog_max_len` (REWRITE into
> `[acl]`), `AclLog::set_max_len` unit test. Gates green: `cargo nextest -p
> frogdb-config` 106/106; server config/param/rewrite filters; `just check`;
> clippy (config+server+acl); `cargo fmt --check`; `docs-gen --check`
> (config-reference.json diff is exactly the 21 new `config_param`/`mutable`
> entries). **Follow-up candidates for propagation wiring** (a later issue could
> add the shared-atomic/handle plumbing to make these live-mutable, matching their
> Redis MODIFIABLE analogues): `tls-ciphersuites` + `tls-client-cert/key-file`
> (needs a wired `TlsManager::reload` + cert watcher — currently reload exists but
> has no runtime caller); `replica-priority`, `cluster-auto-failover` (needs an
> atomic in `FailureDetectorConfig` + a handle injected into `ConfigManager`);
> the four `status.*` thresholds (needs `StatusCollector` to read through a shared
> handle rather than a startup snapshot); and re-wiring the inert `vll.*`/`json.*`
> config sections to their runtime consumers before they could be exposed at all.
>
> **Pass 2a complete (2026-07-21).** The 26 promote-immutable rows were implemented as
> **22 `#[param]` promotions** (CONFIG GET-only) + **4 downgrades to justify**. The 4 downgrades
> are the `metrics.*` OTLP/bind rows: liveness verification (per the Pass-2 caveat, extended from
> `snapshot.*` on the "misleading observability data" principle) found the metrics section's
> OTLP exporter is **never wired** — `frogdb_telemetry::OtlpRecorder::new` is not constructed
> anywhere from `config.metrics`, and the metrics HTTP listener is superseded by the `http`
> section (`StaticConfig` maps the existing `metrics-enabled`/`metrics-port` params to
> `config.http.*`). Exposing `metrics-otlp-*`/`metrics-bind` via CONFIG GET would report values
> the server ignores, so they carry `// skip: config not yet consumed by server` (see the metrics
> rows below). `snapshot.snapshot-dir` was **verified live** (wired through
> `SnapshotConfigExt::to_core_config` into `RocksSnapshotCoordinator`) and kept as promote-immutable.
>
> Registry grew **61 → 83** (22 immutable rows appended after the original 61; first 61 unchanged).
> `ImmutableParamId` grew **16 → 38**; golden snapshot re-captured to 83 rows;
> `id_counts_are_stable`/`test_golden_snapshot_row_count` updated deliberately. Gates green:
> `cargo nextest -p frogdb-config` 106/106; server config tests 52/52 (incl. new
> `test_config_get_promoted_immutable_params`); `cargo check --workspace`; clippy (config+server);
> `cargo fmt --check`; `docs-gen --check` (config-reference.json regenerated — diff is exactly the
> 22 new `config_param`/`mutable:false` entries). **Name choices** (section-prefixed per the
> `persistence-enabled`/`metrics-enabled` convention, Redis-analogue where cited): `aclfile`,
> `enable-debug-command`, `sorted-set-index`, `snapshot-dir`, RocksDB tuning names unprefixed
> (`write-buffer-size-mb`, `compression`, `block-cache-size-mb`, `bloom-filter-bits`,
> `max-write-buffer-number`) matching the existing unprefixed persistence rows; `tls-enabled`,
> `cluster-enabled`, `cluster-data-dir`, `http-enabled/-bind/-port`, `admin-enabled/-port/-bind`,
> `tracing-enabled`, `tracing-otlp-endpoint`; `logfile` (Redis analogue, diverges from field
> `file-path`); `latency-bands` (diverges from field `bands` — `latency-bands-bands` would be
> redundant). Promote-mutable rows (Pass 2b) and the 62 justify rows are untouched.
>
> **Pass 1 complete (2026-07-21).** All 123 `#[param(skip)]` fields classified below
> (promote-mutable / promote-immutable / justify). The 62 **justify** fields have had a
> durable, greppable `// skip: <reason>` comment applied in-tree (config crate compiles,
> `cargo nextest -p frogdb-config` green 106/106, `cargo fmt --check` + clippy clean). No
> registry/enum/lifecycle edits were made — promote rows are the work order for **Pass 2**,
> which implements the `#[param]` / `#[param(mutable)]` promotions + server registry entries +
> round-trip tests. Pass-1 counts: **26 promote-immutable, 35 promote-mutable, 62 justify.**
> **Final counts after the Pass-2a/2b implementation audits: 42 CONFIG GET-only
> (immutable) + 1 CONFIG GET/SET (mutable, `acllog-max-len`) + 80 justify** —
> i.e. of the 26 Pass-1 promote-immutable, 22 were promoted (4 metrics rows fell
> to justify as dead config); of the 35 Pass-1 promote-mutable, 1 stayed mutable,
> 20 fell to immutable, 14 fell to justify. (22 + 20 = 42 immutable; 62 + 4 + 14 =
> 80 justify; 1 mutable; total 123.)
>
> Evidence base: Redis `unstable/src/config.c` and Valkey `unstable/src/config.c`
> `standardConfig` tables (MODIFIABLE_CONFIG / IMMUTABLE_CONFIG flags). Redis and Valkey agreed
> on every cited analogue, so flags are quoted once as "Redis/Valkey". `#[param]` in this
> codebase = CONFIG GET only (immutable); `#[param(mutable)]` = GET + SET.
>
> Convention chosen for justified skips: a same-position `// skip: <reason>` line on the
> `#[param(skip)]` attribute (rustfmt normalizes it to its own line directly above the field).
> Grep: `grep -rn '// skip:' frogdb-server/crates/config/src`.

## What to build

Phase 2 of the config derive-macro migration (proposal
`.scratch/arch-deepening/proposals/13-config-param-single-registry.md`) put
`#[derive(ConfigParams)]` on every struct-backed config section and forced every field to
declare `#[param(...)]` or `#[param(skip)]`. To keep the migration behavior-preserving, every
field that was *not* already a registered CONFIG GET/SET row was annotated `#[param(skip)]` —
**123** fields across `frogdb-server/crates/config/src/*.rs`.

`skip` is now doing double duty: it marks both "genuinely internal, never a CONFIG param" and
"arguably should be a CONFIG param but nobody has wired it up yet". That ambiguity is a silent
hole: a knob a Redis client would reasonably expect under CONFIG GET (e.g.
`persistence.compression`, `persistence.write-buffer-size-mb`, `metrics.otlp-endpoint`,
`http.*`, `admin.*`, the `vll.*` timeouts) is indistinguishable from a field that must never be
exposed (e.g. `server.enable-debug-command`, `memory.doctor-*` internals).

Audit each `#[param(skip)]` field and decide, per field:
- **Promote**: add a real `#[param]` / `#[param(mutable)]` row (and the matching
  `build_typed_params` / `build_param_registry` entry in
  `frogdb-server/crates/server/src/runtime_config.rs`) if it should be reachable via CONFIG.
- **Justify**: keep `skip`, but record *why* it is intentionally not a CONFIG param.

Capture the justification durably. Options (pick one, note the choice): a short `// skip: <reason>`
convention on each skipped field; or a `#[param(skip)]`-adjacent doc line; or a table in the
config crate's CONTEXT.md. Compare against Redis/Valkey CONFIG surface where a directly analogous
parameter exists.

## Acceptance criteria

- [x] Every one of the 123 `#[param(skip)]` fields is classified promote-or-justify
      (Pass 1; final decisions locked in Pass 2a/2b)
- [x] Promoted fields have a metadata row + server registry entry + a test asserting CONFIG
      GET/SET round-trips; `test_param_registry_consistency` and the golden snapshot updated
      deliberately (not auto-recaptured). Pass 2b added the propagation-truth gate: only
      `acllog-max-len` had a genuine live seam (CONFIG SET round-trip test); the other 34
      Pass-1 promote-mutable candidates were downgraded to immutable GET (20) or justify (14)
      because a SET would not change runtime behavior.
- [x] Justified skips carry a durable, greppable reason (`grep -rn '// skip:' …`)
- [x] Redis/Valkey parity checked for each candidate; divergences noted

## Blocked by

None — Phase 2 (this proposal's implementation) is merged/green. Can start immediately.

## Source

Proposal 13 Implementation section, residual gap (c). Filed 2026-07-21.

## Pass 1 decision table (work order for Pass 2)

Legend — decision: `promote-mutable` (add `#[param(mutable)]`), `promote-immutable` (add `#[param]`,
GET-only), `justify` (keep `#[param(skip)]`, reason now in-tree). Analogue mutability from
Redis/Valkey `standardConfig`; "—" = no directly analogous CONFIG parameter.

### persistence.rs — `PersistenceConfig` / `SnapshotConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| persistence.mode | justify | — (Redis: `appendonly`/`save`) | startup-only WAL backend selector; `fake` is a sim-test sink, no runtime meaning |
| persistence.write-buffer-size-mb | promote-immutable ✓done(2a) | — (RocksDB SetOptions) | RocksDB tuning applied at DB open; issue names it expose-worthy; startup-fixed |
| persistence.compression | promote-immutable ✓done(2a) | `rdbcompression` (MOD) loosely | RocksDB CF compression, applied at open; issue names it expose-worthy |
| persistence.block-cache-size-mb | promote-immutable ✓done(2a) | — | RocksDB block-cache sizing, applied at open |
| persistence.bloom-filter-bits | promote-immutable ✓done(2a) | — | RocksDB bloom tuning, applied at open |
| persistence.max-write-buffer-number | promote-immutable ✓done(2a) | — | RocksDB memtable count, applied at open |
| persistence.compaction-rate-limit-mb | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — (RocksDB rate limiter) | RocksDB rate limiter is the canonical live-tunable; throttle background I/O on a live node |
| persistence.batch-size-threshold-kb | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | sibling `batch-timeout-ms` is already `#[param(mutable)]`; write-path flush tuning |
| snapshot.snapshot-dir | promote-immutable ✓done(2a) | `dir` (MOD) | snapshot output path; path exposure is normal (Redis `dir`); startup-fixed |
| snapshot.snapshot-interval-secs | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `save` (MOD) | classic snapshot-cadence knob; **Pass 2: verify snapshot subsystem is live before wiring** |
| snapshot.max-snapshots | justify | — | borderline: retention count, no Redis analogue; snapshot subsystem liveness unverified |

### replication.rs — `ReplicationConfigSection`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| replication.role | justify | — (`REPLICAOF` command) | role set via REPLICAOF/failover, not CONFIG |
| replication.primary-host | justify | — (`REPLICAOF` command) | bootstrap topology; set via REPLICAOF |
| replication.primary-port | justify | — (`REPLICAOF` command) | bootstrap topology; set via REPLICAOF |
| replication.ack-interval-ms | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `repl-ping-replica-period` (MOD) | replication heartbeat cadence (replica→primary half) |
| replication.fullsync-timeout-secs | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `repl-timeout` (MOD) | direct analogue; full-sync timeout |
| replication.fullsync-max-memory-mb | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `client-output-buffer-limit` / `repl-backlog-size` (MOD) | sync buffering memory cap; OOM-safety operator story |
| replication.state-file | justify | — | internal replication state file path; no operator story |
| replication.connect-timeout-ms | justify | — (folded into `repl-timeout`) | borderline: internal replica-connect timeout |
| replication.handshake-timeout-ms | justify | — (folded into `repl-timeout`) | borderline: internal handshake timeout |
| replication.reconnect-backoff-initial-ms | justify | — | borderline: Redis has no reconnect-backoff CONFIG |
| replication.reconnect-backoff-max-ms | justify | — | borderline: Redis has no reconnect-backoff CONFIG |
| replication.replication-lag-threshold-bytes | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `client-output-buffer-limit` slave (MOD) | proactive-disconnect lag threshold |
| replication.replication-lag-threshold-secs | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `client-output-buffer-limit` slave (MOD) | proactive-disconnect lag threshold |
| replication.fullresync-cooldown-secs | justify | — | borderline: FrogDB-internal lag-disconnect cooldown |
| replication.split-brain-log-enabled | justify | — | FrogDB-specific split-brain logging toggle; diagnostic |
| replication.split-brain-buffer-size | justify | — | internal split-brain detection buffer sizing |
| replication.split-brain-buffer-max-mb | justify | — | internal split-brain detection buffer memory cap |
| replication.self-fence-on-replica-loss | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `min-replicas-to-write` (MOD) | CAP-tradeoff safety toggle; refuse writes when replicas lost |
| replication.replica-freshness-timeout-ms | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `min-replicas-max-lag` (MOD) | freshness window for self-fencing |
| replication.replica-write-timeout-ms | justify | — (folded into `repl-timeout`) | borderline: internal replica-stream write timeout |

### cluster.rs — `ClusterConfigSection`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| cluster.enabled | promote-immutable ✓done(2a) | `cluster-enabled` (IMMUT) | direct analogue; visibility of cluster mode, startup-fixed |
| cluster.node-id | justify | — (nodes.conf) | bootstrap identity; Redis keeps node id in nodes.conf, not CONFIG |
| cluster.client-addr | justify | `cluster-announce-ip` (MOD) but derived | bootstrap topology, derived from server bind at startup |
| cluster.cluster-bus-addr | justify | — | startup Raft bus bind; cannot rebind live |
| cluster.initial-nodes | justify | — | bootstrap cluster seed list; join-time only |
| cluster.data-dir | promote-immutable ✓done(2a) | `cluster-config-file` (IMMUT) | Raft state dir path; path exposure normal, startup-fixed |
| cluster.election-timeout-ms | justify | `cluster-node-timeout` (MOD) but Raft | borderline: Raft election timing read at init; live change risks split votes |
| cluster.heartbeat-interval-ms | justify | — | borderline: Raft heartbeat (coupled to election), init-time |
| cluster.connect-timeout-ms | justify | — | borderline: Raft bus connect timing, init-time |
| cluster.request-timeout-ms | justify | — | borderline: Raft RPC timing, init-time |
| cluster.auto-failover | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `cluster-replica-no-failover` (MOD) loosely | operator story: disable failover during maintenance |
| cluster.fail-threshold | justify | — | borderline: Raft failure-detector count, init-time tuning |
| cluster.self-fence-on-quorum-loss | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `cluster-require-full-coverage` (MOD) | refuse writes on quorum/coverage loss; direct concept |
| cluster.replica-priority | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `replica-priority` (MOD) | direct name+concept analogue |

### tls.rs — `TlsConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| tls.enabled | promote-immutable ✓done(2a) | — (Redis enables via `tls-port`) | master TLS on/off visibility; startup lifecycle |
| tls.ciphersuites | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `tls-ciphersuites` (MOD) | direct analogue (applyTlsCfg live-reloads) |
| tls.tls-cluster-migration | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | strong story: enable dual-accept during rolling TLS migration |
| tls.no-tls-on-admin-port | justify | — | startup listener wiring; fixed at bind time |
| tls.no-tls-on-http | justify | — | startup listener wiring; fixed at bind time |
| tls.client-cert-file | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `tls-client-cert-file` (MOD) | direct analogue; outgoing repl/cluster client cert |
| tls.client-key-file | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `tls-client-key-file` (MOD) | direct analogue |
| tls.watch-certs | justify | — | borderline: cert file-watcher lifecycle set at startup |
| tls.watch-debounce-ms | justify | — | internal file-watcher debounce; no operator story |
| tls.handshake-timeout-ms | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | TLS handshake timeout; tunable under slow clients |

### security.rs — `AclFileConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| acl.aclfile | promote-immutable ✓done(2a) | `aclfile` (IMMUT) | direct name analogue; path, immutable in Redis |
| acl.log-max-len | promote-mutable → **2b: MUTABLE ✓done** (`acllog-max-len`; live via Arc<AclManager>) | `acllog-max-len` (MOD) | direct name analogue |

### server.rs — `ServerConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| server.allow-cross-slot-standalone | justify | — | borderline: changes multi-key command semantics; startup-fixed behavior flag |
| server.sorted-set-index | promote-immutable ✓done(2a) | — | doc says restart-required; expose backend for visibility, startup-fixed |
| server.enable-debug-command | promote-immutable ✓done(2a) | `enable-debug-command` (IMMUT) | direct name analogue; Redis exposes GET-only (SET can't enable live) — safe visibility, CONFIG GET is auth-gated. NB: issue's example called this "never expose", but Redis evidence + immutable-only exposure makes GET safe |

### admin.rs — `AdminConfig` (issue names `admin.*` expose-worthy)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| admin.enabled | promote-immutable ✓done(2a) | — | management-surface visibility; startup listener; CONFIG GET auth-gated |
| admin.port | promote-immutable ✓done(2a) | `port`/`tls-port` (MOD) precedent | startup-only listener port; parity with exposed server.port |
| admin.bind | promote-immutable ✓done(2a) | `bind` (MOD) precedent | startup-only listener bind; parity with exposed server.bind |

### http.rs — `HttpConfig` (issue names `http.*` expose-worthy)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| http.enabled | promote-immutable ✓done(2a) | — | HTTP observability endpoint visibility; startup listener |
| http.bind | promote-immutable ✓done(2a) | `bind` (MOD) precedent | startup-only listener bind |
| http.port | promote-immutable ✓done(2a) | `port` (MOD) precedent | startup-only listener port |
| http.token | justify | — (cf. `requirepass` exposed) | security: bearer credential; must not surface via CONFIG GET |

### metrics.rs — `MetricsConfig` (issue names `metrics.otlp-endpoint` expose-worthy)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| metrics.bind | ~~promote-immutable~~ → **justify (Pass 2a: dead config)** | `bind` (MOD) precedent | DOWNGRADED: no listener binds here — metrics HTTP endpoint superseded by `http` section (`StaticConfig` maps `metrics-enabled`/`metrics-port` to `config.http.*`); only used for bind-overlap validation. `// skip: config not yet consumed by server` |
| metrics.otlp-enabled | ~~promote-immutable~~ → **justify (Pass 2a: dead config)** | — | DOWNGRADED: `OtlpRecorder::new` is never constructed from `config.metrics`; OTLP metrics exporter is unwired. Pass-1 rationale ("built at startup") was incorrect. `// skip: config not yet consumed by server` |
| metrics.otlp-endpoint | ~~promote-immutable~~ → **justify (Pass 2a: dead config)** | — | DOWNGRADED: OTLP metrics exporter unwired (see otlp-enabled). `// skip: config not yet consumed by server` |
| metrics.otlp-interval-secs | ~~promote-immutable~~ → **justify (Pass 2a: dead config)** | — | DOWNGRADED: OTLP metrics exporter unwired (see otlp-enabled). `// skip: config not yet consumed by server` |

### distributed_tracing.rs — `TracingConfig` (section `tracing`)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| tracing.enabled | promote-immutable ✓done(2a) | — | tracing pipeline built at startup; expose state |
| tracing.otlp-endpoint | promote-immutable ✓done(2a) | — | trace export target; startup-wired |
| tracing.sampling-rate | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | THE universal tracing live-tunable (dial sampling up to debug / down for cost) |
| tracing.service-name | justify | — | borderline: static trace-identity label, startup-fixed |
| tracing.scatter-gather-spans | justify | — | borderline: fine-grained span category; pipeline startup-wired |
| tracing.shard-spans | justify | — | borderline: fine-grained span category |
| tracing.persistence-spans | justify | — | borderline: fine-grained span category |
| tracing.recent-traces-max | justify | — | internal DEBUG TRACING RECENT ring-buffer size |

### vll.rs — `VllConfig` (issue names `vll.*` timeouts expose-worthy)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| vll.max-queue-depth | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | per-shard backpressure threshold; overload-protection tuning |
| vll.lock-acquisition-timeout-ms | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | issue names vll timeouts expose-worthy; live-checked per op |
| vll.per-shard-lock-timeout-ms | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | issue names vll timeouts expose-worthy |
| vll.timeout-check-interval-ms | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | vll timeout-reaper cadence; part of the vll timeout family |
| vll.max-continuation-lock-ms | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | continuation-lock timeout |

### status.rs — `StatusConfig` / `HotShardsConfig` (observability thresholds)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| status.memory-warning-percent | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | health threshold read live by status endpoint |
| status.connection-warning-percent | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | health threshold |
| status.durability-lag-warning-ms | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | health threshold |
| status.durability-lag-critical-ms | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | — | health threshold |
| hotshards.hot-threshold-percent | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | observability threshold |
| hotshards.warm-threshold-percent | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | observability threshold |
| hotshards.default-period-secs | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | — | observability collection period |

### json.rs — `JsonConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| json.max-depth | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `proto-max-bulk-len` (MOD) loosely | input safety limit, per-command checked |
| json.max-size | promote-mutable → **downgraded(2b): justify** (`// skip: config not yet consumed by server`) | `proto-max-bulk-len` (MOD) loosely | input safety limit, per-command checked |

### latency.rs — `LatencyConfig` / `LatencyBandsConfig`

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| latency.startup-test | justify | — | startup-only intrinsic-latency self-test toggle |
| latency.startup-test-duration-secs | justify | — | startup-only self-test duration |
| latency.warning-threshold-us | justify | — | startup-only self-test threshold |
| latency-bands.enabled | promote-mutable → **downgraded(2b): immutable** (startup-consumed, no propagation seam) | `latency-tracking` (MOD) | toggle SLO latency-band tracking live |
| latency-bands.bands | promote-immutable ✓done(2a) | `latency-tracking-info-percentiles` (MOD) | bucket thresholds; Redis makes percentiles mutable but live-resize resets histograms → GET-only |

### memory.rs — `MemoryConfig` (doctor internals)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| memory.doctor-big-key-threshold | justify | — | internal MEMORY DOCTOR heuristic; issue names doctor-* an internal; weak operator story |
| memory.doctor-max-big-keys | justify | — | internal MEMORY DOCTOR report cap |
| memory.doctor-imbalance-threshold | justify | — | internal MEMORY DOCTOR imbalance heuristic |

### monitor.rs / compat.rs / tiered.rs / blocking.rs / debug_bundle.rs / logging.rs

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| monitor.channel-capacity | justify | — | internal MONITOR broadcast ring-buffer capacity |
| compat.strict-config | justify | — | borderline: FrogDB-specific CONFIG-strictness meta-toggle |
| tiered-storage.enabled | justify | — | startup-only: warm-tier subsystem initialized at boot, requires persistence |
| blocking.max-waiters-per-key | justify | — | borderline: FrogDB-specific per-key waiter cap; no Redis analogue |
| blocking.max-blocked-connections | justify | — | borderline: FrogDB-specific blocked-connection cap; no Redis analogue |
| debug-bundle.directory | justify | `dir` (MOD) loosely | borderline: diagnostic-bundle output path; niche subsystem |
| debug-bundle.max-bundles | justify | — | borderline: diagnostic-bundle retention count; niche |
| debug-bundle.bundle-ttl-secs | justify | — | borderline: diagnostic-bundle retention TTL; niche |
| debug-bundle.max-slowlog-entries | justify | — | internal per-bundle slowlog content cap |
| debug-bundle.max-trace-entries | justify | — | internal per-bundle trace content cap |
| logging.format | justify | — | borderline: log formatter built at startup; Redis has no log-format CONFIG |
| logging.output | justify | — | startup-fixed console sink; writer bound at startup |
| logging.file-path | promote-immutable ✓done(2a) | `logfile` (IMMUT) | direct analogue; log file path, immutable in Redis |
| logging.rotation | justify | — | nested struct; sub-fields intentionally not ConfigParams |

### chaos.rs — `ChaosConfig` (turmoil feature only — all justify)

| section.field | decision | Redis/Valkey analogue (mutability) | rationale |
|---|---|---|---|
| chaos.scatter-inter-send-delay-ms | justify | — | chaos latency injection; turmoil test feature only |
| chaos.shard-delays-ms | justify | — | chaos latency injection; test-only |
| chaos.jitter-ms | justify | — | chaos latency injection; test-only |
| chaos.single-shard-delay-ms | justify | — | chaos latency injection; test-only |
| chaos.transaction-delay-ms | justify | — | chaos latency injection; test-only |
| chaos.unavailable-shards | justify | — | chaos failure injection; test-only |
| chaos.connection-reset-probability | justify | — | chaos failure injection; test-only |
| chaos.error-shards | justify | — | chaos failure injection; test-only |

### Pass 2 caveats

- **Snapshot subsystem**: `snapshot.*` promotions assume the snapshot mechanism is live. FrogDB
  persistence is WAL/RocksDB-based; verify `SnapshotConfig` is actually wired before exposing
  `snapshot-interval-secs` as mutable (else downgrade to justify).
- **`server.enable-debug-command`**: promote-immutable diverges from the issue's offhand
  "never expose" example, but is backed by a direct Redis/Valkey `enable-debug-command`
  (IMMUTABLE) analogue. GET-only exposure cannot enable the DoS vector; CONFIG GET is auth-gated.
  Confirm this reading is acceptable in Pass 2 review.
- **`bands` / `latency-tracking-info-percentiles`**: Redis makes percentiles mutable; held at
  immutable here because live bucket changes reset histogram state. Revisit if a reset-on-set
  semantic is acceptable.
- Roughly even split (61 promote / 62 justify) rather than the anticipated "large majority
  justify": the promotes are concentrated in fields the issue itself named expose-worthy
  (persistence RocksDB tuning, metrics OTLP, `http.*`, `admin.*`, `vll.*` timeouts), fields with
  a direct Redis/Valkey MODIFIABLE analogue (tls-ciphersuites, tls-client-cert/key, acllog-max-len,
  replica-priority, cluster-require-full-coverage), and observability thresholds (status/hotshards).

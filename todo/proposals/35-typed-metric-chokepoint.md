# Proposal: Typed Metric Chokepoint

Status: implemented
Date: 2026-07-04

## Problem

"Emit a metric" is one logical operation — *record a sample against a registered name, type, and
label schema* — but the codebase had **four parallel systems** that each owned a fragment of it and
agreed with each other only by convention:

1. **`metric_names`** — ~100 string constants (`telemetry/src/lib.rs:62-167`), used by 13 files.
2. **`define_metrics!` typed structs** (`telemetry/src/definitions.rs` + the `metrics-derive`
   macro) — generated `inc`/`set`/`observe` methods that were pure pass-throughs to the recorder,
   used by exactly *two* call sites (`server/server/{subsystems,cluster_init}.rs`).
3. **Raw string literals** — ~90 `recorder.increment_counter("frogdb_…", …)` sites across core,
   persistence, and server, each restating the name *and* the label schema inline.
4. **`DashboardMetrics`** — a ~30-field hand-maintained struct filled by one linear scan over the
   gathered families *per field* (`prometheus_recorder.rs:389-417`).

None of these was the interface; each was a partial copy of it. The registry (`ALL_METRICS`) —
which feeds the Grafana dashboard generator and the metrics verification test — described what the
*definitions file* said, not what the server *emitted*. Nothing connected the two, so they drifted,
and the drift audit turned up far more than the one known case:

| Drift class | Instances found |
|---|---|
| Emitted but absent from the registry (invisible to dashboard-gen and the verification test) | `frogdb_snapshot_epoch`, `frogdb_fields_expired_total`, `frogdb_keyspace_notifications_dropped_total`, `frogdb_pubsub_shard_limit_warnings_total`, `frogdb_wal_rollbacks_total`, `frogdb_blocked_migration_moved_total`, `frogdb_tiered_demotions_total`, `frogdb_tiered_bytes_demoted_total`, `frogdb_tls_handshake_errors_total`, six `frogdb_task_*` gauges |
| Registered with the wrong label schema (registry said `[]`, emission had labels) | ~30 metrics: every per-shard keyspace/eviction/blocking/WAL/pubsub/lua metric; `scatter_gather_*` (`[status]` vs emitted `[command, status]`); `transactions_*` (`[]` vs `[outcome]`) |
| Registered with the wrong label *name* | `persistence_errors` (registry `error_type`, emitted `type`); `latency_band_requests` (registry `band`, emitted `le`) |
| Registered with the wrong *type* | `cpu_user/system_seconds_total` and `latency_band_requests` declared `counter`, emitted via `record_gauge` |
| Label enums documenting values nothing emits | `ErrorType` (10 variants; the only emitted value was `"command_error"`), `TransactionOutcome` (4 variants vs 7 real outcome strings), `ScatterGatherStatus`, `BlockingResolution`, `ServerMode` |
| Registered but never emitted anywhere | `frogdb_net_input_bytes_total`, `frogdb_net_output_bytes_total` (the debug UI charted them as a permanent 0 B/s) |

Three of these were not just bookkeeping errors but live misleading-observability bugs, which this
project treats as unacceptable:

- **The debug UI's CPU tiles always read 0.** `dashboard_snapshot` read
  `cpu_user_seconds` with its *counter* extractor (`counter_val`, `prometheus_recorder.rs:405`),
  but the metric was recorded as a *gauge* (`system.rs:83`) — `m.get_counter().value()` on a gauge
  family is always `0.0`. A plausible number, permanently wrong.
- **HELP text was dead.** The recorder created every metric with `Opts::new(name, name)`
  (`prometheus_recorder.rs:160,173,208`), so `/metrics` shipped `# HELP frogdb_commands_total
  frogdb_commands_total` while the carefully written doc-comments in `definitions.rs` fed only the
  dashboard generator.
- **Label arity was fixed by whichever caller ran first.** `get_or_create_counter_vec` cached the
  vec by name with the *first caller's* label names; a second site emitting the same name with a
  different label set hit `with_label_values` with the wrong arity — a panic in the prometheus
  crate, guarded only by ~90 call sites agreeing by hand. The `pubsub_messages_total` /
  `keys_expired_total` sites (per-shard labels) versus their label-less registry entries show how
  close to the cliff this convention lived.

The verification test that was supposed to catch all of this,
`telemetry/tests/metrics_usage.rs`, was `#[ignore = "many metrics defined but unused"]` — and
structurally blind anyway: it grepped for raw name literals, so a `metric_names::`-const call site
was invisible to it, and an unregistered emission (the `snapshot_epoch` class) was out of its
universe entirely.

**Deletion test.** Delete any one of the four systems and the complexity relocates instead of
vanishing: delete `metric_names` and its 13 users restate the literals inline (system 3 grows);
delete the typed structs and the registry loses its only source, taking dashboard-gen with it;
delete the raw literals and there is nothing to migrate *to* at the shard layer, because the typed
structs lived in `frogdb-telemetry`, which **depends on** `frogdb-core` — the crate with the most
emission sites could not import its own metric handles. That dependency inversion is why system 2
had two users: the chokepoint existed but was unreachable from the code that needed it.

## Design

One declaration is both the emission path and the registry entry, and it lives where every emitter
can reach it.

### The registry moves to `frogdb-types`

`frogdb-types/src/metrics/` now hosts the whole contract: `MetricType`, `MetricDefinition`,
`definition_for(name)`, the label enums (`labels.rs`), and every metric declaration
(`definitions.rs`, via the upgraded `define_metrics!`). `frogdb-types` is also where
`MetricsRecorder` lives, so the *what* (typed handles) and the *where* (recorder backends) share a
home at the bottom of the dependency graph — core, persistence, server, and telemetry can all
import the handles. `frogdb-telemetry` re-exports everything (`ALL_METRICS`, `definitions`,
`labels`, …) so existing imports and the dashboard generator did not move.

### One declaration, two artifacts

```rust
/// Total Lua script errors
counter LuaScriptsErrors("frogdb_lua_scripts_errors_total") {
    labels: [shard: &str, error: ScriptError],
}
```

generates:

- the **typed handle** — `LuaScriptsErrors::inc(&dyn MetricsRecorder, shard: &str, error:
  ScriptError)`. The label schema *is* the method signature: a call site cannot omit a label, add
  one, reorder them, or pass an out-of-vocabulary enum value. Counters get `inc`/`inc_by`, gauges
  get `set`, histograms get `observe`. (The old gauge `inc` alias — which silently *set* — is
  deleted.)
- the **registry entry** — `MetricDefinition { name, help, metric_type, labels, handle }`. The new
  `handle` field carries the generated struct's name so the verification test can prove emission.

The macro now emits paths unqualified (`MetricType`, `MetricsRecorder`), resolved in the invoking
module, so it is not welded to any host crate.

### Registration derives from the definition

`PrometheusRecorder` still creates metrics lazily (so `/metrics` stays empty until something is
emitted, and unknown ad-hoc names in tests keep working), but for any name in the registry the
creation now takes **help text and label names from the definition**, not from the first caller:

- `/metrics` carries the real doc-comment as `# HELP …`.
- Arity is fixed by the registry at creation. A raw emission whose type or label schema
  contradicts the registry is **dropped with an error log** (`emission_matches_definition`) instead
  of panicking inside `with_label_values` or silently registering a shadow family the registry
  never scrapes. Typed handles always pass this check by construction; only raw string emissions
  can trip it, and the lint gate (below) is eliminating those.

The `MetricsRecorder` trait itself is unchanged: it remains the *backend* seam (Prometheus, OTLP,
composite, no-op, test recorders, vll's `MetricsSink` adapter all implement or forward it). The
chokepoint sits on top: handles fix what is emitted; the trait object decides where it goes. This
is the depth split that earns both layers their keep — deleting the trait would weld every handle
to Prometheus; deleting the handles reopens every drift class above.

### The registry is reconciled with reality

Definitions now describe what the server emits, not what a spec once wished:

- All 15 previously unregistered metrics are declared (the `snapshot_epoch` fix among them).
- Per-shard label schemas are declared where per-shard labels are emitted.
- `cpu_*_seconds_total` and `latency_band_requests_total` are declared as the gauges they are
  (help text states the CPU gauges are monotonic getrusage samples); the band label is `le`, as
  emitted.
- `persistence_errors` sites now emit the registry's `error_type` label (was `type`).
- Open-vocabulary labels (`command`, `shard`, `outcome`, `error` on command errors) are `&str`;
  closed vocabularies get honest enums that match emission: `ScriptKind` (eval/evalsha),
  `ScriptError` (not_available/execution/noscript), `PubsubLimitResource`, `TlsHandshakeError`,
  plus the surviving `RejectionReason` and `PersistenceErrorType`. The five fiction enums are
  deleted.
- The never-emitted `net_input/output_bytes` metrics are **deleted** — from the registry, from
  `DashboardMetrics`, and from the debug UI (which was charting a permanent flat zero as "Network
  I/O"). If byte accounting is ever implemented, one declaration resurrects the whole path.

### `DashboardMetrics`: registry-fed values, stable JSON shape

The struct stays — its serialized field names are the JSON contract with the debug web UI
(`/debug/api/metrics`, consumed by `debug/assets/js/charts.js`), and a registry-derived map would
break that contract for zero reader benefit. What changed is the *filling*: one pass over the
gathered families folds every family into a name→sum map, reading counter or gauge values according
to the family's own registered type (this is what fixes the CPU-zero bug), and the struct fields
are looked up by the typed handles' `NAME` constants. The per-field × per-family O(n·m) scan is
gone, and per-shard extraction plus top-commands ride the same single pass.

### Verification actually verifies

- **`telemetry/tests/metrics_usage.rs`** (un-ignored, rewritten): walks every `.rs` file under
  `crates/` once and asserts that each `ALL_METRICS` entry is emitted through its handle —
  `Handle::inc(`/`inc_by(`/`set(`/`observe(` for its type — somewhere outside the registry sources.
  Because the search key is the *handle*, const-indirection can no longer hide usage, and because
  the registry is now emission-complete, "defined but unused" means exactly that. Metrics whose
  only emitters live in files other agents owned during this migration are pinned in
  `RAW_EMISSION_EXEMPT` as (metric, file) pairs; the test requires the raw literal to still be in
  that file, so each exemption **self-expires** when the site migrates.
- **`just lint-metrics-chokepoint`** (wired into `just lint`): rejects any
  `.increment_counter(` / `.record_gauge(` / `.record_histogram(` call outside the backend seam
  (the trait + its implementations), the registry/macro sources, test directories, and the
  explicit follow-up allowlist. Clippy cannot express "this method outside those files"; a grep
  gate is the honest tool, same as `lint-info-seam` / `lint-redirect-seam`.
- **Recorder unit tests** pin the new behaviors: definition-driven HELP text, wrong-schema and
  wrong-type raw emissions dropped (not panicking, not shadow-registered), unknown-name fallback,
  and the dashboard snapshot reading typed emissions (including CPU-as-gauge and per-shard
  summing).

### Why this is the right depth

- **Locality.** "What is `frogdb_lua_scripts_errors_total`?" is answered by one declaration: name,
  type, help, labels, and (via the handle) every emitter findable by grep. Previously it took four
  files that disagreed.
- **Leverage.** A new metric is one declaration; the handle, registration, HELP text, dashboard
  panel, and verification coverage all come from it. Forgetting the registry entry is no longer
  possible because there is no second artifact to forget.
- **A bug class deleted, not guarded.** Wrong-label-set emission is now a compile error at every
  migrated site; the runtime guard covers only the shrinking raw remainder — and turns the old
  panic into a logged drop.
- **Not an adapter layer.** `metric_names`, the telemetry-local definitions/labels/typed modules,
  the dead `create_metrics_recorder`/`record_server_info`, and the per-field snapshot scan are
  *removed*, not wrapped. Net: one system where there were four.

## Migration performed

All phases committed separately; `just check` green on the touched crates at each step.

1. **Phase 1** — registry hosted in `frogdb-types` (`types/src/metrics/{mod,labels,definitions}.rs`);
   `metrics-derive` emits `handle`, drops gauge `inc`, unqualifies paths. Definitions reconciled
   (labels/types/missing/dead as above).
2. **Phase 2** — telemetry rewired: `metric_names` and local registry modules deleted (re-exports
   preserve import paths); `CommandTimer`, system collector, task monitors, latency-band flush emit
   through handles; recorder gains definition-driven creation + mismatch guard; single-pass
   `dashboard_snapshot`; `metrics_usage.rs` rewritten and un-ignored.
3. **Phase 3** — core + persistence call sites migrated (shard engine: diagnostics, eviction,
   blocking, scripting, execution, event loop, pubsub dispatch, keyspace coordinator; persistence:
   WAL flush, rocks snapshot coordinator — registering `frogdb_snapshot_epoch`).
4. **Phase 4** — server call sites (acceptor, init, subsystems, transaction handler, INFO's
   counter read-back via `Handle::NAME`); debug UI drops the dead Network I/O tiles/chart.
5. **Gate** — `lint-metrics-chokepoint` added to `just lint`.

### Follow-ups (files other agents owned during the migration)

Raw emission survives, allowlisted in both the lint gate and `RAW_EMISSION_EXEMPT`, in:

- `crates/vll/src/coordinator.rs` — `frogdb_scatter_gather_{total,duration_seconds,shards}` via
  vll's own `MetricsSink` trait (`crates/vll/src/traits.rs`), forwarded by
  `crates/server/src/vll_adapter.rs`. The registry now declares these with their real
  `[command, status]` / `[command]` schemas, so the raw emissions pass the recorder's guard; when
  vll migrates (either by taking `frogdb-types` as a dependency or by widening `MetricsSink`),
  remove all three files from the gate allowlist and the three exemption entries — the
  self-expiring test will insist.

## Testing impact

- `frogdb-types`: registry lookup/handle-constant unit tests; label-enum rendering tests.
- `frogdb-telemetry`: recorder tests for definition-driven HELP, schema/type mismatch dropping,
  unknown-name fallback, dashboard snapshot correctness (CPU gauge, per-shard sums, top commands);
  `prometheus_unit_tests.rs` rewritten onto typed handles; `metrics_usage.rs` un-ignored and
  structurally able to fail.
- The mechanical call-site migration is covered by the existing crate test suites (mock recorders
  in core assert the same counter totals through the handles) plus the new emission-coverage test.

## Correctness flags

1. **`frogdb_snapshot_epoch` unregistered — FIXED.** Emitted at
   `persistence/src/snapshot/rocks_coordinator.rs` but absent from `ALL_METRICS`: invisible to
   dashboard-gen and unverifiable. Now declared and emitted via `SnapshotEpoch::set`.
2. **Debug UI CPU metrics always 0 — FIXED.** Counter-typed snapshot read of gauge-recorded
   values; the registry now declares them gauges and the snapshot reads by family type.
3. **Registry lied about ~30 label schemas and 2 metric types — FIXED** (reconciled to emission;
   dashboard-gen queries now aggregate by the labels that actually exist, e.g.
   `sum by (command, status)` for scatter-gather instead of the nonexistent `status`-only).
4. **`ErrorType`/`TransactionOutcome`/`ScatterGatherStatus`/`BlockingResolution`/`ServerMode`
   enums documented label vocabularies nothing emitted — FIXED** (deleted; schemas are `&str`
   where the vocabulary is open, honest enums where it is closed).
5. **`frogdb_info{mode=…}` always reports `standalone` — OPEN.** `server/src/server/init.rs`
   hardcodes `"standalone"` even for cluster-mode servers. The handle now makes the label explicit
   at the call site; wiring the real mode is a one-line fix once someone owns which config field is
   authoritative.
6. **INFO's `total_net_input_bytes:0` / `total_net_output_bytes:0` stubs — OPEN.** The INFO text
   surface (`commands/info.rs`, `info/sections.rs`) prints Redis-parity zeros for byte counters
   FrogDB does not track; the never-emitted Prometheus twins are deleted by this proposal, but the
   INFO fields belong to the INFO-surface work (proposals 21/22): either implement byte accounting
   or drop the fields there too.

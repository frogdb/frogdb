# Spec: operations/diagnostics.md

Status: rewrite (MERGE of current `performance.mdx` + `debug-ui.mdx`; PLAN §4)
Audiences: A4 (primary), A5

Goal: The reader can find out what a running FrogDB server is doing — capture slow
commands with the slowlog, track latency with the `LATENCY` suite and latency
bands, find hot keys with `HOTKEYS`, use the authenticated debug web UI and its
JSON API, and generate a diagnostic bundle for troubleshooting. Corrects the
current pages' `DEBUG HOTSHARDS` (which is not a runnable command).

Not in scope:
- Metrics/tracing/health endpoints — [Observability](/operations/observability/);
  this page is the interactive/diagnostic tooling, that page is the
  metrics/tracing pipeline. Link, don't duplicate (they currently overlap on
  STATUS and health).
- Benchmarking / published performance numbers — none exist (PLAN §4 cut
  `reference/benchmarks.mdx`); no latency/throughput numbers on this page.

Sources of truth (read before writing):
- `frogdb-server/crates/server/src/connection/handlers/slowlog.rs` +
  `frogdb-server/crates/config/src/slowlog.rs` — SLOWLOG subcommands + `[slowlog]`.
- `frogdb-server/crates/server/src/connection/handlers/latency.rs` — LATENCY
  subcommands (incl. BANDS, HELP).
- `frogdb-server/crates/config/src/latency.rs` — `[latency-bands]` and `[latency]`
  (startup test) keys; `frogdb-server/crates/server/src/main.rs` for
  `--intrinsic-latency` / `--startup-latency-check`.
- `frogdb-server/crates/core/src/hotkeys.rs` +
  `frogdb-server/crates/server/src/connection/handlers/hotkeys.rs` — HOTKEYS
  (START/STOP/RESET/GET).
- `frogdb-server/crates/debug/src/web_ui/routes.rs` and `debug/src/bundle/` — the
  debug UI routes, `/debug/api/*` endpoints, and bundle store.
- `frogdb-server/crates/config/src/http.rs` (`[http].token`) and
  `config/src/debug_bundle.rs` (`[debug-bundle]` keys).
- `frogdb-server/crates/server/src/connection/dispatch.rs` — to CONFIRM which
  DEBUG subcommands are wired (HOTSHARDS is NOT).

Existing content: `performance.mdx` + `debug-ui.mdx` (merge). Keep slowlog,
latency bands, LATENCY, intrinsic-latency, STATUS, health endpoints, debug UI,
debug JSON API, bundles. FIX/REMOVE the hot-shard command (see below).

## Verified facts (authoritative)

**Slowlog.** `SLOWLOG GET/LEN/RESET/HELP` are real. `[slowlog]` keys:
`log-slower-than`, `max-len`, `max-arg-len`. Entry fields and the
`/debug/api/slowlog` JSON endpoint are correct.

**LATENCY suite.** Real subcommands: LATEST, HISTORY, RESET, GRAPH, HISTOGRAM,
DOCTOR, plus **BANDS** and **HELP** (add BANDS — it's a real extra). Keep.

**Latency bands.** `[latency-bands]` `enabled` + `bands` (default
`[1,5,10,50,100,500]` ms); metric `frogdb_latency_band_requests_total{band=...}`
(counter). Keep; verify the metric name against the metrics catalog.

**Intrinsic / startup latency.** `frogdb-server --intrinsic-latency <secs>` (runs
and exits) and `--startup-latency-check` are real flags; `[latency]` keys
`startup-test`, `startup-test-duration-secs`, `warning-threshold-us` (2000). Keep.

**HOTKEYS — the real hot-* tool.** `HOTKEYS START/STOP/RESET/GET`
(`handlers/hotkeys.rs`, dispatched in `dispatch.rs`). Reports a sampling session
of most-accessed keys with per-key `access_count`, `total_cpu_us`,
`total_net_bytes`; session config includes metric type, top-N `count` (default
16), `duration_ms`, `sample_ratio`, `selected_slots`. This is the command to
document for hot-key detection.

**`DEBUG HOTSHARDS` — NOT a runnable command (fix current docs).** The
`debug/src/hotshards.rs` report machinery exists (ShardStats, formatters) but the
DEBUG dispatch table has **no HOTSHARDS arm** — it returns an unknown-subcommand
error, the collector is never invoked, and the connection field is dead code.
Both current pages document `DEBUG HOTSHARDS [PERIOD <seconds>]` as usable — this
is WRONG. Remove it. For per-shard imbalance, point instead to `HOTKEYS` (per-key)
and the Prometheus `max(frogdb_shard_keys)/avg(frogdb_shard_keys)` ratio (verify
that metric exists), and note the hot-shard report exists internally but is not
yet exposed as a command.

**STATUS.** `STATUS` / `STATUS JSON` (RESP) and `GET /status/json` (HTTP) are real.
**This page is the canonical home for STATUS / `/status/json`** — Observability links
here rather than documenting it. `[status]` thresholds `memory-warning-percent`,
`connection-warning-percent`.

**Debug web UI + JSON API.** UI at `/debug`; `/debug/api/*`: `cluster`, `config`,
`metrics`, `clients`, `slowlog`, `latency` — all real. Bundle endpoints:
`/debug/api/bundle/list`, `/debug/api/bundle/generate`, `/debug/api/bundle/{id}` —
all real, backed by a TTL/capacity-bounded store. **Auth: the token is
`[http].token`** (NOT a `[debug-bundle]` token). Threat model (state it, don't
alarm): the HTTP server binds `127.0.0.1` by default (`config/src/http.rs`
`default_http_bind`), so unauthenticated `/debug/*` is loopback-only out of the
box — the intended use is trusted internal environments, same trust model as
local `redis-cli` without AUTH. Document the exposure recipe explicitly: to
serve beyond localhost, set `http.bind` + `http.token`, and front with a
TLS-terminating reverse proxy (config validation already warns that a bearer
token over plaintext HTTP on `0.0.0.0` is unsafe). Link Security. `[debug-bundle]` keys: `directory`,
`max-bundles`, `bundle-ttl-secs`, `max-slowlog-entries`, `max-trace-entries`.

Structure (H2/H3 — one line each):

- Intro: the built-in tools for inspecting a running server — slowlog, latency
  tooling, hot-key detection, the debug UI, and diagnostic bundles. Link
  Observability for metrics/tracing.
- **## Slowlog** — `[slowlog]` config; SLOWLOG GET/LEN/RESET/HELP; entry fields;
  JSON via `/debug/api/slowlog`.
- **## Latency tooling** — the LATENCY suite (incl. BANDS); latency bands config +
  the counter metric + PromQL rate; intrinsic/startup latency flags and `[latency]`
  keys.
- **## Hot-key detection** — `HOTKEYS START/STOP/RESET/GET`, what it samples and
  reports, session options. Replace `DEBUG HOTSHARDS` here; add one honest
  sentence that a per-shard hot-shard report exists internally but is not exposed
  as a command, and point to the `frogdb_shard_keys` ratio for shard-imbalance
  alerting.
- **## Server status** — `STATUS` / `STATUS JSON` / `GET /status/json`; `[status]`
  thresholds. This page is the canonical home for STATUS; Observability links here
  (it does not duplicate this).
- **## Debug web UI** — enable via `[http]` + `token`; what the UI shows;
  default loopback bind + how to expose safely (token + reverse-proxy TLS;
  link Security).
- **## Debug JSON API** — the `/debug/api/*` table with the `Authorization:
  Bearer` example.
- **## Diagnostic bundles** — generate/list/download endpoints; `[debug-bundle]`
  retention keys; what a bundle contains (server state, config, metrics, slowlog,
  traces).
- **## See also** — Observability, Security (debug-endpoint auth), Configuration,
  Reference → Metrics.

Generated data:
- Links to Reference → Configuration reference (`[slowlog]`/`[latency-bands]`/
  `[latency]`/`[status]`/`[debug-bundle]`/`[http]`), Compatibility → Command matrix
  (`compatibility/command-matrix`, SLOWLOG/LATENCY/HOTKEYS via S1/S2), Reference →
  Metrics (latency-band and
  shard metrics via S3). No embedded generated component.

Drift guards:
- `just docs-gen-check` keeps the linked config keys honest.
- Command subcommand lists (SLOWLOG/LATENCY/HOTKEYS) should be validated against
  the command matrix (S1/S2) — the current pages shipped a non-existent DEBUG
  HOTSHARDS, exactly the drift to guard.
- S7 code-path check for cited `crates/debug/...`, `crates/core/src/hotkeys.rs`.
- Metric names validated against the catalog (S3).
- Content policy: no runnable command that isn't wired in dispatch (DEBUG
  HOTSHARDS); no latency/throughput numbers; state the debug-endpoint threat
  model (loopback default, token for remote exposure) rather than framing
  no-auth as a defect.

# frogdb-server admin surface, configuration, and INFO/operations — testing gap audit (round 2)

## Scope

Audited (all under `frogdb-server/crates/server/src/`, ~10.6k LOC of in-scope source):

| path | LOC | notes |
|---|---|---|
| `runtime_config.rs` | 5473 | `ConfigManager`, CONFIG GET/SET/REWRITE, 67 inline `#[test]` |
| `info/` (`mod.rs`, `sections.rs`, ...) | ~1400 | INFO section rendering |
| `observability_server.rs` | 363 | axum router: `/metrics`, `/health/*`, `/debug/*`, `/admin/*`, bearer gate |
| `admin/` (`handlers.rs`, `mod.rs`, `state.rs`) | ~900 | admin REST handlers |
| `config/` (`loader.rs`, `mod.rs`) | ~1100 | `Config::load` (figment), logging init, `default_toml` |
| `config_persister.rs` | 272 | pure TOML merge + atomic write |
| `debug_providers.rs`, `server_observability.rs` | ~700 | debug/INFO data sources |
| `operations/` | ~250 | generic `PhaseResult`/`Operation` state-machine helper |
| `cli.rs` / `main.rs` / `lib.rs` | 99 / 145 / 45 | startup wiring |
| (read-only, owned by another agent) `crates/config/` | — | `admin.rs`, `http.rs`, `params.rs` registry |

Depth classes for the area (from `target/llvm-cov/depth/depth.json`, 1789 functions):
537 `untested`, 379 `monoculture`, 356 `well-covered`, 314 `single-test`, 203 `covered`.
Worst concentrations: `admin/handlers.rs` 38/50 fns `untested`; `config/loader.rs` 50
`untested` (`Config::load` itself is `single-test` at **14/176 regions**); `observability_server.rs`
54 `untested`; `main.rs` 0/79 regions.

## Summary

Configuration is the area's real risk, and it splits three ways. (1) **Load**: `Config::load`
is exercised by one test that never asserts precedence; it contains a live asymmetry —
an explicitly-passed config file is merged plainly, but an implicitly-discovered `./frogdb.toml`
is merged with figment's `.nested()`, which reinterprets top-level tables as *profiles*, so the
operator's file is silently dropped while `config_source_path` is still set from it. (2) **Mutate**:
the 26 newly live-mutable params mostly *do* have runtime-effect tests, but every one of them is a
`ConfigManager` unit test with an injected collaborator; nothing asserts the real `Server` ever
calls the corresponding `set_*` publisher, so a param can be provably live in tests and inert in
production. (3) **Persist**: the `"00"`-instead-of-`"0"` class of bug is *not* generically guarded —
`ROUNDTRIP_SETS` pins 12 of 118 registry params, and no test restarts a server from a rewritten
file. Separately, the HTTP `/admin/*` surface (shutdown, transfer-leader, cluster/role/nodes) has
**zero** tests of its bearer-token gate, and the token defaults to `None` while `http.enabled`
defaults to `true` — so today a regression that removed the gate entirely would be invisible.
Finally, INFO's Server/Stats sections still emit fabricated constants (`tcp_port: 6379`,
`uptime_in_seconds: 0`, `total_connections_received: 1`) and there is no `cluster_enabled` field at
all, which clients use for mode detection.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `runtime_config.rs` inline (67 tests) | CONFIG GET/SET/REWRITE, registry golden snapshot (118 rows), live-effect for cluster flags, status thresholds, tracing sampler, latency bands, hotshards, snapshot interval, WAL batch size, replication lag/self-fence, TLS identity/rotation/ciphersuites | genuinely asserts *runtime effect* via injected collaborators; `tls_identity_sets_are_atomic_against_a_mismatched_pair` is a model test | every live-effect test uses a hand-built `ConfigManager`; no test proves the real `Server` wires the publisher. `ROUNDTRIP_SETS` = 12/118 params |
| `config_persister.rs` inline (9 tests) | typed-value preservation, key removal (`None` ⇒ absent), comment/format preservation, invalid TOML, temp-file cleanup | pure, fast, right boundary; directly guards the `"00"` bug at the *merge* layer | no parent-directory fsync assertion; no crash-during-rename simulation |
| `config/loader.rs` inline (3 tests) | `default_toml()` round-trips to `Config::default()`, section presence | catches drift between `default_toml` and the struct | never calls `validate()` on the generated TOML; **no test of precedence, env mangling, CLI overrides, or the `.nested()` branch** |
| `config/mod.rs` inline | ~12 `validate()` rejection cases | good negative coverage of individual validators | never reached through `Config::load` |
| `tests/integration_admin.rs` (1918 L) | slowlog, bgsave/lastsave, memory, latency, CONFIG SET, DEBUG subcommands, CONFIG REWRITE (L1654–1918) | broad | REWRITE assertions are weak ("file not empty", "contains `[server]`"); no server is ever restarted from the rewritten file |
| `tests/integration_admin_port.rs` (259 L, 9 tests) | NOADMIN blocking on regular port, admin-port allow, disabled-by-default back-compat, shared dataset across ports | exactly the right boundary for the port split | no NOAUTH-under-`requirepass` on the admin port, no ACL, no test of the documented rate-limit/maxclients bypass, no admin-bind isolation |
| `tests/integration_info.rs` (386 L) | section order + CRLF framing, `INFO all`, section filters, fleet keyspace vs DBSIZE, hits/misses from real counters, persistence honest-absence, errorstats | post-round-1 this is strong on *derived* fields | Server/Clients/Memory/Stats field accuracy untested; no `cluster_enabled` |
| `tests/integration_debug_http.rs` (1069 L, 34 tests) | `/debug` UI, assets, JSON APIs, partials, bundles over `reqwest` | proves the HTTP stack boots and serves | **zero** requests to `/admin/*`; zero 401 assertions; token never configured |
| `tests/integration_metrics.rs`, `integration_debug_bundle.rs`, `integration_debug_introspection.rs` | `/metrics` scrape, bundle contents, DEBUG introspection | bundle correctly excludes config (no secret-leak finding) | public-vs-protected route partition unasserted |
| `crates/telemetry/tests/` | exporter/tracer unit behaviour | — | not a server surface |
| `connection/guards.rs` inline | `test_admin_port_gate_rejects_admin_command_on_regular_port` | pins the NOADMIN branch | `check_rate_limit`'s `is_admin` early-return untested |

Note: `crates/server/tests/integration_errorstats.rs` **does not exist** — errorstats tests live in
`integration_info.rs` and `redis-regression/tests/{info_tcl,introspection2_tcl}.rs`.

## Findings

### F1: an implicitly-discovered `./frogdb.toml` is merged with `.nested()` and is silently ignored
- **Severity** 5 — top-level tables become figment *profiles*, not sections, so the operator's entire
  config file has no effect: the node boots on defaults (persistence/AOF/maxmemory/bind all default)
  with no error and no warning. Durability settings silently not applied is a data-loss path.
- **Likelihood** 4 — running `frogdb-server` with no `-c` in a directory containing `frogdb.toml`
  is the documented default discovery path (the `else` branch exists precisely for it).
- **Effort** 2 — crate-level test: tempdir + `Config::load(Some(path))` vs the discovery branch,
  compare extracted values. (Refactoring discovery to take a base dir avoids `set_current_dir` races.)
- **Priority** 21
- **Evidence**: `config/loader.rs:87` `figment.merge(Toml::file(path))` vs `config/loader.rs:91`
  `figment.merge(Toml::file(default_path).nested())` — the two branches use different figment
  semantics for the same file. `config/loader.rs:193–202` then sets `config_source_path` from that
  same discovered file, so CONFIG REWRITE will *write into* a file whose contents were never read.
  `Config::load` is `single-test`, **14/176 regions** — neither branch is asserted.
- **Proposed test**: write `frogdb.toml` containing `[server]\nport = 7777\n[persistence]\naof_enabled = true`;
  load it (a) explicitly and (b) via discovery; assert both yield `port == 7777` and
  `aof_enabled == true`, and that `config_source_path` is `Some(canonicalized path)` in both.
- **Boundary**: 2 (crate-level API test on `Config::load`) — the behaviour is entirely in figment
  merge semantics; a server boot would add nothing but seconds.

### F2: the HTTP `/admin/*` surface and its bearer-token gate have zero test coverage
- **Severity** 5 — this is the auth boundary for `POST /admin/shutdown`, `POST /admin/transfer-leader`,
  `/admin/cluster`, `/admin/role`, `/admin/nodes`. A regression that drops the layer, or reorders
  the router so a route lands outside the protected sub-router, is an unauthenticated admin-op
  bypass — exactly the severity-5 case called out in the dispatch.
- **Likelihood** 4 — `http.enabled` defaults to `true` and `http.token` defaults to `None`, in which
  case the middleware explicitly allows everything; an operator who moves `http.bind` to `0.0.0.0`
  for Prometheus scraping (see F6) exposes the whole admin surface. Route lists are edited often.
- **Effort** 3 — server integration test; `integration_debug_http.rs` already has the `reqwest` pattern.
- **Priority** 20
- **Evidence**: `observability_server.rs:248–270` — protected router layered with
  `bearer_auth_middleware`; `Some(val) if val.as_bytes() == expected_header.as_bytes()` else
  `StatusCode::UNAUTHORIZED`, with `// No token configured — allow all` as the `else`.
  Depth: `bearer_auth_middleware` `untested`; of `admin/handlers.rs` only `health` is `single-test`
  — `cluster_state` 0/20 regions, `role` 0/27, `nodes` 0/29, `upgrade_status`/`shutdown`/
  `transfer_leader` all 0. `integration_debug_http.rs` (34 tests) never issues an `/admin/*` request.
- **Proposed test**: boot with `http.token = Some("t")`; assert (a) every protected path returns 401
  with no header, with a wrong token, and with `Basic` instead of `Bearer`; (b) each returns 200
  with the correct token; (c) `/metrics`, `/health/live`, `/health/ready`, `/healthz`, `/readyz`,
  `/status/json` return 200 *without* a token. Drive the path list from a single const shared with
  `create_router` so a new route added outside the protected group fails the test.
- **Boundary**: 4 (server integration over HTTP) — the behaviour *is* the axum layer/route
  partition; nothing below the router can observe it.

### F3: `Config::load` precedence, env-var mangling, and refusal-to-start are unasserted
- **Severity** 4 — silent misconfiguration: an env var that is mangled wrong is silently dropped and
  the node runs on a default the operator believes they overrode (`maxmemory`, `bind`, TLS paths).
  The refuse-to-start path (`config.validate()?`) is the only thing preventing a node from serving
  with an invalid TLS/persistence config.
- **Likelihood** 4 — container deployments configure exclusively by `FROGDB_*` env vars.
- **Effort** 2 — crate-level, using `figment::Jail` (already a dev-dep of figment) or scoped env.
- **Priority** 18
- **Evidence**: `config/loader.rs:99–104` — the `__`→`\x00`→`__`, `_`→`-` dance is bespoke and
  untested; `config/loader.rs:193` `config.validate()?` is on the boot path but never reached from
  a test. `Config::load` `single-test`, 14/176 regions; `main.rs` 0/79 regions.
- **Proposed test**: table-driven — for a representative param in each section assert
  defaults < TOML < `FROGDB_SECTION__FIELD_NAME` env < CLI override, each layer overriding the
  previous; assert `FROGDB_SERVER__NUM_SHARDS=4` reaches `server.num_shards`; assert a config with
  `tls.enabled = true` and a nonexistent `cert-file` returns `Err` from `load` (not a booted server);
  assert `-c /nonexistent` errors with "config file not found".
- **Boundary**: 2 (crate-level API test) — precedence is a pure function of the figment stack.

### F4: `SO_REUSEPORT` is release-only, so production socket behaviour is structurally untestable
- **Severity** 5 — in a release build two `frogdb-server` processes bind the *same* port
  successfully and the kernel load-balances connections between two independent datasets: writes
  land in one, reads in the other, silently. No test can ever observe this because tests compile
  with `debug_assertions`.
- **Likelihood** 3 — rolling upgrade / restart-before-shutdown-completed / a stale process left
  running by a supervisor. The doc comment says this is *intentional* for hot restart, but no
  handoff protocol exists to make it safe.
- **Effort** 3 — requires making the socket option explicit (a config knob or a `ListenerOpts`
  seam) before any test can cover both modes.
- **Priority** 18
- **Evidence**: `net.rs:50–81` —
  ```rust
  socket.set_reuse_address(true)?;
  #[cfg(not(debug_assertions))]
  socket.set_reuse_port(true)?;
  ```
  with the comment "In debug builds (including tests), SO_REUSEPORT is disabled to prevent the OS
  from assigning the same ephemeral port to concurrent test servers." `server/listeners.rs:26–125`
  binds every listener (RESP, admin, cluster bus, TLS) through this function.
- **Proposed test**: (1) unit test on a `listener_opts(&Config) -> ListenerOpts` seam asserting
  `reuse_port` follows an explicit config field, not the build profile; (2) integration test with
  the flag off: a second server on the same port fails to start with `EADDRINUSE`; (3) with it on:
  both bind — documenting the hot-restart contract explicitly.
- **Boundary**: 1 for the seam + 4 for the bind behaviour — the option must become data before
  either level can see it.
- **OPTIONS**:
  - *(a) Keep `cfg(not(debug_assertions))`, add no test.* Zero cost; leaves a production-only
    behaviour permanently unverified. Not recommended.
  - *(b) Promote to a config field (`server.reuse-port`, default `false`) and test both values.*
    Test and production agree; costs one new param (registry + golden snapshot churn) and changes
    default production behaviour — arguably a *fix*, since silent port sharing has no handoff protocol.
  - *(c) Keep the cfg but add a release-only `#[cfg(not(debug_assertions))] #[test]` run under a
    dedicated `cargo test --release` CI job.* No behaviour change, but the suite is normally run in
    debug so the test rarely executes.
  - **Recommendation**: (b). This is partly a design finding; flag it to the user as such.

### F5: CONFIG REWRITE fidelity is pinned for 12 of 118 registry params — the `"00"` bug class is not generically guarded
- **Severity** 4 — a param whose runtime rendering does not re-parse to the same value corrupts the
  config file; the node then either refuses to start or starts with a different value than it had.
  This has already happened once (`"00"` for `"0"`).
- **Likelihood** 4 — CONFIG SET + CONFIG REWRITE is normal operation, and the registry grew by 26
  live-mutable params this month; each new one is unguarded by default.
- **Effort** 2 — the machinery exists (`rewrite_and_reparse` at `runtime_config.rs:5229`); the test
  needs to be driven from `config_param_registry()` instead of a hand-written list.
- **Priority** 18
- **Evidence**: `runtime_config.rs:5259` `const ROUNDTRIP_SETS: &[(&str, &str)]` — 12 entries;
  `runtime_config.rs:5283` iterates only those. `config_param_registry()` has 118 rows pinned by
  golden snapshot. Nothing forces a new param into `ROUNDTRIP_SETS`.
- **Proposed test**: for *every* mutable, non-noop row in `config_param_registry()`: read the
  current value via `getter`, CONFIG SET it back to itself, rewrite, re-parse the file into a
  `Config`, and assert `getter` on the reloaded manager returns byte-identical output — plus
  `validate()` passes. Then repeat with a per-type non-default probe value (`0`/`1` for ints,
  `no`/`yes` for bools, a temp path for path params) drawn from the param's own type, with an
  explicit allowlist for params that legitimately cannot round-trip. A `#[test]` asserting the
  allowlist is a subset of the registry keeps it honest.
- **Boundary**: 1 (pure unit test over the registry + `ConfigPersister`) — no server needed; the
  bug lives entirely in render→parse. This is the single highest-leverage test in the area.

### F6: `http.bind = 0.0.0.0` with **no** token produces no warning and no refusal
- **Severity** 4 — publishes `/debug/*` (introspection, bundles) and `/admin/*` (shutdown,
  transfer-leader) to the network with no authentication whatsoever.
- **Likelihood** 3 — binding the metrics port to `0.0.0.0` for Prometheus scraping is the obvious
  and common operator action; `http.enabled` defaults to `true`.
- **Effort** 1 — pure unit test on `HttpConfig::validate()`.
- **Priority** 17
- **Evidence**: `crates/config/src/http.rs` — defaults `enabled: true`, `bind: "127.0.0.1"`,
  `port: 9090`, `token: None`; `validate()` warns only in the `token.is_some() && bind == "0.0.0.0"`
  case (i.e. warns when a token *is* configured), and says nothing when there is no token. The 5
  existing unit tests do not cover the no-token case.
- **Proposed test**: assert `validate()` errors (or at minimum emits a warning captured via a
  `tracing` subscriber) for `bind = "0.0.0.0"` + `token = None`; assert it is silent for
  `127.0.0.1` + `None`. Whether this should be a hard refusal is a product decision — flag it.
- **Boundary**: 1 (pure unit test) — `validate()` is a pure function of the struct.
- **Cross-area**: `crates/config/` is owned by another agent; coordinate so this lands once.

### F7: shutdown aborts acceptors **last** and never drains in-flight connections
- **Severity** 4 — shards are shut down before listeners stop accepting, so a command that arrives
  during teardown is dispatched into a dead shard. Per-connection tasks are untracked and simply
  abandoned, so a client can have a request in flight that is neither applied nor errored.
- **Likelihood** 4 — every SIGTERM: rolling restarts, k8s pod eviction, upgrades.
- **Effort** 3 — server integration with a concurrent client.
- **Priority** 17
- **Evidence**: `server/subsystems.rs:664` `shutdown_subsystems` — order is health_checker →
  `ShardMessage::Shutdown` to all shards → await shard supervisor → abort periodic sync/snapshot →
  wait for in-progress snapshot → abort http/system_collector/cluster_bus → abort replica tasks →
  abort failure detector → tracer → persist replication offset → RocksDB flush → **then**
  `handles.acceptor.abort()` at `server/subsystems.rs:767` (plus `admin_acceptor`, `tls_acceptor`,
  `cert_watcher`). No `JoinSet`/registry of connection tasks exists to drain.
- **Proposed test**: drive a steady write load from N clients; trigger shutdown; assert every write
  that received `+OK` is present after restart, and that every write issued after the shutdown
  signal either got `+OK` *and* is present, or got an error/disconnect — never `+OK` and absent.
  A second, cheaper test: assert the listener stops accepting *before* shards shut down (connect
  after the signal ⇒ connection refused, not accept-then-hang).
- **Boundary**: 4 (server integration with the existing harness) — ordering across subsystems is
  only observable from outside the process.

### F8: admin-port authn/authz differences from the data port are untested
- **Severity** 5 — if the admin port ever skipped `requirepass`/ACL, every admin command would be
  reachable unauthenticated. Today `state.is_authenticated()` *is* enforced (verified by reading
  `connection/guards.rs:256` `run_pre_checks`: auth → replica READONLY → CLUSTERDOWN → NOREPLICAS →
  NOADMIN → ACL → pub/sub), so this is regression protection, not a live bug.
- **Likelihood** 2 — requires `admin.enabled` (defaults `false`) plus `requirepass`/ACL.
- **Effort** 3 — extends `integration_admin_port.rs`, which already boots dual-port servers.
- **Priority** 16
- **Evidence**: `integration_admin_port.rs` (9 tests) covers NOADMIN blocking/allowing and shared
  data, but never sets `requirepass` or an ACL user. `connection/guards.rs:123` `check_rate_limit`
  returns `None` early when `self.is_admin` — a *documented* rate-limit bypass on the admin port,
  with no test. `crates/config/src/admin.rs` `validate()` only rejects port 0.
- **Proposed test**: with `requirepass` set, assert an unauthenticated admin-port connection gets
  `NOAUTH` for `DEBUG`/`CONFIG SET`/`SHUTDOWN`, and succeeds after `AUTH`; with an ACL user lacking
  `+@admin`, assert `NOPERM` on the admin port; assert that saturating the data port's rate limit
  still leaves the admin port responsive (the documented bypass — a lockout-escape guarantee);
  assert binding admin to `127.0.0.1` while `server.bind` is `0.0.0.0` leaves the admin port
  unreachable from a non-loopback address.
- **Boundary**: 4 (server integration over RESP) — the gate depends on which listener accepted the
  connection, which only exists at the socket level.

### F9: CONFIG REWRITE is never followed by a real restart, and its integration assertions are weak
- **Severity** 4 — the operator-visible contract of REWRITE is "the node comes back the same".
  Today nothing tests that.
- **Likelihood** 3 — CONFIG SET + REWRITE + later restart is the standard tuning workflow.
- **Effort** 3 — server integration; the harness supports restart-with-same-dir.
- **Priority** 15
- **Evidence**: `integration_admin.rs:1654–1918` — REWRITE tests assert the file is non-empty and
  contains `[server]`. `runtime_config.rs:5229` `rewrite_and_reparse` re-parses and boot-validates
  but never starts a server. Two near-duplicate tests exist:
  `test_rewrite_config_output_is_valid_toml` and `test_rewrite_config_output_is_valid_toml_value`.
- **Proposed test**: boot from a hand-written TOML containing comments and a non-default value;
  `CONFIG SET` 3–4 params of different types (int, bool, string, optional-path-cleared-to-empty);
  `CONFIG REWRITE`; restart the server from the same file; assert `CONFIG GET` returns the new
  values, the untouched hand-written value and its comment survive, and the cleared optional key is
  **absent** (not `""`). Also assert REWRITE with no `config_source_path` returns the documented error.
- **Boundary**: 4 (server integration with restart) — only a real boot proves the file is loadable;
  F5 covers the pure-rendering half at level 1, so this test can stay small.

### F10: live-mutable params are proven live against injected collaborators, never against the real `Server`
- **Severity** 3 — `CONFIG SET` returns `+OK` and `CONFIG GET` reflects the new value while the
  running system keeps the old one. The operator believes a limit/threshold was applied.
- **Likelihood** 4 — the publication seams are "publish once, second call ignored"; a startup path
  that forgets one (or orders it after first use) silently disables live mutation for that param.
- **Effort** 3 — needs a real `Server` plus an observable side effect per param.
- **Priority** 14
- **Evidence**: `runtime_config.rs` publication setters `set_snapshot_coordinator`,
  `set_replication_lag_thresholds`, `set_replication_self_fence`, `set_log_reload_handle`,
  `set_tls_runtime`, `set_config_file_path` — each documented "called at most once; a second call is
  ignored". Production call sites: `server/init.rs:256,328,452`, `server/mod.rs:283,288`,
  `server/cluster_init.rs:116`, `role_manager.rs:685`. Every live-effect test
  (`snapshot_interval_set_reaches_the_published_coordinator`,
  `self_fence_sets_reach_the_published_quorum_checker`, ...) builds its own `ConfigManager` and
  calls the setter itself, so none of them can fail if a production call site is deleted.
- **Proposed test**: a single "publication completeness" test on a booted `Server`: assert every
  `set_*` seam has been populated (an `is_published()` accessor per seam, or one bitmask), and pair
  it with 3–4 end-to-end spot checks over RESP where the effect is externally observable —
  e.g. `CONFIG SET tracing-sampling-rate 0` then assert no spans are emitted; `CONFIG SET
  status-memory-warning-percent 1` then assert `/status/json` reports a warning;
  `CONFIG SET snapshot-interval-secs 1` then assert a snapshot file appears.
- **Boundary**: 4 (server integration) — the gap is precisely the wiring between `Server::new` and
  `ConfigManager`, which no lower level contains. Keep the per-param semantics at the existing unit
  level; only add coverage of the *wiring*.

### F11: INFO `Server`/`Stats`/`Memory` sections report fabricated constants
- **Severity** 2 — wrong INFO fields; monitoring dashboards, client libraries, and ops runbooks
  read these. `tcp_port` in particular is used by tooling to reconnect.
- **Likelihood** 5 — every `INFO` call on default config returns them.
- **Effort** 3 — server integration; `integration_info.rs` already has the plumbing.
- **Priority** 13
- **Evidence**: `info/sections.rs:58` `redis_mode = "standalone"` (hardcoded, although
  `src.cluster_state()` is available in the same function), `:75` `run_id =
  "frogdb0000000000000000000000000000000000"`, `:76` `tcp_port = 6379` (ignores the actual bound
  port), `:78` `uptime_in_seconds = 0`, `:83` `executable = "/usr/local/bin/frogdb"`, `:84`
  `config_file = ""`, `:85` `io_threads_active = 0`; `:293` `total_connections_received = 1`
  (constant); `:159` `used_memory_rss = used`. `integration_info.rs` asserts derived fields
  (keyspace, hits/misses, errorstats) but never these.
- **Proposed test**: boot on an ephemeral port with an explicit config file; assert `tcp_port`
  equals the real bound port, `config_file` equals the resolved path, `uptime_in_seconds` is ≥1
  after a 1s sleep and monotonic across two INFO calls, `run_id` is 40 hex chars and *differs*
  between two servers, and `total_connections_received` increases when a second client connects.
  Per the accuracy rule, fields that cannot be computed should be *omitted*, not faked — assert
  absence for those rather than pinning a lie.
- **Boundary**: 4 (server integration over RESP) — the values are properties of a running process.

### F12: `cluster_enabled` / an INFO `Cluster` section does not exist
- **Severity** 3 — many clients (and `redis-cli --cluster`) probe `INFO cluster` /
  `cluster_enabled:1` to decide whether to use cluster routing; its absence makes a cluster-mode
  node look standalone.
- **Likelihood** 3 — anyone running cluster mode with a stock client.
- **Effort** 3 — server integration on a cluster-enabled node.
- **Priority** 12
- **Evidence**: `info/sections.rs` `all_sections()` = Server, Clients, Memory, Persistence, Stats,
  Replication, CPU, Keyspace, Ratelimit, Commandstats, Errorstats, Latencystats, Latency_Baseline,
  Tiered, Keysizes — no Cluster; `grep cluster_enabled` over `info/` returns nothing.
- **Proposed test**: on a cluster-enabled node assert `INFO cluster` yields `cluster_enabled:1` and
  `redis_mode:cluster`; on standalone assert `cluster_enabled:0` and `redis_mode:standalone`.
- **Boundary**: 4 for the cluster case (needs a cluster-enabled boot), 3 for the standalone half.
- **Cross-area**: overlaps the cluster agent; the *rendering* is ours, the mode source is theirs.

### F13: `default_toml()` is never boot-validated
- **Severity** 3 — the file shipped to every new operator (and written by packaging) could fail
  `validate()` or fail to load, producing a first-run crash.
- **Likelihood** 3 — every fresh install; drift happens whenever a validator is tightened.
- **Effort** 1 — pure unit test, reuses the existing round-trip test's parse.
- **Priority** 14
- **Evidence**: `config/loader.rs` tests `default_toml_round_trips_to_config_default` and
  `default_toml_contains_every_config_section` parse and compare structurally but never call
  `validate()`; `Config::default().validate()` is likewise not asserted anywhere on this path.
- **Proposed test**: `default_toml()` → parse → `validate()` is `Ok`; and
  `Config::default().validate()` is `Ok`. Additionally assert every section named in
  `config_param_registry()`'s `section` column appears in `default_toml()`, so a new param cannot
  land without a documented default.
- **Boundary**: 1 (pure unit test) — no IO, no server.

### F14: CLI surface (`cli.rs`, `main.rs`) is untested, including override semantics with side effects
- **Severity** 3 — a clap definition conflict panics at startup for *all* invocations; and
  `--admin-port` silently *enables* the admin port while `--admin-bind` does not, which is a
  security-relevant asymmetry (an operator passing only `--admin-bind 0.0.0.0` gets no admin port;
  one passing only `--admin-port` gets one they may not have intended).
- **Likelihood** 3 — every process start goes through this; clap conflicts are a known footgun.
- **Effort** 1 — `Cli::command().debug_assert()` plus `Config::load` calls with override args.
- **Priority** 14
- **Evidence**: `cli.rs` (99 L) has no `#[cfg(test)]`; `main.rs` 0/79 regions;
  `config/loader.rs:144–147` `if let Some(port) = admin_port { config.admin.enabled = true; ... }`
  with no equivalent in the `admin_bind` branch at `:148`.
- **Proposed test**: `Cli::command().debug_assert()`; then assert `--admin-port 6382` sets
  `admin.enabled = true`, `--admin-bind` alone leaves it `false`, `--shards auto` resolves to
  `available_parallelism()`, `--shards notanumber` errors, and each TLS override lands on the right
  field (including the `--tls-require-client-cert` string→enum mapping and its error case).
- **Boundary**: 1/2 — `debug_assert` is pure; the override assertions are crate-level on `Config::load`.

### F15: `ConfigPersister::atomic_write` does not fsync the parent directory after rename
- **Severity** 3 — after `CONFIG REWRITE` returns `+OK`, a machine crash can leave the rename
  unpersisted, so the node restarts with the *old* config (or, on some filesystems, no config file
  at all if the old inode was replaced). The operator believes the change is durable.
- **Likelihood** 2 — needs a crash within the writeback window after a REWRITE.
- **Effort** 2 — assert the syscall via a seam, or assert durability with a filesystem-level harness.
- **Priority** 11
- **Evidence**: `config_persister.rs:98–128` — `file.sync_all()` on the temp file, then
  `std::fs::rename`, then `Ok(())`. No `File::open(parent)?.sync_all()`.
- **Proposed test**: the honest unit-level assertion is hard without a syscall seam; the practical
  version is a `Durability` trait with a test double that records `{write, fsync(file),
  rename, fsync(dir)}` and asserts the exact sequence. That double also makes the
  rename-fails/write-fails cleanup paths (currently untested `map_err` arms) assertable.
- **Boundary**: 1 (pure unit test against an injected IO seam) — real crash testing is out of
  proportion for a config file.

### F16: `POST /admin/shutdown` is permanently inert
- **Severity** 2 — the endpoint always fails, but visibly (503); no silent corruption. It is a bug,
  not just a test gap, and F2's test would have caught it on day one.
- **Likelihood** 3 — anyone using the documented REST admin API to drain a node.
- **Effort** 3 — folded into F2's integration test.
- **Priority** 9
- **Evidence**: `server/subsystems.rs:253` `shutdown_tx: None, // TODO: wire up shutdown channel
  from Server`. Also at `admin/handlers.rs:113,115`: `state: Some("ok") // TODO: Determine actual
  cluster state` and `slots_ok: slots_assigned // TODO: Track unhealthy slots` — `/admin/cluster`
  reports "ok" unconditionally, so a health check built on it can never fail.
  `admin/handlers.rs:413` `transfer_leader` returns HTTP 200 with `{"status":"error"}` in its body.
- **Proposed test**: assert `POST /admin/shutdown` with a valid token actually terminates the
  server (subsequent RESP `PING` fails); assert `/admin/cluster` reports a non-ok state when slots
  are unassigned; assert `transfer_leader` returns a 4xx/5xx status, not 200, on failure.
- **Boundary**: 4 (server integration over HTTP) — same test file as F2.

### F17: the debug UI's config panel shows a frozen 5-entry snapshot
- **Severity** 1 — cosmetic/misleading operator display; no data effect.
- **Likelihood** 3 — visible to anyone using `/debug` after a `CONFIG SET`.
- **Effort** 2 — HTTP integration assertion.
- **Priority** 7
- **Evidence**: `server/subsystems.rs:175` `config_entries` builds a fixed 5-entry vector (bind,
  port, num_shards, http_bind, http_port) at construction time and never re-reads `ConfigManager`.
- **Proposed test**: `CONFIG SET` a displayed param, then assert the `/debug` config partial shows
  the new value. Better: source the panel from `config_param_registry()` so the test is one
  assertion over the whole panel.
- **Boundary**: 4 (HTTP integration) — it is a rendering concern of the debug server.

## Deprioritised

- **Constant-time comparison for the bearer token** (`observability_server.rs:264` uses `==` on
  bytes). Real but requires an adversarial local-network timing attack against a token that is
  usually absent; Likelihood 1. Worth fixing (use `subtle`), not worth a test.
- **`init_logging_inner` 0/104 regions.** Asserting on tracing subscriber construction requires a
  capture layer and mostly pins implementation detail; the observable part (log level changes via
  `CONFIG SET loglevel`) is already covered through the `set_log_reload_handle` seam.
- **`operations/` (`PhaseResult`/`Operation`).** A thin generic helper; its real behaviour is
  exercised through MIGRATE/COPY, which belong to the slot-migration agent. Testing the generic in
  isolation would assert almost nothing.
- **Duplicate tests `test_rewrite_config_output_is_valid_toml` and `..._value`** in
  `runtime_config.rs` — near-identical; collapse into one when F5 lands. Cleanup, not a gap.
- **`/metrics` and `/status/json` being unauthenticated.** Deliberate (Prometheus scraping) and
  matches Redis-exporter conventions; the payload contains counts, not key data. F6 covers the
  network-exposure half.
- **Debug bundle secret leakage.** Checked: the bundle does not collect config, so `requirepass`,
  `http.token`, and TLS key paths are not included. No finding.
- **`server_observability.rs` / `debug_providers.rs` per-provider unit tests.** Mostly thin
  adapters over already-tested collectors; effort is high relative to a Severity-2 payoff. The
  accuracy issues that matter surface as INFO fields (F11).

## Cross-area notes

- **`crates/config/`** (owned by another agent): F6 (`HttpConfig::validate` no-token +
  `0.0.0.0`) and the `AdminConfig::validate` weakness noted in F8 are theirs to implement; F5's
  registry-wide round-trip test consumes `config_param_registry()` from that crate but should live
  next to `ConfigPersister`. Coordinate so F6 lands once.
- **Cluster agent**: F12 (`cluster_enabled` / `redis_mode`) needs the cluster-mode boot; the INFO
  rendering side is ours. `admin/handlers.rs` `cluster_state`/`role`/`nodes` (0 regions covered)
  read cluster state — their *correctness* is cluster-owned, their *HTTP contract* is ours.
- **Networking agent**: F4 (`SO_REUSEPORT`) lives in `net.rs`, which is their file, but it surfaces
  as a startup/bind-safety property. It should be assigned once, to whoever owns `net.rs`.
- **Shared infrastructure requested**:
  1. an `is_published()`-style accessor (or a single publication bitmask) on `ConfigManager` so F10
     can assert wiring completeness without reflection;
  2. a small IO seam for `ConfigPersister` (F15) that also unlocks the untested error arms;
  3. a shared const list of protected-vs-public HTTP routes, exported from
     `observability_server.rs`, so F2's test fails when a route is added outside the guarded group;
  4. a `TestServer` restart-in-place helper (F9) if one does not already exist.
- **Environment quirk (not a finding)**: in this shell, `rg` output silently strips the literal
  substring `admi` (`is_admin` printed as `is_n`). All greps in this audit were done with
  `grep -n`. Other agents auditing admin code should be warned.

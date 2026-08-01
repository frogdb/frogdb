# server admin / config / INFO — residual test gaps (13 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/05 — residual findings after promotion to issues 19–76
Score: 13 findings, priority range 7–18
Area: `frogdb-server/crates/server/src/` — `runtime_config.rs`, `info/`, `observability_server.rs`, `admin/`, `config/`, `config_persister.rs`, `debug_providers.rs`, `operations/`, `cli.rs`/`main.rs`

## Context

This area is the operator surface: configuration load/mutate/persist, the INFO sections, the
observability HTTP server and its `/admin/*` handlers, and the CLI/startup wiring — ~10.6k
LOC of in-scope source. Depth classes over the area's 1789 functions (from
`target/llvm-cov/depth/depth.json`) are 537 `untested`, 379 `monoculture`, 356
`well-covered`, 314 `single-test`, 203 `covered`, with the worst concentrations in
`admin/handlers.rs` (38/50 functions `untested`), `config/loader.rs` (50 `untested`;
`Config::load` itself is `single-test` at **14/176 regions**), `observability_server.rs` (54
`untested`) and `main.rs` (0/79 regions). The proposal's verdict on the shape of that
coverage: **configuration is the area's real risk, and it splits three ways** — load
(`Config::load` exercised by one test that never asserts precedence), mutate (26 live-mutable
params proven live only against injected collaborators, never against the real `Server`), and
persist (the `"00"`-instead-of-`"0"` bug class is not generically guarded; `ROUNDTRIP_SETS`
pins 12 of 118 registry params and no test restarts a server from a rewritten file).

## Promoted elsewhere

- F1 → issue 49, `.scratch/testing-improvements-round2/issues/` (operator config silently ignored — an implicitly-discovered `./frogdb.toml` is merged with figment `.nested()`)
- F2 → issue 40, `.scratch/testing-improvements-round2/issues/` (admin HTTP bearer gate untested, default-open)
- F6 → issue 40, `.scratch/testing-improvements-round2/issues/` (same defect — `http.bind = 0.0.0.0` with no token produces neither warning nor refusal)
- F10 → issue 21, `.scratch/testing-improvements-round2/issues/` (theme T3 — config that parses, sets, and does nothing)

## Residual findings

### F3 — `Config::load` precedence, env-var mangling, and refusal-to-start are unasserted

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

### F4 — `SO_REUSEPORT` is release-only, so production socket behaviour is structurally untestable

**BLOCKED on an unmade semantics decision** — `MASTER.md` §7 lists the `SO_REUSEPORT`
release-only gate as a decision that must be settled before this test can assert anything;
the OPTIONS block below is the decision, and the proposal's recommendation is (b).

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
- **Cross-area** (from proposal 05): F4 lives in `net.rs`, which belongs to the networking agent,
  but it surfaces as a startup/bind-safety property. It should be assigned once, to whoever owns
  `net.rs`.

### F5 — CONFIG REWRITE fidelity is pinned for 12 of 118 registry params — the `"00"` bug class is not generically guarded

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

### F7 — shutdown aborts acceptors **last** and never drains in-flight connections

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

### F8 — admin-port authn/authz differences from the data port are untested

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

### F9 — CONFIG REWRITE is never followed by a real restart, and its integration assertions are weak

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

### F13 — `default_toml()` is never boot-validated

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

### F14 — CLI surface (`cli.rs`, `main.rs`) is untested, including override semantics with side effects

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

### F11 — INFO `Server`/`Stats`/`Memory` sections report fabricated constants

**BLOCKED on an unmade semantics decision** — `MASTER.md` §7 lists "INFO fields that are
currently fabricated constants — omit rather than fake" as a decision that must be settled
before the test can assert anything: the test must know whether an uncomputable field is
expected to be absent or to carry a pinned placeholder.

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

### F12 — `cluster_enabled` / an INFO `Cluster` section does not exist

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

### F15 — `ConfigPersister::atomic_write` does not fsync the parent directory after rename

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

### F16 — `POST /admin/shutdown` is permanently inert

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

Note: F2 is owned by issue 40, `.scratch/testing-improvements-round2/issues/`; F16's test
belongs in the same file, so land it with that issue's integration test rather than building a
second harness.

### F17 — the debug UI's config panel shows a frozen 5-entry snapshot

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

## Acceptance criteria

- [ ] F3: a table-driven test asserts, for a representative param in each config section, that defaults < TOML < `FROGDB_SECTION__FIELD_NAME` env < CLI override each override the previous; that `FROGDB_SERVER__NUM_SHARDS=4` reaches `server.num_shards`; that `tls.enabled = true` with a nonexistent `cert-file` returns `Err` from `Config::load`; and that `-c /nonexistent` errors with "config file not found".
- [ ] F4: a unit test asserts `reuse_port` follows an explicit config field rather than the build profile, plus integration assertions that a second server on the same port fails with `EADDRINUSE` when it is off and that both bind when it is on. *(Blocked until the `SO_REUSEPORT` decision — `MASTER.md` §7 — is made; recommendation is OPTION (b).)*
- [ ] F5: a test iterates *every* mutable, non-noop row of `config_param_registry()`, CONFIG SETs the current value back, rewrites, re-parses into a `Config`, and asserts the reloaded `getter` output is byte-identical and `validate()` passes — repeated with a per-type non-default probe value — with an explicit allowlist, and a second test asserting the allowlist is a subset of the registry.
- [ ] F7: a test asserts that every write which received `+OK` before shutdown is present after restart and that no write ever gets `+OK` and is absent; plus a cheaper test asserting the listener stops accepting *before* shards shut down (post-signal connect ⇒ refused, not accept-then-hang).
- [ ] F8: a test asserts an unauthenticated admin-port connection gets `NOAUTH` for `DEBUG`/`CONFIG SET`/`SHUTDOWN` under `requirepass` and succeeds after `AUTH`; that an ACL user lacking `+@admin` gets `NOPERM` on the admin port; that saturating the data port's rate limit leaves the admin port responsive; and that admin bound to `127.0.0.1` is unreachable from a non-loopback address while `server.bind` is `0.0.0.0`.
- [ ] F9: a test boots from a hand-written TOML with comments and a non-default value, CONFIG SETs 3–4 params of different types, CONFIG REWRITEs, **restarts the server from the same file**, and asserts `CONFIG GET` returns the new values, the untouched hand-written value and its comment survive, and a cleared optional key is absent rather than `""`; plus REWRITE with no `config_source_path` returns the documented error.
- [ ] F13: a test asserts `default_toml()` parses and `validate()`s `Ok`, that `Config::default().validate()` is `Ok`, and that every section named in `config_param_registry()`'s `section` column appears in `default_toml()`.
- [ ] F14: a test calls `Cli::command().debug_assert()` and asserts `--admin-port 6382` sets `admin.enabled = true`, `--admin-bind` alone leaves it `false`, `--shards auto` resolves to `available_parallelism()`, `--shards notanumber` errors, and each TLS override lands on the right field including the `--tls-require-client-cert` string→enum mapping and its error case.
- [ ] F11: a test asserts INFO's `tcp_port` equals the real bound port, `config_file` equals the resolved path, `uptime_in_seconds` is ≥1 after a 1 s sleep and monotonic across two calls, `run_id` is 40 hex chars and differs between two servers, and `total_connections_received` increases with a second client — with uncomputable fields asserted *absent* rather than pinned. *(Blocked until the "omit rather than fake" decision — `MASTER.md` §7 — is made.)*
- [ ] F12: a test asserts `INFO cluster` yields `cluster_enabled:1` and `redis_mode:cluster` on a cluster-enabled node, and `cluster_enabled:0` with `redis_mode:standalone` on a standalone node.
- [ ] F15: a test asserts `ConfigPersister::atomic_write` performs exactly `{write, fsync(file), rename, fsync(dir)}` against an injected IO seam, and asserts the cleanup behaviour of the rename-fails and write-fails arms.
- [ ] F16: a test asserts `POST /admin/shutdown` with a valid token actually terminates the server (a subsequent RESP `PING` fails), that `/admin/cluster` reports a non-ok state when slots are unassigned, and that `transfer_leader` returns a 4xx/5xx status rather than 200 on failure.
- [ ] F17: a test `CONFIG SET`s a displayed param and asserts the `/debug` config partial shows the new value (ideally by sourcing the panel from `config_param_registry()`).

## Depends on

- issue 12, `.scratch/testing-improvements-round2/issues/` (I12 — config observability seams: item 2, the `ConfigPersister` IO seam, is what F15 asserts against and it also unlocks that file's untested error arms; item 4, a `TestServer` restart-in-place helper, is what F9's restart needs)

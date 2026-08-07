# Admin HTTP `/admin/*` bearer gate has zero coverage and is default-open

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/05 F2 + proposals/05 F6 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 3 · priority 20 (F2); severity 4 · likelihood 3 · effort 1 · priority 17 (F6)
Area: frogdb-server / observability HTTP + frogdb-config / HttpConfig

## Context

The bearer-token middleware guarding `POST /admin/shutdown`, `POST /admin/transfer-leader`,
`/admin/cluster`, `/admin/role` and `/admin/nodes` has no test at all, and its default path is
`// No token configured — allow all`. `http.enabled` defaults to `true` and `http.token` defaults
to `None`, so an operator who moves `http.bind` to `0.0.0.0` for Prometheus scraping publishes the
entire admin surface — plus `/debug/*` introspection and bundles — unauthenticated, with neither a
warning nor a refusal. A regression that drops the layer, or reorders the router so a route lands
outside the protected sub-router, would be an unauthenticated admin-op bypass that nothing detects.

**This is a suspected live defect found by reading, not by test failure — the proposed tests fail
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `observability_server.rs:248–270` — protected router layered with `bearer_auth_middleware`;
  `Some(val) if val.as_bytes() == expected_header.as_bytes()` else `StatusCode::UNAUTHORIZED`, with
  `// No token configured — allow all` as the `else`.
- Depth: `bearer_auth_middleware` `untested`; of `admin/handlers.rs` only `health` is
  `single-test` — `cluster_state` 0/20 regions, `role` 0/27, `nodes` 0/29,
  `upgrade_status`/`shutdown`/`transfer_leader` all 0.
- **Why nothing catches it**: `integration_debug_http.rs` (34 tests) never issues an `/admin/*`
  request.
- `crates/config/src/http.rs` — defaults `enabled: true`, `bind: "127.0.0.1"`, `port: 9090`,
  `token: None`; `validate()` warns only in the `token.is_some() && bind == "0.0.0.0"` case (i.e.
  warns when a token *is* configured), and says nothing when there is no token. The 5 existing unit
  tests do not cover the no-token case.

## What to fix

1. Add the integration coverage for the protected/unprotected route partition (criteria below),
   driving the path list from a single const shared with `create_router`.
2. Make `HttpConfig::validate()` reject — or at minimum warn loudly on — `bind = "0.0.0.0"` with
   `token = None`. Whether this is a hard refusal is a product call; flag it in the PR rather than
   deciding silently.
3. Decide and document the `token: None` middleware default. "Allow all" is defensible on
   loopback and is not on `0.0.0.0`; the gate should consider the bind address.

## Acceptance criteria

- [ ] New server integration test boots with `http.token = Some("t")` and asserts every protected
      path returns 401 with no header, with a wrong token, and with `Basic` instead of `Bearer`.
      **Fails today** (no such test exists; the routes are 0-region).
- [ ] The same test asserts each protected path returns 200 with the correct token, and that
      `/metrics`, `/health/live`, `/health/ready`, `/healthz`, `/readyz`, `/status/json` return 200
      *without* a token.
- [ ] The path list is a single const shared with `create_router`, so a new route added outside the
      protected group fails the test.
- [ ] Unit test asserts `HttpConfig::validate()` errors (or emits a warning captured via a
      `tracing` subscriber) for `bind = "0.0.0.0"` + `token = None`, and is silent for
      `127.0.0.1` + `None`. **Fails today.**

## Test boundary

**4** for the router half (server integration over HTTP) — the behaviour *is* the axum layer/route
partition; nothing below the router can observe which routes the middleware wraps. `1` for the
config-validation half: `validate()` is a pure function of the struct, so a server boot would add
seconds and no signal. `integration_debug_http.rs` already has the `reqwest` pattern to copy.

## Depends on

Nothing. Cross-area: `crates/config/` is owned by another area — coordinate so the
`HttpConfig::validate()` change lands once.

## Re-triage 2026-08-06

**Verdict: still-valid**

Both halves reproduce. `bearer_auth_middleware` is now at
`frogdb-server/crates/server/src/observability_server.rs:256-271` (was 248-270), still with
`// No token configured — allow all` → `next.run(req).await` at `observability_server.rs:267-270`;
the protected sub-router (`/debug*`, `/admin/{health,cluster,role,nodes,upgrade-status,shutdown,transfer-leader}`)
is `observability_server.rs:232-249` and there is still no shared path const. Config half:
`HttpConfig::validate()` at `frogdb-server/crates/config/src/http.rs:70-92` still warns only on
`token.is_some() && bind == "0.0.0.0"` (`http.rs:83-89`) and is silent for `token: None`
(default at `http.rs:58`) on any bind. Coverage is still zero: an `rg` for
`admin/shutdown|admin/role|admin/nodes|admin/cluster|admin/transfer-leader` across every `.rs` in
the repo hits only `observability_server.rs`, `admin/handlers.rs`, `acceptor.rs` and
`frogdb-operator/src/health.rs` — no test file, and `observability_server.rs` has no `mod tests`.
The rework-05 per-subcommand admin gating flagged as possibly relevant is a *different* gate: it
covers the RESP admin-port `NOADMIN` split (FM-CLUSTER-061/062, `command_spec.rs`) and does not
touch the HTTP surface. File history since filing is `9499641a`/`1828d3db` (TLS + config-param
work), neither touching the middleware.

# TLS harness cannot express cert rotation, watched certs, additional certs or ECDSA

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I9
LOE: 1–2 days (estimated)
Tier: B
Area: frogdb-test-harness / TLS (`test-harness/src/tls.rs`)
Asked by: 03 (F9, F13)

## Context

The connection audit found TLS configuration surfaces — cert watching, additional certs, and
in-place rotation — that no test can currently reach, because the fixture generates exactly
one RSA cert once and the server config has no knobs for the rest. Cert rotation while the
server runs is the specific behaviour operators depend on and the suite cannot express.

## Evidence

- **Needs**: `TestServerConfig.tls_watch_certs` + `tls_additional_certs`; `TlsFixture`
  (`test-harness/src/tls.rs`, currently a single `generate()`) gains an ECDSA variant and an
  in-place regeneration helper so rotation can happen while the server runs.
- **Note**: TLS-replication and cluster-TLS tests elsewhere likely want the same. One owner.

## What to build

1. `tls_watch_certs` and `tls_additional_certs` fields on `TestServerConfig`, plumbed to the
   server the same way the existing TLS options are.
2. An ECDSA variant on `TlsFixture` (`test-harness/src/tls.rs`) alongside the current
   `generate()`.
3. An in-place regeneration helper on `TlsFixture` that rewrites the cert files a running
   server is watching, so rotation is observable mid-test.
4. One owner for the fixture, since TLS-replication and cluster-TLS tests will want the same
   surface — do not fork a second TLS fixture.

## Acceptance criteria

- [ ] `TestServerConfig` exposes `tls_watch_certs` and `tls_additional_certs`, and a test
      sets each and asserts the server honours it.
- [ ] `TlsFixture` can generate an ECDSA cert, and a test completes a handshake against it.
- [ ] A test regenerates certs in place against a running watched-cert server and asserts a
      new connection uses the new cert while the old one is no longer accepted.
- [ ] No second TLS fixture is introduced; TLS-replication / cluster-TLS call sites use this
      one.

## Test boundary

Level 4 — TLS handshakes, cert watching and rotation are properties of the live connection
layer and the running server's file watcher; nothing below server integration can observe a
handshake at all.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

Nothing on the test side moved. `TlsFixture` (`frogdb-server/crates/test-harness/src/tls.rs:35`) is still
a single `generate()` producing one CA + one server cert + one client cert with rcgen's default key
algorithm — no algorithm variant and no in-place regeneration helper (the struct exposes only paths and
DER bytes, `:14-33`). `TestServerConfig` still carries only the original eleven TLS knobs
(`test-harness/src/server.rs:166-186`: cert/key/ca/client_auth/handshake_timeout/replication/cluster/
cluster_migration/no_tls_on_http/client_cert/client_key) — no `tls_watch_certs`, no
`tls_additional_certs`, and the `to_server_config` mapping at `:263-273` has nothing to plumb. The
*production* surface is real and reachable, which is what makes this a pure harness gap:
`frogdb_config::TlsConfig::watch_certs` (`config/src/tls.rs:199`, defaults `true`) and
`additional_certs` (`:108`) exist and are consumed by `server/src/tls.rs:428-439`, currently tested only
by in-crate unit tests (`server/src/tls.rs:748`, `:861`), never through a live handshake or a rotation.

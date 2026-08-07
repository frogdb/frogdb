# Config observability seams — four asks blocking the admin/config audit's tests

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I12
LOE: 1–2 days total (estimated)
Tier: B
Area: frogdb-server / config + observability seams
Asked by: 05 (four separate asks, listed under "Shared infrastructure requested")

## Context

The admin/config audit could not write four of its tests without small production-side seams:
there is no way to ask `ConfigManager` whether a parameter was actually published, no IO seam
on `ConfigPersister`, no shared list of which HTTP routes are meant to be behind the admin
gate, and possibly no restart-in-place helper. The third is the important one — it turns the
default-open admin-gate finding from a one-off test into a check that keeps failing as new
routes are added.

## Evidence

1. `is_published()` accessor (or a single publication bitmask) on `ConfigManager`, so
   05/F10 can assert wiring completeness without reflection.
2. A small IO seam for `ConfigPersister` (05/F15), which also unlocks its untested error arms.
3. A shared const list of protected-vs-public HTTP routes exported from
   `observability_server.rs`, so 05/F2's test **fails when a route is added outside the
   guarded group** — this is the durable form of the default-open admin-gate finding.
4. A `TestServer` restart-in-place helper (05/F9), if one does not already exist.

## What to build

Build the four seams above. For (4), check first whether a restart-in-place helper already
exists on `TestServer` and reuse it rather than adding a second.

**Note**: 15 asks that 05 own the registry round-trip (F9) and `noop:false ⇒ observable`
(F11) tests, written **once** in `server/tests/`, with 15's findings as the spec.

## Acceptance criteria

- [ ] `ConfigManager` exposes publication state (accessor or bitmask); a test asserts wiring
      completeness for every parameter with no reflection.
- [ ] `ConfigPersister` reads/writes through an injectable IO seam, and its error arms are
      covered by tests that force IO failures.
- [ ] `observability_server.rs` exports one const list of protected vs public routes, the gate
      is driven from it, and adding a route outside the guarded group fails a test.
- [ ] A `TestServer` restart-in-place helper exists (new or pre-existing and documented), used
      by at least one config-persistence test.
- [ ] The registry round-trip and `noop:false ⇒ observable` tests exist once, in
      `server/tests/`, with 15's findings as the spec.

## Test boundary

Level 4 for the route-gate and restart tests — HTTP routing, auth gating and process
lifecycle are server-level. The `ConfigManager` and `ConfigPersister` seams pull their own
tests down to level 2, which is the point of adding them.

## Depends on

Nothing.

## Re-triage 2026-08-06

**Verdict: still-valid**

All four seams are still absent on today's tree. (1) `ConfigManager`
(`frogdb-server/crates/server/src/runtime_config.rs:742`) has no `is_published`/bitmask accessor
— its public surface is per-parameter getters and `all_param_names`/`mutable_param_names`
(`:3429-3446`); the mutability round added live params but no publication oracle. (2)
`ConfigPersister` (`frogdb-server/crates/server/src/config_persister.rs:51`) is still a unit
struct of associated fns whose `atomic_write(path, contents)` (`:98`) does real filesystem IO with
no injectable seam, so its error arms remain untested. (3) `create_router`
(`frogdb-server/crates/server/src/observability_server.rs:221-251`) still builds the
public/protected split inline as two `Router::new()` chains with no exported const route list —
adding a route outside the guarded group breaks nothing, and no test enumerates the routes
(`grep '/admin/'` in `server/tests/` hits only `cluster_misc.rs`, which exercises
`/admin/upgrade-status` alone). (4) `TestServer` still has no restart-in-place helper —
`frogdb-server/crates/test-harness/src/server.rs:1110` gives `shutdown_mut()` (documented as
"before any restart on the same data directory") but callers must re-start by hand; the only
`restart` helpers are `ClusterHarness::restart`/`restart_node`
(`test-harness/src/cluster_harness.rs:422,1106`). No `server/tests/` config integration file
exists, so the registry round-trip and `noop:false ⇒ observable` tests are also unwritten.

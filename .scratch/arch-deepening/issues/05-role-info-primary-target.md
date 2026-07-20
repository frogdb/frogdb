# 05 — ROLE and INFO report the real Primary target after Role Demotion

Status: needs-triage

## What to build

`RoleCommand` hardcodes an empty `master_host` and port `0` in its replica reply, and INFO's
replication section has the same gap — a runtime-demoted Replica (round-8 `RoleManager`,
`REPLICAOF <host> <port>`) reports `slave` correctly but not who its Primary is. End-to-end:
after `REPLICAOF host port`, both `ROLE` and `INFO replication` show the actual target address
(wire-compat field names `master_host`/`master_port` stay per the Primary/Replica vocabulary
rule — master/slave only at the wire boundary).

Thread `RoleManager::primary_target()` (already exposed) into both renderers; boot-configured
replicas (config-file `replicaof`) must report the same fields — decide whether boot init also
records its target into `RoleManager` (preferred: one source) or the renderers fall back.

## Acceptance criteria

- [ ] `ROLE` on a demoted node returns `["slave", <host>, <port>, ...]` with real values
- [ ] `INFO replication` shows `master_host`/`master_port` matching the demotion target
- [ ] Boot-config replica reports identically (single source of truth for the target)
- [ ] `REPLICAOF NO ONE` clears both surfaces back to primary shape
- [ ] Integration test extends `integration_replication.rs` demotion cases

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report; `.scratch/arch-deepening/proposals/05-role-manager.md`.

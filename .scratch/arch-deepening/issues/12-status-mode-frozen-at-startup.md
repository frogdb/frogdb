# 12 — `/status` + `STATUS JSON` `cluster.mode` frozen at startup; runtime role changes invisible

Status: needs-triage

## What to build

The status `mode` string is computed once from config at boot
(`server/subsystems.rs::start` — `cluster` / `primary` / `replica` / `standalone`) and passed
into `StatusCollector` as a plain `String`. A runtime role change (`REPLICAOF` promote/demote via
`RoleManager`, issues 05/06) never updates it, so both status surfaces keep reporting the boot
role forever. Issue 11 unified `/status` and `STATUS JSON` behind one collector, so they at
least agree with each other — but both can disagree with reality, and with INFO: issue 05 fixed
INFO's replication section to key on the live `is_replica` state, so after a runtime demotion
INFO says `role:slave` while `/status` still says `"mode": "primary"`. That violates the
observability accuracy rule (misleading data is not acceptable).

End-to-end: after a runtime `REPLICAOF <host> <port>` (and back with `REPLICAOF NO ONE`),
`/status`, `STATUS JSON`, and INFO all report the new role. Preferred shape: `StatusCollector`
holds a live role source instead of a frozen `String` — e.g. a `watch::Receiver` fed by
`RoleManager`, or the same shared role state INFO reads (whatever seam issue 05 introduced;
reuse it, don't add a second role-tracking path). Cluster mode is genuinely static
(config-only) and may stay a constant; only the primary/replica/standalone axis is dynamic.
Check the debug node-state provider too — `subsystems.rs` clones the same frozen `mode` string
into it (line ~122); it must draw from the same live source.

## Acceptance criteria

- [ ] Runtime demotion test: start as primary, apply `REPLICAOF`, assert `/status` and
      `STATUS JSON` report the replica role (and flip back on `REPLICAOF NO ONE`)
- [ ] `/status`, `STATUS JSON`, INFO, and the debug node-state provider all read role from one
      shared live source; no frozen `String` copies remain
- [ ] `just test frogdb-server` observability + role_manager tests green

## Blocked by

None — issues 05/06/11 already merged; the live-role seam and unified collector both exist.

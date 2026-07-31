# 12 — `/status` + `STATUS JSON` `cluster.mode` frozen at startup; runtime role changes invisible

Status: ready-for-human

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

- [x] Runtime demotion test: start as primary, apply `REPLICAOF`, assert `/status` and
      `STATUS JSON` report the replica role (and flip back on `REPLICAOF NO ONE`)
- [x] `/status`, `STATUS JSON`, INFO, and the debug node-state provider all read role from one
      shared live source; no frozen `String` copies remain
- [x] `just test frogdb-server` observability + role_manager tests green

## Blocked by

None — issues 05/06/11 already merged; the live-role seam and unified collector both exist.

## Comments

### Resolution (2026-07-21)

The status `mode` is now computed at read time from the **same** `Arc<AtomicBool>` role flag the
issue-05 seam gave INFO/ROLE — no watch channel, no second role-tracking path.

**Seam.** New `frogdb_telemetry::LiveMode` (`status.rs`): `cluster_enabled: bool` (genuinely
static, config-only) + the shared `Arc<AtomicBool>` replica flag + a boot-chosen
`non_replica_label`. `current()` derives the label per read: `cluster` → live flag → label.
`is_replica()` exposes the raw flag read (independent of the cluster label) for primary-target
gating. Label rule in `start_subsystems`: config role `standalone` → `"standalone"`; `primary`
**or** `replica` → `"primary"` — a boot replica promoted via `REPLICAOF NO ONE` is a primary,
matching INFO's `role:master`.

**Consumers.** `StatusCollector` holds `LiveMode` instead of a frozen `String` (both `/status`
HTTP and `STATUS JSON` render through it, per issue 11). `ServerDebugProvider` likewise drops its
frozen `role: String` — and its frozen `master_host`/`master_port` (the same boot-only fork issue
05 fixed for INFO but not here): it now holds a `RoleManagerHandle` and derives the primary
target live from `primary_target()`, gated on `LiveMode::is_replica()` (raw flag, not the label —
in cluster mode the label is `"cluster"`, so a label-string gate would never fire; caught in
review). The frozen 4-way mode computation and static master host/port block in `subsystems.rs`
are deleted; no frozen mode/role `String` copies remain.

**Tests (all green):**
- `integration_replication::test_status_mode_tracks_runtime_replicaof` — two primaries, runtime
  `REPLICAOF host port` then `NO ONE`; asserts **both** the `STATUS JSON` conn command and the
  HTTP `/status/json` endpoint flip primary → replica → primary alongside ROLE.
- `status::tests::status_mode_tracks_live_role_flag`,
  `status::tests::live_mode_cluster_is_static_regardless_of_flag` (telemetry unit).
- `debug_providers::tests::replication_view_tracks_live_role_and_primary_target` +
  `replication_view_reports_master_in_cluster_mode_when_replica_flag_set` — role and master
  host/port track demote/promote through a real `RoleManagerHandle`.
- `frogdb-telemetry` status suite 27 passed; `frogdb-server`
  role_manager/debug_providers/observability_conn_command/replication-integration 52 passed;
  `just check` + `just lint` (server, telemetry) + `cargo fmt --check` clean.

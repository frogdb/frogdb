# 05 — ROLE and INFO report the real Primary target after Role Demotion

Status: ready-for-human

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

- [x] `ROLE` on a demoted node returns `["slave", <host>, <port>, ...]` with real values
- [x] `INFO replication` shows `master_host`/`master_port` matching the demotion target
- [x] Boot-config replica reports identically (single source of truth for the target)
- [x] `REPLICAOF NO ONE` clears both surfaces back to primary shape
- [x] Integration test extends `integration_replication.rs` demotion cases

## Blocked by

None - can start immediately

## Source

Round-8 P05 agent report; `.scratch/arch-deepening/proposals/05-role-manager.md`.

## Comments

### 2026-07-20 — Implemented

`RoleController` (frogdb-core) gained `primary_target()`. `ShardIdentity::master_host()`/
`master_port()` no longer cache a boot-only `String`/`u16` written once in `shards.rs` — they
derive live from the shared role controller on every call, so `RoleCommand` (already reading
`ctx.master_host`/`master_port`) and INFO's per-shard `InfoShardSnapshot` (already reading
`self.identity.master_host()`) both pick up a runtime `REPLICAOF host port` demotion with zero
propagation code, by construction: one `Arc<Mutex<RoleManager>>` behind every shard's controller
clone, no per-shard copy to go stale.

**Boot-config sourcing (single source of truth, as preferred by this issue):**
`RoleManager::new()` takes a `boot_target: Option<SocketAddr>` and seeds `primary_target` with it
directly at construction — no renderer-side fallback. The target is *not* re-resolved a second
time: `replication_init::init_replication()` already resolves the config `replicaof` host:port
into a `SocketAddr` for the boot replication stream; that same `primary_addr` is now returned on
`ReplicationInitResult`, threaded through `server/mod.rs` into `cluster_init::init_cluster`'s new
`boot_primary_addr` parameter, and passed straight into `RoleManager::new`. One resolution, one
place that owns the value for the process lifetime.

**Second bug found and fixed in the same pass (blocking acceptance criterion 2, not pre-existing
to this issue's scope in effect though pre-existing in the code):** INFO's replication section
(`connection/handlers/info.rs`) built its `primary: Option<PrimarySnapshot>` field from the mere
*presence* of `self.cluster.replication_tracker`, not from the live `is_replica` flag. A node that
booted as a primary (so it has a replication tracker) and was later demoted at runtime kept
`replication_tracker.is_some() == true` forever — `RoleManager` never tears that tracker down, it
only owns the read-only flag / primary target / inbound stream — so `sections.rs`'s renderer took
the `if let Some(primary) = &r.primary` branch unconditionally and rendered `role:master`,
`master_host`, never printed. Fixed by gating `primary` construction on `!is_replica`, matching the
field's own doc comment ("`None` for replica/standalone").

Why the diff reaches `shard/worker.rs`, `shard/types.rs`, `command.rs`, `cluster_init.rs`: the
issue's "single source of truth" instruction (line 16) required picking a home for the boot target
that both `ROLE` and `INFO` read through the *same* path as the runtime-demotion value, rather than
adding a second, parallel plumbing route. That path is `ShardIdentity` (read by every shard's
`CommandContext` construction in `worker.rs`, and by `diagnostics.rs`'s `InfoShardSnapshot`
collection) — so the old boot-only `master_host`/`master_port` fields and their setter had to be
removed from `types.rs` in favor of a live derivation through `role_controller.primary_target()`
(the new `command.rs` trait method), and `cluster_init.rs` had to gain a parameter to hand
`RoleManager::new` the boot address it now seeds `primary_target` with at construction. Threading
through `worker.rs` was the mechanical consequence of `command_context()` calling the changed
`ShardIdentity` accessors.

**Test evidence** (all via `just test frogdb-server <pattern>` / `just test frogdb-core <pattern>`,
targeted, foreground):

- `integration_replication::test_role_and_info_report_real_primary_after_demotion`
  (`case_1_in_memory`, `case_2_with_persistence`) — PASS. New test: runtime `REPLICAOF host port`
  demotion, `ROLE` and `INFO replication` agree on the real host/port, `REPLICAOF NO ONE` clears
  both back to primary shape.
- `integration_replication::test_boot_configured_replica_reports_primary_target`
  (`case_1_in_memory`, `case_2_with_persistence`) — PASS. New test: a config-`replicaof` boot
  replica reports the same fields with no runtime `REPLICAOF` call.
- `integration_replication::test_replicaof_host_port_demotes` (both cases) — PASS (regression).
- `integration_replication::test_role_command`, `test_role_standalone`,
  `test_info_replication_connected` (all cases) — PASS (regression).
- `role_manager::tests::{boot_target_seeds_primary_target, no_boot_target_on_primary_boot,
  role_controller_primary_target_matches_boot_seed, demote_sets_flag_records_target_and_starts_stream,
  promote_clears_flag_and_stops_stream, demote_promote_round_trip,
  demote_is_idempotent_per_target_but_switches_primaries}` — PASS.
- `frogdb-core`: `shard::types::identity_tests::{master_address_derives_from_role_controller,
  master_address_absent_without_role_controller}`,
  `shard::worker::command_context_tests::command_context_carries_replica_identity` — PASS.
- `just check` (full workspace) — clean.

All five acceptance criteria satisfied. Commit `5bc24e62`.

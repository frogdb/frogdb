# CLUSTER's blanket ADMIN flag blocks client slot-map bootstrap on the client port

Status: done

## Resolution — a declarative per-subcommand admin surface (2026-08-04)

Shipped in `c05d66cc` (mechanism + sweep + tests) and `6d1b59b4` (regenerated command golden).

`AdminSurface` lives next to `CommandSpec` (`frogdb-server/crates/core/src/command_spec.rs`) with
three shapes: `Open`, `Whole` (the old whole-command `CommandFlags::ADMIN`), and
`SplitBySubcommand { public }`. `admin_surface(command, flags)` resolves one, and
`requires_admin(subcommand)` answers the gate's question. The table `SPLIT_ADMIN_SURFACES` lists the
**public** subcommands rather than the admin ones, which is what makes the gate fail closed by
construction: a container invoked bare, or with a subcommand nobody declared, matches nothing and is
refused. That matters because `run_pre_checks` runs *before* arity and subcommand dispatch, so an
unrecognized subcommand reaches the gate as an unrecognized string, not as an error.

The admin-port guard (`connection/guards.rs`) is the single consumer, feeding it the subcommand the
existing `extract_subcommand` hook already extracts — the same shape as the adjacent ACL check.

A command with a split surface **drops** its whole-command `ADMIN` flag, so `COMMAND INFO`,
`COMMAND LIST FILTERBY ACLCAT admin` and docs-gen keep reading the flag directly and stay honest:
Redis likewise reports no `admin` flag on these containers, because there the mark lives on the
subcommands. `split_admin_surfaces_agree_with_command_flags` (`server/register.rs`) walks the live
registry and fails if a split command also carries `ADMIN`, so the table and the flag cannot drift
into two competing descriptions. ACL enforcement is untouched — `frogdb-acl`'s category map is
hardcoded, not derived from `CommandFlags` — and `OpClass::from_flags` only reads READONLY/WRITE.

Public surface as shipped (everything else on these commands is admin-only):

| Command | Public on a client port |
| --- | --- |
| CLUSTER | INFO, NODES, MYID, SLOTS, SHARDS, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, HELP |
| CONFIG | HELP |
| ACL | WHOAMI, CAT, GENPASS, HELP |
| CLIENT | ID, SETNAME, GETNAME, INFO, SETINFO, NO-TOUCH, TRACKING, TRACKINGINFO, GETREDIR, CACHING, REPLY, HELP |
| MEMORY | DOCTOR, HELP, MALLOC-SIZE, STATS, USAGE (i.e. PURGE is the only admin one) |

DEBUG, SHUTDOWN, REPLICAOF and the rest keep the whole-command flag and are unaffected.
`SLOWLOG`, `LATENCY` and `HOTKEYS` deliberately have **no** split entry: Redis marks every `SLOWLOG`
and `LATENCY` subcommand admin (`SLOWLOG GET` replays executed command arguments), and FrogDB's
`HOTKEYS GET` discloses key names with access frequencies — a wider surface than the `LATENCY`
readers, so splitting it while conforming those would be incoherent. All three stay wholly admin,
i.e. unchanged from before this change.

Classification was verified against a live Redis 8.6.1 with `COMMAND INFO <cmd>|<sub>`, and where the
audit brief's parity table disagreed with the live server the live server won. Three brief rows were
**wrong and were tightened**: `ACL LIST`/`USERS`/`GETUSER`/`LOG`/`DRYRUN` are admin (not just
SETUSER/DELUSER/LOAD/SAVE); `CLIENT LIST`/`UNBLOCK`/`NO-EVICT` are admin while `CLIENT NO-TOUCH` is
not; and `SLOWLOG GET`/`LEN` plus the `LATENCY` readers are admin, not public — which is why those
containers keep the whole-command flag. `CLIENT STATS` is gated as a judgment call: it enumerates
every connection via `format_all_client_stats`, the same disclosure surface as `CLIENT LIST`.

One deliberate deviation from Redis: `MEMORY PURGE` is gated (Redis leaves it open) — it is an
allocator-wide operation, not a read.

Tests: the pin `test_admin_port_blocks_cluster_discovery_subcommands_on_regular_port` was **inverted**
into `test_admin_port_allows_cluster_discovery_subcommands_on_regular_port`; four new
`*_subcommand_split_on_regular_port` tests (CONFIG, ACL, CLIENT, MEMORY) each assert public-allowed /
admin-blocked on the client port *and* admin-allowed on the admin port, via an `assert_split` helper;
`test_admin_port_blocks_observability_containers_on_regular_port` pins the blanket gate on every
`SLOWLOG`/`LATENCY`/`HOTKEYS` subcommand in both directions with the same helper; the misnamed
CONFIG-GET test (which its own comment admitted never tested CONFIG GET) became
`test_admin_port_allows_regular_commands_on_regular_port`. Nine unit tests in `command_spec.rs` pin
the resolver, the fail-closed defaults, case-insensitivity, the whole classification table, and
(`observability_containers_are_wholly_admin`) that the three observability containers resolve to
`AdminSurface::Whole`. `cluster_harness.rs`'s two discovery call sites dropped
`try_send_admin_aware`, which survives — with a corrected comment — for the mutating call sites.

Verified: `just test frogdb-server` 1935/1935 (one run showed a pre-existing flake unrelated to this
change — `integration_replication::test_replica_handles_rapid_reconnect::case_2_with_persistence`
answering `CLUSTERDOWN (quorum lost)` under whole-suite load; it passes in isolation),
`just core-test cluster` 174/174, `just lint` (clippy `--all-targets -D warnings` + failure-modes),
`just fmt-check`, `just docs-gen-check`.

---

Triage decision (2026-08-04, user): Option 1 — per-subcommand flags — AND full sweep of the other
blanket-ADMIN commands (`CONFIG`, `ACL`, `CLIENT`, `DEBUG`, `MEMORY`) in the same change, mirroring
Redis's per-subcommand admin marks. One client-breaking change to the admin-port contract, not
several. Scheduled into Phase 4 (cluster hardening); the cluster failure-mode spec describes the
final shape.
PRD: [replication-cluster-rework/epoch-fold-redesign.md](../epoch-fold-redesign.md)
Type: AFK
Origin: adversarial review of the epoch-fold redesign implementation, 2026-07-28
Severity: likelihood 3/3 (any deployment with `admin-port` set), consequence 2/3 (cluster clients
cannot discover topology; workaround exists but exposes the admin port)
Area: Cluster / connection gating

## Context

`CLUSTER` is registered with a single `CommandFlags::ADMIN` for the whole command
(`frogdb-server/crates/server/src/commands/cluster/mod.rs`, `CommandSpec.flags`). The admin-port
gate is per-command and runs before subcommand dispatch
(`frogdb-server/crates/server/src/connection/guards.rs:338-347`):

```rust
if self.admin_enabled
    && !self.is_admin
    && let Some(cmd_info) = self.registry.get_entry(cmd_name)
    && cmd_info.flags().contains(CommandFlags::ADMIN)
{
    return Some(Response::error(
        "NOADMIN Admin commands are disabled on this port. Use the admin port.",
    ));
}
```

So whenever `admin-port` is configured, the *client* port answers `-NOADMIN` to
`CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER INFO`, `CLUSTER NODES` and `CLUSTER MYID` — the
read-only discovery surface every cluster-aware client uses to build and refresh its slot map.
In Redis these subcommands are not admin-gated at all; only the mutating ones
(`SETSLOT`, `FORGET`, `MEET`, `RESET`, `FAILOVER`, `ADDSLOTS`, ...) are `admin`/`dangerous`.

Consequences observed:

- Cluster clients (redis-py, lettuce, go-redis, redis-cli `-c`) cannot bootstrap against a FrogDB
  cluster whose nodes run an admin port. They see `-NOADMIN` on the first `CLUSTER SLOTS` and fail
  to initialize, with no `MOVED`-based fallback.
- FrogDB's own Jepsen workload reads `CLUSTER INFO` on the normal port
  (`testing/jepsen/frogdb/src/jepsen/frogdb/leader_election.clj`), so any Jepsen run against an
  admin-port deployment reports a bogus topology failure.
- The only workaround — pointing clients at the admin port — hands every client the full mutating
  admin surface, which is the opposite of what the admin port is for.

The cluster test harness already routes around this
(`frogdb-server/crates/test-harness/src/cluster_harness.rs`, `try_send_admin_aware`), which is why
no in-repo test caught it.

## What to build

- Split the gate so the read-only `CLUSTER` subcommands are reachable on the client port. Two
  candidate shapes, to be decided during triage:
  1. **Per-subcommand flags.** Give the dispatch table a per-subcommand flag set and have the
     guard consult it (`extract_subcommand` already exists in `guards.rs` for the ACL check).
     Matches Redis exactly and generalizes to `CONFIG GET` vs `CONFIG SET`, `ACL WHOAMI` vs
     `ACL SETUSER`, and `CLIENT` — all of which have the same blanket-flag problem today.
  2. **Narrow allowlist.** Keep `CLUSTER` ADMIN-flagged and special-case the discovery
     subcommands in the guard. Smaller change, but a second place that encodes command semantics.
  Option 1 is preferred: the blanket flag is the actual defect and it is not specific to `CLUSTER`.
- Whichever shape lands, the mutating subcommands must stay gated, and
  `test_admin_port_blocks_cluster_discovery_subcommands_on_regular_port`
  (`frogdb-server/crates/server/tests/integration_admin_port.rs`) must be inverted, not deleted —
  it currently pins the broken behavior deliberately so the fix is visible in the diff.
- Audit the other blanket-ADMIN commands (`CONFIG`, `ACL`, `CLIENT`, `DEBUG`, `MEMORY`) and record
  which of their subcommands Redis exposes to normal clients.

## Acceptance criteria

- With `admin-port` configured, a client connected to the client port can run `CLUSTER SLOTS`,
  `CLUSTER SHARDS`, `CLUSTER INFO`, `CLUSTER NODES` and `CLUSTER MYID` successfully.
- The same connection still gets `-NOADMIN` for `CLUSTER SETSLOT`, `CLUSTER FORGET`,
  `CLUSTER MEET`, `CLUSTER RESET`, `CLUSTER FAILOVER`, `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`.
- A real cluster client (or the Jepsen `leader_election` workload) bootstraps against an
  admin-port deployment without touching the admin port.
- The harness's admin-aware fallback is no longer needed for the discovery subcommands (it may
  stay for the mutating ones).

## Notes

Filed rather than fixed inline because it is a security-relevant behavior change to the admin-port
contract, and because the right fix (per-subcommand flags) is a cross-command refactor well outside
the epoch-fold PRD's scope.

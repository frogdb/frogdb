# CLUSTER's blanket ADMIN flag blocks client slot-map bootstrap on the client port

Status: needs-triage
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

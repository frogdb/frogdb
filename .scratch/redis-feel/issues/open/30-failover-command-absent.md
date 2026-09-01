# `FAILOVER` (Redis 6.2 coordinated failover) is absent — replies "unknown command"

Status: needs-triage
Type: gap (missing command) / ruling needed
Area: replication

## Problem

`FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT ms]` — Redis 6.2, group `server`, flags
`ADMIN NOSCRIPT STALE`, arity -1 — is not registered anywhere in FrogDB. The compat matrix marks
it `unsupported` with the generic note "Present in Redis 8.6.1; not implemented in FrogDB"
(`website/src/data/command-matrix.json`). Under ADR-0005 that makes it a gap by definition: we
advertise the 8.6.1 target, so an unadvertised absence is a bug, not a deviation.

It is not even a stub — a client gets `ERR unknown command 'FAILOVER'`, so tooling cannot discover
that FrogDB knows the command exists.

Note the difference from `CLUSTER FAILOVER`, which FrogDB *does* implement
(`server/src/commands/cluster/admin.rs:243`): `FAILOVER` is issued **on the primary** and
coordinates a handoff to a chosen replica (pause writes -> wait for the target replica to match the
offset -> demote self -> promote target), and it applies to plain replication, not just cluster
mode.

Most of the machinery exists:

- `RoleManager` performs live role changes; `REPLICAOF` already demotes a writable primary and
  connects to the new primary (`server/src/role_manager.rs`).
- `CLUSTER FAILOVER [FORCE|TAKEOVER]` is implemented for cluster mode, replica-side.
- Replication offsets and acks needed for the catch-up wait are tracked — `INFO replication`
  renders real `slaveN:...offset=...,lag=...`.

## Why `needs-triage`

1. **Scope**: standalone replication only, cluster mode only, or both? In cluster mode a
   FrogDB-native failover must go through Raft, so `FAILOVER` there is either a thin front for the
   existing `CLUSTER FAILOVER` path or a refusal that names it.
2. **`master_failover_state`**: `INFO replication` hardcodes `no-failover` (`commands/info.rs:507`,
   `:544`). Implementing `FAILOVER` means implementing `waiting-for-sync` /
   `failover-in-progress` too, or that field stays a fabrication with real state behind it.
3. **Interim behavior**: if the answer is "not now", the truthful move under ADR-0005 is a
   registered `NotSupported` stub with a reason pointing at `CLUSTER FAILOVER` / `REPLICAOF`, not
   silence.

## Acceptance criteria (draft, pending ruling)

- [ ] `FAILOVER` is registered — implemented, or a `NotSupported` stub naming the alternative
- [ ] If implemented: `FAILOVER TO <host> <port>` promotes that replica and demotes the old
      primary with no lost acknowledged writes
- [ ] `FAILOVER ABORT` cancels an in-progress failover; `TIMEOUT` bounds the catch-up wait
- [ ] `INFO replication`'s `master_failover_state` reflects real state throughout
- [ ] Failure-mode coverage for the handoff under a partition, per `specs/replication.md`

Size: L

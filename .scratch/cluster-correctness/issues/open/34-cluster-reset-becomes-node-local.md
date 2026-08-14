# 34: `CLUSTER RESET` becomes node-local — destructive scope equals invocation scope

Status: ready-for-agent

## Origin

Distsys-review MAJ-6 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **node-local** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`ResetCluster` (`cluster/src/commands.rs:~815-863`) is a *replicated* state-machine
command: applying it clears `slot_assignment`, `migrations`, `handoff_seq`, and `nodes`
on **every** node. Redis's `CLUSTER RESET` is node-local — it resets the node issuing
it and nothing else. The deviation is not in the deviations table, and it is no
improvement: an operator following Redis muscle memory ("this one node is confused,
reset it and let it rejoin") destroys the entire cluster's topology and slot map in one
command — no confirmation, no `HARD`/`SOFT` blast-radius distinction, and replicated,
so reconnecting undoes nothing.

etcd deliberately has no cluster-wide reset; `etcdctl member remove` is per-member and
the documented recovery is "stop the member, wipe its data dir, re-add". The
destructive-scope-equals-invocation-scope principle is what makes that safe. A
cluster-wide wipe remains achievable node-by-node — scope stays equal to invocation.

## What to build (spec-first; cluster is locked, gate 0.80)

1. Spec rows first:
   - TR row: `CLUSTER RESET` affects only the issuing node — it leaves the cluster
     (demote-shaped departure per issue 20's demote-don't-remove ruling, never a raft
     eviction of *other* nodes), clears its **local** cluster state, and is ready to be
     re-added; the replicated topology on surviving nodes keeps every other node's
     assignments intact.
   - FM row: reset issued on one node of a populated cluster → other nodes' slot
     ownership and membership unchanged; NOT observable: any change to another node's
     `slot_assignment`/`nodes` entries caused by a reset it did not issue.
   - Redis deviations table: no row needed once node-local (parity); remove/adjust any
     existing text implying cluster-wide semantics. Honor `HARD`/`SOFT` distinction to
     Redis semantics or document the deviation if collapsed.
2. Code: `ResetCluster` stops being a replicated wipe of shared topology. The issuing
   node proposes its own departure (existing demote/remove-self path) and locally
   resets its cluster runtime state; it must not clear `slot_assignment` entries owned
   by other nodes, `migrations` it is not a party to, or the `nodes` map beyond its own
   entry.
3. Forcing test: three-node cluster with assigned slots + one open migration not
   involving node C; `CLUSTER RESET` on C → A/B topology, slots, and the migration are
   untouched; C is departed and re-addable. Pre-fix this fails (everything wiped).
4. Check `frogctl` and docs/website for text describing cluster-wide reset; update.

## Acceptance criteria

- [ ] TR + FM rows landed; `just lint-spec` green
- [ ] Reset touches only the issuing node's membership + local state
- [ ] Forcing test fails pre-fix, passes post-fix
- [ ] Docs/website + frogctl surfaces updated
- [ ] `just mutants-diff` on frogdb-cluster / frogdb-cluster-runtime (locked, 0.80)
      triaged

## Blocked by

None — can start immediately. Coordinate with
[issue 20](20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md)
(demote-don't-remove departure shape) if in flight simultaneously.

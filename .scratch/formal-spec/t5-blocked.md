# T5 blocked — steering surfaced a real `inv_source_keeps_its_copy_until_promotion_attested` violation

Status: **BLOCKED — needs a design ruling.** Do not weaken the invariant, the guard, or the
steering to make the gate green.

Filed 2026-08-19 by the T5 (walk steering) task. The steering work itself is complete and
committed; the gate `just quint-run` is now **intermittently RED on
`specs/quint/cluster_migration_failover.qnt`** because the deeper walk reaches a state the flat
walk never sampled.

## What is red

```
quint run specs/quint/cluster_migration_failover.qnt \
  --max-samples=200 --max-steps=20 --seed=2 \
  --invariants inv_source_keeps_its_copy_until_promotion_attested
→ [violation] Found an issue
```

Hit rate:

| model | config | seeds violating |
|---|---|---|
| pre-steering (`2eb66e35`) | 2000 samples x 40 steps | **0 / 10** (20 000 traces, clean) |
| steered (HEAD) | 200 x 20 (the `just quint-run` config) | 1 / 8 (seed 2) |
| steered (HEAD) | 500 x 40 | **8 / 8** |

So the transition relation did not change — the walk simply now reaches the state. This is the
counterexample-protocol case: a latent model/design inconsistency, not a steering bug.

## The invariant

`specs/quint/cluster_migration_failover.qnt`:

```quint
val inv_source_keeps_its_copy_until_promotion_attested: bool =
  SLOTS.forall(s => match residue.get(s) {
    | Some(r) => r.promoted or r.source_gone or shardHoldsCopy(nodes, r.source, s)
    | None => true
  })
```

`shardHoldsCopy` (`..._logic.qnt`) reaches a copy either directly or **through one shard hop**:

```quint
pure def shardHoldsCopy(allNodes, q, s): bool =
  allNodes.get(q).keys.contains(s)
    or NODES.exists(x => allNodes.get(x).keys.contains(s) and shardPrimary(allNodes, x) == Some(q))

pure def shardPrimary(allNodes, n): Option[NodeId] =
  if (isLivePrimary(allNodes, n)) Some(n)
  else match allNodes.get(n).parent {
    | Some(p) => if (isLivePrimary(allNodes, p)) Some(p) else None   // one hop, parent must be a live Primary
    | None => None
  }
```

## Minimal counterexample

Seed 2, 200 x 20. Init is the standard one (node 1 Primary owning slots 1-4; 2,3 replicas of 1;
node 4 a slotless Primary). The load-bearing suffix, with the state index from the ITF trace:

| step | action | effect |
|---|---|---|
| 8 | `prepareHandoff(4)` | slot 4 record → `Draining` |
| 10 | `completeMigration(4, 1)` | slot 4 owner → node 4; `residue[4] = { mig: 1, source: 1, target: 4, promoted: false, source_gone: false }`. Node 4 holds **no** copy yet — that is what `promoted == false` means; node 1 keeps the only copy (`keys = {1,2,3,4}`). |
| 13 | `adoptReplicatedRole(1)` | **node 1 — the residue source and the sole physical holder of slot 4 — becomes a Replica parented at node 4.** It keeps its keys. |
| 15 | `retargetSlotResidue(4)` | `canRetargetSlotResidue` sees `shardPrimary(nodes, 1) == Some(4) != 1`, so the entry's `source` is re-homed `1 → 4`. The entry now names a node that holds the copy **only derivatively**, through the shard-closure edge `1 → 4`. |
| 16 | `stageFlip(4, 3)` | node 4 stages a `Demotion` with upstream 3 |
| 18 | `adoptReplicatedRole(4)` | node 4 becomes a Replica parented at 3 |

State 18 evaluates to a violation:

- `residue[4] = { source: 4, promoted: false, source_gone: false }`
- `nodes[4].keys` does not contain 4 (it never did)
- the closure disjunct: `x = 1` holds the copy, but `shardPrimary(nodes, 1)` is now `None` — node
  1's parent (4) is no longer a live Primary — so the copy is unreachable.

Nothing was ever deleted. The copy of slot 4 is still physically at node 1 in the violating
state; only the *derived reachability relation* lost it, because the shard walk is one hop deep
and the model reached a **chained replica** — `1 → 4 → 3`.

## The two candidate root causes (needs a ruling — do not pick one unilaterally)

1. **Chained replicas are reachable and the shard abstraction does not model them.**
   `stageFlip`/`adoptReplicatedRole` demote node 4 while node 1 is still parented at it, producing
   `1 → 4 → 3`. Real Redis/Valkey re-point a demoted primary's replicas at the new primary (or
   the chain is a supported sub-replica topology and `shardPrimary` must walk transitively). Fix
   is either in the demotion arm (refuse/repoint dependants) or in `shardPrimary` (transitive
   walk, with a cycle bound).

2. **`retargetSlotResidue` re-homes onto a node that does not hold the copy.**
   `canRetargetSlotResidue` requires only `shardPrimary(r.source) != r.source`; it never checks
   that the new source physically holds `s`. After the re-home the entry's whole copy claim rests
   on one closure edge that any later role change can cut. Tightening the guard to demand
   `nodes.get(p).keys.contains(s)` — or leaving the entry pinned to the physical holder — removes
   this class.

Both readings are consistent with the design doc as written, which is exactly why this is a
ruling and not a patch. Related surface worth checking under the same ruling:
`inv_slot_copy_survives_until_owned_and_served` and `keyIsTracked` also lean on the one-hop
closure and would move with option 1.

## Reproduce

```bash
eval "$(mise activate bash)"
quint run specs/quint/cluster_migration_failover.qnt \
  --max-samples=200 --max-steps=20 --seed=2 \
  --invariants inv_source_keeps_its_copy_until_promotion_attested \
  --out-itf=/tmp/cex.itf.json
```

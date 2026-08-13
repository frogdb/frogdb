# 25 — A freshly-started node's solo self-bootstrap makes it briefly (and wrongly) reachable as "the leader" while it is still joining

Status: ready-for-agent

## Parent

[Issue 08](../done/08-raft-workload-gaps.md) — found while running the deferred 16-workload jepsen
sweep. `membership-routing` failed reproducibly (2/2 attempts, ~40 minutes and a full docker
teardown/recreate apart, ruling out stale volumes or a one-off scheduling fluke).

## What is wrong

Every node — including one that has never joined any real cluster — unconditionally
bootstraps itself as a **solo, single-node Raft group** at startup and immediately becomes
"leader" of that trivial group:

`frogdb-server/crates/server/src/server/cluster_init.rs:437-450`:

```rust
if should_bootstrap && !initial_members.is_empty() {
    ...
    if !already_initialized {
        info!(node_id = node_id, member_count = initial_members.len(), "Bootstrapping Raft cluster");
        if let Err(e) = raft.initialize(initial_members.clone()).await { ... }
    }
```

For a node started standalone (not yet `CLUSTER MEET`'d into an existing cluster),
`initial_members` is just itself, so `raft.initialize` runs with `member_count=1` and the node
self-elects within the same event loop tick. Confirmed in a captured server log
(`frogdb-raft-n4`, container's own log, 2026-08-12T00:36:29 UTC):

```
00:36:29.583996  Bootstrapping Raft cluster node_id=2322142712591902554
00:36:29.584238  received RaftMsg::Initialize members={2322142712591902554: ...}
00:36:29.584263  elect, new candidate: progress:{2322142712591902554: false}
00:36:29.587134  become leader id=2322142712591902554
```

— all within 3.1ms of "Bootstrapping Raft cluster".

Later, when the real cluster's leader tries to add this node as a Raft learner (via
`CLUSTER MEET` → `frogdb-server/crates/cluster/src/network.rs:779` `add_learner` retry loop,
`MAX_ATTEMPTS = 5`, linear 500ms·attempt backoff), the promotion **fails every attempt** because
the joining node's stale self-vote from its solo bootstrap conflicts with the real leader's
term. From `frogdb-raft-n1`'s log (the real cluster's leader) in the same run:

```
00:36:43.715692  Adding node to cluster node_id=2322142712591902554 addr=172.21.0.5:6379
00:36:43.716747  quit leader id=1521519314445575349          <- n1 (real leader) steps down
00:36:43.718163  ERROR the first step error: has to forward request to: Some(2322142712591902554), ...
00:36:43.718210  WARN  Failed to promote Raft learner to voter; retrying attempt=1
00:36:44.219871  WARN  Failed to add Raft learner; retrying attempt=2
00:36:45.223816  WARN  Failed to add Raft learner; retrying attempt=3
00:36:46.730236  WARN  Failed to add Raft learner; retrying attempt=4
00:36:48.731655  ERROR Failed to add Raft learner after 5 attempts; node is in cluster state
                       but NOT a Raft voter node_id=2322142712591902554
```

`frogdb-raft-n4`'s own log confirms it is still on its stale solo-bootstrap vote/term the
whole time and only relinquishes it at the very end of that window:

```
00:36:43.716630  vote T1-N1521519314445575349:committed is rejected by local vote: T1-N2322142712591902554:committed
00:36:47.372963  vote is changing from T1-N2322142712591902554:committed to T2-N15725705562390610400:committed
00:36:47.372980  quit leader id=2322142712591902554
```

So for a ~5-second window (00:36:43.7 → 00:36:48.7), the real leader (n1) has stepped down and
the joining node (n4) is still internally claiming leadership of its own irrelevant one-node
group, is externally reachable, and does not yet know about nodes that have belonged to the
real cluster since bootstrap.

The `has to forward request to: Some(<new-node-id>)` error openraft returns during this window
is exactly the shape the command layer's `-REDIRECT <node-id> <ip:port>` reply construction
uses for a legitimate "not leader, ask the real leader" signal — so a client sending a
Raft-needed admin command (e.g. `CLUSTER SETSLOT ... MIGRATING`) during this window gets
redirected to the joining node as if it were the genuine leader. It answers `-ERR node <id>
not found` for any node the client references that predates its own join, because it hasn't
actually absorbed the real cluster's membership yet.

## Second witness

`raft-membership` (same `membership-routing` workload code, `raft-cluster-membership`
nemesis) hit the identical fingerprint on its first run: `:add-node "n4"` →
`:start-migration {:slot 7700, :dest "n4"}` → `REDIRECT -> retrying on leader n4` →
`ERR node 15092003070405494904 not found` (same node id — n3 — as the `membership-routing`
reproductions above). Store dir:
`testing/jepsen/frogdb/store/frogdb-membership-routing-raft-cluster-membership-docker-cluster/20260811T204234...-0400/`.
Not re-diagnosed separately; same root cause, recorded here as a second independent
reproduction under a different workload/nemesis combination.

## Evidence (jepsen reproduction)

`membership-routing` workload, two independent runs (2026-08-11 19:22 and 20:36, full
container teardown/recreate between them — rules out stale docker volumes):

```
20:36:46,739  :invoke :start-migration {:slot 7700, :dest "n4"}
20:36:46,752  REDIRECT -> retrying on leader n4 (attempt 1)
              => clojure.lang.ExceptionInfo: ERR node 15092003070405494904 not found
```

`15092003070405494904` is n3's own `CLUSTER MYID` — n3 is one of the *original* 3 bootstrap
members, not the newly-joined n4. Confirmed by direct `docker exec ... CLUSTER MYID` on all 5
containers immediately after a fresh (third) reproduction, decoded from hex:

| container | addr | node_id (decimal) |
|---|---|---|
| n1 | 172.21.0.2 | 1521519314445575349 |
| n2 | 172.21.0.3 | 15725705562390610400 |
| n3 | 172.21.0.4 | **15092003070405494904** |
| n4 | 172.21.0.5 | 2322142712591902554 |
| n5 | 172.21.0.6 | 5900838575221618495 |

The failing op's timestamp (20:36:46.752 = 00:36:46.752 UTC) falls squarely inside the
00:36:43.7–00:36:48.7 window above: n4 was still leader of its own stale solo term, had not
yet been successfully promoted to a real voter (5th and final retry attempt was still pending,
landing at 00:36:48.731), and genuinely did not know about n3.

Full logs captured in this session at `/tmp/n1-server.log`..`/tmp/n5-server.log` were from a
stale prior container incarnation (an artifact of a `docker logs -f ... & wait` capture
pattern that silently rebinds to whatever container exists at launch time and exits early if
that container is recreated mid-capture — do not reuse that capture pattern without pinning to
a specific container ID). The evidence quoted above is from a clean, dedicated repro
(`just jepsen membership-routing --no-build --no-teardown`, store dir
`testing/jepsen/frogdb/store/frogdb-membership-routing-docker-cluster/20260811T203638.482-0400/`)
with containers inspected live via `docker exec`/`docker logs` immediately after, not through
that unreliable side-capture.

## Checker gap

The `DEBUG CLUSTER CHECK` invariant checker (issue 07) stayed green through all three
reproductions — `:cluster-invariants {:valid? true, :sweeps-run 2, :violating-sweeps 0,
:connectivity-errors 0}` every time. It doesn't currently probe the admission window
itself (a node self-reporting Raft leadership of a term the rest of the cluster doesn't
recognize), only post-hoc topology/connectivity snapshots between sweeps, so it never
observes the ~5s of transient bad state that produces the visible failure. Worth
considering as a candidate addition when this is fixed: an invariant that a node
answering as Raft leader is either the leader the rest of the cluster agrees on, or is
not yet externally reachable/redirectable at all.

## Not a harness bug

The jepsen client (`testing/jepsen/frogdb/src/jepsen/frogdb/slot_migration.clj`'s
`with-leader-retry`/`resolve-node-id`) is behaving correctly: it followed a `-REDIRECT` the
server itself issued, and correctly reported the server's own `-ERR node ... not found` reply.
Nothing here should be patched in the jepsen harness.

## What to build

Not yet triaged/ruled. Candidate directions:

1. **Don't let a not-yet-joined node self-elect as a reachable "leader" at all.** A node with
   `member_count=1` that is expected to join an existing cluster (vs. genuinely bootstrapping a
   fresh deployment) shouldn't complete `raft.initialize`/leader-election in a way that's
   externally observable/routable until `CLUSTER MEET` either confirms it as a real bootstrap
   node or folds it into the joining cluster's term.
2. **Don't translate `has to forward request to: Some(<node>)` into a client-facing
   `-REDIRECT`** when that error originates from a learner-promotion conflict rather than a
   genuine "ask the real leader" signal — distinguish the two error shapes at the command
   layer.
3. **Have the joining node relinquish/never take its solo-bootstrap vote once `add_learner`
   from a real cluster is in flight**, closing the ~5s window during which it holds a
   conflicting term.

## Acceptance criteria

- [ ] Root cause ruled (which of the above, or another) and an FM row added if it lands in a
      locked crate's failure-mode spec (cluster/cluster-runtime)
- [ ] Forcing test reproducing the "-REDIRECT to a node that isn't a real voter yet" window
- [ ] `membership-routing` reruns clean post-fix; re-added to issue 08's sweep results

## Blocked by

None.

## Ruling (2026-08-13)

**Options 1 AND 2 together.**
1. Explicit bootstrap-vs-join (etcd `initial-cluster-state` shape): a node never self-elects into an externally routable leader unless explicitly configured to bootstrap a fresh deployment; a joining node defers raft init/election until MEET folds it in.
2. Error-shape fix: learner-promotion conflicts are no longer translated into client `-REDIRECT`.
Option 3 becomes unnecessary once (1) lands. If cheap, also add the cross-node checker invariant: a node answering as leader must be the agreed leader or unreachable.

## Amendment (2026-08-13)

Two additions from the review, both accepted:

1. **Refuse MEET of a node with non-empty Raft state.** A node that was ever a member of another cluster carries Raft log/vote state; MEETing it in can absorb foreign state. etcd refuses exactly this — the data dir must be wiped first.
2. **Persist bootstrap intent.** A node restarted mid-bootstrap re-decides its mode from config today; persisted intent makes the restart deterministic (no accidental re-bootstrap or mode flip).

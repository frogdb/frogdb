# PRD: Cluster epoch fold redesign — make `cluster_current_epoch` mean the config-epoch counter

Status: implemented (pending review)
Area: Cluster
Origin: follow-up to issue 47 (`.scratch/testing-improvements/issues/47`,
done) — the observability gap was pinned, the fold itself was left in place. Written 2026-07-28.

Related issues:

- [47 — Pin INFO-vs-NODES cluster epoch relationship deliberately](../testing-improvements/issues/)
  (done; added `fold_current_epoch`, five unit tests, two integration tests, docs)
- [16 — Cluster epoch persistence assertion is masked by raft term folding](../testing-improvements/issues/)
  (done; strengthened `test_cluster_epoch_persists`, left the "no persisted state-machine snapshot"
  residue open)
- [64 — No epoch-collision detection/prevention exists anywhere in FrogDB cluster mode](../testing-improvements/issues/)
  (in flight, being implemented in parallel; this PRD assumes its resolution policy lands and mints
  fresh epochs from the cluster-wide counter)

---

## 1. Summary

`CLUSTER INFO` reports `cluster_current_epoch` as `max(cluster-wide config_epoch, local Raft term)`.
The `max` fold is lossy in both directions:

- **Inflation.** A Raft election with zero topology change raises the reported epoch. The term is a
  *local, unreplicated* value, so different nodes can report different `cluster_current_epoch`
  values for the same cluster state, and a node stuck campaigning inflates its own reported epoch
  without bound.
- **Masking.** A real topology event is invisible whenever the term already dominates (the pinned
  `config_epoch 0 → 1` under `raft_term == 1` case).

The fold also makes `CLUSTER INFO` the *only* epoch surface in the product that reports a folded
value: `CLUSTER NODES`, the HTTP admin API, the debug web UI, and the in-process test harness all
report the raw counter. This PRD proposes removing the fold, restoring the
`current_epoch >= max(per-node config_epoch)` invariant at its source (a ratchet in the state
machine rather than a `max()` at the reporting site), and exposing the Raft term as its own field.

---

## 2. Current state

### 2.1 Where the fold happens

| Concern | Location |
|---|---|
| Fold helper | `frogdb-server/crates/server/src/commands/cluster/mod.rs:251-253` — `fold_current_epoch(config_epoch, raft_term) = config_epoch.max(raft_term)` |
| Contract doc comment | `frogdb-server/crates/server/src/commands/cluster/mod.rs:216-250` |
| Term read | `frogdb-server/crates/server/src/commands/cluster/mod.rs:288-291` — `ctx.raft.map(\|r\| r.metrics().borrow().current_term).unwrap_or(0)` |
| Call site | `frogdb-server/crates/server/src/commands/cluster/mod.rs:321` |
| Emission | `frogdb-server/crates/server/src/commands/cluster/mod.rs:338, 354` (`cluster_current_epoch:{}`) |
| `cluster_my_epoch` | `frogdb-server/crates/server/src/commands/cluster/mod.rs:267-271, 339, 355` — this node's `NodeInfo.config_epoch`, **raw** |
| Standalone branch | `frogdb-server/crates/server/src/commands/cluster/mod.rs:369-370` — hardcoded `0`/`0` |
| Fold unit tests | `frogdb-server/crates/server/src/commands/cluster/mod.rs:778-822` (five `test_fold_current_epoch_*`) |

This is the **only** place the Raft term leaks out of the consensus layer. Verified: no `term` in any
`INFO` section (`frogdb-server/crates/server/src/commands/info.rs`,
`frogdb-server/crates/server/src/info/sections.rs`), no term metric in
`frogdb-server/crates/types/src/metrics/definitions.rs`, none in the admin API or debug UI. Other
`raft.metrics()` reads use `state` / `current_leader` / `millis_since_quorum_ack`
(`commands/cluster/mod.rs:295-313`) or `membership_config` (`cluster/src/network.rs:644`).

**Issue 47 did not add a separate raft-term observability field** — it added the named helper, unit
tests, integration tests and docs only. The term remains unobservable except through the fold.

### 2.2 How the cluster-wide epoch counter is maintained

The counter is `ClusterStateInner.config_epoch` (`frogdb-server/crates/cluster/src/state.rs:36-37`),
a `u64` (`frogdb-server/crates/cluster/src/types.rs:16-18`), replicated by Raft and read via
`ClusterState::config_epoch()` (`state.rs:122-125`) or `snapshot()` (`state.rs:96`).

Exactly three write paths increment it, all inside `ClusterState::apply_command`
(`frogdb-server/crates/cluster/src/commands.rs`):

| Site | Command | Effect |
|---|---|---|
| `commands.rs:156-160` | `IncrementEpoch` | `config_epoch += 1`, returns `ClusterResponse::Epoch` |
| `commands.rs:229-238` | `Failover` | `config_epoch += 1` **and** stamps the promoted node: `new_node.config_epoch = epoch` (`:237`) |
| `commands.rs:260-273` | `MarkNodeFailed` | `config_epoch += 1` in the same transition as the FAIL flag |

Two paths move a per-node epoch *without* the counter:

| Site | Command | Effect |
|---|---|---|
| `commands.rs:25-56` (insert at `:54`) | `AddNode` | inserts the incoming `NodeInfo` **verbatim**, including its `config_epoch`, with no uniqueness check and **no ratchet of the cluster-wide counter** — this is issue 64's hole and it is also the one way `cluster_my_epoch > cluster_current_epoch` can occur |
| `commands.rs:403-443` (`:422`, `:424`) | `ResetCluster` HARD | resets both the cluster-wide counter and this node's epoch to `0` — i.e. the counter is **not** globally monotonic (Redis `CLUSTER RESET HARD` behaves the same way) |

Proposal paths: `frogdb-server/crates/cluster/src/writer.rs` (`propose`), failure detector
(`frogdb-server/crates/server/src/failure_detector.rs:289-292` for `MarkNodeFailed`, `:405-425` for
the atomic `Failover`), and `CLUSTER SET-CONFIG-EPOCH`, which is a stub: it parses the argument,
**discards it** (`frogdb-server/crates/server/src/commands/cluster/admin.rs:453` — `let _epoch_num`)
and issues a plain `IncrementEpoch` (`admin.rs:462-468`). `CLUSTER BUMPEPOCH` is documented
unsupported (`website/src/content/docs/compatibility/overview.mdx:62, 134`).

A lint gate pins the atomicity story and bans new `client_write(ClusterCommand::IncrementEpoch`
call sites: `Justfile:888-920` (`lint-failover-atomicity`, ban at `:901`).

There is **no gossip epoch plane**: `ping-sent`/`pong-recv` are hardcoded `0 0`
(`frogdb-server/crates/cluster/src/wire.rs:92`), failure detection is leader-only
(`failure_detector.rs:8-9`), and gossip-dependent Redis tcl suites are excluded
(`frogdb-server/crates/redis-regression/src/lib.rs:83-95`).

### 2.3 What consumes epoch values

**Wire surfaces**

- `CLUSTER NODES` field 7 = raw per-node `config_epoch` — `frogdb-server/crates/cluster/src/wire.rs:91, 136`
  (the only wire emission of an epoch).
- `CLUSTER SLOTS` / `CLUSTER SHARDS` carry **no** epoch (`wire.rs:171-214`;
  `commands/cluster/mod.rs:412-422`, `:458-500`).
- MOVED / ASK redirects carry **no** epoch: `frogdb-server/crates/server/src/slot_migration/redirect.rs`,
  `routing.rs`, `events.rs`, `connection/guards.rs`, `connection/dispatch.rs` contain zero `epoch`
  references. Slot-config authority in FrogDB is the Raft log, not epoch comparison.

**Operator surfaces, all reporting the raw counter (unfolded)**

- HTTP admin API: `frogdb-server/crates/server/src/admin/handlers.rs:45, 117, 127`
  (`ClusterStateResponse.config_epoch`).
- Debug web UI: `frogdb-server/crates/server/src/debug_providers.rs:138` (overview), `:174` (per-node);
  `frogdb-server/crates/debug/src/web_ui/state.rs:207-208, 232-233`;
  `frogdb-server/crates/debug/src/web_ui/handlers.rs:611, 626, 835-836, 857`.

**Clients / tooling**

- No Redis client library reads `cluster_current_epoch`; routing is `CLUSTER SLOTS`/`SHARDS` +
  MOVED. Grep confirms no consumer in `frogctl`, benchmarks, Prometheus metrics, Grafana dashboards,
  or Python scripts.
- `frogctl cluster check` / `fix` / `info` are unimplemented stubs
  (`frogctl/src/commands/cluster.rs:123-135`, `check` bails at `:128-129`). When implemented, these
  are the natural `cluster_current_epoch` consumers (`redis-cli --cluster fix` sets epochs relative
  to `currentEpoch`).
- Jepsen reads the per-node epoch, not INFO:
  `testing/jepsen/frogdb/src/jepsen/frogdb/cluster_db.clj:207` (`:config-epoch (nth parts 6)`),
  `.../leader_election.clj:41` (docstring at `:32` still promises `:term`, a stale reference).

**Tests**

| Test | file:line | Path used | Depends on the fold? |
|---|---|---|---|
| `test_node_restart_preserves_raft_state` | `integration_cluster.rs:4521` (reads `:4541`, `:4558`, asserts `>=` at `:4564`) | harness (synthesized) | No — comment "Get current epoch (term)" is already wrong |
| `test_cluster_set_config_epoch_returns_ok` | `:5988` (reads `:6004`, `:6019`, strict `>` at `:6021`) | harness (synthesized) | No — already asserts the unfolded semantics |
| `test_current_epoch_gte_my_epoch_invariant` | `:6722` (assert `:6759-6763`) | RESP | Partly — the fold currently guarantees it; a ratchet must replace that guarantee |
| `test_cluster_epoch_increases_after_failover` | `:6776` (strict `>` at `:6823-6828`) | RESP | **Yes** — kills the leader and expects a strict increase; today the term bump satisfies it |
| `test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change` | `:6860` (`:6902`, `:6953`, strict `>` at `:6967`) | RESP | **Yes** — `:6967` exists precisely to pin the fold |
| `test_cluster_info_epoch_monotonic_across_failover` | `:7002` (`:7027`, `:7083`, `:7096`) | RESP | Yes (weakened to non-decrease *because* of the fold) |
| `test_cluster_epoch_persists` | `:9246` (asserts raw per-node equality at `:9348-9353`) | `CLUSTER NODES` | No (issue 16 deliberately avoided INFO) |
| `test_fold_current_epoch_*` | `commands/cluster/mod.rs:784, 793, 800, 805, 811` | unit | Yes — these are the fold's spec |
| `test_config_epoch_round_trips_through_storage_restart` | `cluster/src/storage.rs:475` | unit | No |
| Counter unit tests | `cluster/src/state.rs:623, 1155, 1197, 1521-1596, 1753, 1770, 1821, 2035` | unit | No |

**Test-harness fidelity defect (found while writing this PRD).**
`ClusterTestHarness::get_cluster_info` (`frogdb-server/crates/test-harness/src/cluster_harness.rs:634-655`)
does **not** issue `CLUSTER INFO`; it reads cluster state directly and synthesizes the struct, setting
`cluster_current_epoch: snapshot.config_epoch` **and** `cluster_my_epoch: snapshot.config_epoch`
(`:652-653`). So (a) every harness-based test exercises unfolded semantics the real server does not
produce, and (b) `cluster_my_epoch` is the cluster-wide counter in the harness but the per-node value
in the server — the `current >= my` invariant is vacuously true there. The real RESP path is only
exercised through `parse_cluster_info` (`frogdb-server/crates/test-harness/src/cluster_helpers.rs:51,
139-140`).

### 2.4 Durability story

The Raft **log** is durable; the **state machine** is not.

- State machine state is in-memory only: `ClusterState { inner: Arc<RwLock<ClusterStateInner>> }`
  (`cluster/src/state.rs:20-27`); the state machine holds no storage handle
  (`state.rs:200-209`). Restart constructs it empty (`server/src/server/cluster_init.rs:133-135`).
- `build_snapshot` (`state.rs:436-470`) and `get_current_snapshot` (`state.rs:406-433`) serialize the
  live in-memory state to a `Cursor<Vec<u8>>` (`types.rs:30`) and never write to RocksDB.
  `get_current_snapshot` returns `None` when `last_applied_log` is `None` (`state.rs:411-413`), which
  is always the case immediately after a restart.
- The log lives in RocksDB at `<data_dir>/raft` (`server/src/recovery/cluster.rs:23-24`), CFs
  `raft_logs` / `raft_meta` (`cluster/src/storage.rs:18-19`); appends use default `WriteOptions`
  (no per-append fsync, `storage.rs:333-335`).
- **Purge is durable, the snapshot that justified it is not.** `purge` deletes entries `<= index` and
  persists `KEY_LAST_PURGED` (`storage.rs:369-400`, `:389`); `get_log_state` reads it back
  (`storage.rs:249, 270-273`). Raft config takes openraft defaults except timers
  (`cluster_init.rs:293-298`) → `snapshot_policy = LogsSinceLast(5000)`,
  `max_in_snapshot_log_to_keep = 1000`. After a purge, a restart finds `last_applied = None` and no
  snapshot, so openraft rebuilds a snapshot from the **empty** state machine; the purged prefix
  (nodes, slot assignments, `config_epoch`) is unrecoverable locally and must be re-derived from a
  live leader.
- `save_committed` writes `KEY_COMMITTED` (`storage.rs:298-308`) but `read_committed` is not
  overridden, so openraft's default `Ok(None)` applies and the key is write-only.
- By contrast the **Raft term is durably persisted**: `save_vote` writes `KEY_VOTE` and explicitly
  flushes (`storage.rs:290-296`); openraft's `Vote` embeds the term.

Net effect today: the fold takes the *durable-but-local* term and the *replicated-but-fragile*
counter and reports their max — so a counter regression after a purged restart is partially masked by
a plausible-looking term. Removing the fold makes such a regression visible, which is desirable, but
it raises the priority of persisting the state machine.

### 2.5 Documentation

- `website/src/content/docs/architecture/clustering.md:148-169` (Config Epoch), `:171-210` (the whole
  "`CLUSTER INFO`'s current epoch folds in the Raft term" section added by issue 47). Note `:209`
  cites "issue 63" for collision detection — the correct reference is issue **64** (63 is the
  errorstats dispatch-stage gap).
- `website/src/content/docs/operations/clustering.md:146`;
  `website/docs-spec/specs/architecture/clustering.md:11, 40`;
  `website/docs-spec/specs/operations/clustering.md:148`.
- `frogdb-server/CONTEXT.md:29, 33-35, 153, 166, 171-172`;
  `frogdb-server/docs/adr/0001-raft-cluster-metadata.md:3`.
- `.claude/skills/jepsen-testing/references/troubleshooting.md:94`.
- `frogdb-server/crates/cluster/src/storage.rs:462-473` (doc comment referencing the fold).

---

## 3. Reference behavior (Redis / Valkey)

Redis Cluster keeps two epoch clocks:

- **`currentEpoch`** — a cluster-wide logical clock. Every node keeps its own view and adopts any
  higher value it sees in a gossip packet header, so healthy clusters converge to a single value. It
  is incremented when a replica starts a failover election (the replica requests votes at
  `currentEpoch + 1`, and the increment happens per *election attempt*, including failed ones), and
  by `CLUSTER BUMPEPOCH`.
- **`configEpoch`** — per-master, intended to be unique cluster-wide. It is the authority for slot
  ownership conflicts: when two masters claim the same slot, the higher `configEpoch` wins, and the
  loser reconfigures. A promoted replica claims the `currentEpoch` of the election it won as its new
  `configEpoch`. `redis-cli --cluster create` hands out distinct epochs via `CLUSTER SET-CONFIG-EPOCH`;
  `clusterHandleConfigEpochCollision` resolves duplicates by having the node with the smaller node ID
  bump `currentEpoch` and re-claim it as its own `configEpoch`.

Invariants that clients and tooling rely on:

1. **`currentEpoch >= max(configEpoch)` on every node.** Guaranteed at the source — a `configEpoch`
   is only ever set to a value claimed from `currentEpoch`, and `currentEpoch` ratchets up to any
   larger value observed. Tooling that mints a fresh epoch (`--cluster fix`) computes it from
   `currentEpoch`, so a `currentEpoch` that trails a live `configEpoch` produces immediate collisions.
2. **`currentEpoch` converges across nodes.** The Redis test suite's cluster helpers wait for all
   nodes to agree on `currentEpoch` before asserting a stable configuration; a value that differs
   per node with no convergence mechanism is not usable that way.
3. **`configEpoch` uniqueness across masters.** This — not `currentEpoch` drift — is what
   `redis-cli --cluster check` flags (already established by issue 47's verification and issue 64).
4. **Epoch as slot-config authority.** Only relevant to gossip-based conflict resolution; MOVED
   replies never carry an epoch. FrogDB resolves slot config through the Raft log instead, so
   FrogDB's epochs are *observability + collision-resolution* values, not routing authority.

**What breaks when the reported epoch inflates spuriously.**

- *No cross-node agreement.* FrogDB's fold uses the **local** Raft term. A follower that has not seen
  the latest election, or a partitioned node repeatedly campaigning (standard Raft: each attempt
  raises the local term), reports a different `cluster_current_epoch` than the leader for identical
  replicated state. Invariant 2 is unachievable by construction. This is strictly worse than Redis,
  where the gossip header ratchet drives convergence.
- *Unbounded inflation from a flapping node.* A node isolated for an hour can report an epoch
  hundreds above the cluster's actual config epoch, then converge back down when it rejoins and its
  reported value is dominated by... nothing — `metrics().current_term` never decreases, but the
  *reported* value across the cluster is inconsistent, and a monitoring rule "alert when the cluster
  epoch moves" fires on election churn with zero topology change.
- *Masking of real topology events* (already pinned): a `config_epoch` bump is invisible whenever the
  term dominates, so `cluster_current_epoch` cannot be used as a topology-change detector — the exact
  use an operator would reach for.
- *Future tooling hazard.* Once `frogctl cluster check/fix` and a real `CLUSTER SET-CONFIG-EPOCH`
  exist (both are stubs today), a fix tool that mints "next free epoch" from an inflated
  `cluster_current_epoch` would stamp per-node epochs far above the replicated counter, leaving the
  counter permanently behind live `configEpoch`s and breaking invariant 1 at the source. Landing the
  redesign **before** issue 64's resolution policy and before implementing `SET-CONFIG-EPOCH` avoids
  building tooling on top of a value that does not mean what tooling assumes.
- *Internal inconsistency.* `CLUSTER INFO` is the only surface that folds; `CLUSTER NODES`, the admin
  API, the debug UI and the harness report the raw counter. An operator comparing the debug UI's
  "Config Epoch" tile with `CLUSTER INFO` sees two different numbers for the same thing.

Nothing in the Redis client ecosystem depends on `cluster_current_epoch` monotonicity across the
cluster — clients ignore the field entirely. The consumers of the invariant are humans, `--cluster
check`-style tooling, and tests. That makes this a low-compatibility-risk change.

---

## 4. Design options

### Option A — Stop folding; report the counter; ratchet at the source (recommended)

1. `cluster_current_epoch` = `snapshot.config_epoch` (the replicated counter). Delete
   `fold_current_epoch` and the term read from the `CLUSTER INFO` path.
2. Restore invariant 1 **at the source**: `AddNode` ratchets the cluster-wide counter to any larger
   incoming per-node epoch —
   `inner.config_epoch = inner.config_epoch.max(node.config_epoch)` in
   `cluster/src/commands.rs:25-56`. This mirrors Redis's "adopt the higher epoch you observe" rule,
   is replicated and deterministic (it happens inside `apply_command`, so every node computes the
   same result), and closes the only path that can make `cluster_my_epoch > cluster_current_epoch`.
3. Expose the Raft term as its own field rather than deleting the information: add
   `cluster_raft_term:<n>` to `CLUSTER INFO` (cluster mode only; `0` in standalone), and surface it
   in the admin API and debug UI alongside `config_epoch`.

Pros: the reported value becomes replicated, cluster-consistent, strictly increasing on every
epoch-owning event, and identical to what every other FrogDB surface already reports; enables a
real cross-node agreement test; no new Raft traffic; the term remains observable.
Cons: `cluster_current_epoch` no longer moves on a pure re-election, so operators watching it for
"the control plane moved" must watch `cluster_raft_term` instead (a strictly better signal — it is
the actual term); a purged-log restart can now visibly regress the value (see §5 risks).

### Option B — Keep the fold, but persist the ratchet

On observing `raft_term > config_epoch`, commit an `IncrementEpoch` (or a new
`RaiseEpochTo(term)` command) so the stored counter catches up, then report the stored counter.

Rejected as specified: the observation point is `CLUSTER INFO`, i.e. a **read path issuing Raft
writes** — unbounded write amplification from a monitoring loop, and it cannot work on a follower
or during a quorum outage (exactly when INFO is most likely to be polled). The only sane form of
"persist the ratchet" is Option C.

### Option C — Redis-faithful: elections bump the counter

Have each node, on becoming Raft leader, propose one `IncrementEpoch` (or `RaiseEpochTo(term)`) as
part of leadership initialization. The counter then absorbs election events legitimately — replicated,
durable, cluster-consistent — and the reporting site still just reads the counter (no fold).

This is the closest structural mapping of Redis semantics (Redis bumps `currentEpoch` on failover
elections, which are exactly Raft elections here).

Pros: preserves today's "epoch moves when the control plane moves" property, without the local-term
divergence; keeps `cluster_current_epoch` a single replicated number.
Cons: one extra Raft entry per leadership change (on top of the leader's blank entry); the counter
inflates on election churn, so `--cluster fix`-minted epochs grow with instability; requires
amending the `lint-failover-atomicity` gate (`Justfile:888-920`, ban at `:901`) which currently
forbids new `client_write(ClusterCommand::IncrementEpoch)` sites; and it buys nothing operationally
because FrogDB epochs are not routing authority (redirects carry no epoch). Option A + a real
`cluster_raft_term` field delivers the same operator signal for free.

### Option D — Status quo + documentation only

Already done by issue 47. Leaves three inconsistent epoch semantics in the product, blocks a
cross-node agreement test, and lands issue 64's collision tooling on top of a value tooling
misreads. Rejected.

### Comparison

| Property | A (unfold + ratchet) | B (read-path ratchet) | C (election bump) | D (status quo) |
|---|---|---|---|---|
| All nodes report the same value | yes | yes | yes | **no** |
| `current >= max(per-node)` guaranteed | yes (at source) | yes | yes | only by accident of `max()` |
| Detects a topology change | yes (strictly increases) | no (still folded) | yes | **no** |
| Reports control-plane churn | via `cluster_raft_term` | yes | yes | yes |
| Raft writes added | none | **on every INFO** | 1 per election | none |
| Consistent with NODES / admin / debug UI | yes | no | yes | **no** |
| Safe base for issue 64 + `--cluster fix` | yes | no | yes | **no** |

---

## 5. Recommendation

**Adopt Option A**, in this order: land the `AddNode` ratchet first (so invariant 1 holds by
construction), then remove the fold, then add `cluster_raft_term`. Treat Option C as a deferred,
independently-decidable follow-up — Option A does not preclude it, since both report the same field
from the same replicated counter.

Coordinate the ratchet with issue 64: if 64's resolution policy already rewrites `AddNode` to
validate/renumber a colliding incoming epoch, the ratchet belongs in that same edit (one change to
`commands.rs:25-56`, one set of unit tests). If 64 lands first, this PRD's task T1 becomes "verify
64's policy also ratchets the counter, and add the missing case if not".

### Risks and mitigations

| Risk | Assessment | Mitigation |
|---|---|---|
| A purged-log restart makes the reported epoch visibly regress (§2.4) | Real. Today the durable term partially masks it. Requires ≥5000 log entries then a restart before the leader re-replicates | Task T7 (persist the state-machine snapshot). Until then, document that a regression indicates state loss, and keep the issue-16 per-node equality test as the tripwire |
| `CLUSTER RESET HARD` resets the counter to 0 (`commands.rs:422`), so the value is not globally monotonic | Real but Redis-parity (`CLUSTER RESET HARD` resets `currentEpoch` there too) | Document explicitly; the current docs' "monotonic" claim (`clustering.md:199`) is already wrong and must be corrected either way |
| `test_cluster_epoch_increases_after_failover` (`:6776`) currently passes on the term bump | Will need reworking — after unfolding, a leader kill only moves the epoch once the failure detector commits `MarkNodeFailed`/`Failover` | Task T5: replace the fixed 2s sleep with a poll for the real topology event, or drive an explicit `CLUSTER FAILOVER` |
| Adding a non-Redis `cluster_raft_term` line to `CLUSTER INFO` | Low — `CLUSTER INFO` is a `key:value` bulk string; clients and `redis-cli` read named fields and ignore unknown ones. FrogDB already emits fabricated gossip-stat lines (`mod.rs:340-346`) | Document as a FrogDB extension; alternative placement in `INFO`'s cluster section is an open question (§8) |
| Issue 64 lands concurrently in `commands.rs:25-56` | Certain — same function | Sequence T1 with the 64 implementer; do not merge a competing edit |
| Operators/dashboards keyed on today's inflating value | Negligible — pre-production software, no external consumers found (§2.3) | Release note in the docs update |

---

## 6. Testing plan

Extends issue 47's pinning tests rather than deleting them: each one keeps its "why the naive bound
is wrong" commentary and gains the new, stronger contract.

**Unit**

1. `cluster/src/commands.rs` (or `state.rs` tests): `AddNode` with an incoming `config_epoch`
   greater than / equal to / less than the counter — assert the counter ratchets, stays, stays.
   Assert the incoming node's own epoch is preserved (or renumbered, per issue 64's policy).
2. `commands/cluster/mod.rs`: replace the five `test_fold_current_epoch_*` tests
   (`:784, 793, 800, 805, 811`) with tests for the new reporting behavior — `cluster_current_epoch`
   equals the counter, `cluster_raft_term` equals the metrics term, both `0` in standalone.
   Keep the "do not assert `INFO epoch <= max(NODES epoch)`" commentary; that bound is still wrong
   (the counter legitimately exceeds every per-node epoch after `IncrementEpoch`/`MarkNodeFailed`).
3. `cluster/src/state.rs`: property-style check that after any command sequence,
   `config_epoch >= max(node.config_epoch)` — the invariant the ratchet exists to preserve.

**Integration** (`frogdb-server/crates/server/tests/integration_cluster.rs`, RESP path via
`parse_cluster_info`, never the synthesized harness value)

4. Rewrite `test_cluster_info_epoch_vs_nodes_epoch_after_reelection_no_topology_change` (`:6860`):
   after a pure re-election, assert `cluster_current_epoch` is **unchanged** and still
   `>= max(per-node config_epoch)`; assert `cluster_raft_term` **did** increase. Delete the strict
   `>` at `:6967` (it pins the fold) and replace its comment with the new contract.
5. Strengthen `test_cluster_info_epoch_monotonic_across_failover` (`:7002`) from non-decrease to
   **strict increase** — the masking case that forced the weaker assertion is gone.
6. Rebase `test_cluster_epoch_increases_after_failover` (`:6776`) on a real topology event
   (poll for the failure detector's commit, or issue `CLUSTER FAILOVER`).
7. New: **cross-node agreement** — after convergence, every node's `cluster_current_epoch` is equal.
   This test is impossible under the fold and is the headline benefit of the change.
8. New: `cluster_current_epoch >= cluster_my_epoch` on a cluster where a node joined carrying a
   large `config_epoch` (drives the ratchet through `CLUSTER MEET`/`AddNode`); pairs with issue 64's
   collision test.
9. Cross-restart (issue 16 residue): extend `test_cluster_epoch_persists` (`:9246`) to also assert
   `cluster_current_epoch` equality pre/post restart — now a meaningful assertion, whereas before the
   term made it vacuous. Add a purge-then-restart test once T7 lands (today no test exercises purge,
   `install_snapshot`, or startup with a non-empty `last_purged`).
10. Issue 64 interplay: after collision resolution mints a fresh epoch, assert
    `cluster_current_epoch >= every per-node config_epoch` and that all nodes agree.

**Harness fidelity**

11. Fix `ClusterTestHarness::get_cluster_info` (`test-harness/src/cluster_harness.rs:634-655`) to
    either issue the real `CLUSTER INFO` or mirror the server's field semantics
    (`cluster_my_epoch` = this node's `NodeInfo.config_epoch`, not the cluster counter). Add a test
    asserting the harness value equals the RESP value for both epoch fields — otherwise the ~25
    harness-based call sites keep testing a different server than the one shipped.

---

## 7. Task breakdown

Ordered; each task is independently reviewable and leaves the tree green.

| # | Task | Files | Notes |
|---|---|---|---|
| T1 | Ratchet the cluster-wide counter on `AddNode` | `frogdb-server/crates/cluster/src/commands.rs:25-56`; unit tests in `crates/cluster/src/state.rs` | **Coordinate with issue 64** — same function. Land first: it makes `current >= my` true by construction before the fold is removed |
| T2 | Remove the fold | `frogdb-server/crates/server/src/commands/cluster/mod.rs` — delete `fold_current_epoch` (`:251-253`) and the term read (`:288-291`); report `snapshot.config_epoch` at `:321`; rewrite the doc comment `:216-250`; replace unit tests `:778-822` | Keep the "`<=` bound is wrong" note |
| T3 | Expose the Raft term separately | `commands/cluster/mod.rs` (new `cluster_raft_term` line in both INFO branches); `crates/server/src/admin/handlers.rs:37-48, 110-130`; `crates/server/src/debug_providers.rs:125-142`; `crates/debug/src/web_ui/state.rs:225-235` + `handlers.rs:611-630`; `crates/test-harness/src/cluster_helpers.rs:45-145` (parser field) | Decide placement first (§8 Q1) |
| T4 | Fix harness `CLUSTER INFO` fidelity | `frogdb-server/crates/test-harness/src/cluster_harness.rs:634-655` | Pre-req for T5's assertions to mean anything; may surface pre-existing failures in the ~25 harness call sites — fix them, do not weaken the harness |
| T5 | Rewrite/extend integration tests | `frogdb-server/crates/server/tests/integration_cluster.rs:4521, 5988, 6722, 6776, 6860, 7002, 9246` + new tests | Plan items 4-11 |
| T6 | Documentation | `website/src/content/docs/architecture/clustering.md:148-210` (rewrite the fold section as "current epoch vs. Raft term"); `website/src/content/docs/operations/clustering.md:146`; `website/docs-spec/specs/architecture/clustering.md:11, 40`; `website/docs-spec/specs/operations/clustering.md:148`; `frogdb-server/CONTEXT.md:29, 33-35, 153, 166, 171-172`; `crates/cluster/src/storage.rs:462-473` | Also fix the wrong "issue 63" reference at `clustering.md:209` → issue 64, correct the "monotonic" claim (`:199`) to note `CLUSTER RESET HARD`, and fix the stale `:term` docstring at `testing/jepsen/frogdb/src/jepsen/frogdb/leader_election.clj:32` |
| T7 | Durability: persist the state-machine snapshot | `frogdb-server/crates/cluster/src/state.rs:406-470` (write/read a snapshot through storage); `crates/cluster/src/storage.rs:18-19` (new CF or meta key), `:369-400` (purge ordering); optionally override `read_committed` | Independent of T1-T6 but required before claiming the epoch never regresses. **File as its own issue if it is not folded into this plan** — it is the open residue from issue 16 |
| T8 | Deferred: implement `CLUSTER SET-CONFIG-EPOCH` for real | `frogdb-server/crates/server/src/commands/cluster/admin.rs:441-469`; new `ClusterCommand` variant in `crates/cluster/src/types.rs` + `commands.rs`; `Justfile:888-920` lint gate | Only meaningful once the counter is authoritative (T1+T2). Prerequisite for a real `frogctl cluster check/fix` (`frogctl/src/commands/cluster.rs:128-129`) |

Suggested sequencing: T1 → T2 → T3 → T4 → T5 → T6, with T7 in parallel and T8 deferred behind issue 64.

---

## 8. Open questions

1. **Where does the Raft term belong?** `CLUSTER INFO` as `cluster_raft_term` (proposed: co-located
   with the value it replaces), or `INFO`'s cluster section, or operator surfaces only? Redis has no
   equivalent field, so any placement is a FrogDB extension.
2. **Should Option C (election bumps the counter) be adopted later?** It would restore
   "epoch moves on control-plane churn" in a replicated way, at the cost of Raft traffic and epoch
   inflation. Defer until issue 64's tooling exists and there is a concrete operator need.
3. **Should `AddNode`'s ratchet also renumber the incoming node?** That is issue 64's decision
   (Redis renumbers the lower node ID on collision). This PRD only requires the counter never trail a
   live per-node epoch.
4. **Does T7 belong in this plan or as its own issue?** It is the last open residue of issue 16 and
   is independently valuable; the recommendation is a separate issue referenced from here.

---

## 9. Implementation notes (2026-07-28)

Implemented on branch `worktree-agent-a8eb890ba0f01c596`, six commits:

| Commit | Task | Summary |
|---|---|---|
| `41c1727d` | T2, T3 | Fold deleted; `cluster_current_epoch` = counter verbatim; new `cluster_raft_term` |
| `7805e778` | T4 | Harness `get_cluster_info` issues a real `CLUSTER INFO` |
| `16340389` | T7 | `ClusterSnapshotStore` — state-machine snapshots persist across restart |
| `0d25d20a` | T1 | Invariant sweep test for counter-dominates-node-epoch |
| `b4d69430` | (fallout) | Failure detector reconciles verdicts; harness admin-port routing |
| `e4959e25` | T6 | Docs |

### T1 — verified, not re-implemented

Issue 64's `ClusterStateInner::reconcile_incoming_epoch` (`crates/cluster/src/state.rs`) landed on
main and does exactly what §5 assumed: an uncontested nonzero claim raises the cluster-wide counter
to at least that value, a colliding claim is admitted at a freshly minted epoch above every claimed
one, and a zero claim is recorded as-is (with an already-known nonzero epoch preserved rather than
reset). No ratchet code was added. What was added is
`test_config_epoch_counter_dominates_every_node_epoch_across_command_sequence` — a sweep over a
mixed command sequence asserting `config_epoch >= max(per-node config_epoch)` after every step,
because the failure mode is a *future* command forgetting the invariant, which per-command tests
cannot catch.

### T2/T3 — deviations from the plan

- Open question §8 Q1 resolved as proposed: `cluster_raft_term` in `CLUSTER INFO`, co-located with
  the value it replaces. It is documented as node-local and unreplicated, so nobody mistakes it for
  a cluster-wide clock.
- The `CLUSTER INFO` field list had been duplicated across the cluster and standalone branches. It
  now lives once in a `ClusterInfoReport` struct that renders the reply and is unit-testable without
  a `CommandContext`; the standalone branch is the same renderer with zeroed fields. This was not in
  the plan but the alternative was a third copy of the field list.
- `LeaderReader` → `RaftMetricsReader` (it reads more than the leader now). Admin API gains
  `raft_term`, the debug web UI a "Raft Term" tile. All report `None` without Raft, so "no cluster"
  stays distinguishable from "term 0".

### T4 — what the honest harness revealed

`get_cluster_info` synthesized a `ClusterInfo` from the node's in-process `ClusterState`, so ~25
tests asserted against data that had never crossed the wire. Making it send the command surfaced
two real problems:

1. **`-NOADMIN` on every `CLUSTER` call to an admin-enabled node.** `CLUSTER` carries
   `CommandFlags::ADMIN`, so with an admin port configured the client port refuses it
   (`connection/guards.rs`). Four harness tests were timing out in convergence loops with no
   diagnosis. Fixed with `ClusterTestNode::try_send_admin_aware`, which routes to whichever port
   the node accepts admin commands on. Confirmed pre-existing (stashing the detector fix left the
   same four failing). See "Risks" below — the underlying gating is a product question, not a test
   question.
2. **The failure detector never flagged a dead leader.** See below.

`try_send` was added alongside `send` (which retries then panics) so polling loops against a
stopped node get an `Err` instead of a panic.

### Fallout — failure detector was edge-triggered (out of plan scope, real bug)

`test_cluster_epoch_increases_after_failover` passed before only because the term fold moved the
reported epoch on election. Rebased onto the real counter it failed, and the reason was not the
test: `FailureDetector` proposed `MarkNodeFailed` inline, at the instant a probe crossed
`fail-threshold`, and only if this node was leader at that instant. The latch is one-shot, so a
survivor that crossed the threshold *while it was still a follower* never retried after winning the
election — the shape of every leader-death failover, since the survivors accumulate missed probes
during the ~1s election. The dead node stayed unflagged indefinitely: no epoch bump, no
auto-failover.

Fixed by separating probing from propagation. All nodes record probe outcomes locally; the leader
reconciles its `HealthTable` against the replicated flags once per check interval
(`FailureDetector::reconcile_topology`), writing `MarkNodeFailed`/`MarkNodeRecovered` only on
disagreement. Level-triggered convergence does not care who was leader when the threshold was
crossed; a converged cluster writes nothing, so there is no epoch churn; `LocalVerdict::Unknown`
(never probed, or last-seen older than `check_interval * (fail_threshold + 2)`) leaves replicated
flags alone so a fresh leader does not flap peers it has not probed yet.

Second-order test fallout: `test_raft_snapshot_during_migration` killed a slot-owning primary for
good and then asserted the cluster returned to `cluster_state:ok`. With the FAIL flag now actually
landing, `ok` is unreachable and the old assertion was asserting the bug. It now asserts what the
test means — the survivors agree on (known nodes, assigned slots, epoch) — via the new
`ClusterTestHarness::wait_for_topology_agreement`.

Also fixed: `test_join_with_large_epoch_keeps_current_epoch_above_my_epoch` read `CLUSTER INFO`
before `CLUSTER NODES`, which manufactures a failure out of read skew when the join commits between
the two reads. Reversed.

### T7 — snapshot persistence

`ClusterSnapshotStore` (`crates/cluster/src/storage.rs`) stores openraft's `SnapshotMeta` plus the
serialized `ClusterStateInner` under meta key `state_machine_snapshot` in the same RocksDB as the
log, so a snapshot and the log it authorizes purging cannot land in different databases.
`ClusterStateMachine::attach_snapshot_store` restores any persisted snapshot at startup and is
wired in `server/src/server/cluster_init.rs`; the store is opt-in, so existing in-memory tests keep
their old behaviour. Regression test:
`test_state_machine_snapshot_survives_restart_without_log_replay` — build a snapshot, drop the
state machine, re-attach, and assert `config_epoch` is restored with no log replay available.

Deviation: `read_committed` was **not** overridden. `append` does not fsync, so a restored commit
index can outrun the surviving log tail, which openraft treats as corruption. Left as-is
deliberately.

### T8 — deferred, not implemented

`CLUSTER SET-CONFIG-EPOCH` still parses its argument, discards it, and issues a plain
`IncrementEpoch`. Unchanged by this work.

### Test results

All local, with `RUSTC_WRAPPER=""` and Homebrew RocksDB/Snappy paths:

| Command | Result |
|---|---|
| `cargo nextest run -p frogdb-cluster` | 126/126 pass |
| `cargo nextest run -p frogdb-server -E 'binary(main) and test(/integration_cluster::/)' --test-threads 4` | 169/169 pass |
| `cargo nextest run -p frogdb-server -E 'test(/failure_detector\|health_table\|epoch_increases_after_failover\|large_epoch/)'` | 18/18 pass |
| `cargo nextest run -p frogdb-server -E 'test(/integration_pubsub::\|integration_tls_extended::/)' --test-threads 4` | 161/161 pass |
| `cargo nextest run --manifest-path frogdb-operator/Cargo.toml --test-threads 2` | pass (separate workspace; harness signature change) |
| Final re-run after `just fmt`, all of the above cluster scope in one invocation (`-p frogdb-cluster -p frogdb-server -E 'test(/failure_detector\|health_table/) or binary(main) and test(/integration_cluster::/) or package(frogdb-cluster)'`) | **311/311 pass**, 189s, zero flakes |

Full suite, `just lint`, and any testbox run were deliberately not executed — the orchestrator gates
those.

### Risks / known gaps

1. **`CLUSTER` is ADMIN-gated wholesale.** With an admin port configured, read-only discovery
   subcommands (`INFO`, `NODES`, `SLOTS`, `SHARDS`) are refused on the client port with `-NOADMIN`.
   Any cluster-aware Redis client bootstraps via `CLUSTER SLOTS`/`SHARDS` on the normal port, so
   this looks like it breaks client-side clustering whenever an admin port exists. Not touched here
   (out of scope, and the fix is a policy decision: per-subcommand flags vs. a read-only exemption).
   Worth its own issue.
2. **Reconciliation adds a per-interval scan** over `get_all_nodes()` on the leader. O(nodes) with
   no allocation beyond one `Vec` per tick at `check_interval` (default 100ms in tests, heartbeat
   interval in production). Fine at current cluster sizes; would want batching at hundreds of nodes.
3. **`cluster_state:fail` is now reachable where it previously was not** in any deployment that
   permanently loses a slot-owning primary. That is correct behaviour, but it is a visible change
   for anyone alerting on the field.
4. **`cluster_raft_term` is node-local.** Two nodes can legitimately report different values.
   Documented, but it is a new field an operator could misread as cluster-wide.
5. The `frogdb-operator` workspace consumes the test harness; its build is verified but its tests
   were not run under the orchestrator's full gate.

---

## 10. Review-fix round (2026-07-28)

Adversarial review returned **FIX-FIRST** with 18 findings. All 18 addressed in this round (F12
partially — see disagreements). Files touched: `crates/cluster/src/{storage,state}.rs`,
`crates/server/src/{failure_detector.rs,commands/cluster/mod.rs}`,
`crates/test-harness/src/cluster_harness.rs`,
`crates/server/tests/{integration_cluster,integration_admin_port}.rs`,
`testing/jepsen/.../leader_election.clj`, both clustering docs, plus a new issue file.

### Blocking

- **F1 — clippy red.** `cluster_harness.rs`: `last_seen` declared without an initializer, so the
  dead store is gone. `just lint` for both crates is green.
- **F2 — `save()` did not fsync what it wrote.** `DB::flush()` flushes the *default* CF, which this
  store never writes. Replaced with a `WriteBatch` + `WriteOptions::set_sync(true)` `write_opt`,
  so the record is durable before `save` returns and the doc comment is now true.
- **F3 — last-writer-wins on the snapshot key.** `ClusterSnapshotStore::save` is now monotonic:
  under a `save_lock` it reads the stored `meta.last_log_id` and no-ops unless the incoming
  snapshot is strictly newer. The lock is owned by `ClusterStorage` and cloned into every handle,
  so the read-compare-write cannot interleave across the spawned builder and the inline installer.
  Pinned by `test_snapshot_save_never_moves_backwards`.
- **F4 — Raft writes blocked the probe loop.** `reconcile_topology` is sync again: it decides, then
  `spawn_reconcile`s each `MarkNodeFailed`/`MarkNodeRecovered` onto its own task, guarded by an
  in-flight `HashSet<NodeId>` so a stuck write is not re-issued every tick. All three
  `client_write` calls are wrapped in `tokio::time::timeout(raft_write_timeout())`
  (`check_interval * 5`, floored at 1s).
- **F5 — unsatisfiable equality in `test_cluster_current_epoch_agrees_across_all_nodes`.** The loop
  now breaks on *all nodes equal* **and** `>= CLAIMED_EPOCH`; the phantom joiner legitimately gets
  FAIL-latched and pushes the counter past the claim, which the old `==` could never accept again.
- **F6 — `cluster_state:fail` for a slotless failed primary.** The predicate is now
  `count_slot_health(&snapshot).fail > 0`, i.e. slot coverage, matching Redis. A FAIL-flagged
  primary owning zero slots no longer downs the cluster; covered by
  `test_count_slot_health_fail_flagged_slotless_primary_leaves_slots_ok`.
- **F7 — asymmetric hysteresis.** `HealthTable` now requires `fail_threshold` *consecutive
  successes* to clear a latched FAIL, mirroring the failure side (`success_count`, reset by any
  failure). Chosen over Redis's time-based `FAIL_UNDO_TIME_MULT` to keep the state machine pure and
  deterministically unit-testable. Pinned by `test_health_table_threshold_latching_is_symmetric`
  and `test_health_table_latch_survives_flapping_recovery`.

### Strongly recommended

- **F8 — vacuous harness-vs-RESP test.** `test_harness_cluster_info_matches_resp_reply` now shapes
  a cluster where the compared fields are all distinguishable (`add_node_claiming` pushes the
  counter off zero; an added replica makes `cluster_size != cluster_known_nodes`), asserts that
  non-vacuity explicitly, then compares seven fields instead of four.
- **F9 — the `-NOADMIN` gap was hidden, not pinned.** Filed
  `.scratch/replication-cluster-rework/issues/05`
  (`needs-triage`) and added
  `test_admin_port_blocks_cluster_discovery_subcommands_on_regular_port`, which pins today's
  behaviour and says in its doc block that fixing issue 05 must *invert* it.

### Cheap

- **F10** — snapshot meta and payload are now two keys in one `WriteBatch`; the payload is stored
  raw instead of as a serde_json integer array (~4x smaller). A meta record without its payload is
  treated as "no snapshot" rather than trusted.
- **F11** — the T1 sweep asserts `Ok` per step (`unwrap_or_else(panic)`), pins the post-reset
  membership and cleared slot map, and `assert_invariant` now also enforces issue 64's headline
  invariant: no two primaries share a nonzero `config_epoch`.
- **F12** — `wait_for_topology_agreement` compares a full `TopologyView` (every known node's
  canonicalized `CLUSTER NODES` line — flags, primary, config epoch, owned slots and
  `[slot->-node]` migration markers — plus the cluster-wide epoch and slots-assigned) instead of
  three aggregate counters. `test_raft_snapshot_during_migration` now *asserts* the migration
  outcome it used to only `eprintln!`.
- **F13** — `test_state_machine_snapshot_survives_restart_without_log_replay` writes the log
  entries, `purge`s them, asserts the purge survived the restart, and drives
  `StorageHelper::get_initial_state` over the reopened storage + state machine — the actual path
  the hazard lives on.
- **F14** — `SuccessOutcome`/`FailureOutcome` deleted; `record_success`/`record_failure` return
  `()`. The only enum left is `ReconcileAction`, which has a production consumer.
- **F15** — standalone `CLUSTER INFO` omits the `cluster_raft_term` line entirely rather than
  claiming term 0. `ClusterInfoReport::raft_term` is `Option<u64>` and the renderer emits nothing
  for `None`. **Deviation from §6 of this PRD**, which specified `0` in standalone; the
  observability-accuracy bar wins.
- **F16** — `MissedTickBehavior::Delay` on the detector's interval, so a stalled tick cannot
  produce a catch-up probe storm.
- **F17** — the `read_committed` comment now states the operative hazard (see disagreement below).
- **F18** — the jepsen `leader_election.clj` namespace docstring no longer says "per term" or
  advertises a `:read-term` op that does not exist; no "term" string remains in the file.

### Fallout the fix round found

`test_concurrent_failover_attempts` and `test_auto_failover_selects_most_caught_up_replica` were
both **latent coin-flips of the same shape as F5**, and F4's non-blocking writes made the latch land
early enough to expose them (`test_concurrent_failover_attempts` failed 3/3 in the sweep, then
passed on a fourth attempt in isolation). Both kill a slot-owning primary in a cluster with no
replica and then assert `cluster_state:ok` — unreachable once the FAIL flag latches, since the
slots are uncovered. Fixed by asserting the true steady state: survivor topology agreement for the
first, and a polled `cluster_state:fail` with `cluster_slots_fail > 0` for the second. Both are now
deterministic (verified 3x back-to-back).

### Test results (all local, `RUSTC_WRAPPER=""`)

| Command | Result |
|---|---|
| `cargo nextest run -p frogdb-cluster` | **127/127 pass** |
| `cargo nextest run -p frogdb-server -E 'test(/cluster/) or test(/failure_detector/) or test(/epoch/) or test(/admin_port/)'` | **266/266 pass** |
| `just lint frogdb-cluster`, `just lint frogdb-server` | green |
| `just check`, `just fmt` | clean |

### Disagreements with the review

1. **F17's mechanism.** The review says `get_initial_state` "purges the log to `last_applied` when
   `last_log_id < last_applied`", making a restored commit index harmless. In openraft 0.9.21 it
   clamps `committed` **up** to `last_applied` and never down to `last_log_id`; the damage lands
   later, when it re-applies `(last_applied, committed]` and `read_log_at_index` cannot find the
   entries. Conclusion (leave `read_committed` unimplemented) is unchanged; the comment now states
   this mechanism, not the review's.
2. **F12, partially declined.** The `slots_assigned == CLUSTER_SLOTS` gate in
   `wait_for_topology_agreement` is kept deliberately: it is the wait condition ("the topology has
   finished settling"), not the agreement predicate, and every caller today runs a fully-slotted
   cluster. A scenario that deliberately leaves slots unassigned should get a variant rather than
   weaken this one.
3. **F7's live coverage.** Driving the latch through `test_flapping_node` under real timing would
   be inherently flaky (it depends on probe cadence vs. kill/restart timing), so the hysteresis
   contract is pinned deterministically in `HealthTable` unit tests, and the integration test only
   asserts the observable consequence: after the flapping stops, no node carries a `fail` flag.

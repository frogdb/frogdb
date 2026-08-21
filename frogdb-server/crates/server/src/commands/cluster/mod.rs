//! CLUSTER command implementation.
//!
//! Provides Redis Cluster protocol commands for:
//! - Cluster state inspection (INFO, NODES, SLOTS, MYID)
//! - Cluster topology management (MEET, FORGET, ADDSLOTS, DELSLOTS)
//! - Key routing (KEYSLOT, COUNTKEYSINSLOT)
//! - Failover coordination (FAILOVER)
//!
//! Cluster commands that modify state (MEET, FORGET, ADDSLOTS, etc.) return
//! `Response::RaftNeeded` which is intercepted by the connection handler.
//! The connection handler executes the Raft operation asynchronously and
//! updates the NetworkFactory after successful commit.

mod admin;

use std::sync::Arc;

use bytes::Bytes;
use frogdb_cluster::wire;
use frogdb_core::{
    AccessSpec, Arity, CLUSTER_SLOTS, Command, CommandContext, CommandError, CommandFlags,
    CommandSpec, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
    slot_for_key,
};
use frogdb_protocol::Response;

// ============================================================================
// CLUSTER - Cluster management command
// ============================================================================

/// Per-slot health breakdown for `CLUSTER INFO`'s `cluster_slots_{ok,pfail,fail}`
/// fields, mirroring Redis semantics: every assigned slot is counted exactly
/// once, bucketed by the FAIL/PFAIL state of the node that currently owns it
/// (`fail` takes precedence over `pfail`, matching `wire::node_health`).
/// Slots owned by a node absent from `snapshot.nodes` (should not happen in
/// practice) are conservatively treated as ok rather than silently dropped.
///
/// **PFAIL is structurally supported but never produced today.** FrogDB has no
/// gossip-based suspicion phase: failure detection is the Raft leader's
/// single-observer TCP probe, which commits `MarkNodeFailed` (setting
/// `flags.fail`) directly -- no production path ever sets `flags.pfail` to
/// `true` (`commands.rs` only ever clears it, in `MarkNodeRecovered`).
/// `cluster_slots_pfail` therefore reports a *derived* 0 rather than a
/// hardcoded one: it is accurate because no node is ever PFAIL, and it will
/// start reporting real counts for free if a suspicion phase is ever added.
/// See `.scratch/testing-improvements/issues/36`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SlotHealthCounts {
    ok: u16,
    pfail: u16,
    fail: u16,
}

fn count_slot_health(snapshot: &frogdb_cluster::ClusterSnapshot) -> SlotHealthCounts {
    let mut counts = SlotHealthCounts::default();
    for owner in snapshot.slot_assignment.values() {
        match snapshot.nodes.get(owner) {
            Some(node) if node.flags.fail => counts.fail += 1,
            Some(node) if node.flags.pfail => counts.pfail += 1,
            _ => counts.ok += 1,
        }
    }
    counts
}

/// The local node's replication offset, as `CLUSTER SHARDS` / `CLUSTER SLOTS`
/// report it.
///
/// This is the *data* replication offset — the same number `INFO replication`
/// publishes as `master_repl_offset` and the same one a replica ACKs — read
/// from the replication tracker in every mode.
///
/// It used to be the Raft last-applied log index whenever Raft was running,
/// which was a different quantity wearing this one's name: Raft carries cluster
/// metadata only (ADR-0001), so that index counted membership and slot-map
/// entries, not bytes of client writes. A client comparing a primary's
/// `replication-offset` against its replica's would have been comparing two
/// numbers from unrelated planes.
fn local_replication_offset(ctx: &CommandContext) -> i64 {
    ctx.replication_tracker
        .map(|tracker| tracker.current_offset() as i64)
        .unwrap_or(0)
}

pub struct ClusterCommand;

impl Command for ClusterCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "CLUSTER",
            docs: frogdb_core::CommandDocs {
                summary: "A container for Redis Cluster commands.",
                since: "3.0.0",
                group: "cluster",
                complexity: Some("Depends on subcommand."),
            },
            arity: Arity::AtLeast(1),
            // No whole-command ADMIN: the admin surface is split per subcommand
            // (`SPLIT_ADMIN_SURFACES` in `frogdb_core::command_spec`), so
            // discovery stays reachable from the client port.
            flags: CommandFlags::STALE,
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::None,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArgCount {
                command: "cluster".to_string(),
            });
        }

        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid subcommand".to_string(),
            })?
            .to_ascii_uppercase();

        match subcommand.as_str() {
            "INFO" => cluster_info(ctx),
            "NODES" => cluster_nodes(ctx),
            "MYID" => cluster_myid(ctx),
            "SLOTS" => cluster_slots(ctx),
            "SHARDS" => cluster_shards(ctx),
            "KEYSLOT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster keyslot".to_string(),
                    });
                }
                cluster_keyslot(&args[1])
            }
            "COUNTKEYSINSLOT" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster countkeysinslot".to_string(),
                    });
                }
                cluster_countkeysinslot(ctx, &args[1])
            }
            "GETKEYSINSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster getkeysinslot".to_string(),
                    });
                }
                cluster_getkeysinslot(ctx, &args[1], &args[2])
            }
            "MEET" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster meet".to_string(),
                    });
                }
                // CLUSTER MEET <ip> <port> [<cluster-bus-port>]
                let cluster_bus_port = if args.len() > 3 { Some(&args[3]) } else { None };
                admin::cluster_meet(ctx, &args[1], &args[2], cluster_bus_port)
            }
            "FORGET" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster forget".to_string(),
                    });
                }
                admin::cluster_forget(ctx, &args[1])
            }
            "ADDSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster addslots".to_string(),
                    });
                }
                admin::cluster_addslots(ctx, &args[1..])
            }
            "DELSLOTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster delslots".to_string(),
                    });
                }
                admin::cluster_delslots(ctx, &args[1..])
            }
            "FAILOVER" => admin::cluster_failover(ctx, &args[1..]),
            "REPLICATE" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster replicate".to_string(),
                    });
                }
                admin::cluster_replicate(ctx, &args[1])
            }
            "RESET" => admin::cluster_reset(ctx, &args[1..]),
            "SAVECONFIG" => admin::cluster_saveconfig(ctx),
            "SET-CONFIG-EPOCH" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster set-config-epoch".to_string(),
                    });
                }
                admin::cluster_set_config_epoch(ctx, &args[1])
            }
            "SETSLOT" => {
                if args.len() < 3 {
                    return Err(CommandError::WrongArgCount {
                        command: "cluster setslot".to_string(),
                    });
                }
                admin::cluster_setslot(ctx, &args[1..])
            }
            "HELP" => cluster_help(),
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown subcommand or wrong number of arguments for '{}'",
                    subcommand
                ),
            }),
        }
    }
}

/// CLUSTER INFO - Returns cluster state information.
///
/// # `cluster_state`
///
/// `fail` means the keyspace is unservable from this node: some assigned slot's
/// owner is FAIL-flagged (`cluster_slots_fail > 0`), this node cannot form a
/// quorum, or Raft reports no usable leader. A FAIL-flagged node that owns no
/// slots does **not** make the cluster `fail` — same rule as Redis, whose
/// `clusterUpdateState` only degrades on slot coverage or majority loss.
///
/// # Epoch fields
///
/// `cluster_current_epoch` is the cluster-wide **replicated** config-epoch
/// counter (`ClusterStateInner::config_epoch`), reported verbatim. It is the
/// same number `CLUSTER NODES`, the HTTP admin API and the debug UI report, so
/// every FrogDB surface agrees on one value, and — because it is replicated
/// through the Raft log rather than derived from node-local runtime state —
/// every node converges on the same value for the same cluster state.
///
/// It moves only on an epoch-owning event (`IncrementEpoch`, `Failover`,
/// `MarkNodeFailed`, or a collision resolution at `AddNode`), which makes it a
/// usable topology-change detector. It is *not* globally monotonic:
/// `CLUSTER RESET HARD` resets it to `0`, exactly as Redis's `CLUSTER RESET
/// HARD` resets `currentEpoch`.
///
/// `cluster_my_epoch` is this node's own `NodeInfo::config_epoch`. The
/// `cluster_current_epoch >= cluster_my_epoch` invariant holds at the source:
/// a per-node epoch is only ever set to a value minted from the counter, and
/// `ClusterStateInner::reconcile_incoming_epoch` ratchets the counter up to any
/// larger epoch an incoming node claims.
///
/// **Do NOT assert `cluster_current_epoch <= max(NODES config_epoch)`.** That
/// bound does not hold and its failing is not a bug: `IncrementEpoch` and
/// `MarkNodeFailed` bump the counter without stamping any node, so the counter
/// legitimately exceeds every per-node epoch. Redis behaves the same way —
/// `currentEpoch` may exceed every `configEpoch`, and `redis-cli --cluster
/// check` flags epoch *collisions*, never exceedance.
///
/// `cluster_raft_term` is the local Raft leadership term
/// (`RaftMetrics::current_term`) — a FrogDB extension with no Redis
/// equivalent, reported as its own field rather than folded into
/// `cluster_current_epoch`. Unlike the epoch it is **node-local and
/// unreplicated**: it moves on every election attempt (including failed ones),
/// so two nodes may legitimately report different terms. Watch it for
/// control-plane churn; watch `cluster_current_epoch` for topology change. The
/// line is **omitted entirely** whenever there is no Raft handle to read a term
/// from — standalone, or cluster state without a wired-up Raft — rather than
/// reporting a term of 0 that does not exist.
///
/// See `.scratch/replication-cluster-rework/epoch-fold-redesign.md`,
/// `.scratch/testing-improvements/issues/47`, and
/// the "Config epoch vs. Raft term" section of
/// `website/src/content/docs/architecture/clustering.md`.
fn cluster_info(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Use ClusterState if available, otherwise return standalone mode info
    let report = if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let slots_assigned = snapshot.slot_assignment.len() as u16;
        let known_nodes = snapshot.nodes.len();

        // Count primaries (cluster_size)
        let cluster_size = snapshot.nodes.values().filter(|n| n.is_primary()).count();

        // Get current node's epoch
        let my_epoch = ctx
            .node_id
            .and_then(|id| snapshot.nodes.get(&id))
            .map(|n| n.config_epoch)
            .unwrap_or(0);

        // Check if we have a leader (quorum) via Raft metrics
        // We use millis_since_quorum_ack to detect if quorum has been lost.
        // If too much time has passed since a quorum ack, the cluster is unhealthy.
        const QUORUM_TIMEOUT_MS: u64 = 2000; // 2 seconds without quorum = fail

        // Per-slot FAIL/PFAIL accounting: a slot only counts as `ok` when its
        // owning node is neither FAIL- nor PFAIL-flagged.
        let health = count_slot_health(&snapshot);

        // `cluster_state` follows Redis: the cluster is only `fail` when the
        // keyspace is actually unservable — some slot's owner is FAIL-flagged —
        // or when this node has lost quorum. A FAIL-flagged primary that owns
        // *no* slots (a freshly joined node, a drained one, a phantom entry
        // from a failed join) costs the cluster nothing and must not flip the
        // field: every client would see `fail` while every key stayed served.
        let has_failed_slot_owner = health.fail > 0;

        // Check if we can form a quorum with reachable nodes (local perspective)
        let has_local_quorum = ctx.quorum_checker.map(|qc| qc.has_quorum()).unwrap_or(true); // If no quorum checker, assume healthy

        // The local Raft leadership term, reported as its own field. Node-local
        // and unreplicated -- never folded into `cluster_current_epoch`.
        //
        // Stays `None` without a Raft handle, so the line is omitted rather
        // than reporting term 0 for a node that has no term. Cluster state can
        // be present without Raft (a `ClusterState` snapshot restored on a node
        // whose Raft is not wired up), and that case must not fabricate one.
        let raft_term: Option<u64> = ctx.raft.map(|r| r.metrics().borrow().current_term);

        let cluster_state_str = if has_failed_slot_owner || !has_local_quorum {
            "fail"
        } else if let Some(raft) = ctx.raft {
            use openraft::ServerState;
            let metrics = raft.metrics().borrow().clone();

            match (
                metrics.state,
                metrics.current_leader,
                metrics.millis_since_quorum_ack,
            ) {
                (ServerState::Candidate, _, _) => "fail", // Trying to elect but can't get quorum
                (_, None, _) => "fail",                   // No leader known
                // Leader: require RECENT quorum ack (within timeout) to report healthy
                (ServerState::Leader, _, Some(millis)) if millis <= QUORUM_TIMEOUT_MS => "ok",
                // Leader: stale quorum ack OR no quorum ack (None) = partitioned/unhealthy
                (ServerState::Leader, _, _) => "fail",
                // Follower: trust that we have a leader (they'll discover otherwise via election)
                (ServerState::Follower, Some(_), _) => "ok",
                _ => "ok", // Learner with leader, etc.
            }
        } else {
            "ok" // No Raft = standalone mode, always ok
        };

        ClusterInfoReport {
            state: cluster_state_str,
            slots_assigned,
            health,
            known_nodes,
            cluster_size,
            current_epoch: snapshot.config_epoch,
            my_epoch,
            raft_term,
            // Without a network factory this node has no handle on the bus it
            // is (or is not) running, so the two totals are omitted rather than
            // reported as zero traffic.
            bus_stats: ctx.network_factory.map(|nf| nf.bus_stats().snapshot()),
        }
    } else {
        ClusterInfoReport::standalone()
    };

    Ok(Response::bulk(Bytes::from(report.render())))
}

/// Every value `CLUSTER INFO` reports, collected before rendering.
///
/// Cluster and standalone mode share one renderer so the field list — its
/// names, order and the CRLF framing — has a single definition, and so a new
/// field can never be added to one branch and forgotten in the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClusterInfoReport {
    state: &'static str,
    slots_assigned: u16,
    health: SlotHealthCounts,
    known_nodes: usize,
    /// Number of primaries, matching Redis's `cluster_size`.
    cluster_size: usize,
    /// The cluster-wide replicated config-epoch counter, verbatim.
    current_epoch: frogdb_cluster::ConfigEpoch,
    /// This node's own `NodeInfo::config_epoch`.
    my_epoch: frogdb_cluster::ConfigEpoch,
    /// The local Raft leadership term (FrogDB extension). `None` in standalone
    /// mode, where there is no Raft group: the line is omitted rather than
    /// reported as a fake `0`.
    raft_term: Option<u64>,
    /// Cluster-bus packet totals. `None` when this node has no handle on the
    /// bus, which omits both lines instead of claiming an idle bus.
    bus_stats: Option<frogdb_cluster::ClusterBusStatsSnapshot>,
}

impl ClusterInfoReport {
    /// The fixed report for a standalone (non-cluster) server: one node owning
    /// every slot, no epochs, no Raft.
    fn standalone() -> Self {
        Self {
            state: "ok",
            slots_assigned: CLUSTER_SLOTS,
            health: SlotHealthCounts {
                ok: CLUSTER_SLOTS,
                pfail: 0,
                fail: 0,
            },
            known_nodes: 1,
            cluster_size: 1,
            current_epoch: 0,
            my_epoch: 0,
            raft_term: None,
            // A standalone server runs no cluster bus, so zero packets is the
            // measured truth, not a placeholder.
            bus_stats: Some(frogdb_cluster::ClusterBusStatsSnapshot {
                messages_sent: 0,
                messages_received: 0,
            }),
        }
    }

    /// Render the `key:value\r\n` bulk-string body.
    ///
    /// Two groups of lines are conditional. `cluster_raft_term` is a FrogDB
    /// extension omitted when there is no Raft group, because a term of `0`
    /// would read as "term zero" rather than "no term". The two
    /// `cluster_stats_messages_*` totals are omitted when this node has no
    /// handle on the bus, for the same reason.
    ///
    /// The per-message-type breakdown (`..._ping_sent` and friends) is absent
    /// entirely: FrogDB has no gossip protocol, so those messages are never
    /// sent, and Redis itself omits a per-type line whose counter is zero. See
    /// FM-CLUSTER-077.
    fn render(&self) -> String {
        let raft_term = match self.raft_term {
            Some(term) => format!("cluster_raft_term:{term}\r\n"),
            None => String::new(),
        };
        let bus_stats = match self.bus_stats {
            Some(stats) => format!(
                "cluster_stats_messages_sent:{}\r\ncluster_stats_messages_received:{}\r\n",
                stats.messages_sent, stats.messages_received
            ),
            None => String::new(),
        };
        format!(
            "\
cluster_state:{}\r\n\
cluster_slots_assigned:{}\r\n\
cluster_slots_ok:{}\r\n\
cluster_slots_pfail:{}\r\n\
cluster_slots_fail:{}\r\n\
cluster_known_nodes:{}\r\n\
cluster_size:{}\r\n\
cluster_current_epoch:{}\r\n\
cluster_my_epoch:{}\r\n\
{}\
{}\
total_cluster_links_buffer_limit_exceeded:0\r\n",
            self.state,
            self.slots_assigned,
            self.health.ok,
            self.health.pfail,
            self.health.fail,
            self.known_nodes,
            self.cluster_size,
            self.current_epoch,
            self.my_epoch,
            raft_term,
            bus_stats,
        )
    }
}

/// CLUSTER NODES - Returns the cluster nodes configuration.
///
/// Rendering is owned by `frogdb_cluster::wire::render_cluster_nodes`; this
/// adapter just picks the snapshot (live cluster state, or a synthetic
/// single-primary standalone snapshot) and the local node id.
fn cluster_nodes(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let text = if let Some(cluster_state) = ctx.cluster_state {
        let snapshot = cluster_state.snapshot();
        let my_id = ctx.node_id.unwrap_or(0);
        wire::render_cluster_nodes(&snapshot, my_id)
    } else {
        // Standalone mode - single primary owning all slots.
        let node_id = ctx.node_id.unwrap_or(1);
        wire::render_cluster_nodes(&wire::standalone_snapshot(node_id), node_id)
    };

    Ok(Response::bulk(Bytes::from(text)))
}

/// CLUSTER MYID - Returns this node's unique ID.
fn cluster_myid(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    // Return this node's ID (40-character hex string)
    let node_id = ctx.node_id.unwrap_or(1);
    Ok(Response::bulk(Bytes::from(format!("{:040x}", node_id))))
}

/// CLUSTER SLOTS - Returns slot to node mappings (deprecated, use CLUSTER SHARDS).
///
/// Format: `[[start, end, [ip, port, id], [replica_ip, replica_port, replica_id], ...], ...]`.
/// Grouping/sorting is owned by `frogdb_cluster::wire::shard_views`; this adapter
/// maps each shard's slot ranges to RESP. Shards whose primary owns zero slots are
/// skipped (matching the historical SLOTS behavior; SHARDS keeps them).
fn cluster_slots(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let snapshot = match ctx.cluster_state {
        Some(cluster_state) => cluster_state.snapshot(),
        // Standalone mode - single primary owning all slots.
        None => Arc::new(wire::standalone_snapshot(ctx.node_id.unwrap_or(1))),
    };

    Ok(map_slots_response(&wire::shard_views(&snapshot)))
}

/// Map grouped [`wire::ShardView`]s to the `CLUSTER SLOTS` RESP array. Skips
/// shards whose primary owns no slots (their `slots` vec is empty).
fn map_slots_response(views: &[wire::ShardView<'_>]) -> Response {
    let mut slot_info = Vec::new();
    for view in views {
        if view.slots.is_empty() {
            continue;
        }
        for range in &view.slots {
            let mut entry = vec![
                Response::Integer(range.start as i64),
                Response::Integer(range.end as i64),
                Response::Array(vec![
                    Response::bulk(Bytes::from(view.primary.node.addr.ip().to_string())),
                    Response::Integer(view.primary.node.addr.port() as i64),
                    Response::bulk(Bytes::from(wire::format_node_id(view.primary.id))),
                ]),
            ];
            for replica in &view.replicas {
                entry.push(Response::Array(vec![
                    Response::bulk(Bytes::from(replica.node.addr.ip().to_string())),
                    Response::Integer(replica.node.addr.port() as i64),
                    Response::bulk(Bytes::from(wire::format_node_id(replica.id))),
                ]));
            }
            slot_info.push(Response::Array(entry));
        }
    }
    Response::Array(slot_info)
}

/// CLUSTER SHARDS - Returns information about cluster shards (Redis 7.0+).
///
/// Grouping/sorting is owned by `frogdb_cluster::wire::shard_views`; this adapter
/// maps each shard to RESP, overlaying the server-only replication offset for the
/// local node (which the `cluster` crate does not and must not see).
fn cluster_shards(ctx: &mut CommandContext) -> Result<Response, CommandError> {
    let my_offset = local_replication_offset(ctx);
    let (snapshot, my_id) = match ctx.cluster_state {
        Some(cluster_state) => (cluster_state.snapshot(), ctx.node_id),
        // Standalone mode - single primary owning all slots. Attribute the local
        // offset to that node regardless of whether `ctx.node_id` is set.
        None => {
            let node_id = ctx.node_id.unwrap_or(1);
            (Arc::new(wire::standalone_snapshot(node_id)), Some(node_id))
        }
    };

    Ok(map_shards_response(
        &wire::shard_views(&snapshot),
        my_id,
        my_offset,
    ))
}

/// Map grouped [`wire::ShardView`]s to the `CLUSTER SHARDS` RESP array,
/// reporting `my_offset` as the `replication-offset` of the node whose id equals
/// `my_id`.
///
/// Peers get **no** `replication-offset` field at all. The metadata plane
/// carries topology, not stream positions, so this node does not know how far
/// any peer has replicated; rendering the 0 it would otherwise have to invent
/// would read as "that node is infinitely behind" and drive exactly the
/// failover/lag decisions the field exists to inform. An absent field is the
/// truthful answer, and the client asks the node itself (`INFO replication`,
/// or `CLUSTER SHARDS` against that node) for the real one. Redis fills this in
/// from gossip, which FrogDB's Raft plane deliberately does not carry.
fn map_shards_response(
    views: &[wire::ShardView<'_>],
    my_id: Option<frogdb_cluster::NodeId>,
    my_offset: i64,
) -> Response {
    let node_entry = |view: &wire::NodeView<'_>, role: &'static str| -> Response {
        let mut entry = vec![
            Response::bulk(Bytes::from("id")),
            Response::bulk(Bytes::from(wire::format_node_id(view.id))),
            Response::bulk(Bytes::from("port")),
            Response::Integer(view.node.addr.port() as i64),
            Response::bulk(Bytes::from("ip")),
            Response::bulk(Bytes::from(view.node.addr.ip().to_string())),
            Response::bulk(Bytes::from("endpoint")),
            Response::bulk(Bytes::from(view.node.addr.ip().to_string())),
            Response::bulk(Bytes::from("role")),
            Response::bulk(Bytes::from(role)),
        ];
        if Some(view.id) == my_id {
            entry.push(Response::bulk(Bytes::from("replication-offset")));
            entry.push(Response::Integer(my_offset));
        }
        entry.push(Response::bulk(Bytes::from("health")));
        entry.push(Response::bulk(Bytes::from(view.health)));
        Response::Array(entry)
    };

    let mut shards = Vec::new();
    for view in views {
        let mut slots = Vec::new();
        for range in &view.slots {
            slots.push(Response::Integer(range.start as i64));
            slots.push(Response::Integer(range.end as i64));
        }

        let mut nodes = Vec::with_capacity(1 + view.replicas.len());
        nodes.push(node_entry(&view.primary, "master"));
        for replica in &view.replicas {
            nodes.push(node_entry(replica, "slave"));
        }

        shards.push(Response::Array(vec![
            Response::bulk(Bytes::from("slots")),
            Response::Array(slots),
            Response::bulk(Bytes::from("nodes")),
            Response::Array(nodes),
        ]));
    }
    Response::Array(shards)
}

/// CLUSTER KEYSLOT - Returns the hash slot for a key.
fn cluster_keyslot(key: &Bytes) -> Result<Response, CommandError> {
    let slot = slot_for_key(key);
    Ok(Response::Integer(slot as i64))
}

/// CLUSTER COUNTKEYSINSLOT - Returns the number of keys in a slot.
fn cluster_countkeysinslot(
    ctx: &mut CommandContext,
    slot_arg: &Bytes,
) -> Result<Response, CommandError> {
    let slot: u16 = std::str::from_utf8(slot_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?;

    if slot >= 16384 {
        return Err(CommandError::InvalidArgument {
            message: "Invalid or out of range slot".to_string(),
        });
    }

    // Query the actual key count for this slot from the store
    let count = ctx.store.count_keys_in_slot(slot);
    Ok(Response::Integer(count as i64))
}

/// CLUSTER GETKEYSINSLOT - Returns keys in a slot.
fn cluster_getkeysinslot(
    ctx: &mut CommandContext,
    slot_arg: &Bytes,
    count_arg: &Bytes,
) -> Result<Response, CommandError> {
    let slot: u16 = std::str::from_utf8(slot_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid slot".to_string(),
        })?;

    let count: usize = std::str::from_utf8(count_arg)
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid count".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "invalid count".to_string(),
        })?;

    if slot >= 16384 {
        return Err(CommandError::InvalidArgument {
            message: "Invalid or out of range slot".to_string(),
        });
    }

    // Query the actual keys in this slot from the store
    let keys = ctx.store.keys_in_slot(slot, count);
    let response_keys: Vec<Response> = keys.into_iter().map(Response::bulk).collect();
    Ok(Response::Array(response_keys))
}

/// CLUSTER HELP - Returns help for CLUSTER commands.
fn cluster_help() -> Result<Response, CommandError> {
    let help = vec![
        Response::bulk(Bytes::from(
            "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        )),
        Response::bulk(Bytes::from("ADDSLOTS <slot> [<slot> ...]")),
        Response::bulk(Bytes::from("    Assign slots to this node.")),
        Response::bulk(Bytes::from("COUNTKEYSINSLOT <slot>")),
        Response::bulk(Bytes::from("    Return the number of keys in slot.")),
        Response::bulk(Bytes::from("DELSLOTS <slot> [<slot> ...]")),
        Response::bulk(Bytes::from("    Delete slots from this node.")),
        Response::bulk(Bytes::from("FAILOVER [FORCE|TAKEOVER]")),
        Response::bulk(Bytes::from("    Trigger a manual failover.")),
        Response::bulk(Bytes::from("FORGET <node-id>")),
        Response::bulk(Bytes::from("    Remove a node from the cluster.")),
        Response::bulk(Bytes::from("GETKEYSINSLOT <slot> <count>")),
        Response::bulk(Bytes::from("    Return keys in slot.")),
        Response::bulk(Bytes::from("HELP")),
        Response::bulk(Bytes::from("    Prints this help.")),
        Response::bulk(Bytes::from("INFO")),
        Response::bulk(Bytes::from("    Return information about the cluster.")),
        Response::bulk(Bytes::from("KEYSLOT <key>")),
        Response::bulk(Bytes::from("    Return the hash slot for key.")),
        Response::bulk(Bytes::from("MEET <ip> <port>")),
        Response::bulk(Bytes::from("    Connect nodes into a working cluster.")),
        Response::bulk(Bytes::from("MYID")),
        Response::bulk(Bytes::from("    Return this node's ID.")),
        Response::bulk(Bytes::from("NODES")),
        Response::bulk(Bytes::from("    Return cluster node information.")),
        Response::bulk(Bytes::from("REPLICATE <node-id>")),
        Response::bulk(Bytes::from(
            "    Configure this node as a replica of the specified node.",
        )),
        Response::bulk(Bytes::from("RESET [HARD|SOFT]")),
        Response::bulk(Bytes::from("    Reset the cluster state.")),
        Response::bulk(Bytes::from("SAVECONFIG")),
        Response::bulk(Bytes::from(
            "    Force saving cluster configuration on disk.",
        )),
        Response::bulk(Bytes::from("SET-CONFIG-EPOCH <epoch>")),
        Response::bulk(Bytes::from("    Set config epoch in this node.")),
        Response::bulk(Bytes::from(
            "SETSLOT <slot> IMPORTING|MIGRATING|NODE|STABLE [<node-id>]",
        )),
        Response::bulk(Bytes::from("    Set slot state.")),
        Response::bulk(Bytes::from("SHARDS")),
        Response::bulk(Bytes::from("    Return information about cluster shards.")),
        Response::bulk(Bytes::from("SLOTS")),
        Response::bulk(Bytes::from(
            "    Return slot range information (deprecated, use SHARDS).",
        )),
    ];

    Ok(Response::Array(help))
}

// ASKING / READONLY / READWRITE were migrated behind the ConnCtx seam as
// mutating connection commands (they set per-connection cluster-redirect flags).
// See `crate::connection::connection_state_conn_command`.

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_cluster::{ClusterSnapshot, NodeInfo};

    fn addr(s: &str) -> std::net::SocketAddr {
        s.parse().unwrap()
    }

    fn as_arr(r: &Response) -> &[Response] {
        match r {
            Response::Array(v) => v,
            _ => panic!("expected array"),
        }
    }

    fn as_int(r: &Response) -> i64 {
        match r {
            Response::Integer(i) => *i,
            _ => panic!("expected integer"),
        }
    }

    fn as_bulk(r: &Response) -> String {
        match r {
            Response::Bulk(Some(b)) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected bulk"),
        }
    }

    /// Look up a value in a flat `[k1, v1, k2, v2, ...]` bulk-keyed node entry.
    fn opt_field<'a>(entry: &'a [Response], key: &str) -> Option<&'a Response> {
        entry
            .chunks_exact(2)
            .find(|pair| as_bulk(&pair[0]) == key)
            .map(|pair| &pair[1])
    }

    fn field<'a>(entry: &'a [Response], key: &str) -> &'a Response {
        opt_field(entry, key).unwrap_or_else(|| panic!("field {key} not found"))
    }

    /// Primary 1 owns slots 0-9 with replica 3; primary 2 owns zero slots.
    fn fixture() -> ClusterSnapshot {
        let mut snap = ClusterSnapshot::new();
        snap.nodes.insert(
            1,
            NodeInfo::new_primary(1, addr("127.0.0.1:7001"), addr("127.0.0.1:17001")),
        );
        snap.nodes.insert(
            2,
            NodeInfo::new_primary(2, addr("127.0.0.1:7002"), addr("127.0.0.1:17002")),
        );
        snap.nodes.insert(
            3,
            NodeInfo::new_replica(3, addr("127.0.0.1:7003"), addr("127.0.0.1:17003"), 1),
        );
        for slot in 0..10 {
            snap.slot_assignment.insert(slot, 1);
        }
        snap
    }

    // FM-CLUSTER-072
    #[test]
    fn test_map_slots_response_skips_zero_slot_primary() {
        let snap = fixture();
        let resp = map_slots_response(&wire::shard_views(&snap));
        let entries = as_arr(&resp);

        // Only primary 1 (owns slots) is emitted; primary 2 (zero slots) skipped.
        assert_eq!(entries.len(), 1);
        let entry = as_arr(&entries[0]);
        assert_eq!(as_int(&entry[0]), 0);
        assert_eq!(as_int(&entry[1]), 9);

        // [ip, port, id] for the primary, then the replica.
        let primary = as_arr(&entry[2]);
        assert_eq!(as_bulk(&primary[0]), "127.0.0.1");
        assert_eq!(as_int(&primary[1]), 7001);
        assert_eq!(
            as_bulk(&primary[2]),
            "0000000000000000000000000000000000000001"
        );
        let replica = as_arr(&entry[3]);
        assert_eq!(
            as_bulk(&replica[2]),
            "0000000000000000000000000000000000000003"
        );
    }

    // FM-CLUSTER-072
    #[test]
    fn test_map_shards_response_reports_offset_for_local_node_only() {
        let snap = fixture();
        // Local node is the primary (id 1); its offset is reported. Peers get no
        // `replication-offset` field at all: this node does not know theirs, and
        // a rendered 0 would read as "infinitely behind". Zero-slot primary 2 is
        // still present (SHARDS keeps it, unlike SLOTS).
        let resp = map_shards_response(&wire::shard_views(&snap), Some(1), 42);
        let shards = as_arr(&resp);
        assert_eq!(shards.len(), 2);

        // Shard 0 = primary 1 + replica 3.
        let shard0 = as_arr(&shards[0]);
        let nodes0 = as_arr(field(shard0, "nodes"));
        assert_eq!(nodes0.len(), 2);

        let primary = as_arr(&nodes0[0]);
        assert_eq!(as_bulk(field(primary, "role")), "master");
        assert_eq!(as_int(field(primary, "replication-offset")), 42);
        assert_eq!(as_bulk(field(primary, "health")), "online");

        let replica = as_arr(&nodes0[1]);
        assert_eq!(as_bulk(field(replica, "role")), "slave");
        assert!(
            opt_field(replica, "replication-offset").is_none(),
            "a peer's replication offset is unknown here, so the field is omitted"
        );
        assert_eq!(
            as_bulk(field(replica, "health")),
            "online",
            "omitting one field must not disturb the pairs after it"
        );

        // Shard 1 = zero-slot primary 2: present, empty slots, no offset.
        let shard1 = as_arr(&shards[1]);
        assert!(as_arr(field(shard1, "slots")).is_empty());
        let p2 = as_arr(&as_arr(field(shard1, "nodes"))[0]);
        assert!(opt_field(p2, "replication-offset").is_none());
    }

    // ------------------------------------------------------------------
    // `CLUSTER INFO` epoch reporting. The Raft term used to be folded into
    // `cluster_current_epoch` via `max(config_epoch, raft_term)` (issue 47
    // pinned that fold; the epoch-fold redesign removed it). These tests pin
    // the replacement contract: the counter is reported verbatim and the term
    // is its own field.
    // ------------------------------------------------------------------

    /// A report with the epoch triple set and everything else at a benign
    /// default -- these tests are about the three epoch/term fields only.
    fn epoch_report(current_epoch: u64, my_epoch: u64, raft_term: u64) -> ClusterInfoReport {
        ClusterInfoReport {
            state: "ok",
            slots_assigned: 0,
            health: SlotHealthCounts::default(),
            known_nodes: 1,
            cluster_size: 1,
            current_epoch,
            my_epoch,
            raft_term: Some(raft_term),
            bus_stats: Some(frogdb_cluster::ClusterBusStatsSnapshot::default()),
        }
    }

    /// Extract a `key:value` field from a rendered `CLUSTER INFO` body.
    fn info_field(body: &str, key: &str) -> String {
        body.lines()
            .find_map(|line| line.strip_prefix(&format!("{}:", key)))
            .unwrap_or_else(|| panic!("CLUSTER INFO must report a `{}` field", key))
            .trim()
            .to_string()
    }

    // FM-CLUSTER-016
    #[test]
    fn test_cluster_info_reports_config_epoch_counter_verbatim() {
        // A Raft re-election with no topology change: the term outruns the
        // replicated counter. The counter is what gets reported -- the term
        // no longer inflates it, so `cluster_current_epoch` stays a usable
        // topology-change detector and agrees across nodes.
        let body = epoch_report(3, 3, 7).render();
        assert_eq!(info_field(&body, "cluster_current_epoch"), "3");
        assert_eq!(info_field(&body, "cluster_raft_term"), "7");
    }

    // FM-CLUSTER-016
    #[test]
    fn test_cluster_info_reports_raft_term_as_its_own_field() {
        // A stable leader (low term) with several topology events already
        // committed: both values are reported, neither shadows the other.
        let body = epoch_report(9, 9, 2).render();
        assert_eq!(info_field(&body, "cluster_current_epoch"), "9");
        assert_eq!(info_field(&body, "cluster_raft_term"), "2");
    }

    // FM-CLUSTER-016
    #[test]
    fn test_cluster_info_epoch_bump_is_visible_under_a_higher_term() {
        // The masking case the fold produced, now pinned as fixed: on a young
        // cluster the first election takes raft_term to 1 while config_epoch
        // is still 0, so the first topology event (config_epoch 0 -> 1) used
        // to leave the reported value unchanged at 1. It is now visible.
        let before = epoch_report(0, 0, 1).render();
        let after = epoch_report(1, 1, 1).render();
        assert_eq!(info_field(&before, "cluster_current_epoch"), "0");
        assert_eq!(info_field(&after, "cluster_current_epoch"), "1");
    }

    // FM-CLUSTER-016
    #[test]
    fn test_cluster_info_my_epoch_is_the_per_node_value_not_the_counter() {
        // `cluster_my_epoch` is this node's own `NodeInfo::config_epoch`; the
        // counter legitimately runs ahead of it (IncrementEpoch and
        // MarkNodeFailed bump the counter without stamping any node). The
        // `current >= my` invariant is upheld at the source by
        // `ClusterStateInner::reconcile_incoming_epoch`, not by a `max()` here.
        //
        // Do NOT assert `cluster_current_epoch <= max(NODES config_epoch)`:
        // that bound does not hold and its failing is not a bug.
        let body = epoch_report(12, 4, 3).render();
        assert_eq!(info_field(&body, "cluster_current_epoch"), "12");
        assert_eq!(info_field(&body, "cluster_my_epoch"), "4");
    }

    /// Standalone has no Raft group, so it reports no term at all: a `0` there
    /// would read as "term zero" and let a scrape chart a value that does not
    /// exist. The epochs *are* reported as `0` -- those fields are Redis's and
    /// clients parse them unconditionally.
    // FM-CLUSTER-016
    #[test]
    fn test_cluster_info_standalone_omits_the_raft_term_line() {
        let body = ClusterInfoReport::standalone().render();
        assert_eq!(info_field(&body, "cluster_current_epoch"), "0");
        assert_eq!(info_field(&body, "cluster_my_epoch"), "0");
        assert!(
            !body.contains("cluster_raft_term"),
            "standalone CLUSTER INFO must omit cluster_raft_term entirely, got:\n{body}"
        );
        assert_eq!(info_field(&body, "cluster_state"), "ok");
        assert_eq!(info_field(&body, "cluster_slots_assigned"), "16384");
        assert_eq!(info_field(&body, "cluster_slots_ok"), "16384");
    }

    /// FrogDB has no gossip protocol, so `ping`/`pong` messages are not merely
    /// uncounted — they are never sent. Redis omits a per-type counter line
    /// whose value is zero, so omitting them is parity, and a confident
    /// `cluster_stats_messages_ping_sent:0` on a busy bus is exactly the
    /// misleading value the observability rule forbids.
    // FM-CLUSTER-077
    #[test]
    fn cluster_info_omits_gossip_counters_that_have_no_source() {
        let body = epoch_report(1, 1, 1).render();
        for absent in [
            "cluster_stats_messages_ping_sent",
            "cluster_stats_messages_pong_sent",
            "cluster_stats_messages_ping_received",
            "cluster_stats_messages_pong_received",
        ] {
            assert!(
                !body.contains(absent),
                "CLUSTER INFO must not report `{absent}`, which has no source; got:\n{body}"
            );
        }
        // The totals and the link-buffer counter, which Redis always emits,
        // stay. `total_cluster_links_buffer_limit_exceeded` is a measured zero:
        // there is no per-link output buffer limit to exceed.
        assert_eq!(info_field(&body, "cluster_stats_messages_sent"), "0");
        assert_eq!(info_field(&body, "cluster_stats_messages_received"), "0");
        assert_eq!(
            info_field(&body, "total_cluster_links_buffer_limit_exceeded"),
            "0"
        );
    }

    // FM-CLUSTER-077
    #[test]
    fn cluster_info_reports_the_live_bus_counters() {
        let mut report = epoch_report(1, 1, 1);
        report.bus_stats = Some(frogdb_cluster::ClusterBusStatsSnapshot {
            messages_sent: 17,
            messages_received: 4,
        });
        let body = report.render();
        assert_eq!(info_field(&body, "cluster_stats_messages_sent"), "17");
        assert_eq!(info_field(&body, "cluster_stats_messages_received"), "4");
    }

    /// A node with no handle on the bus cannot report its traffic, so it
    /// reports nothing rather than an idle bus. `CLUSTER INFO` is a key-value
    /// block; clients look keys up rather than reading fixed positions.
    // FM-CLUSTER-077
    #[test]
    fn cluster_info_omits_the_bus_totals_when_they_cannot_be_read() {
        let mut report = epoch_report(1, 1, 1);
        report.bus_stats = None;
        let body = report.render();
        assert!(
            !body.contains("cluster_stats_messages_sent")
                && !body.contains("cluster_stats_messages_received"),
            "unknown bus traffic must be absent, not zero; got:\n{body}"
        );
        // The rest of the report still renders.
        assert_eq!(info_field(&body, "cluster_current_epoch"), "1");
    }

    // FM-CLUSTER-074
    #[test]
    fn test_cluster_info_render_is_crlf_framed_key_value_lines() {
        let body = epoch_report(1, 1, 1).render();
        assert!(body.ends_with("\r\n"));
        for line in body.split_terminator("\r\n") {
            assert!(
                line.contains(':') && !line.contains('\n'),
                "every CLUSTER INFO line must be a single `key:value` pair, got {:?}",
                line
            );
        }
    }

    // ------------------------------------------------------------------
    // count_slot_health: the CLUSTER INFO `cluster_slots_{ok,pfail,fail}`
    // accounting (issue 36). These exercise the exact helper `cluster_info`
    // calls, so they pin the INFO rendering without needing a full
    // CommandContext (store/raft/quorum-checker plumbing).
    // ------------------------------------------------------------------

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_all_ok_when_no_node_flagged() {
        let snap = fixture();
        let health = count_slot_health(&snap);
        assert_eq!(health.ok, 10);
        assert_eq!(health.pfail, 0);
        assert_eq!(health.fail, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_fail_flagged_owner_counts_as_fail_not_ok() {
        let mut snap = fixture();
        // Primary 1 owns slots 0-9 in the fixture; flag it FAIL.
        snap.nodes.get_mut(&1).unwrap().flags.fail = true;
        let health = count_slot_health(&snap);
        assert_eq!(health.fail, 10, "all 10 slots owned by the FAIL primary");
        assert_eq!(health.ok, 0, "slots_ok must exclude FAIL-owned slots");
        assert_eq!(health.pfail, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_pfail_flagged_owner_counts_distinct_from_fail() {
        // PFAIL has no producer in FrogDB today (see `SlotHealthCounts`), so
        // this can only be pinned at the unit level -- but the bucketing must
        // be correct so the field stays honest if a suspicion phase lands.
        let mut snap = fixture();
        snap.nodes.get_mut(&1).unwrap().flags.pfail = true;
        let health = count_slot_health(&snap);
        assert_eq!(health.pfail, 10, "PFAIL is tracked separately from FAIL");
        assert_eq!(health.fail, 0);
        assert_eq!(health.ok, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_fail_takes_precedence_over_pfail() {
        let mut snap = fixture();
        snap.nodes.get_mut(&1).unwrap().flags.fail = true;
        snap.nodes.get_mut(&1).unwrap().flags.pfail = true;
        let health = count_slot_health(&snap);
        assert_eq!(
            health.fail, 10,
            "a node latched FAIL is never double counted as pfail"
        );
        assert_eq!(health.pfail, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_recovery_restores_full_ok() {
        let mut snap = fixture();
        snap.nodes.get_mut(&1).unwrap().flags.fail = true;
        assert_eq!(count_slot_health(&snap).ok, 0);

        // MarkNodeRecovered clears the flag; slots_ok must return to the full
        // assigned count.
        snap.nodes.get_mut(&1).unwrap().flags.fail = false;
        let health = count_slot_health(&snap);
        assert_eq!(health.ok, 10);
        assert_eq!(health.fail, 0);
        assert_eq!(health.pfail, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_only_counts_flagged_owners_slots_others_unaffected() {
        let mut snap = fixture();
        // Give primary 2 a couple of slots alongside primary 1's 0-9, then fail
        // only primary 1: primary 2's slots must stay ok.
        snap.slot_assignment.insert(10, 2);
        snap.slot_assignment.insert(11, 2);
        snap.nodes.get_mut(&1).unwrap().flags.fail = true;
        let health = count_slot_health(&snap);
        assert_eq!(health.fail, 10, "only primary 1's slots are fail");
        assert_eq!(health.ok, 2, "primary 2's slots remain ok");
        assert_eq!(health.pfail, 0);
    }

    /// The `cluster_state:fail` predicate is `slots_fail > 0`, not "some
    /// primary is FAIL-flagged": primary 2 owns no slots, so failing it leaves
    /// the whole keyspace served and the cluster reports `ok`. A phantom joiner
    /// at a dead address (a very common transient) is exactly this shape, and
    /// under the old predicate it flipped every node's `cluster_state` to
    /// `fail` while every key kept answering.
    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_fail_flagged_slotless_primary_leaves_slots_ok() {
        let mut snap = fixture();
        snap.nodes.get_mut(&2).unwrap().flags.fail = true;
        let health = count_slot_health(&snap);
        assert_eq!(health.fail, 0, "a slotless primary owns no failed slots");
        assert_eq!(health.ok, 10, "every assigned slot is still served");
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_totals_always_equal_slots_assigned() {
        // The invariant `slots_ok + slots_pfail + slots_fail == slots_assigned`
        // is what makes the breakdown non-misleading; nothing may be dropped.
        let mut snap = fixture();
        snap.slot_assignment.insert(10, 2);
        snap.nodes.get_mut(&1).unwrap().flags.fail = true;
        snap.nodes.get_mut(&2).unwrap().flags.pfail = true;
        let health = count_slot_health(&snap);
        assert_eq!(
            health.ok + health.pfail + health.fail,
            snap.slot_assignment.len() as u16
        );
        assert_eq!(health.fail, 10);
        assert_eq!(health.pfail, 1);
        assert_eq!(health.ok, 0);
    }

    // FM-CLUSTER-073
    #[test]
    fn test_count_slot_health_unknown_owner_counted_ok_not_dropped() {
        // Defensive: a slot pointing at a node missing from `nodes` must still
        // be counted, or the three fields would not sum to slots_assigned.
        let mut snap = fixture();
        snap.slot_assignment.insert(10, 99);
        let health = count_slot_health(&snap);
        assert_eq!(health.ok, 11);
        assert_eq!(
            health.ok + health.pfail + health.fail,
            snap.slot_assignment.len() as u16
        );
    }
}

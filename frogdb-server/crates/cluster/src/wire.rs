//! Single source of truth for rendering a [`ClusterSnapshot`] to the Redis
//! Cluster wire representations consumed by `CLUSTER NODES`, `CLUSTER SLOTS`,
//! and `CLUSTER SHARDS`.
//!
//! Before proposal 34, each of those three server commands independently
//! re-derived the same facts from a snapshot — how to render a node id as hex,
//! how to assemble a node's flag/role label, and how to group a primary with its
//! replicas — and a fourth, dead `NodeInfo::to_cluster_nodes_line` disagreed with
//! all of them. This module owns those concerns once:
//!
//! - [`format_node_id`] — the canonical 40-hex node-id width.
//! - [`node_flags_label`] — the `CLUSTER NODES` flag token.
//! - [`node_health`] — the `CLUSTER SHARDS` health token.
//! - [`render_cluster_nodes`] — the full `CLUSTER NODES` text.
//! - [`shard_views`] / [`ShardView`] / [`NodeView`] — the neutral, grouped
//!   primary+replica topology the server maps to RESP for SLOTS and SHARDS.
//!
//! Crate direction: this module depends only on `cluster` types. The RESP
//! `Response` type lives in the protocol/server layer, which `cluster` must not
//! depend on, so SLOTS/SHARDS emit neutral view structs that the server maps to
//! RESP (overlaying server-only data such as the replication offset).

use crate::types::{CLUSTER_SLOTS, ClusterSnapshot, NodeFlags, NodeId, NodeInfo};

/// Render a node id in the canonical Redis width: 40 hex chars (160-bit ids).
///
/// `NodeId` is a `u64`, so this zero-pads to 40 rather than to its natural
/// 16-hex width. This is the single owner of that padding decision — every
/// wire rendering of a node id must route through here.
pub fn format_node_id(id: NodeId) -> String {
    format!("{:040x}", id)
}

/// Assemble the `CLUSTER NODES` flag token for `node`, e.g. `"myself,master"`
/// or `"slave,fail"`.
///
/// Order matches Redis: `myself` (if this is the local node), the role
/// (`master`/`slave`), then `fail`, `fail?`, `handshake`, `noaddr`. An empty set
/// renders as `"noflags"` (unreachable in practice, since every node has a role,
/// but preserved to match the prior hand-rolled renderer exactly).
pub fn node_flags_label(node: &NodeInfo, myself: bool) -> String {
    let mut flags = Vec::new();
    if myself {
        flags.push("myself");
    }
    if node.is_primary() {
        flags.push("master");
    }
    if node.is_replica() {
        flags.push("slave");
    }
    if node.flags.fail {
        flags.push("fail");
    }
    if node.flags.pfail {
        flags.push("fail?");
    }
    if node.flags.handshake {
        flags.push("handshake");
    }
    if node.flags.noaddr {
        flags.push("noaddr");
    }
    if flags.is_empty() {
        "noflags".to_string()
    } else {
        flags.join(",")
    }
}

/// Redis-wire health token for `CLUSTER SHARDS`: `"fail"`, `"loading"`
/// (suspected-failed / `pfail`), or `"online"`.
pub fn node_health(flags: &NodeFlags) -> &'static str {
    if flags.fail {
        "fail"
    } else if flags.pfail {
        "loading"
    } else {
        "online"
    }
}

/// Render the full `CLUSTER NODES` payload text for `snapshot`.
///
/// Owns id width, flag/role assembly, master-id, migration markers, slot-range
/// compaction (via [`ClusterSnapshot::get_node_slots`]), and link-state. Nodes are
/// emitted in `snapshot.nodes` order (`BTreeMap`, so ascending node id). The
/// returned string ends with a trailing newline, matching the historical output.
///
/// Line format:
/// `<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots…>`
/// with `ping-sent`/`pong-recv` always `0 0` (FrogDB does not track gossip timing).
pub fn render_cluster_nodes(snapshot: &ClusterSnapshot, myself: NodeId) -> String {
    let mut lines = Vec::new();
    for node in snapshot.nodes.values() {
        let flags_str = node_flags_label(node, node.id == myself);

        let master_id = node
            .primary_id
            .map(format_node_id)
            .unwrap_or_else(|| "-".to_string());

        let slot_ranges = snapshot.get_node_slots(node.id);
        let slots_str: String = slot_ranges
            .iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        // Migration markers for slots this node is exporting / importing.
        let mut migration_str = String::new();
        for migration in snapshot.migrations.values() {
            if migration.source_node == node.id {
                migration_str.push_str(&format!(
                    " [{}->-{}]",
                    migration.slot,
                    format_node_id(migration.target_node)
                ));
            } else if migration.target_node == node.id {
                migration_str.push_str(&format!(
                    " [{}-<-{}]",
                    migration.slot,
                    format_node_id(migration.source_node)
                ));
            }
        }

        let line = format!(
            "{} {}:{}@{} {} {} 0 0 {} connected {}{}",
            format_node_id(node.id),
            node.addr.ip(),
            node.addr.port(),
            node.cluster_addr.port(),
            flags_str,
            master_id,
            node.config_epoch,
            slots_str,
            migration_str
        );
        lines.push(line);
    }

    lines.join("\n") + "\n"
}

/// A neutral, protocol-agnostic view of one node within a shard.
///
/// Borrows the [`NodeInfo`] from the snapshot; the server reads ip/port/role off
/// `node` and overlays server-only fields (replication offset) itself.
pub struct NodeView<'a> {
    /// The node's id.
    pub id: NodeId,
    /// The node record (ip/port/cport/flags/role) borrowed from the snapshot.
    pub node: &'a NodeInfo,
    /// Redis-wire health token ([`node_health`] of `node.flags`).
    pub health: &'static str,
}

impl<'a> NodeView<'a> {
    fn new(node: &'a NodeInfo) -> Self {
        Self {
            id: node.id,
            node,
            health: node_health(&node.flags),
        }
    }
}

/// A neutral, protocol-agnostic view of one shard: a primary, its replicas, and
/// the slot ranges the primary owns. The single source for the primary/replica
/// grouping that `CLUSTER SLOTS` and `CLUSTER SHARDS` both need.
pub struct ShardView<'a> {
    /// The slot ranges owned by the primary (compacted, possibly empty).
    pub slots: Vec<crate::types::SlotRange>,
    /// The shard's primary node.
    pub primary: NodeView<'a>,
    /// The primary's replicas, in ascending node-id order.
    pub replicas: Vec<NodeView<'a>>,
}

/// Group `snapshot` into shards: one per primary, replicas nested under their
/// primary, primaries sorted by id (deterministic), replicas in ascending
/// node-id order.
///
/// Every primary is enumerated, **including primaries that own zero slots**.
/// `CLUSTER SHARDS` includes those; `CLUSTER SLOTS` historically did not, so the
/// SLOTS mapper must filter out shards with an empty `slots` vec (see the server
/// mapping). Sorting primaries by id makes SLOTS/SHARDS emission deterministic,
/// fixing latent `HashMap`-iteration nondeterminism in the prior SLOTS path.
pub fn shard_views(snapshot: &ClusterSnapshot) -> Vec<ShardView<'_>> {
    let mut primaries: Vec<&NodeInfo> =
        snapshot.nodes.values().filter(|n| n.is_primary()).collect();
    primaries.sort_by_key(|n| n.id);

    primaries
        .into_iter()
        .map(|primary| {
            let slots = snapshot.get_node_slots(primary.id);
            // `snapshot.nodes.values()` is ascending node-id order (BTreeMap),
            // so replicas come out sorted without an extra sort.
            let replicas: Vec<NodeView<'_>> = snapshot
                .nodes
                .values()
                .filter(|n| n.primary_id == Some(primary.id))
                .map(NodeView::new)
                .collect();
            ShardView {
                slots,
                primary: NodeView::new(primary),
                replicas,
            }
        })
        .collect()
}

/// Build the single-primary snapshot used to render `CLUSTER NODES/SLOTS/SHARDS`
/// in standalone (non-clustered) mode, so the standalone and clustered paths
/// share one renderer instead of hand-rolling parallel output.
///
/// The lone node owns every slot (`0..CLUSTER_SLOTS`) and lives at the historical
/// standalone address `127.0.0.1:6379@16379`.
pub fn standalone_snapshot(node_id: NodeId) -> ClusterSnapshot {
    let mut snapshot = ClusterSnapshot::new();
    let node = NodeInfo::new_primary(
        node_id,
        "127.0.0.1:6379"
            .parse()
            .expect("valid standalone client addr"),
        "127.0.0.1:16379"
            .parse()
            .expect("valid standalone cluster addr"),
    );
    snapshot.nodes.insert(node_id, node);
    for slot in 0..CLUSTER_SLOTS {
        snapshot.slot_assignment.insert(slot, node_id);
    }
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClusterSnapshot, NodeInfo, SlotMigration, SlotRange};

    fn addr(s: &str) -> std::net::SocketAddr {
        s.parse().unwrap()
    }

    /// Primary id 1 (epoch 5) owning slots 0-9, with replica id 2, plus one
    /// migration in each direction (slot 100 out of 1 -> 2, slot 200 into 1
    /// <- 2).
    fn fixture() -> ClusterSnapshot {
        let mut snap = ClusterSnapshot::new();

        let mut primary = NodeInfo::new_primary(1, addr("127.0.0.1:7001"), addr("127.0.0.1:17001"));
        primary.config_epoch = 5;
        let mut replica =
            NodeInfo::new_replica(2, addr("127.0.0.1:7002"), addr("127.0.0.1:17002"), 1);
        replica.config_epoch = 5;

        snap.nodes.insert(1, primary);
        snap.nodes.insert(2, replica);
        for slot in 0..10 {
            snap.slot_assignment.insert(slot, 1);
        }
        snap.migrations.insert(
            100,
            SlotMigration {
                slot: 100,
                source_node: 1,
                target_node: 2,
            },
        );
        snap.migrations.insert(
            200,
            SlotMigration {
                slot: 200,
                source_node: 2,
                target_node: 1,
            },
        );
        snap.config_epoch = 5;
        snap
    }

    #[test]
    fn test_format_node_id_is_40_hex() {
        let s = format_node_id(1);
        assert_eq!(s.len(), 40, "node id must be 40 hex chars wide");
        assert_eq!(s, "0000000000000000000000000000000000000001");
        assert_eq!(
            format_node_id(0xdead_beef),
            "00000000000000000000000000000000deadbeef"
        );
    }

    #[test]
    fn test_node_flags_label() {
        // myself primary.
        let primary = NodeInfo::new_primary(1, addr("127.0.0.1:7001"), addr("127.0.0.1:17001"));
        assert_eq!(node_flags_label(&primary, true), "myself,master");
        assert_eq!(node_flags_label(&primary, false), "master");

        // replica with fail.
        let mut replica =
            NodeInfo::new_replica(2, addr("127.0.0.1:7002"), addr("127.0.0.1:17002"), 1);
        replica.flags.fail = true;
        assert_eq!(node_flags_label(&replica, false), "slave,fail");

        // pfail renders as "fail?"; handshake and noaddr tokens present.
        let mut fancy =
            NodeInfo::new_replica(3, addr("127.0.0.1:7003"), addr("127.0.0.1:17003"), 1);
        fancy.flags.pfail = true;
        fancy.flags.handshake = true;
        fancy.flags.noaddr = true;
        assert_eq!(
            node_flags_label(&fancy, false),
            "slave,fail?,handshake,noaddr"
        );
    }

    #[test]
    fn test_node_health() {
        let mut flags = NodeFlags::default();
        assert_eq!(node_health(&flags), "online");
        flags.pfail = true;
        assert_eq!(node_health(&flags), "loading");
        flags.fail = true; // fail takes precedence over pfail
        assert_eq!(node_health(&flags), "fail");
    }

    #[test]
    fn test_render_cluster_nodes_golden() {
        let snap = fixture();
        let out = render_cluster_nodes(&snap, 1);

        let id1 = "0000000000000000000000000000000000000001";
        let id2 = "0000000000000000000000000000000000000002";
        // Node 1: myself primary, epoch 5, slots 0-9, exporting slot 100 to 2
        // and importing slot 200 from 2 (migrations ordered by slot).
        // Node 2: replica of 1, no slots (note the double space after
        // "connected " when the slot list is empty), importing slot 100 from 1
        // and exporting slot 200 to 1.
        let expected = format!(
            "{id1} 127.0.0.1:7001@17001 myself,master - 0 0 5 connected 0-9 [100->-{id2}] [200-<-{id2}]\n\
             {id2} 127.0.0.1:7002@17002 slave {id1} 0 0 5 connected  [100-<-{id1}] [200->-{id1}]\n"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn test_render_cluster_nodes_standalone() {
        // The standalone snapshot renders the historical single-primary line.
        let out = render_cluster_nodes(&standalone_snapshot(1), 1);
        assert_eq!(
            out,
            "0000000000000000000000000000000000000001 127.0.0.1:6379@16379 \
             myself,master - 0 0 0 connected 0-16383\n"
        );
    }

    #[test]
    fn test_shard_views_grouping_and_order() {
        // Two primaries (ids 10 and 5) each with a replica; primaries must come
        // out sorted by id (5 before 10) regardless of insertion order.
        let mut snap = ClusterSnapshot::new();
        let mut p_hi = NodeInfo::new_primary(10, addr("127.0.0.1:7010"), addr("127.0.0.1:17010"));
        p_hi.config_epoch = 1;
        let mut p_lo = NodeInfo::new_primary(5, addr("127.0.0.1:7005"), addr("127.0.0.1:17005"));
        p_lo.config_epoch = 1;
        let r_hi = NodeInfo::new_replica(11, addr("127.0.0.1:7011"), addr("127.0.0.1:17011"), 10);
        let r_lo = NodeInfo::new_replica(6, addr("127.0.0.1:7006"), addr("127.0.0.1:17006"), 5);
        snap.nodes.insert(10, p_hi);
        snap.nodes.insert(5, p_lo);
        snap.nodes.insert(11, r_hi);
        snap.nodes.insert(6, r_lo);
        // primary 5 owns 0-9 (+20-24, non-contiguous), primary 10 owns 100-199.
        for slot in 0..10 {
            snap.slot_assignment.insert(slot, 5);
        }
        for slot in 20..25 {
            snap.slot_assignment.insert(slot, 5);
        }
        for slot in 100..200 {
            snap.slot_assignment.insert(slot, 10);
        }

        let views = shard_views(&snap);
        assert_eq!(views.len(), 2);

        // Sorted by primary id.
        assert_eq!(views[0].primary.id, 5);
        assert_eq!(views[1].primary.id, 10);

        // Slot compaction preserved.
        assert_eq!(
            views[0].slots,
            vec![SlotRange::new(0, 9), SlotRange::new(20, 24)]
        );
        assert_eq!(views[1].slots, vec![SlotRange::new(100, 199)]);

        // Replicas nested under their primary.
        assert_eq!(views[0].replicas.len(), 1);
        assert_eq!(views[0].replicas[0].id, 6);
        assert_eq!(views[1].replicas.len(), 1);
        assert_eq!(views[1].replicas[0].id, 11);
    }

    #[test]
    fn test_shard_views_includes_zero_slot_primary() {
        // A primary owning zero slots is still enumerated (SHARDS needs it; the
        // SLOTS mapper filters it out on its own).
        let mut snap = ClusterSnapshot::new();
        snap.nodes.insert(
            1,
            NodeInfo::new_primary(1, addr("127.0.0.1:7001"), addr("127.0.0.1:17001")),
        );
        let views = shard_views(&snap);
        assert_eq!(views.len(), 1);
        assert!(views[0].slots.is_empty());
    }
}

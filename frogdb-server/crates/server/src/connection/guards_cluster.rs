//! Cluster-mode slot validation guards.

use bytes::Bytes;
use frogdb_core::{CommandFlags, ExecutionStrategy, ScatterOp, ShardMessage, shard_for_key, slot_for_key};
use frogdb_protocol::{ParsedCommand, Response};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use crate::connection::ConnectionHandler;
use crate::connection::next_txid;

impl ConnectionHandler {
    pub(crate) fn is_cluster_exempt(&self, cmd_name: &str) -> bool {
        if matches!(cmd_name, "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG") { return true; }
        self.registry.get_entry(cmd_name).is_some_and(|entry| matches!(entry.execution_strategy(), ExecutionStrategy::ConnectionLevel(_) | ExecutionStrategy::ScatterGather { .. } | ExecutionStrategy::ServerWide(_)))
    }

    pub(crate) fn validate_cluster_slots(&mut self, cmd: &ParsedCommand) -> Option<Response> {
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        if self.is_cluster_exempt(&cmd_name) { return None; }
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) { cmd_impl.keys(&cmd.args) } else { return None; };
        if keys.is_empty() { return None; }
        let first_slot = slot_for_key(keys[0]);
        for key in &keys[1..] { let slot = slot_for_key(key); if slot != first_slot { return Some(Response::error("CROSSSLOT Keys in request don't hash to the same slot")); } }
        let snapshot = cluster_state.snapshot();
        match snapshot.slot_assignment.get(&first_slot) {
            Some(&owner) if owner == node_id => { if snapshot.migrations.contains_key(&first_slot) && self.state.asking { self.state.asking = false; } None }
            Some(&owner) => {
                if let Some(migration) = snapshot.migrations.get(&first_slot) && migration.target_node == node_id { if self.state.asking || cmd_name.as_ref() == "RESTORE" { self.state.asking = false; return None; } }
                if self.state.asking { self.state.asking = false; }
                if self.state.readonly { let is_read_cmd = self.registry.get(&cmd_name).is_some_and(|c| c.flags().contains(CommandFlags::READONLY)); if is_read_cmd { return None; } }
                if let Some(owner_node) = snapshot.nodes.get(&owner) { Some(Response::error(format!("MOVED {} {}:{}", first_slot, owner_node.addr.ip(), owner_node.addr.port()))) } else { Some(Response::error(format!("CLUSTERDOWN Hash slot {} not served", first_slot))) }
            }
            None => {
                if let Some(migration) = snapshot.migrations.get(&first_slot) && migration.target_node == node_id && (self.state.asking || cmd_name.as_ref() == "RESTORE") { self.state.asking = false; return None; }
                if self.state.asking { self.state.asking = false; }
                Some(Response::error(format!("CLUSTERDOWN Hash slot {} not served", first_slot)))
            }
        }
    }

    pub(crate) async fn check_migrating_multikey(&self, cmd: &ParsedCommand) -> Option<Response> {
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) { cmd_impl.keys(&cmd.args) } else { return None; };
        if keys.len() < 2 { return None; }
        let slot = slot_for_key(keys[0]);
        let snapshot = cluster_state.snapshot();
        let owner = snapshot.slot_assignment.get(&slot)?;
        if *owner != node_id { return None; }
        let migration = snapshot.migrations.get(&slot)?;
        let target_addr = snapshot.nodes.get(&migration.target_node)?.addr;
        let shard_id = shard_for_key(keys[0], self.num_shards);
        let keys_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::copy_from_slice(k)).collect();
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest { request_id: next_txid(), keys: keys_bytes, operation: ScatterOp::Exists, conn_id: self.state.id, response_tx };
        if self.shard_senders[shard_id].send(msg).await.is_err() { return Some(Response::error("ERR shard unavailable")); }
        let partial = match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await { Ok(Ok(partial)) => partial, _ => return Some(Response::error("ERR shard unavailable")) };
        let mut any_present = false;
        let mut any_absent = false;
        for (_, response) in &partial.results { match response { Response::Integer(1) => any_present = true, Response::Integer(0) => any_absent = true, _ => {} } if any_present && any_absent { return Some(Response::error("TRYAGAIN Multiple keys request during rehashing of slot")); } }
        if any_absent && !any_present { return Some(Self::ask_response(slot, target_addr)); }
        None
    }

    pub(crate) fn migrating_ask_for_nil(&self, cmd: &ParsedCommand, response: &Response) -> Option<Response> {
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;
        if !Self::is_nil_response(response) { return None; }
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) { cmd_impl.keys(&cmd.args) } else { return None; };
        if keys.is_empty() { return None; }
        let slot = slot_for_key(keys[0]);
        let snapshot = cluster_state.snapshot();
        if let Some(&owner) = snapshot.slot_assignment.get(&slot) && owner == node_id && let Some(migration) = snapshot.migrations.get(&slot) && let Some(target_node) = snapshot.nodes.get(&migration.target_node) { return Some(Self::ask_response(slot, target_node.addr)); }
        None
    }

    pub(crate) fn is_nil_response(response: &Response) -> bool { matches!(response, Response::Null | Response::Bulk(None)) }
    pub(crate) fn ask_response(slot: u16, addr: SocketAddr) -> Response { Response::error(format!("ASK {} {}:{}", slot, addr.ip(), addr.port())) }
}

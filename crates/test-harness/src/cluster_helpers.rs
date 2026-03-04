//! Cluster test helper utilities.
//!
//! Provides utility functions for cluster testing including:
//! - Slot calculation and key generation
//! - Response parsing for CLUSTER commands
//! - Redirect detection (MOVED/ASK)

#![allow(dead_code)]

use frogdb_protocol::Response;
use std::collections::HashMap;

// Re-export slot_for_key from frogdb_core
pub use frogdb_core::slot_for_key;

// Re-export response helpers already defined in crate::server
pub use crate::server::{get_error_message, is_error};

/// Error type for cluster operations.
#[derive(Debug, Clone)]
pub struct ClusterError {
    pub message: String,
}

impl ClusterError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClusterError: {}", self.message)
    }
}

impl std::error::Error for ClusterError {}

/// Parsed cluster info from CLUSTER INFO response.
#[derive(Debug, Clone, Default)]
pub struct ClusterInfo {
    pub cluster_state: String,
    pub cluster_slots_assigned: u16,
    pub cluster_slots_ok: u16,
    pub cluster_slots_pfail: u16,
    pub cluster_slots_fail: u16,
    pub cluster_known_nodes: usize,
    pub cluster_size: usize,
    pub cluster_current_epoch: u64,
    pub cluster_my_epoch: u64,
}

/// Parsed node info from CLUSTER NODES response.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: String,
    pub addr: String,
    pub cluster_port: Option<u16>,
    pub flags: Vec<String>,
    pub master_id: Option<String>,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String,
    pub slots: Vec<(u16, u16)>,
}

impl NodeInfo {
    /// Check if this node is a master.
    pub fn is_master(&self) -> bool {
        self.flags.iter().any(|f| f == "master")
    }

    /// Check if this node is marked as myself.
    pub fn is_myself(&self) -> bool {
        self.flags.iter().any(|f| f == "myself")
    }

    /// Check if this node is connected.
    pub fn is_connected(&self) -> bool {
        self.link_state == "connected"
    }
}

/// Generate a key that hashes to a specific slot.
pub fn key_for_slot(slot: u16) -> String {
    // Use hash tags to force a specific slot
    // Try simple keys first, then use hash tags
    for i in 0..100000 {
        let key = format!("key{}", i);
        if slot_for_key(key.as_bytes()) == slot {
            return key;
        }
    }
    // Fallback: use a hash tag approach
    format!("{{slot{}}}", slot)
}

/// Parse CLUSTER INFO response into structured data.
pub fn parse_cluster_info(response: &Response) -> Result<ClusterInfo, ClusterError> {
    let text = match response {
        Response::Bulk(Some(b)) => std::str::from_utf8(b)
            .map_err(|_| ClusterError::new("Invalid UTF-8 in CLUSTER INFO response"))?,
        _ => return Err(ClusterError::new("Expected bulk string response")),
    };

    let mut info = ClusterInfo::default();
    let map: HashMap<&str, &str> = text
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(2, ':');
            Some((parts.next()?, parts.next()?.trim()))
        })
        .collect();

    if let Some(state) = map.get("cluster_state") {
        info.cluster_state = state.to_string();
    }
    if let Some(val) = map.get("cluster_slots_assigned") {
        info.cluster_slots_assigned = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_slots_ok") {
        info.cluster_slots_ok = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_slots_pfail") {
        info.cluster_slots_pfail = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_slots_fail") {
        info.cluster_slots_fail = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_known_nodes") {
        info.cluster_known_nodes = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_size") {
        info.cluster_size = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_current_epoch") {
        info.cluster_current_epoch = val.parse().unwrap_or(0);
    }
    if let Some(val) = map.get("cluster_my_epoch") {
        info.cluster_my_epoch = val.parse().unwrap_or(0);
    }

    Ok(info)
}

/// Parse CLUSTER NODES response.
pub fn parse_cluster_nodes(response: &Response) -> Result<Vec<NodeInfo>, ClusterError> {
    let text = match response {
        Response::Bulk(Some(b)) => std::str::from_utf8(b)
            .map_err(|_| ClusterError::new("Invalid UTF-8 in CLUSTER NODES response"))?,
        _ => return Err(ClusterError::new("Expected bulk string response")),
    };

    let mut nodes = Vec::new();

    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }

        // Parse address (ip:port@cport or ip:port)
        let (addr, cluster_port) = parse_node_addr(parts[1]);

        // Parse flags
        let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();

        // Parse master id (- means no master)
        let master_id = if parts[3] == "-" {
            None
        } else {
            Some(parts[3].to_string())
        };

        // Parse slots (remaining parts after index 8)
        let mut slots = Vec::new();
        for slot_spec in parts.iter().skip(8) {
            if let Some((start, end)) = parse_slot_range(slot_spec) {
                slots.push((start, end));
            }
        }

        nodes.push(NodeInfo {
            id: parts[0].to_string(),
            addr,
            cluster_port,
            flags,
            master_id,
            ping_sent: parts[4].parse().unwrap_or(0),
            pong_recv: parts[5].parse().unwrap_or(0),
            config_epoch: parts[6].parse().unwrap_or(0),
            link_state: parts[7].to_string(),
            slots,
        });
    }

    Ok(nodes)
}

/// Parse node address from CLUSTER NODES format.
/// Format: ip:port[@cport]
fn parse_node_addr(addr_str: &str) -> (String, Option<u16>) {
    if let Some((addr, cport)) = addr_str.split_once('@') {
        (addr.to_string(), cport.parse().ok())
    } else {
        (addr_str.to_string(), None)
    }
}

/// Parse slot range from CLUSTER NODES format.
/// Format: slot or start-end
fn parse_slot_range(slot_spec: &str) -> Option<(u16, u16)> {
    // Skip importing/migrating slot specs
    if slot_spec.starts_with('[') {
        return None;
    }

    if let Some((start, end)) = slot_spec.split_once('-') {
        let start: u16 = start.parse().ok()?;
        let end: u16 = end.parse().ok()?;
        Some((start, end))
    } else {
        let slot: u16 = slot_spec.parse().ok()?;
        Some((slot, slot))
    }
}

/// Check if response is a MOVED redirect.
/// Returns (slot, target_addr) if it is.
pub fn is_moved_redirect(response: &Response) -> Option<(u16, String)> {
    if let Response::Error(e) = response {
        let msg = std::str::from_utf8(e).ok()?;
        if msg.starts_with("MOVED ") {
            let parts: Vec<&str> = msg.split_whitespace().collect();
            if parts.len() >= 3 {
                let slot: u16 = parts[1].parse().ok()?;
                let addr = parts[2].to_string();
                return Some((slot, addr));
            }
        }
    }
    None
}

/// Check if response is an ASK redirect.
/// Returns (slot, target_addr) if it is.
pub fn is_ask_redirect(response: &Response) -> Option<(u16, String)> {
    if let Response::Error(e) = response {
        let msg = std::str::from_utf8(e).ok()?;
        if msg.starts_with("ASK ") {
            let parts: Vec<&str> = msg.split_whitespace().collect();
            if parts.len() >= 3 {
                let slot: u16 = parts[1].parse().ok()?;
                let addr = parts[2].to_string();
                return Some((slot, addr));
            }
        }
    }
    None
}

/// Check if response is a CLUSTERDOWN error.
pub fn is_cluster_down(response: &Response) -> bool {
    if let Response::Error(e) = response
        && let Ok(msg) = std::str::from_utf8(e)
    {
        return msg.starts_with("CLUSTERDOWN ");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_slot_for_key_basic() {
        // Test deterministic hashing
        let slot1 = slot_for_key(b"hello");
        let slot2 = slot_for_key(b"hello");
        assert_eq!(slot1, slot2);

        // Different keys should (likely) have different slots
        let slot3 = slot_for_key(b"world");
        // Note: Could be same slot by chance, so we don't assert inequality
        let _ = slot3;
    }

    #[test]
    fn test_slot_for_key_hash_tag() {
        // Keys with same hash tag should hash to same slot
        let slot1 = slot_for_key(b"user:{123}:name");
        let slot2 = slot_for_key(b"user:{123}:email");
        assert_eq!(slot1, slot2);

        // Empty hash tag should be ignored
        let slot3 = slot_for_key(b"key{}value");
        let slot4 = slot_for_key(b"key{}value");
        assert_eq!(slot3, slot4);
    }

    #[test]
    fn test_key_for_slot() {
        // Generate key for slot 0
        let key = key_for_slot(0);
        assert_eq!(slot_for_key(key.as_bytes()), 0);

        // Generate key for slot 1000
        let key = key_for_slot(1000);
        assert_eq!(slot_for_key(key.as_bytes()), 1000);
    }

    #[test]
    fn test_parse_cluster_info() {
        let info_text = "cluster_state:ok\r\n\
            cluster_slots_assigned:16384\r\n\
            cluster_slots_ok:16384\r\n\
            cluster_slots_pfail:0\r\n\
            cluster_slots_fail:0\r\n\
            cluster_known_nodes:3\r\n\
            cluster_size:3\r\n\
            cluster_current_epoch:5\r\n\
            cluster_my_epoch:2\r\n";

        let response = Response::Bulk(Some(Bytes::from(info_text)));
        let info = parse_cluster_info(&response).unwrap();

        assert_eq!(info.cluster_state, "ok");
        assert_eq!(info.cluster_slots_assigned, 16384);
        assert_eq!(info.cluster_slots_ok, 16384);
        assert_eq!(info.cluster_known_nodes, 3);
        assert_eq!(info.cluster_size, 3);
        assert_eq!(info.cluster_current_epoch, 5);
        assert_eq!(info.cluster_my_epoch, 2);
    }

    #[test]
    fn test_parse_cluster_nodes() {
        let nodes_text = "abc123 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460\n\
            def456 127.0.0.1:6380@16380 master - 0 0 2 connected 5461-10922\n\
            ghi789 127.0.0.1:6381@16381 master - 0 0 3 connected 10923-16383\n";

        let response = Response::Bulk(Some(Bytes::from(nodes_text)));
        let nodes = parse_cluster_nodes(&response).unwrap();

        assert_eq!(nodes.len(), 3);

        // First node
        assert_eq!(nodes[0].id, "abc123");
        assert_eq!(nodes[0].addr, "127.0.0.1:6379");
        assert_eq!(nodes[0].cluster_port, Some(16379));
        assert!(nodes[0].is_master());
        assert!(nodes[0].is_myself());
        assert_eq!(nodes[0].slots, vec![(0, 5460)]);

        // Second node
        assert_eq!(nodes[1].id, "def456");
        assert!(nodes[1].is_master());
        assert!(!nodes[1].is_myself());
    }

    #[test]
    fn test_is_moved_redirect() {
        let moved = Response::Error(Bytes::from("MOVED 3999 127.0.0.1:6381"));
        let result = is_moved_redirect(&moved);
        assert_eq!(result, Some((3999, "127.0.0.1:6381".to_string())));

        let ok = Response::Simple(Bytes::from("OK"));
        assert_eq!(is_moved_redirect(&ok), None);
    }

    #[test]
    fn test_is_ask_redirect() {
        let ask = Response::Error(Bytes::from("ASK 3999 127.0.0.1:6381"));
        let result = is_ask_redirect(&ask);
        assert_eq!(result, Some((3999, "127.0.0.1:6381".to_string())));
    }

    #[test]
    fn test_is_cluster_down() {
        let down = Response::Error(Bytes::from("CLUSTERDOWN The cluster is down"));
        assert!(is_cluster_down(&down));

        let ok = Response::Simple(Bytes::from("OK"));
        assert!(!is_cluster_down(&ok));
    }
}

//! Client connection information and kill filtering.

use std::net::SocketAddr;
use std::time::Instant;

use bytes::Bytes;

use super::ClientFlags;
use super::ClientMemoryUsage;
use super::stats::ClientStats;
use crate::clock;

/// Client connection information snapshot.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Unique connection ID.
    pub id: u64,
    /// Remote client address.
    pub addr: SocketAddr,
    /// Local server address.
    pub local_addr: Option<SocketAddr>,
    /// Client name.
    pub name: Option<Bytes>,
    /// When the connection was created.
    pub created_at: Instant,
    /// When the last command was executed.
    pub last_command_at: Instant,
    /// Current client flags.
    pub flags: ClientFlags,
    /// Number of channel subscriptions.
    pub sub_count: usize,
    /// Number of pattern subscriptions.
    pub psub_count: usize,
    /// Number of sharded subscriptions.
    pub ssub_count: usize,
    /// Whether client is in MULTI/EXEC.
    pub in_multi: bool,
    /// Number of commands queued in MULTI.
    pub multi_queue_len: usize,
    /// Number of keys currently watched (WATCH).
    pub watch_count: usize,
    /// Library name (from CLIENT SETINFO).
    pub lib_name: Option<Bytes>,
    /// Library version (from CLIENT SETINFO).
    pub lib_ver: Option<Bytes>,
    /// Per-client statistics (optional, only populated for stats queries).
    pub stats: Option<ClientStats>,
    /// Currently executing command (e.g. "client|list").
    pub current_cmd: Option<String>,
    /// Per-client memory usage breakdown.
    pub memory: ClientMemoryUsage,
}

impl ClientInfo {
    /// Format as CLIENT LIST entry.
    pub fn to_client_list_entry(&self) -> String {
        let age = clock::elapsed(self.created_at).as_secs();
        let idle = clock::elapsed(self.last_command_at).as_secs();
        let addr_str = self.addr.to_string();
        let laddr_str = self.local_addr.map(|a| a.to_string()).unwrap_or_default();
        let name_str = self
            .name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let lib_name_str = self
            .lib_name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let lib_ver_str = self
            .lib_ver
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();
        let flags_str = self.flags.to_flag_string();
        let multi_qlen = if self.in_multi {
            self.multi_queue_len as i64
        } else {
            -1
        };

        let cmd_str = self.current_cmd.as_deref().unwrap_or("NULL");
        let tot_mem = self.memory.total();
        // tot-net-in/tot-net-out are cumulative per-client byte counters
        // tracked in `ClientStats` (see `to_client_list_entry`'s caller in
        // `client_conn_command.rs`, which is where `tot-cmds` comes from the
        // same struct). `stats` is `None` only in contexts that never render
        // (e.g. `KillFilter` matching), so 0 there is truthful — no client
        // has ever seen those bytes reported as anything else.
        let (tot_net_in, tot_net_out) = self
            .stats
            .as_ref()
            .map(|s| (s.bytes_recv, s.bytes_sent))
            .unwrap_or((0, 0));
        format!(
            "id={} addr={} laddr={} fd=0 name={} age={} idle={} flags={} db=0 sub={} psub={} ssub={} multi={} watch={} qbuf={} qbuf-free=0 argv-mem={} multi-mem={} tot-net-in={} tot-net-out={} rbs={} rbp={} obl={} oll={} omem={} tot-mem={} events=r cmd={} user=default redir=-1 resp=2 lib-name={} lib-ver={}",
            self.id,
            addr_str,
            laddr_str,
            name_str,
            age,
            idle,
            flags_str,
            self.sub_count,
            self.psub_count,
            self.ssub_count,
            multi_qlen,
            self.watch_count,
            self.memory.query_buf_size,
            self.memory.argv_mem,
            self.memory.multi_mem,
            tot_net_in,
            tot_net_out,
            self.memory.query_buf_size,
            self.memory.query_buf_peak,
            self.memory.output_buf_len,
            self.memory.output_list_len,
            self.memory.output_list_mem,
            tot_mem,
            cmd_str,
            lib_name_str,
            lib_ver_str
        )
    }

    /// Get client type string.
    pub fn client_type(&self) -> &'static str {
        if self.flags.contains(ClientFlags::MASTER) {
            "master"
        } else if self.flags.contains(ClientFlags::REPLICA) {
            "replica"
        } else if self.flags.contains(ClientFlags::PUBSUB) {
            "pubsub"
        } else {
            "normal"
        }
    }
}

/// Filter for CLIENT KILL command.
#[derive(Debug, Default)]
pub struct KillFilter {
    /// Kill by connection ID.
    pub id: Option<u64>,
    /// Kill by client address.
    pub addr: Option<SocketAddr>,
    /// Kill by local address.
    pub laddr: Option<SocketAddr>,
    /// Kill by client type.
    pub client_type: Option<String>,
    /// Skip the current connection.
    pub skip_me: bool,
    /// Current connection ID (for SKIPME).
    pub current_conn_id: Option<u64>,
}

impl KillFilter {
    /// Check if a client matches this filter.
    pub fn matches(&self, id: u64, info: &ClientInfo) -> bool {
        // Check SKIPME
        if self.skip_me
            && let Some(current_id) = self.current_conn_id
            && id == current_id
        {
            return false;
        }

        // Check ID filter
        if let Some(filter_id) = self.id
            && id != filter_id
        {
            return false;
        }

        // Check ADDR filter
        if let Some(ref filter_addr) = self.addr
            && info.addr != *filter_addr
        {
            return false;
        }

        // Check LADDR filter
        if let Some(ref filter_laddr) = self.laddr
            && info.local_addr.as_ref() != Some(filter_laddr)
        {
            return false;
        }

        // Check TYPE filter
        if let Some(ref filter_type) = self.client_type {
            let client_type = info.client_type();
            if client_type != filter_type {
                return false;
            }
        }

        true
    }
}

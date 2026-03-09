//! USDT (DTrace/bpftrace) probe definitions for FrogDB.
//!
//! When compiled with `--features usdt`, these probes are registered with the
//! kernel tracing infrastructure and can be observed via `dtrace` (macOS/illumos)
//! or `bpftrace` (Linux) with zero overhead when not actively traced.
//!
//! ```bash
//! # List probes
//! sudo dtrace -l -n 'frogdb*:::'
//!
//! # Trace commands
//! sudo dtrace -n 'frogdb*:::command-start { printf("%s\n", copyinstr(arg0)); }'
//! ```

// ---------------------------------------------------------------------------
// Provider definition (only compiled with the `usdt` feature)
// ---------------------------------------------------------------------------
#[cfg(feature = "usdt")]
#[usdt::provider]
mod frogdb {
    fn command__start(_command: &str, _key: &str, _conn_id: u64) {}
    fn command__done(_command: &str, _latency_us: u64, _status: &str) {}
    fn shard__message__sent(_from_shard: u64, _to_shard: u64, _msg_type: &str) {}
    fn shard__message__received(_shard: u64, _msg_type: &str, _queue_depth: u64) {}
    fn key__expired(_key: &str, _shard_id: u64) {}
    fn key__evicted(_key: &str, _shard_id: u64, _policy: &str) {}
    fn memory__pressure(_used: u64, _max: u64, _action: &str) {}
    fn wal__write(_shard_id: u64, _key: &str, _bytes: u64) {}
    fn scatter__start(_command: &str, _shard_count: u64, _txid: u64) {}
    fn scatter__done(_command: &str, _latency_us: u64, _shard_count: u64) {}
    fn pubsub__publish(_channel: &str, _subscribers: u64) {}
    fn connection__accept(_conn_id: u64, _addr: &str) {}
}

// ---------------------------------------------------------------------------
// Public fire helpers — always available, no-op when feature is disabled.
// ---------------------------------------------------------------------------

/// Register USDT probes with the kernel. Call once at startup.
#[cfg(feature = "usdt")]
pub fn register() -> Result<(), Box<dyn std::error::Error>> {
    usdt::register_probes().map_err(|e| e.to_string())?;
    Ok(())
}

/// No-op when USDT is disabled.
#[cfg(not(feature = "usdt"))]
pub fn register() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[inline(always)]
pub fn fire_command_start(command: &str, key: &str, conn_id: u64) {
    #[cfg(feature = "usdt")]
    frogdb::command__start!(|| (command, key, conn_id));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (command, key, conn_id);
    }
}

#[inline(always)]
pub fn fire_command_done(command: &str, latency_us: u64, status: &str) {
    #[cfg(feature = "usdt")]
    frogdb::command__done!(|| (command, latency_us, status));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (command, latency_us, status);
    }
}

#[inline(always)]
pub fn fire_shard_message_sent(from_shard: u64, to_shard: u64, msg_type: &str) {
    #[cfg(feature = "usdt")]
    frogdb::shard__message__sent!(|| (from_shard, to_shard, msg_type));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (from_shard, to_shard, msg_type);
    }
}

#[inline(always)]
pub fn fire_shard_message_received(shard: u64, msg_type: &str, queue_depth: u64) {
    #[cfg(feature = "usdt")]
    frogdb::shard__message__received!(|| (shard, msg_type, queue_depth));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (shard, msg_type, queue_depth);
    }
}

#[inline(always)]
pub fn fire_key_expired(key: &str, shard_id: u64) {
    #[cfg(feature = "usdt")]
    frogdb::key__expired!(|| (key, shard_id));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (key, shard_id);
    }
}

#[inline(always)]
pub fn fire_key_evicted(key: &str, shard_id: u64, policy: &str) {
    #[cfg(feature = "usdt")]
    frogdb::key__evicted!(|| (key, shard_id, policy));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (key, shard_id, policy);
    }
}

#[inline(always)]
pub fn fire_memory_pressure(used: u64, max: u64, action: &str) {
    #[cfg(feature = "usdt")]
    frogdb::memory__pressure!(|| (used, max, action));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (used, max, action);
    }
}

#[inline(always)]
pub fn fire_wal_write(shard_id: u64, key: &str, bytes: u64) {
    #[cfg(feature = "usdt")]
    frogdb::wal__write!(|| (shard_id, key, bytes));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (shard_id, key, bytes);
    }
}

#[inline(always)]
pub fn fire_scatter_start(command: &str, shard_count: u64, txid: u64) {
    #[cfg(feature = "usdt")]
    frogdb::scatter__start!(|| (command, shard_count, txid));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (command, shard_count, txid);
    }
}

#[inline(always)]
pub fn fire_scatter_done(command: &str, latency_us: u64, shard_count: u64) {
    #[cfg(feature = "usdt")]
    frogdb::scatter__done!(|| (command, latency_us, shard_count));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (command, latency_us, shard_count);
    }
}

#[inline(always)]
pub fn fire_pubsub_publish(channel: &str, subscribers: u64) {
    #[cfg(feature = "usdt")]
    frogdb::pubsub__publish!(|| (channel, subscribers));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (channel, subscribers);
    }
}

#[inline(always)]
pub fn fire_connection_accept(conn_id: u64, addr: &str) {
    #[cfg(feature = "usdt")]
    frogdb::connection__accept!(|| (conn_id, addr));
    #[cfg(not(feature = "usdt"))]
    {
        let _ = (conn_id, addr);
    }
}

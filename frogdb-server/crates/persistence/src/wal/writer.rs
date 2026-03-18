//! RocksDB-backed WAL writer implementation.
use super::config::{DurabilityMode, WalConfig, WalLagStats};
use super::flush::{WalCommand, WalEntry, WalLagAtomics, current_timestamp_ms, flush_thread_loop};
use crate::rocks::RocksStore;
use crate::serialization::serialize;
use bytes::Bytes;
use frogdb_types::traits::{WalOperation, WalWriter};
use frogdb_types::types::{KeyMetadata, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error};

pub struct RocksWalWriter {
    shard_id: usize,
    sequence: AtomicU64,
    config: WalConfig,
    cmd_tx: flume::Sender<WalCommand>,
    lag: Arc<WalLagAtomics>,
    last_sync_timestamp_ms: AtomicU64,
    flush_thread: Option<std::thread::JoinHandle<()>>,
}

impl RocksWalWriter {
    pub fn new(
        rocks: Arc<RocksStore>,
        shard_id: usize,
        config: WalConfig,
        metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Self {
        let dm = match &config.mode {
            DurabilityMode::Async => "async".to_string(),
            DurabilityMode::Periodic { interval_ms } => format!("periodic_{}ms", interval_ms),
            DurabilityMode::Sync => "sync".to_string(),
        };
        debug!(shard_id, durability_mode = %dm, "WAL writer created");
        let now_ms = current_timestamp_ms();
        let (cmd_tx, cmd_rx) = flume::bounded(config.channel_capacity);
        let lag = Arc::new(WalLagAtomics {
            pending_ops: std::sync::atomic::AtomicUsize::new(0),
            pending_bytes: std::sync::atomic::AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(now_ms),
        });
        let flush_thread = {
            let lag = Arc::clone(&lag);
            let bst = config.batch_size_threshold;
            let bt = Duration::from_millis(config.batch_timeout_ms);
            let mode = config.mode.clone();
            std::thread::Builder::new()
                .name(format!("wal-flush-{shard_id}"))
                .spawn(move || {
                    flush_thread_loop(
                        cmd_rx,
                        rocks,
                        shard_id,
                        mode,
                        bst,
                        bt,
                        lag,
                        metrics_recorder,
                    );
                })
                .expect("failed to spawn WAL flush thread")
        };
        Self {
            shard_id,
            sequence: AtomicU64::new(0),
            config,
            cmd_tx,
            lag,
            last_sync_timestamp_ms: AtomicU64::new(now_ms),
            flush_thread: Some(flush_thread),
        }
    }
    pub async fn write_set(
        &self,
        key: &[u8],
        value: &Value,
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        let serialized = serialize(value, metadata);
        let size_estimate = key.len() + serialized.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Put {
                key: Bytes::copy_from_slice(key),
                value: serialized,
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_set)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }
    pub async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        let size_estimate = key.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Delete {
                key: Bytes::copy_from_slice(key),
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_delete)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }
    pub async fn flush_async(&self) -> std::io::Result<()> {
        let (done_tx, done_rx) = flume::bounded(1);
        self.cmd_tx
            .send_async(WalCommand::Flush { done_tx })
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (flush_async)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        done_rx.recv_async().await.map_err(|_| {
            error!(shard_id = self.shard_id, "WAL flush response lost");
            std::io::Error::other("WAL flush response channel disconnected")
        })?
    }
    pub fn lag_stats(&self) -> WalLagStats {
        let now_ms = current_timestamp_ms();
        let lft = self.lag.last_flush_timestamp_ms.load(Ordering::Acquire);
        let dlm = now_ms.saturating_sub(lft);
        let (slm, lstm) = match &self.config.mode {
            DurabilityMode::Periodic { .. } | DurabilityMode::Sync => {
                let ls = self.last_sync_timestamp_ms.load(Ordering::Acquire);
                (Some(now_ms.saturating_sub(ls)), Some(ls))
            }
            DurabilityMode::Async => (None, None),
        };
        WalLagStats {
            pending_ops: self.lag.pending_ops.load(Ordering::Acquire),
            pending_bytes: self.lag.pending_bytes.load(Ordering::Acquire),
            durability_lag_ms: dlm,
            sync_lag_ms: slm,
            sequence: self.sequence.load(Ordering::SeqCst),
            shard_id: self.shard_id,
            last_flush_timestamp_ms: lft,
            last_sync_timestamp_ms: lstm,
        }
    }
    pub fn record_sync(&self) {
        self.last_sync_timestamp_ms
            .store(current_timestamp_ms(), Ordering::Release);
    }
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
}

impl Drop for RocksWalWriter {
    fn drop(&mut self) {
        let (dead_tx, _) = flume::bounded(0);
        let _ = std::mem::replace(&mut self.cmd_tx, dead_tx);
        if let Some(h) = self.flush_thread.take() {
            let _ = h.join();
        }
    }
}

impl WalWriter for RocksWalWriter {
    fn append(&mut self, operation: &WalOperation) -> u64 {
        match operation {
            WalOperation::Set { key, value } => {
                let m = KeyMetadata::new(value.len());
                let s = serialize(&Value::string(value.clone()), &m);
                let se = key.len() + s.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Put {
                    key: key.clone(),
                    value: s,
                    size_estimate: se,
                }));
                seq
            }
            WalOperation::SetWithExpiry {
                key,
                value,
                expires_at,
            } => {
                let mut m = KeyMetadata::new(value.len());
                m.expires_at = Some(*expires_at);
                let s = serialize(&Value::string(value.clone()), &m);
                let se = key.len() + s.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Put {
                    key: key.clone(),
                    value: s,
                    size_estimate: se,
                }));
                seq
            }
            WalOperation::Delete { key } => {
                let se = key.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Delete {
                    key: key.clone(),
                    size_estimate: se,
                }));
                seq
            }
            WalOperation::Expire { .. } => self.sequence.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let (dt, dr) = flume::bounded(1);
        self.cmd_tx
            .send(WalCommand::Flush { done_tx: dt })
            .map_err(|_| std::io::Error::other("WAL flush thread disconnected"))?;
        dr.recv()
            .map_err(|_| std::io::Error::other("WAL flush response channel disconnected"))?
    }
    fn current_sequence(&self) -> u64 {
        self.sequence()
    }
}

//! WAL flush thread and batch operations.
use super::config::DurabilityMode;
use crate::rocks::RocksStore;
use bytes::Bytes;
use rocksdb::{WriteBatch, WriteOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{error, info, trace};

pub(super) enum WalEntry {
    Put {
        key: Bytes,
        value: Vec<u8>,
        size_estimate: usize,
    },
    Delete {
        key: Bytes,
        size_estimate: usize,
    },
}
pub(super) enum WalCommand {
    Write(WalEntry),
    Flush {
        done_tx: flume::Sender<std::io::Result<()>>,
    },
}
pub(super) struct WalLagAtomics {
    pub(super) pending_ops: AtomicUsize,
    pub(super) pending_bytes: AtomicUsize,
    pub(super) last_flush_timestamp_ms: AtomicU64,
}
pub(super) fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_thread_loop(
    rx: flume::Receiver<WalCommand>,
    rocks: Arc<RocksStore>,
    shard_id: usize,
    mode: DurabilityMode,
    batch_size_threshold: usize,
    batch_timeout: Duration,
    lag: Arc<WalLagAtomics>,
    metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
) {
    let mut batch = WriteBatch::default();
    let mut batch_size: usize = 0;
    let mut batch_ops: usize = 0;
    let mut last_flush = Instant::now();
    let is_sync = matches!(mode, DurabilityMode::Sync);
    let shard_label = shard_id.to_string();
    let metrics: &dyn frogdb_types::traits::MetricsRecorder = &*metrics_recorder;
    loop {
        match rx.recv_timeout(batch_timeout) {
            Ok(cmd) => match cmd {
                WalCommand::Write(entry) => {
                    apply_entry(
                        &rocks,
                        shard_id,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &lag,
                        metrics,
                        &shard_label,
                        entry,
                    );
                    while batch_size < batch_size_threshold {
                        match rx.try_recv() {
                            Ok(WalCommand::Write(e)) => {
                                apply_entry(
                                    &rocks,
                                    shard_id,
                                    &mut batch,
                                    &mut batch_size,
                                    &mut batch_ops,
                                    &lag,
                                    metrics,
                                    &shard_label,
                                    e,
                                );
                            }
                            Ok(WalCommand::Flush { done_tx }) => {
                                let _ = done_tx.send(do_flush(
                                    &rocks,
                                    shard_id,
                                    is_sync,
                                    &mut batch,
                                    &mut batch_size,
                                    &mut batch_ops,
                                    &mut last_flush,
                                    &lag,
                                    metrics,
                                    &shard_label,
                                ));
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    if batch_size >= batch_size_threshold || last_flush.elapsed() >= batch_timeout {
                        let _ = do_flush(
                            &rocks,
                            shard_id,
                            is_sync,
                            &mut batch,
                            &mut batch_size,
                            &mut batch_ops,
                            &mut last_flush,
                            &lag,
                            metrics,
                            &shard_label,
                        );
                    }
                }
                WalCommand::Flush { done_tx } => {
                    let _ = done_tx.send(do_flush(
                        &rocks,
                        shard_id,
                        is_sync,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &mut last_flush,
                        &lag,
                        metrics,
                        &shard_label,
                    ));
                }
            },
            Err(flume::RecvTimeoutError::Timeout) => {
                if !batch.is_empty() {
                    let _ = do_flush(
                        &rocks,
                        shard_id,
                        is_sync,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &mut last_flush,
                        &lag,
                        metrics,
                        &shard_label,
                    );
                }
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                while let Ok(cmd) = rx.try_recv() {
                    match cmd {
                        WalCommand::Write(e) => {
                            apply_entry(
                                &rocks,
                                shard_id,
                                &mut batch,
                                &mut batch_size,
                                &mut batch_ops,
                                &lag,
                                metrics,
                                &shard_label,
                                e,
                            );
                        }
                        WalCommand::Flush { done_tx } => {
                            let _ = done_tx.send(do_flush(
                                &rocks,
                                shard_id,
                                is_sync,
                                &mut batch,
                                &mut batch_size,
                                &mut batch_ops,
                                &mut last_flush,
                                &lag,
                                metrics,
                                &shard_label,
                            ));
                        }
                    }
                }
                if !batch.is_empty() {
                    let _ = do_flush(
                        &rocks,
                        shard_id,
                        is_sync,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &mut last_flush,
                        &lag,
                        metrics,
                        &shard_label,
                    );
                }
                info!(shard_id, "WAL flush thread exiting");
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_entry(
    rocks: &RocksStore,
    shard_id: usize,
    batch: &mut WriteBatch,
    batch_size: &mut usize,
    batch_ops: &mut usize,
    lag: &WalLagAtomics,
    metrics_recorder: &dyn frogdb_types::traits::MetricsRecorder,
    shard_label: &str,
    entry: WalEntry,
) {
    match entry {
        WalEntry::Put {
            key,
            value,
            size_estimate,
        } => {
            if let Err(e) = rocks.batch_put(batch, shard_id, &key, &value) {
                error!(shard_id, error = %e, "WAL batch_put failed");
                return;
            }
            *batch_size += size_estimate;
            *batch_ops += 1;
            lag.pending_ops.fetch_add(1, Ordering::Release);
            lag.pending_bytes
                .fetch_add(size_estimate, Ordering::Release);
            metrics_recorder.increment_counter(
                "frogdb_wal_writes_total",
                1,
                &[("shard", shard_label)],
            );
            metrics_recorder.increment_counter(
                "frogdb_wal_bytes_total",
                size_estimate as u64,
                &[("shard", shard_label)],
            );
        }
        WalEntry::Delete { key, size_estimate } => {
            if let Err(e) = rocks.batch_delete(batch, shard_id, &key) {
                error!(shard_id, error = %e, "WAL batch_delete failed");
                return;
            }
            *batch_size += size_estimate;
            *batch_ops += 1;
            lag.pending_ops.fetch_add(1, Ordering::Release);
            lag.pending_bytes
                .fetch_add(size_estimate, Ordering::Release);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn do_flush(
    rocks: &RocksStore,
    shard_id: usize,
    is_sync: bool,
    batch: &mut WriteBatch,
    batch_size: &mut usize,
    batch_ops: &mut usize,
    last_flush: &mut Instant,
    lag: &WalLagAtomics,
    metrics_recorder: &dyn frogdb_types::traits::MetricsRecorder,
    shard_label: &str,
) -> std::io::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let start = Instant::now();
    let flushed_batch = std::mem::take(batch);
    let flushed_bytes = *batch_size;
    let flushed_ops = *batch_ops;
    let batch_len = flushed_batch.len();
    *batch_size = 0;
    *batch_ops = 0;
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(is_sync);
    rocks
        .write_batch_opt(flushed_batch, &write_opts)
        .map_err(|e| {
            error!(shard_id, error = %e, "WAL flush failed");
            std::io::Error::other(e)
        })?;
    lag.pending_ops.fetch_sub(flushed_ops, Ordering::Release);
    lag.pending_bytes
        .fetch_sub(flushed_bytes, Ordering::Release);
    lag.last_flush_timestamp_ms
        .store(current_timestamp_ms(), Ordering::Release);
    *last_flush = Instant::now();
    let duration = start.elapsed();
    metrics_recorder.record_histogram(
        "frogdb_wal_flush_duration_seconds",
        duration.as_secs_f64(),
        &[("shard", shard_label)],
    );
    trace!(
        shard_id,
        entries = batch_len,
        bytes = flushed_bytes,
        duration_ms = duration.as_millis() as u64,
        "WAL batch flushed"
    );
    Ok(())
}

pub fn spawn_periodic_sync(
    rocks: Arc<RocksStore>,
    interval_ms: u64,
    monitor: Option<tokio_metrics::TaskMonitor>,
) -> tokio::task::JoinHandle<()> {
    info!(interval_ms, "Periodic WAL sync started");
    let interval = Duration::from_millis(interval_ms);
    let future = async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _span = tracing::info_span!("wal_sync").entered();
            if let Err(e) = rocks.flush() {
                tracing::warn!(error = %e, "Failed to sync WAL");
            }
        }
    };
    if let Some(monitor) = monitor {
        tokio::spawn(monitor.instrument(future))
    } else {
        tokio::spawn(future)
    }
}

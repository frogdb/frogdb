//! WAL streaming to replicas.

use bytes::{Buf, BytesMut};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

use frogdb_types::ReplicationTracker;

use crate::frame::ReplicationFrame;
use crate::tracker::ReplicaState;

use super::{
    LAG_CHECK_INTERVAL, PrimaryReplicationHandler, ReplicaConnectionHandle, parse_replconf_ack,
};

impl PrimaryReplicationHandler {
    pub(crate) async fn handle_partial_sync(
        &self,
        mut stream: TcpStream,
        addr: SocketAddr,
        offset: u64,
    ) -> io::Result<()> {
        let state = self.state.read().await;
        let response = format!("+CONTINUE {}\r\n", state.replication_id);
        stream.write_all(response.as_bytes()).await?;
        drop(state);
        let replica_id = self.tracker.register_replica(addr);
        self.tracker.set_state(replica_id, ReplicaState::Streaming);
        self.tracker.record_ack(replica_id, offset);
        self.start_streaming(stream, addr, replica_id).await
    }

    pub(crate) async fn start_streaming(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        replica_id: u64,
    ) -> io::Result<()> {
        let (frame_tx, mut frame_rx) = mpsc::channel::<ReplicationFrame>(1000);
        let mut wal_rx = self.wal_broadcast.subscribe();
        {
            let handle = ReplicaConnectionHandle {
                _replica_id: replica_id,
                _address: addr,
                _frame_tx: frame_tx,
                _state: ReplicaState::Streaming,
                _connected_at: Instant::now(),
            };
            self.connections.write().await.insert(replica_id, handle);
        }
        let (mut read_half, mut write_half) = stream.into_split();
        let tracker = self.tracker.clone();
        let read_task = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            loop {
                match read_half.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        while let Some((ack_offset, consumed)) = parse_replconf_ack(&buf) {
                            tracker.record_ack(replica_id, ack_offset);
                            buf.advance(consumed);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Error reading from replica");
                        break;
                    }
                }
            }
        });
        let lag_threshold_bytes = self.lag_config.threshold_bytes;
        let lag_threshold_secs = self.lag_config.threshold_secs;
        let lag_cooldown = self.lag_config.cooldown;
        let lag_tracker = self.tracker.clone();
        let lag_enabled = lag_threshold_bytes > 0 || lag_threshold_secs > 0;
        let write_timeout = if self.write_timeout_ms > 0 {
            Some(Duration::from_millis(self.write_timeout_ms))
        } else {
            None
        };
        let write_task = tokio::spawn(async move {
            let mut frame_count: u64 = 0;
            loop {
                tokio::select! {
                    frame = wal_rx.recv() => {
                        match frame {
                            Ok(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => { tracing::warn!(replica_id = replica_id, timeout_ms = timeout_dur.as_millis() as u64, "Write to replica timed out, disconnecting"); break; }
                                    }
                                } else { write_half.write_all(&encoded).await };
                                if let Err(e) = write_result { tracing::warn!(error = %e, "Error writing to replica"); break; }
                                if lag_enabled {
                                    frame_count += 1;
                                    if frame_count.is_multiple_of(LAG_CHECK_INTERVAL) {
                                        let byte_exceeded = lag_threshold_bytes > 0 && lag_tracker.replica_lag(replica_id).is_some_and(|lag| lag >= lag_threshold_bytes);
                                        let time_exceeded = lag_threshold_secs > 0 && lag_tracker.replica_lag_secs(replica_id).is_some_and(|secs| secs >= lag_threshold_secs as f64);
                                        if (byte_exceeded || time_exceeded) && !lag_tracker.is_in_lag_cooldown(replica_id, lag_cooldown) {
                                            tracing::warn!(replica_id = replica_id, byte_exceeded, time_exceeded, "Replica exceeded lag threshold, disconnecting for FULLRESYNC");
                                            lag_tracker.record_lag_disconnect(replica_id);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(n)) => { tracing::warn!(replica_id = replica_id, lagged = n, "Replica lagged in WAL stream, disconnecting for resync"); break; }
                        }
                    }
                    frame = frame_rx.recv() => {
                        match frame {
                            Some(frame) => {
                                let encoded = frame.encode();
                                let write_result = if let Some(timeout_dur) = write_timeout {
                                    match tokio::time::timeout(timeout_dur, write_half.write_all(&encoded)).await {
                                        Ok(r) => r,
                                        Err(_) => { tracing::warn!(replica_id = replica_id, timeout_ms = timeout_dur.as_millis() as u64, "Write to replica timed out (direct channel), disconnecting"); break; }
                                    }
                                } else { write_half.write_all(&encoded).await };
                                if let Err(e) = write_result { tracing::warn!(error = %e, "Error writing to replica"); break; }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
        tokio::select! { _ = read_task => {} _ = write_task => {} }
        self.connections.write().await.remove(&replica_id);
        self.tracker.unregister_replica(replica_id);
        tracing::info!(replica_id = replica_id, addr = %addr, "Replica disconnected");
        Ok(())
    }
}

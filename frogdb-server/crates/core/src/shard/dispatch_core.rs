use tracing::Instrument;

use super::message::ShardMessage;
use super::worker::ShardWorker;
use crate::store::Store;

impl ShardWorker {
    /// Dispatch core execution messages (Execute, ScatterRequest, GetVersion, ExecTransaction).
    pub(super) async fn dispatch_core(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::Execute {
                command,
                conn_id,
                txid: _,
                protocol_version,
                track_reads,
                no_touch,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(err);
                    return false;
                }
                // Set suppress_touch on the store before execution
                self.store.set_suppress_touch(no_touch);
                let shard_id = self.shard_id();
                let response = if self
                    .per_request_spans
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    self.execute_command(command.as_ref(), conn_id, protocol_version, track_reads)
                        .instrument(tracing::info_span!("shard_execute", shard_id))
                        .await
                } else {
                    self.execute_command(command.as_ref(), conn_id, protocol_version, track_reads)
                        .await
                };
                // Reset suppress_touch after execution
                self.store.set_suppress_touch(false);
                let _ = response_tx.send(response);
            }
            ShardMessage::ScatterRequest {
                request_id: _,
                keys,
                operation,
                conn_id,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let error_results: Vec<(bytes::Bytes, frogdb_protocol::Response)> =
                        keys.iter().map(|k| (k.clone(), err.clone())).collect();
                    let _ = response_tx.send(super::types::PartialResult {
                        results: error_results,
                    });
                    return false;
                }
                let result = self.execute_scatter_part(&keys, &operation, conn_id).await;
                let _ = response_tx.send(result);
            }
            ShardMessage::GetVersion { response_tx } => {
                let _ = response_tx.send(self.shard_version);
            }
            ShardMessage::ExecTransaction {
                commands,
                watches,
                conn_id,
                protocol_version,
                response_tx,
            } => {
                let result = if self
                    .per_request_spans
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    let shard_id = self.shard_id();
                    self.execute_transaction(commands, &watches, conn_id, protocol_version)
                        .instrument(tracing::info_span!("shard_exec_txn", shard_id))
                        .await
                } else {
                    self.execute_transaction(commands, &watches, conn_id, protocol_version)
                        .await
                };
                let _ = response_tx.send(result);
            }
            _ => unreachable!(),
        }
        false
    }
}

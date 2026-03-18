//! EVAL, EVALSHA, EVAL_RO, EVALSHA_RO handlers.

use bytes::Bytes;
use frogdb_core::{ShardMessage, ShardReadyResult, shard_for_key};
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::connection::{ConnectionHandler, next_txid};

impl ConnectionHandler {
    /// Handle EVAL / EVAL_RO command.
    pub(crate) async fn handle_eval(&self, args: &[Bytes], read_only: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'eval' command");
        }

        // Parse arguments
        let script_source = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self
                .execute_single_shard_script(script_source, keys, argv, 0, read_only)
                .await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys
            .iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script(script_source, keys, argv, shards[0], read_only)
                .await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script(script_source, keys, argv, shards, read_only)
                .await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a Lua script on a single shard.
    async fn execute_single_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
        read_only: bool,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
            response_tx,
        };

        if self.core.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a Lua script across multiple shards using VLL continuation locks.
    ///
    /// This acquires continuation locks on all involved shards (in sorted order to
    /// prevent deadlocks), executes the script on the primary shard, then releases
    /// all locks.
    async fn execute_cross_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
        read_only: bool,
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0]; // Execute on first shard

        // Phase 1: Acquire continuation locks on all shards (in sorted order)
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> =
            Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.core.shard_senders[shard_id].send(msg).await.is_err() {
                // Abort: release already-locked shards
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {
                    // Shard is ready
                }
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    // Lock acquisition failed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    // Channel closed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    // Timeout - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
            response_tx,
        };

        let response = if self.core.shard_senders[primary_shard]
            .send(msg)
            .await
            .is_ok()
        {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
    }

    /// Handle EVALSHA / EVALSHA_RO command.
    pub(crate) async fn handle_evalsha(&self, args: &[Bytes], read_only: bool) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'evalsha' command");
        }

        // Parse arguments
        let script_sha = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self
                .execute_single_shard_script_sha(script_sha, keys, argv, 0, read_only)
                .await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys
            .iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script_sha(script_sha, keys, argv, shards[0], read_only)
                .await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script_sha(script_sha, keys, argv, shards, read_only)
                .await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a cached Lua script (by SHA) on a single shard.
    async fn execute_single_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
        read_only: bool,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
            response_tx,
        };

        if self.core.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a cached Lua script (by SHA) across multiple shards using VLL continuation locks.
    async fn execute_cross_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
        read_only: bool,
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0];

        // Phase 1: Acquire continuation locks on all shards
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> =
            Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.core.shard_senders[shard_id].send(msg).await.is_err() {
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
            response_tx,
        };

        let response = if self.core.shard_senders[primary_shard]
            .send(msg)
            .await
            .is_ok()
        {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
    }
}

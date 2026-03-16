//! Persistence command handlers.
//!
//! This module handles persistence-related commands as extension methods
//! on `ConnectionHandler`:
//! - BGSAVE - Background save
//! - LASTSAVE - Last save time
//! - MIGRATE - Key migration (single-key and scatter-gather versions)

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{PartialResult, ScatterOp, ShardMessage, shard_for_key};
use frogdb_protocol::{ParsedCommand, Response};

use crate::connection::ConnectionHandler;
use crate::migrate::{MigrateArgs, MigrateClient, MigrateError};
use crate::server::next_txid;

impl ConnectionHandler {
    /// Handle a MIGRATE command asynchronously (single-key version).
    ///
    /// This method performs the actual key migration:
    /// 1. Serialize the key(s) with DUMP format
    /// 2. Connect to the target server
    /// 3. Authenticate if needed
    /// 4. Send RESTORE command(s)
    /// 5. Delete local key(s) if not COPY
    pub(crate) async fn handle_migrate_command(&mut self, args: Vec<Bytes>) -> Response {
        // Parse arguments
        let migrate_args = match MigrateArgs::parse(&args) {
            Ok(args) => args,
            Err(e) => return Response::error(e),
        };

        // Must have at least one key to migrate
        if migrate_args.keys.is_empty() {
            return Response::error("ERR No keys to migrate");
        }

        let timeout = Duration::from_millis(migrate_args.timeout_ms);

        // Connect to target server
        let mut client =
            match MigrateClient::connect(&migrate_args.host, migrate_args.port, timeout).await {
                Ok(c) => c,
                Err(e) => return Response::error(format!("IOERR error or timeout {}", e)),
            };

        // Authenticate if needed
        if let Some(ref auth) = migrate_args.auth
            && let Err(e) = client.auth(&auth.password, auth.username.as_deref()).await
        {
            return Response::error(format!("ERR Target authentication error: {}", e));
        }

        // Select database if not 0
        if migrate_args.dest_db != 0
            && let Err(e) = client.select_db(migrate_args.dest_db).await
        {
            return Response::error(format!("ERR Target database error: {}", e));
        }

        // Process each key
        let mut migrated_count = 0;
        for key in &migrate_args.keys {
            // Get the key's serialized data using DUMP format
            // We need to send a command to the shard to serialize the key
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let target_shard = shard_for_key(key, self.num_shards);

            if let Some(sender) = self.shard_senders.get(target_shard) {
                let dump_cmd = ParsedCommand::new(Bytes::from("DUMP"), vec![key.clone()]);
                if sender
                    .send(ShardMessage::Execute {
                        command: std::sync::Arc::new(dump_cmd),
                        conn_id: self.state.id,
                        txid: None,
                        protocol_version: self.state.protocol_version,
                        track_reads: false,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    return Response::error("ERR Internal error: shard unreachable");
                }
            } else {
                return Response::error("ERR Internal error: invalid shard");
            }

            let dump_response = match response_rx.await {
                Ok(r) => r,
                Err(_) => return Response::error("ERR Internal error: no response from shard"),
            };

            // Check if key exists (DUMP returns null for missing keys)
            let serialized = match &dump_response {
                Response::Bulk(Some(data)) => data.clone(),
                Response::Bulk(None) | Response::Null => {
                    // Key doesn't exist, skip it (Redis behavior)
                    continue;
                }
                Response::Error(e) => {
                    return Response::error(format!(
                        "ERR Error dumping key: {}",
                        String::from_utf8_lossy(e)
                    ));
                }
                _ => return Response::error("ERR Unexpected DUMP response"),
            };

            // Get TTL for the key
            let (ttl_tx, ttl_rx) = tokio::sync::oneshot::channel();
            if let Some(sender) = self.shard_senders.get(target_shard) {
                let pttl_cmd = ParsedCommand::new(Bytes::from("PTTL"), vec![key.clone()]);
                if sender
                    .send(ShardMessage::Execute {
                        command: std::sync::Arc::new(pttl_cmd),
                        conn_id: self.state.id,
                        txid: None,
                        protocol_version: self.state.protocol_version,
                        track_reads: false,
                        response_tx: ttl_tx,
                    })
                    .await
                    .is_err()
                {
                    return Response::error("ERR Internal error: shard unreachable");
                }
            }

            let ttl = match ttl_rx.await {
                Ok(Response::Integer(t)) => {
                    if t < 0 {
                        0 // No TTL or key doesn't exist
                    } else {
                        t
                    }
                }
                _ => 0,
            };

            // RESTORE the key on target
            if let Err(e) = client
                .restore(key, ttl, &serialized, migrate_args.replace)
                .await
            {
                return Response::error(format!("ERR Target error: {}", e));
            }

            // Delete local key if not COPY
            if !migrate_args.copy {
                let (del_tx, _del_rx) = tokio::sync::oneshot::channel();
                if let Some(sender) = self.shard_senders.get(target_shard) {
                    let del_cmd = ParsedCommand::new(Bytes::from("DEL"), vec![key.clone()]);
                    let _ = sender
                        .send(ShardMessage::Execute {
                            command: std::sync::Arc::new(del_cmd),
                            conn_id: self.state.id,
                            txid: None,
                            protocol_version: self.state.protocol_version,
                            track_reads: false,
                            response_tx: del_tx,
                        })
                        .await;
                }
            }

            migrated_count += 1;
        }

        if migrated_count == 0 && !migrate_args.keys.is_empty() {
            Response::Simple(Bytes::from("NOKEY"))
        } else {
            Response::ok()
        }
    }

    /// Handle MIGRATE command - scatter-gather version for multi-shard keys.
    pub(crate) async fn handle_migrate(&self, args: &[Bytes]) -> Response {
        // Parse arguments
        let parsed = match MigrateArgs::parse(args) {
            Ok(p) => p,
            Err(e) => return Response::error(e),
        };

        // Check if we have any keys to migrate
        if parsed.keys.is_empty() {
            return Response::Simple(Bytes::from_static(b"NOKEY"));
        }

        // Group keys by shard
        let mut shard_keys: HashMap<usize, Vec<Bytes>> = HashMap::new();
        for key in &parsed.keys {
            let shard_id = shard_for_key(key, self.num_shards);
            shard_keys.entry(shard_id).or_default().push(key.clone());
        }

        let txid = next_txid();
        let timeout_dur = Duration::from_millis(parsed.timeout_ms);

        // Scatter-gather DUMP from all shards
        let mut handles: Vec<(usize, tokio::sync::oneshot::Receiver<PartialResult>)> = Vec::new();

        for (shard_id, keys) in &shard_keys {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: txid,
                keys: keys.clone(),
                operation: ScatterOp::Dump,
                conn_id: self.state.id,
                response_tx: tx,
            };

            if self.shard_senders[*shard_id].send(msg).await.is_err() {
                return Response::error("IOERR error accessing local shard");
            }
            handles.push((*shard_id, rx));
        }

        // Collect serialized dumps from all shards
        let mut dumps: Vec<(Bytes, Vec<u8>)> = Vec::new();

        for (_shard_id, rx) in handles {
            let partial = match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => return Response::error("IOERR error accessing local shard"),
                Err(_) => return Response::error("IOERR timeout reading local keys"),
            };

            for (key, resp) in partial.results {
                if let Response::Bulk(Some(data)) = resp {
                    dumps.push((key, data.to_vec()));
                }
                // Skip keys that don't exist (Response::Null)
            }
        }

        // If no keys actually exist, return NOKEY
        if dumps.is_empty() {
            return Response::Simple(Bytes::from_static(b"NOKEY"));
        }

        // Connect to target server
        let mut client = match MigrateClient::connect(&parsed.host, parsed.port, timeout_dur).await
        {
            Ok(c) => c,
            Err(MigrateError::Timeout) => {
                return Response::error("IOERR timeout connecting to target");
            }
            Err(e) => return Response::error(format!("IOERR error connecting to target: {}", e)),
        };

        // Authenticate if needed
        if let Some(ref auth) = parsed.auth
            && let Err(e) = client.auth(&auth.password, auth.username.as_deref()).await
        {
            return Response::error(format!("IOERR authentication failed: {}", e));
        }

        // Select destination database
        if parsed.dest_db != 0
            && let Err(e) = client.select_db(parsed.dest_db).await
        {
            return Response::error(format!("IOERR error selecting database: {}", e));
        }

        // RESTORE each key on target
        for (key, data) in &dumps {
            // Extract TTL from serialized data (stored in header bytes 2-10 as i64 milliseconds)
            let ttl = if data.len() >= 10 {
                let expires_ms = i64::from_le_bytes(data[2..10].try_into().unwrap_or([0; 8]));
                if expires_ms > 0 {
                    // Convert absolute timestamp to relative TTL
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    let remaining = expires_ms - now_ms;
                    if remaining > 0 { remaining } else { 0 }
                } else {
                    0 // No expiry
                }
            } else {
                0
            };

            if let Err(e) = client.restore(key, ttl, data, parsed.replace).await {
                return Response::error(format!("IOERR error restoring key on target: {}", e));
            }
        }

        // Delete source keys (unless COPY option was specified)
        if !parsed.copy {
            // Group keys by shard for deletion
            let mut delete_shard_keys: HashMap<usize, Vec<Bytes>> = HashMap::new();
            for (key, _) in &dumps {
                let shard_id = shard_for_key(key, self.num_shards);
                delete_shard_keys
                    .entry(shard_id)
                    .or_default()
                    .push(key.clone());
            }

            let delete_txid = next_txid();
            let mut delete_handles: Vec<tokio::sync::oneshot::Receiver<PartialResult>> = Vec::new();

            for (shard_id, keys) in delete_shard_keys {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let msg = ShardMessage::ScatterRequest {
                    request_id: delete_txid,
                    keys,
                    operation: ScatterOp::Del,
                    conn_id: self.state.id,
                    response_tx: tx,
                };

                if self.shard_senders[shard_id].send(msg).await.is_err() {
                    // Log but don't fail - keys already migrated
                    tracing::warn!("Failed to delete source key after MIGRATE");
                }
                delete_handles.push(rx);
            }

            // Wait for deletes to complete (but don't fail if they time out)
            for rx in delete_handles {
                let _ = tokio::time::timeout(self.scatter_gather_timeout, rx).await;
            }
        }

        Response::ok()
    }

    /// Handle BGSAVE command - trigger a background snapshot.
    pub(crate) fn handle_bgsave(&self, args: &[Bytes]) -> Response {
        // Check for SCHEDULE option
        if !args.is_empty() {
            let opt = args[0].to_ascii_uppercase();
            if opt.as_slice() == b"SCHEDULE" {
                // BGSAVE SCHEDULE - schedule a save if one is already running,
                // otherwise start immediately
                if self.snapshot_coordinator.in_progress() {
                    self.snapshot_coordinator.schedule_snapshot();
                    return Response::Simple(Bytes::from_static(b"Background saving scheduled"));
                }
                // No save in progress, fall through to start one immediately
            }
        }

        match self.snapshot_coordinator.start_snapshot() {
            Ok(handle) => {
                tracing::info!(epoch = handle.epoch(), "BGSAVE started");
                Response::Simple(Bytes::from_static(b"Background saving started"))
            }
            Err(frogdb_core::persistence::SnapshotError::AlreadyInProgress) => {
                // Return a simple status like Redis does
                Response::Simple(Bytes::from_static(b"Background save already in progress"))
            }
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle LASTSAVE command - return Unix timestamp of last successful save.
    pub(crate) fn handle_lastsave(&self) -> Response {
        use std::time::{SystemTime, UNIX_EPOCH};

        match self.snapshot_coordinator.last_save_time() {
            Some(instant) => {
                // Convert Instant to Unix timestamp
                // We calculate how long ago the save was and subtract from current time
                let elapsed = instant.elapsed();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                let save_time = now.as_secs().saturating_sub(elapsed.as_secs());
                Response::Integer(save_time as i64)
            }
            None => {
                // No snapshot has been taken yet
                Response::Integer(0)
            }
        }
    }
}

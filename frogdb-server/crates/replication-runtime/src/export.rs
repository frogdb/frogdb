//! Reading this node's **live** keyspace for a full resync it must serve.
//!
//! The mirror image of [`install`](crate::install). A primary with
//! `persistence.enabled = false` has no RocksDB to checkpoint, but a full resync
//! still owes the replica the whole dataset — Redis serves exactly this case by
//! forking and serializing the RDB straight to the socket. FrogDB serializes
//! each shard's live keyspace into one blob instead (issue 67).
//!
//! The replication crate owns no shards, so it takes the reader as an injected
//! [`LiveSnapshotSource`], the same way it takes the pre-checkpoint drain hook.
//! This module is that reader.

use std::io;

use frogdb_core::sync::Arc;
use frogdb_core::{ReplicationMsg, ShardSender};
use frogdb_replication::primary::LiveSnapshotSource;
use tokio::sync::oneshot;

/// The live-keyspace reader for `shard_senders`, erased into the seam the
/// primary handler holds.
pub fn live_snapshot_source(shard_senders: Arc<Vec<ShardSender>>) -> LiveSnapshotSource {
    Arc::new(move || {
        let senders = shard_senders.clone();
        Box::pin(async move { export_live_dataset(&senders).await })
    })
}

/// Ask every shard for its dataset blob, in shard order.
///
/// **Sequential, one shard at a time**, which is the same cross-shard
/// granularity the checkpoint path has: each blob is one instant of its own
/// shard, and the offset the replica was granted was captured before any of
/// this ran, so the exported data can only run *ahead* of that offset. The
/// `(offset, current]` window is replayed from the backlog at the streaming
/// handoff.
///
/// **A shard that cannot export fails the whole sync.** A blob is installed as
/// a complete replacement of the receiving shard's keyspace, so a missing or
/// partial one is silent data loss on the replica — strictly worse than a
/// failed sync the replica retries.
async fn export_live_dataset(senders: &[ShardSender]) -> io::Result<Vec<Vec<u8>>> {
    let mut blobs = Vec::with_capacity(senders.len());
    for (shard_id, sender) in senders.iter().enumerate() {
        let (response_tx, response_rx) = oneshot::channel();
        sender
            .send(ReplicationMsg::ExportSnapshot { response_tx })
            .await
            .map_err(|_| io::Error::other(format!("shard {shard_id} is gone")))?;
        let blob = response_rx
            .await
            .map_err(|_| io::Error::other(format!("shard {shard_id} dropped the export ack")))?
            .map_err(io::Error::other)?;
        blobs.push(blob);
    }
    Ok(blobs)
}

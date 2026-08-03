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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_shards::{drop_export_ack, fake_shards, serve_export};

    // FM-REPLICATION-001
    /// One blob per shard, in shard order, is the whole contract: the trailer's
    /// checksum is folded under positional names (`shard-<n>.dataset`), so a
    /// dropped or reordered blob is a sync the replica cannot verify.
    #[tokio::test]
    async fn every_shard_contributes_its_blob_in_shard_order() {
        let mut shards = fake_shards(3);
        let source = live_snapshot_source(shards.senders());

        let (exported, ()) = tokio::join!(source(), async {
            serve_export(shards.shard(0), Ok(b"shard-zero".to_vec())).await;
            // An empty shard still owes a blob: its slot is what tells the
            // receiving side that shard held nothing, rather than that the
            // dataset stopped early.
            serve_export(shards.shard(1), Ok(Vec::new())).await;
            serve_export(shards.shard(2), Ok(b"shard-two".to_vec())).await;
        });

        let blobs = exported.expect("every shard answered, so the export succeeds");
        assert_eq!(
            blobs,
            vec![b"shard-zero".to_vec(), Vec::new(), b"shard-two".to_vec()],
            "the dataset is every shard's blob, in shard order"
        );
    }

    // FM-REPLICATION-001
    /// A shard that cannot serialize its keyspace fails the whole sync. The
    /// blob is installed as a complete replacement, so shipping the shards that
    /// did answer is silent data loss on the replica — strictly worse than a
    /// failed sync it retries.
    #[tokio::test]
    async fn a_shard_that_cannot_export_fails_the_whole_sync() {
        let mut shards = fake_shards(3);
        let senders = shards.senders();

        let (exported, ()) = tokio::join!(export_live_dataset(&senders), async {
            serve_export(shards.shard(0), Ok(b"shard-zero".to_vec())).await;
            serve_export(shards.shard(1), Err("key `big` is not hot".to_string())).await;
        });

        let err = exported.expect_err("a shard that cannot export must fail the sync");
        assert!(
            err.to_string().contains("key `big` is not hot"),
            "the shard's own reason must reach the operator, got {err}"
        );
        assert!(
            shards.untouched(2),
            "the export stops at the first failure rather than collecting a partial dataset"
        );
    }

    // FM-REPLICATION-001
    /// The two ways a shard can vanish mid-export — the channel already closed,
    /// and the worker dying after taking the request — are both failures of the
    /// sync, and both name the shard.
    #[tokio::test]
    async fn a_shard_that_vanishes_mid_export_fails_the_sync() {
        let mut shards = fake_shards(2);
        shards.disconnect(1);
        let senders = shards.senders();
        let (exported, ()) = tokio::join!(export_live_dataset(&senders), async {
            serve_export(shards.shard(0), Ok(b"shard-zero".to_vec())).await;
        });
        let err = exported.expect_err("a gone shard must fail the sync");
        assert!(err.to_string().contains("shard 1 is gone"), "got {err}");

        let mut shards = fake_shards(2);
        let senders = shards.senders();
        let (exported, ()) = tokio::join!(export_live_dataset(&senders), async {
            serve_export(shards.shard(0), Ok(b"shard-zero".to_vec())).await;
            drop_export_ack(shards.shard(1)).await;
        });
        let err = exported.expect_err("a dropped export ack must fail the sync");
        assert!(
            err.to_string().contains("shard 1 dropped the export ack"),
            "got {err}"
        );
    }
}

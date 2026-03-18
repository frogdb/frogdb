use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch search and pub/sub limits messages.
    pub(super) fn dispatch_search(&mut self, msg: ShardMessage) {
        match msg {
            ShardMessage::FlushSearchIndexes { response_tx } => {
                let sid = self.identity.shard_id;
                for idx in self.search.indexes.values_mut() {
                    if idx.is_dirty()
                        && let Err(e) = idx.commit()
                    {
                        tracing::error!(shard_id = sid, error = %e, "Failed to flush search index for snapshot");
                    }
                }
                let _ = response_tx.send(());
            }
            ShardMessage::GetPubSubLimitsInfo { response_tx } => {
                let info = super::types::PubSubLimitsInfo {
                    total_subscriptions: self.subscriptions.total_subscription_count(),
                    unique_channels: self.subscriptions.unique_channel_count(),
                    unique_patterns: self.subscriptions.unique_pattern_count(),
                };
                let _ = response_tx.send(info);
            }
            _ => unreachable!(),
        }
    }
}

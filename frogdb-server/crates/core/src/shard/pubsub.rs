use bytes::Bytes;

use crate::pubsub::{ConnId, IntrospectionRequest, IntrospectionResponse, PubSubSender};

use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle SUBSCRIBE - subscribe to broadcast channels.
    pub(crate) fn handle_subscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        // This returns the total subscription count after each subscription
        // The count is just a placeholder here since we don't track across shards
        let counts: Vec<usize> = channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions
                    .subscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect();
        self.subscriptions.check_thresholds_after_subscribe(
            self.identity.shard_id,
            &self.observability.metrics_recorder,
        );
        counts
    }

    /// Handle UNSUBSCRIBE - unsubscribe from broadcast channels.
    pub(crate) fn handle_unsubscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
    ) -> Vec<usize> {
        let counts: Vec<usize> = channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.unsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect();
        self.subscriptions.reset_thresholds_if_needed();
        counts
    }

    /// Handle PSUBSCRIBE - subscribe to patterns.
    pub(crate) fn handle_psubscribe(
        &mut self,
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        let counts: Vec<usize> = patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions
                    .psubscribe(pattern, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect();
        self.subscriptions.check_thresholds_after_subscribe(
            self.identity.shard_id,
            &self.observability.metrics_recorder,
        );
        counts
    }

    /// Handle PUNSUBSCRIBE - unsubscribe from patterns.
    pub(crate) fn handle_punsubscribe(
        &mut self,
        patterns: Vec<Bytes>,
        conn_id: ConnId,
    ) -> Vec<usize> {
        let counts: Vec<usize> = patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions.punsubscribe(&pattern, conn_id);
                i // Placeholder remaining count
            })
            .collect();
        self.subscriptions.reset_thresholds_if_needed();
        counts
    }

    /// Handle SSUBSCRIBE - subscribe to sharded channels.
    pub(crate) fn handle_ssubscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        let counts: Vec<usize> = channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions
                    .ssubscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect();
        self.subscriptions.check_thresholds_after_subscribe(
            self.identity.shard_id,
            &self.observability.metrics_recorder,
        );
        counts
    }

    /// Handle SUNSUBSCRIBE - unsubscribe from sharded channels.
    pub(crate) fn handle_sunsubscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
    ) -> Vec<usize> {
        let counts: Vec<usize> = channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.sunsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect();
        self.subscriptions.reset_thresholds_if_needed();
        counts
    }

    /// Handle slot migration for sharded pubsub subscribers.
    ///
    /// Sends `SUNSUBSCRIBE` notification to all subscribers of channels in the
    /// migrated slot, then removes those channels from the subscription table.
    pub(crate) fn handle_slot_migrated_pubsub(&mut self, slot: u16) {
        let count = self.subscriptions.drain_sharded_channels_for_slot(slot);
        if count > 0 {
            tracing::debug!(
                shard_id = self.identity.shard_id,
                slot,
                notifications = count,
                "Sent SUNSUBSCRIBE notifications for slot migration"
            );
            self.subscriptions.reset_thresholds_if_needed();
        }
    }

    /// Handle introspection requests.
    pub(crate) fn handle_introspection(
        &self,
        request: IntrospectionRequest,
    ) -> IntrospectionResponse {
        match request {
            IntrospectionRequest::Channels { pattern } => {
                let channels = self.subscriptions.channels(pattern.as_ref());
                IntrospectionResponse::Channels(channels)
            }
            IntrospectionRequest::NumSub { channels } => {
                let counts = self.subscriptions.numsub(&channels);
                IntrospectionResponse::NumSub(counts)
            }
            IntrospectionRequest::NumPat => {
                IntrospectionResponse::NumPat(self.subscriptions.unique_pattern_count())
            }
            IntrospectionRequest::ShardChannels { pattern } => {
                let channels = self.subscriptions.shard_channels(pattern.as_ref());
                IntrospectionResponse::Channels(channels)
            }
            IntrospectionRequest::ShardNumSub { channels } => {
                let counts = self.subscriptions.shard_numsub(&channels);
                IntrospectionResponse::NumSub(counts)
            }
        }
    }
}

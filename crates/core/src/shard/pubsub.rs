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
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.subscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle UNSUBSCRIBE - unsubscribe from broadcast channels.
    pub(crate) fn handle_unsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.unsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle PSUBSCRIBE - subscribe to patterns.
    pub(crate) fn handle_psubscribe(
        &mut self,
        patterns: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions.psubscribe(pattern, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle PUNSUBSCRIBE - unsubscribe from patterns.
    pub(crate) fn handle_punsubscribe(&mut self, patterns: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        patterns
            .into_iter()
            .enumerate()
            .map(|(i, pattern)| {
                self.subscriptions.punsubscribe(&pattern, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle SSUBSCRIBE - subscribe to sharded channels.
    pub(crate) fn handle_ssubscribe(
        &mut self,
        channels: Vec<Bytes>,
        conn_id: ConnId,
        sender: PubSubSender,
    ) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.ssubscribe(channel, conn_id, sender.clone());
                i + 1 // Placeholder count
            })
            .collect()
    }

    /// Handle SUNSUBSCRIBE - unsubscribe from sharded channels.
    pub(crate) fn handle_sunsubscribe(&mut self, channels: Vec<Bytes>, conn_id: ConnId) -> Vec<usize> {
        channels
            .into_iter()
            .enumerate()
            .map(|(i, channel)| {
                self.subscriptions.sunsubscribe(&channel, conn_id);
                i // Placeholder remaining count
            })
            .collect()
    }

    /// Handle introspection requests.
    pub(crate) fn handle_introspection(&self, request: IntrospectionRequest) -> IntrospectionResponse {
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
                IntrospectionResponse::NumPat(self.subscriptions.pattern_count())
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

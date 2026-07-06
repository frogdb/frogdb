use frogdb_types::metrics::definitions::PubsubMessages;

use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch pub/sub and connection lifecycle messages.
    pub(super) fn dispatch_pubsub(&mut self, msg: ShardMessage) {
        match msg {
            ShardMessage::Subscribe {
                channels,
                conn_id,
                sender,
                response_tx,
            } => {
                let counts = self.handle_subscribe(channels, conn_id, sender);
                let _ = response_tx.send(counts);
            }
            ShardMessage::Unsubscribe {
                channels,
                conn_id,
                response_tx,
            } => {
                let counts = self.handle_unsubscribe(channels, conn_id);
                let _ = response_tx.send(counts);
            }
            ShardMessage::PSubscribe {
                patterns,
                conn_id,
                sender,
                response_tx,
            } => {
                let counts = self.handle_psubscribe(patterns, conn_id, sender);
                let _ = response_tx.send(counts);
            }
            ShardMessage::PUnsubscribe {
                patterns,
                conn_id,
                response_tx,
            } => {
                let counts = self.handle_punsubscribe(patterns, conn_id);
                let _ = response_tx.send(counts);
            }
            ShardMessage::Publish {
                channel,
                message,
                response_tx,
            } => {
                let count = self.subscriptions.publish(&channel, &message);
                crate::probes::fire_pubsub_publish(
                    std::str::from_utf8(&channel).unwrap_or("<binary>"),
                    count as u64,
                );
                let shard_label = self.shard_id().to_string();
                PubsubMessages::inc(self.observability.metrics(), &shard_label);
                let _ = response_tx.send(count);
            }
            ShardMessage::PublishKeyspace { channel, payload } => {
                // Forwarded from a non-coordinator shard's keyspace emit. This
                // is the coordinator shard (shard 0), whose `subscriptions` table
                // is where every broadcast subscriber — including keyspace and
                // keyevent subscribers — is registered. Deliver into it exactly
                // as the `Local` fast path would on shard 0. No `frogdb_pubsub_*`
                // counter here: the `Local` keyspace path is uncounted too, so
                // counting only the forwarded hop would make multi-shard totals
                // diverge from single-shard for the same event.
                self.subscriptions.publish(&channel, &payload);
            }
            ShardMessage::ShardedSubscribe {
                channels,
                conn_id,
                sender,
                response_tx,
            } => {
                let counts = self.handle_ssubscribe(channels, conn_id, sender);
                let _ = response_tx.send(counts);
            }
            ShardMessage::ShardedUnsubscribe {
                channels,
                conn_id,
                response_tx,
            } => {
                let counts = self.handle_sunsubscribe(channels, conn_id);
                let _ = response_tx.send(counts);
            }
            ShardMessage::ShardedPublish {
                channel,
                message,
                response_tx,
            } => {
                let count = self.subscriptions.spublish(&channel, &message);
                let shard_label = self.shard_id().to_string();
                PubsubMessages::inc(self.observability.metrics(), &shard_label);
                let _ = response_tx.send(count);
            }
            ShardMessage::PubSubIntrospection {
                request,
                response_tx,
            } => {
                let response = self.handle_introspection(request);
                let _ = response_tx.send(response);
            }
            ShardMessage::ConnectionClosed { conn_id } => {
                self.subscriptions.remove_connection(conn_id);
                self.subscriptions.reset_thresholds_if_needed();
                self.tracking.tracking_table.remove_connection(conn_id);
                self.tracking.broadcast_table.remove_connection(conn_id);
                self.tracking.invalidation_registry.unregister(conn_id);
            }
            _ => unreachable!(),
        }
    }
}

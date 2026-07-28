use frogdb_types::metrics::definitions::PubsubMessages;

use super::message::PubSubMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch pub/sub and connection lifecycle messages.
    pub(super) fn dispatch_pubsub(&mut self, msg: PubSubMsg) {
        match msg {
            PubSubMsg::Subscribe {
                channels,
                conn_id,
                sender,
                response_tx,
            } => {
                self.handle_subscribe(channels, conn_id, sender);
                // Barrier ack: registration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::Unsubscribe {
                channels,
                conn_id,
                response_tx,
            } => {
                self.handle_unsubscribe(channels, conn_id);
                // Barrier ack: deregistration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::PSubscribe {
                patterns,
                conn_id,
                sender,
                response_tx,
            } => {
                self.handle_psubscribe(patterns, conn_id, sender);
                // Barrier ack: registration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::PUnsubscribe {
                patterns,
                conn_id,
                response_tx,
            } => {
                self.handle_punsubscribe(patterns, conn_id);
                // Barrier ack: deregistration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::Publish {
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
            PubSubMsg::PublishKeyspace { channel, payload } => {
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
            PubSubMsg::ShardedSubscribe {
                channels,
                conn_id,
                sender,
                response_tx,
            } => {
                self.handle_ssubscribe(channels, conn_id, sender);
                // Barrier ack: registration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::ShardedUnsubscribe {
                channels,
                conn_id,
                response_tx,
            } => {
                self.handle_sunsubscribe(channels, conn_id);
                // Barrier ack: deregistration is now visible in this shard.
                let _ = response_tx.send(());
            }
            PubSubMsg::ShardedPublish {
                channel,
                message,
                response_tx,
            } => {
                let count = self.subscriptions.spublish(&channel, &message);
                let shard_label = self.shard_id().to_string();
                PubsubMessages::inc(self.observability.metrics(), &shard_label);
                let _ = response_tx.send(count);
            }
            PubSubMsg::PubSubIntrospection {
                request,
                response_tx,
            } => {
                let response = self.handle_introspection(request);
                let _ = response_tx.send(response);
            }
            PubSubMsg::ConnectionClosed { conn_id } => {
                self.subscriptions.remove_connection(conn_id);
                self.subscriptions.reset_thresholds_if_needed();
                self.tracking.unregister(conn_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    use super::PubSubMsg;
    use crate::registry::CommandRegistry;
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::connection::NewConnection;
    use crate::shard::message::{Envelope, ShardReceiver};
    use crate::shard::worker::ShardWorker;

    fn minimal_worker() -> ShardWorker {
        let (_mtx, mrx) = mpsc::channel::<Envelope>(1);
        let (_ntx, nrx) = mpsc::channel::<NewConnection>(1);
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(mrx))
            .with_new_conn_rx(nrx)
            .with_shard_senders(Arc::new(vec![]))
            .with_registry(Arc::new(CommandRegistry::new()))
            .build()
    }

    /// The registration round trip is a barrier: the bare ack fires only after
    /// the subscription is durable in the shard's table. A `PUBLISH` that
    /// observes the ack (or is processed after it) is therefore guaranteed to
    /// see this subscriber. The `()` payload makes the barrier the contract —
    /// the previous `Vec<usize>` invited callers to read a fabricated count.
    #[test]
    fn subscribe_ack_fires_after_registration_is_visible() {
        let mut worker = minimal_worker();
        let channel = Bytes::from_static(b"barrier-chan");
        let (subscriber_tx, _subscriber_rx) = crate::pubsub::PubSubSender::unbounded();
        let (response_tx, mut response_rx) = oneshot::channel::<()>();

        worker.dispatch_pubsub(PubSubMsg::Subscribe {
            channels: vec![channel.clone()],
            conn_id: 1,
            sender: subscriber_tx,
            response_tx,
        });

        // The ack resolved to the bare unit barrier...
        assert_eq!(
            response_rx.try_recv(),
            Ok(()),
            "registration ack must be a bare unit barrier"
        );
        // ...and by the time it fired, the registration was visible in the table.
        assert_eq!(
            worker.subscriptions.numsub(std::slice::from_ref(&channel)),
            vec![(channel, 1)],
            "the subscriber must be visible in the table before the ack fires"
        );
    }
}

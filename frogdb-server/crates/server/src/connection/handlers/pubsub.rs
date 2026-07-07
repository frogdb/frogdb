//! Pub/Sub shared constants.
//!
//! The Pub/Sub command handlers (SUBSCRIBE/UNSUBSCRIBE/PUBLISH/PUBSUB/…) were
//! migrated behind the [`ConnectionCommand`](frogdb_core::ConnectionCommand)
//! seam; their logic now lives in
//! [`crate::connection::pubsub_conn_command`]. This module retains only the
//! broadcast-coordinator shard constant, which is also referenced by the
//! cluster bus and the connection lifecycle.

/// The broadcast pub/sub coordinator shard.
///
/// Broadcast (SUBSCRIBE/PSUBSCRIBE) registrations and PUBLISH delivery all go
/// through this single shard so each subscriber is registered exactly once and
/// each message is delivered exactly once, with a subscriber count that is not
/// multiplied by the number of shards. Forwarded keyspace notifications
/// (`ShardMessage::PublishKeyspace`) and the CLIENT TRACKING BCAST redirect
/// path rely on the same invariant.
pub(crate) const BROADCAST_SHARD: usize = 0;

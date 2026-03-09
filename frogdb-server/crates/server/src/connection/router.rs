//! Command routing logic.
//!
//! This module provides [`CommandRouter`] which determines how commands should
//! be routed - whether to the connection level, a specific shard, or across
//! multiple shards (scatter-gather).

use std::time::Duration;

use frogdb_core::{ConnectionLevelOp, ExecutionStrategy, MergeStrategy, ServerWideOp};

/// Result of routing a command.
///
/// Commands are routed based on their execution strategy and the current
/// connection state (transaction mode, pub/sub mode, etc.).
#[derive(Debug, Clone)]
pub enum RouteResult {
    /// Handle at the connection level (AUTH, HELLO, SUBSCRIBE, MULTI, etc.).
    ConnectionLevel(ConnectionLevelHandler),

    /// Route to a specific shard by key.
    RouteToShard {
        /// Target shard ID.
        shard_id: usize,
    },

    /// Execute across all shards and merge results.
    ScatterGather {
        /// Strategy for merging results from all shards.
        strategy: ScatterStrategy,
    },

    /// Queue the command in a transaction (MULTI mode).
    QueueInTransaction,

    /// Command requires Raft consensus (cluster mode).
    RaftConsensus,

    /// Command requires async external I/O (MIGRATE, etc.).
    AsyncExternal,

    /// Command is blocked waiting for a key.
    Blocking {
        /// Default timeout (None = block forever).
        default_timeout: Option<Duration>,
    },

    /// Server-wide command that needs to execute across all shards.
    ServerWide {
        /// The server-wide operation to perform.
        op: ServerWideOp,
    },
}

/// Connection-level command handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionLevelHandler {
    /// Authentication commands (AUTH).
    Auth,
    /// Protocol negotiation (HELLO).
    Hello,
    /// ACL commands.
    Acl,
    /// Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.).
    PubSub,
    /// Sharded Pub/Sub commands (SSUBSCRIBE, SPUBLISH).
    ShardedPubSub,
    /// Transaction commands (MULTI, EXEC, DISCARD).
    Transaction,
    /// Scripting commands (EVAL, EVALSHA, SCRIPT).
    Scripting,
    /// Function commands (FCALL, FUNCTION).
    Function,
    /// Client commands (CLIENT ID, CLIENT LIST, etc.).
    Client,
    /// Config commands (CONFIG GET, CONFIG SET).
    Config,
    /// Info commands.
    Info,
    /// Debug commands.
    Debug,
    /// Slowlog commands.
    Slowlog,
    /// Memory commands.
    Memory,
    /// Latency commands.
    Latency,
    /// Status commands.
    Status,
    /// Monitor command (MONITOR).
    Monitor,
    /// Connection state commands (RESET, SELECT, QUIT).
    ConnectionState,
    /// Cluster commands.
    Cluster,
    /// Replication commands (PSYNC, REPLCONF, etc.).
    Replication,
    /// Persistence commands (BGSAVE, LASTSAVE).
    Persistence,
}

/// Strategy for scatter-gather operations.
#[derive(Debug, Clone)]
pub enum ScatterStrategy {
    /// Preserve ordered array (MGET).
    OrderedArray,
    /// Sum integer results (DEL, EXISTS).
    SumIntegers,
    /// Collect keys from all shards (KEYS).
    CollectKeys,
    /// Merge scan cursors (SCAN).
    CursoredScan,
    /// All shards must return OK (FLUSHDB).
    AllOk,
    /// First non-null result (RANDOMKEY).
    FirstNonNull,
    /// Custom merge logic.
    Custom,
}

impl From<MergeStrategy> for ScatterStrategy {
    fn from(strategy: MergeStrategy) -> Self {
        match strategy {
            MergeStrategy::OrderedArray => ScatterStrategy::OrderedArray,
            MergeStrategy::SumIntegers => ScatterStrategy::SumIntegers,
            MergeStrategy::CollectKeys => ScatterStrategy::CollectKeys,
            MergeStrategy::CursoredScan => ScatterStrategy::CursoredScan,
            MergeStrategy::AllOk => ScatterStrategy::AllOk,
            MergeStrategy::Custom => ScatterStrategy::Custom,
        }
    }
}

/// Command router that determines how to route commands.
///
/// The router examines the command name and execution strategy to determine
/// the appropriate routing decision.
pub struct CommandRouter;

impl CommandRouter {
    /// Route a command based on its name and execution strategy.
    ///
    /// # Arguments
    ///
    /// * `cmd_name` - The uppercase command name
    /// * `strategy` - The execution strategy from the command definition
    /// * `in_transaction` - Whether we're in MULTI mode
    /// * `in_pubsub` - Whether we're in pub/sub mode
    ///
    /// # Returns
    ///
    /// The routing decision for the command.
    pub fn route(
        cmd_name: &str,
        strategy: &ExecutionStrategy,
        in_transaction: bool,
        in_pubsub: bool,
    ) -> RouteResult {
        // In pub/sub mode, only allow pub/sub commands and a few others
        if in_pubsub {
            return Self::route_pubsub_mode(cmd_name);
        }

        // Check if this should be queued in a transaction
        if in_transaction && !Self::is_immediate_in_transaction(cmd_name) {
            return RouteResult::QueueInTransaction;
        }

        // Route based on execution strategy
        match strategy {
            ExecutionStrategy::Standard => RouteResult::RouteToShard { shard_id: 0 }, // Will be calculated by key
            ExecutionStrategy::ConnectionLevel(op) => {
                RouteResult::ConnectionLevel(Self::op_to_handler(op))
            }
            ExecutionStrategy::Blocking { default_timeout } => RouteResult::Blocking {
                default_timeout: *default_timeout,
            },
            ExecutionStrategy::ScatterGather { merge } => RouteResult::ScatterGather {
                strategy: merge.clone().into(),
            },
            ExecutionStrategy::RaftConsensus => RouteResult::RaftConsensus,
            ExecutionStrategy::AsyncExternal => RouteResult::AsyncExternal,
            ExecutionStrategy::ServerWide(op) => RouteResult::ServerWide { op: op.clone() },
        }
    }

    /// Route commands when in pub/sub mode.
    fn route_pubsub_mode(cmd_name: &str) -> RouteResult {
        match cmd_name {
            // Subscription management
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "SSUBSCRIBE"
            | "SUNSUBSCRIBE" => RouteResult::ConnectionLevel(ConnectionLevelHandler::PubSub),
            // Allowed in pub/sub mode
            "PING" | "RESET" | "QUIT" | "DEBUG" => {
                RouteResult::ConnectionLevel(ConnectionLevelHandler::ConnectionState)
            }
            // All other commands are not allowed in pub/sub mode
            _ => RouteResult::ConnectionLevel(ConnectionLevelHandler::PubSub),
        }
    }

    /// Check if a command should be executed immediately even in MULTI mode.
    fn is_immediate_in_transaction(cmd_name: &str) -> bool {
        matches!(
            cmd_name,
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" | "QUIT" | "RESET"
        )
    }

    /// Convert a ConnectionLevelOp to a ConnectionLevelHandler.
    fn op_to_handler(op: &ConnectionLevelOp) -> ConnectionLevelHandler {
        match op {
            ConnectionLevelOp::Auth => ConnectionLevelHandler::Auth,
            ConnectionLevelOp::PubSub => ConnectionLevelHandler::PubSub,
            ConnectionLevelOp::Transaction => ConnectionLevelHandler::Transaction,
            ConnectionLevelOp::Scripting => ConnectionLevelHandler::Scripting,
            ConnectionLevelOp::Admin => ConnectionLevelHandler::Client,
            ConnectionLevelOp::ConnectionState => ConnectionLevelHandler::ConnectionState,
            ConnectionLevelOp::Replication => ConnectionLevelHandler::Replication,
            ConnectionLevelOp::Persistence => ConnectionLevelHandler::Persistence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_standard_command() {
        let result = CommandRouter::route("GET", &ExecutionStrategy::Standard, false, false);
        assert!(matches!(result, RouteResult::RouteToShard { .. }));
    }

    #[test]
    fn test_route_connection_level() {
        let result = CommandRouter::route(
            "AUTH",
            &ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth),
            false,
            false,
        );
        assert!(matches!(
            result,
            RouteResult::ConnectionLevel(ConnectionLevelHandler::Auth)
        ));
    }

    #[test]
    fn test_route_in_transaction() {
        let result = CommandRouter::route("GET", &ExecutionStrategy::Standard, true, false);
        assert!(matches!(result, RouteResult::QueueInTransaction));
    }

    #[test]
    fn test_exec_in_transaction() {
        let result = CommandRouter::route(
            "EXEC",
            &ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Transaction),
            true,
            false,
        );
        // EXEC should execute immediately even in transaction mode
        assert!(matches!(
            result,
            RouteResult::ConnectionLevel(ConnectionLevelHandler::Transaction)
        ));
    }
}

//! Command trait and related types.

use std::sync::Arc;

use bitflags::bitflags;
use bytes::Bytes;
use frogdb_protocol::Response;
use tokio::sync::mpsc;

use crate::error::CommandError;
use crate::shard::ShardMessage;
use crate::store::Store;

/// Command trait that all Redis commands implement.
pub trait Command: Send + Sync {
    /// Command name (e.g., "GET", "SET", "ZADD").
    fn name(&self) -> &'static str;

    /// Expected argument count.
    fn arity(&self) -> Arity;

    /// Command behavior flags.
    fn flags(&self) -> CommandFlags;

    /// Execute the command.
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError>;

    /// Extract key(s) from arguments for routing.
    ///
    /// Returns empty slice for keyless commands (PING, INFO, etc.).
    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]>;

    /// Whether this command requires all keys to be in the same slot.
    ///
    /// When true, the command will return CROSSSLOT error even if
    /// `allow_cross_slot_standalone` is enabled. This is used for
    /// commands like MSETNX that require atomicity across all keys.
    fn requires_same_slot(&self) -> bool {
        false
    }
}

/// Specifies the expected number of arguments for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arity {
    /// Exactly N arguments (e.g., GET = Fixed(1)).
    Fixed(usize),

    /// At least N arguments (e.g., MGET = AtLeast(1)).
    AtLeast(usize),

    /// Between min and max arguments inclusive.
    Range { min: usize, max: usize },
}

impl Arity {
    /// Check if the given argument count is valid for this arity.
    pub fn check(&self, count: usize) -> bool {
        match self {
            Arity::Fixed(n) => count == *n,
            Arity::AtLeast(n) => count >= *n,
            Arity::Range { min, max } => count >= *min && count <= *max,
        }
    }
}

bitflags! {
    /// Command behavior flags for routing and optimization.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CommandFlags: u32 {
        /// Command modifies data (SET, DEL, ZADD).
        const WRITE = 0b0000_0000_0001;

        /// Command only reads data (GET, ZRANGE).
        const READONLY = 0b0000_0000_0010;

        /// O(1) operation, suitable for latency-sensitive paths.
        const FAST = 0b0000_0000_0100;

        /// May block the connection (BLPOP, BRPOP).
        const BLOCKING = 0b0000_0000_1000;

        /// Operates on multiple keys (MGET, MSET, DEL with multiple keys).
        const MULTI_KEY = 0b0000_0001_0000;

        /// Pub/sub command, connection enters pub/sub mode.
        const PUBSUB = 0b0000_0010_0000;

        /// Script execution (EVAL, EVALSHA).
        const SCRIPT = 0b0000_0100_0000;

        /// Command cannot be called from Lua scripts.
        const NOSCRIPT = 0b0000_1000_0000;

        /// Command allowed during database loading (startup recovery).
        const LOADING = 0b0001_0000_0000;

        /// Command allowed on stale replica.
        const STALE = 0b0010_0000_0000;

        /// Command should not be logged to slowlog.
        const SKIP_SLOWLOG = 0b0100_0000_0000;

        /// Command may involve random data.
        const RANDOM = 0b1000_0000_0000;

        /// Command modifies server state (not data).
        const ADMIN = 0b0001_0000_0000_0000;

        /// Command returns data that varies by time.
        const NONDETERMINISTIC = 0b0010_0000_0000_0000;

        /// Command should not be propagated to replicas.
        const NO_PROPAGATE = 0b0100_0000_0000_0000;
    }
}

/// Context provided to commands during execution.
pub struct CommandContext<'a> {
    /// Local shard's data store.
    pub store: &'a mut dyn Store,

    /// For commands that need to reach other shards.
    pub shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// This shard's ID.
    pub shard_id: usize,

    /// Total number of shards.
    pub num_shards: usize,

    /// Connection ID (for client-specific operations).
    pub conn_id: u64,
}

impl<'a> CommandContext<'a> {
    /// Create a new command context.
    pub fn new(
        store: &'a mut dyn Store,
        shard_senders: &'a Arc<Vec<mpsc::Sender<ShardMessage>>>,
        shard_id: usize,
        num_shards: usize,
        conn_id: u64,
    ) -> Self {
        Self {
            store,
            shard_senders,
            shard_id,
            num_shards,
            conn_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arity_fixed() {
        let arity = Arity::Fixed(2);
        assert!(!arity.check(1));
        assert!(arity.check(2));
        assert!(!arity.check(3));
    }

    #[test]
    fn test_arity_at_least() {
        let arity = Arity::AtLeast(1);
        assert!(!arity.check(0));
        assert!(arity.check(1));
        assert!(arity.check(5));
    }

    #[test]
    fn test_arity_range() {
        let arity = Arity::Range { min: 1, max: 3 };
        assert!(!arity.check(0));
        assert!(arity.check(1));
        assert!(arity.check(2));
        assert!(arity.check(3));
        assert!(!arity.check(4));
    }

    #[test]
    fn test_command_flags() {
        let flags = CommandFlags::READONLY | CommandFlags::FAST;
        assert!(flags.contains(CommandFlags::READONLY));
        assert!(flags.contains(CommandFlags::FAST));
        assert!(!flags.contains(CommandFlags::WRITE));
    }
}

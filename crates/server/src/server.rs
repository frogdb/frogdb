//! Main server implementation.

use anyhow::Result;
use frogdb_core::sync::{Arc, AtomicU64, Ordering};
use frogdb_core::{CommandRegistry, ShardMessage, ShardWorker};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::acceptor::Acceptor;
use crate::config::Config;

/// Channel capacity for shard message queues.
const SHARD_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for new connection queues.
const NEW_CONN_CHANNEL_CAPACITY: usize = 256;

/// Global connection ID counter.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Global transaction ID counter for VLL (Very Lightweight Locking).
static NEXT_TXID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique connection ID.
pub fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// Generate a unique transaction ID for scatter-gather operations.
pub fn next_txid() -> u64 {
    NEXT_TXID.fetch_add(1, Ordering::SeqCst)
}

/// FrogDB server.
pub struct Server {
    /// Server configuration.
    config: Config,

    /// TCP listener.
    listener: TcpListener,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,

    /// New connection senders (one per shard).
    new_conn_senders: Vec<mpsc::Sender<frogdb_core::shard::NewConnection>>,

    /// Shard worker handles.
    shard_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Server {
    /// Create a new server instance.
    pub async fn new(config: Config) -> Result<Self> {
        // Bind TCP listener
        let listener = TcpListener::bind(config.bind_addr()).await?;

        info!(
            addr = %config.bind_addr(),
            "TCP listener bound"
        );

        // Create command registry
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);
        let registry = Arc::new(registry);

        // Determine number of shards
        let num_shards = if config.server.num_shards == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        } else {
            config.server.num_shards
        };

        info!(num_shards, "Initializing shards");

        // Create channels for each shard
        let mut shard_senders = Vec::with_capacity(num_shards);
        let mut shard_receivers = Vec::with_capacity(num_shards);
        let mut new_conn_senders = Vec::with_capacity(num_shards);
        let mut new_conn_receivers = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let (msg_tx, msg_rx) = mpsc::channel(SHARD_CHANNEL_CAPACITY);
            let (conn_tx, conn_rx) = mpsc::channel(NEW_CONN_CHANNEL_CAPACITY);

            shard_senders.push(msg_tx);
            shard_receivers.push(msg_rx);
            new_conn_senders.push(conn_tx);
            new_conn_receivers.push(conn_rx);
        }

        let shard_senders = Arc::new(shard_senders);

        // Spawn shard workers
        let mut shard_handles = Vec::with_capacity(num_shards);

        for (shard_id, (msg_rx, conn_rx)) in shard_receivers
            .into_iter()
            .zip(new_conn_receivers.into_iter())
            .enumerate()
        {
            let worker = ShardWorker::new(
                shard_id,
                num_shards,
                msg_rx,
                conn_rx,
                shard_senders.clone(),
                registry.clone(),
            );

            let handle = tokio::spawn(async move {
                worker.run().await;
            });

            shard_handles.push(handle);
        }

        Ok(Self {
            config,
            listener,
            registry,
            shard_senders,
            new_conn_senders,
            shard_handles,
        })
    }

    /// Run the server.
    pub async fn run(self) -> Result<()> {
        let acceptor = Acceptor::new(
            self.listener,
            self.new_conn_senders,
            self.shard_senders.clone(),
            self.registry.clone(),
            self.config.server.allow_cross_slot_standalone,
            self.config.server.scatter_gather_timeout_ms,
        );

        // Spawn acceptor task
        let acceptor_handle = tokio::spawn(async move {
            if let Err(e) = acceptor.run().await {
                error!(error = %e, "Acceptor error");
            }
        });

        info!(
            addr = %self.config.bind_addr(),
            "FrogDB server ready"
        );

        // Wait for shutdown signal
        shutdown_signal().await;

        info!("Shutdown signal received, stopping server...");

        // Send shutdown to all shards
        for sender in self.shard_senders.iter() {
            let _ = sender.send(ShardMessage::Shutdown).await;
        }

        // Wait for shard workers to finish
        for handle in self.shard_handles {
            let _ = handle.await;
        }

        // Abort acceptor
        acceptor_handle.abort();

        info!("Server shutdown complete");

        Ok(())
    }
}

/// Wait for a shutdown signal (SIGTERM or SIGINT).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Register all built-in commands.
pub fn register_commands(registry: &mut CommandRegistry) {
    // Connection commands
    registry.register(commands::PingCommand);
    registry.register(commands::EchoCommand);
    registry.register(commands::QuitCommand);
    registry.register(commands::CommandCommand);

    // String commands (basic)
    registry.register(commands::GetCommand);
    registry.register(commands::SetCommand);
    registry.register(commands::DelCommand);
    registry.register(commands::ExistsCommand);

    // String commands (extended)
    registry.register(crate::commands::string::SetnxCommand);
    registry.register(crate::commands::string::SetexCommand);
    registry.register(crate::commands::string::PsetexCommand);
    registry.register(crate::commands::string::AppendCommand);
    registry.register(crate::commands::string::StrlenCommand);
    registry.register(crate::commands::string::GetrangeCommand);
    registry.register(crate::commands::string::SetrangeCommand);
    registry.register(crate::commands::string::GetdelCommand);
    registry.register(crate::commands::string::GetexCommand);

    // Numeric commands
    registry.register(crate::commands::string::IncrCommand);
    registry.register(crate::commands::string::DecrCommand);
    registry.register(crate::commands::string::IncrbyCommand);
    registry.register(crate::commands::string::DecrbyCommand);
    registry.register(crate::commands::string::IncrbyfloatCommand);

    // Multi-key string commands
    registry.register(crate::commands::string::MgetCommand);
    registry.register(crate::commands::string::MsetCommand);
    registry.register(crate::commands::string::MsetnxCommand);

    // TTL/Expiry commands
    registry.register(crate::commands::expiry::ExpireCommand);
    registry.register(crate::commands::expiry::PexpireCommand);
    registry.register(crate::commands::expiry::ExpireatCommand);
    registry.register(crate::commands::expiry::PexpireatCommand);
    registry.register(crate::commands::expiry::TtlCommand);
    registry.register(crate::commands::expiry::PttlCommand);
    registry.register(crate::commands::expiry::PersistCommand);
    registry.register(crate::commands::expiry::ExpiretimeCommand);
    registry.register(crate::commands::expiry::PexpiretimeCommand);

    // Generic commands
    registry.register(crate::commands::generic::TypeCommand);
    registry.register(crate::commands::generic::RenameCommand);
    registry.register(crate::commands::generic::RenamenxCommand);
    registry.register(crate::commands::generic::TouchCommand);
    registry.register(crate::commands::generic::UnlinkCommand);
    registry.register(crate::commands::generic::ObjectCommand);
    registry.register(crate::commands::generic::DebugCommand);

    // Sorted set commands - basic
    registry.register(crate::commands::sorted_set::ZaddCommand);
    registry.register(crate::commands::sorted_set::ZremCommand);
    registry.register(crate::commands::sorted_set::ZscoreCommand);
    registry.register(crate::commands::sorted_set::ZmscoreCommand);
    registry.register(crate::commands::sorted_set::ZcardCommand);
    registry.register(crate::commands::sorted_set::ZincrbyCommand);

    // Sorted set commands - ranking
    registry.register(crate::commands::sorted_set::ZrankCommand);
    registry.register(crate::commands::sorted_set::ZrevrankCommand);

    // Sorted set commands - range queries
    registry.register(crate::commands::sorted_set::ZrangeCommand);
    registry.register(crate::commands::sorted_set::ZrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZrevrangebylexCommand);
    registry.register(crate::commands::sorted_set::ZcountCommand);
    registry.register(crate::commands::sorted_set::ZlexcountCommand);

    // Sorted set commands - pop & random
    registry.register(crate::commands::sorted_set::ZpopminCommand);
    registry.register(crate::commands::sorted_set::ZpopmaxCommand);
    registry.register(crate::commands::sorted_set::ZmpopCommand);
    registry.register(crate::commands::sorted_set::ZrandmemberCommand);

    // Sorted set commands - set operations
    registry.register(crate::commands::sorted_set::ZunionCommand);
    registry.register(crate::commands::sorted_set::ZunionstoreCommand);
    registry.register(crate::commands::sorted_set::ZinterCommand);
    registry.register(crate::commands::sorted_set::ZinterstoreCommand);
    registry.register(crate::commands::sorted_set::ZintercardCommand);
    registry.register(crate::commands::sorted_set::ZdiffCommand);
    registry.register(crate::commands::sorted_set::ZdiffstoreCommand);

    // Sorted set commands - other
    registry.register(crate::commands::sorted_set::ZscanCommand);
    registry.register(crate::commands::sorted_set::ZrangestoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyrankCommand);
    registry.register(crate::commands::sorted_set::ZremrangebyscoreCommand);
    registry.register(crate::commands::sorted_set::ZremrangebylexCommand);
}

// Commands module
pub mod commands {
    use bytes::Bytes;
    use frogdb_core::{
        Arity, Command, CommandContext, CommandError, CommandFlags, Expiry, SetCondition,
        SetOptions, SetResult, Value,
    };
    use frogdb_protocol::Response;

    /// PING command.
    pub struct PingCommand;

    impl Command for PingCommand {
        fn name(&self) -> &'static str {
            "PING"
        }

        fn arity(&self) -> Arity {
            Arity::Range { min: 0, max: 1 }
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::STALE | CommandFlags::LOADING
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            if args.is_empty() {
                Ok(Response::pong())
            } else {
                Ok(Response::bulk(args[0].clone()))
            }
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// ECHO command.
    pub struct EchoCommand;

    impl Command for EchoCommand {
        fn name(&self) -> &'static str {
            "ECHO"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            Ok(Response::bulk(args[0].clone()))
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// QUIT command.
    pub struct QuitCommand;

    impl Command for QuitCommand {
        fn name(&self) -> &'static str {
            "QUIT"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(0)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, CommandError> {
            Ok(Response::ok())
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// COMMAND command (placeholder).
    pub struct CommandCommand;

    impl Command for CommandCommand {
        fn name(&self) -> &'static str {
            "COMMAND"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(0)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::LOADING | CommandFlags::STALE
        }

        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, CommandError> {
            // Placeholder - return empty array
            Ok(Response::Array(vec![]))
        }

        fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
            vec![] // Keyless command
        }
    }

    /// GET command.
    pub struct GetCommand;

    impl Command for GetCommand {
        fn name(&self) -> &'static str {
            "GET"
        }

        fn arity(&self) -> Arity {
            Arity::Fixed(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            let key = &args[0];

            match ctx.store.get_with_expiry_check(key) {
                Some(value) => {
                    if let Some(sv) = value.as_string() {
                        Ok(Response::bulk(sv.as_bytes()))
                    } else {
                        Err(CommandError::WrongType)
                    }
                }
                None => Ok(Response::null()),
            }
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    }

    /// SET command with full option support.
    pub struct SetCommand;

    impl Command for SetCommand {
        fn name(&self) -> &'static str {
            "SET"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(2) // SET key value [options...]
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            let key = args[0].clone();
            let value = args[1].clone();

            // Parse options
            let mut opts = SetOptions::default();
            let mut i = 2;

            while i < args.len() {
                let opt = args[i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"NX" => {
                        if opts.condition != SetCondition::Always {
                            return Err(CommandError::SyntaxError);
                        }
                        opts.condition = SetCondition::NX;
                    }
                    b"XX" => {
                        if opts.condition != SetCondition::Always {
                            return Err(CommandError::SyntaxError);
                        }
                        opts.condition = SetCondition::XX;
                    }
                    b"GET" => {
                        opts.return_old = true;
                    }
                    b"KEEPTTL" => {
                        opts.keep_ttl = true;
                    }
                    b"EX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let secs = parse_u64(&args[i])?;
                        if secs == 0 {
                            return Err(CommandError::InvalidArgument {
                                message: "invalid expire time in 'set' command".to_string(),
                            });
                        }
                        opts.expiry = Some(Expiry::Ex(secs));
                    }
                    b"PX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ms = parse_u64(&args[i])?;
                        if ms == 0 {
                            return Err(CommandError::InvalidArgument {
                                message: "invalid expire time in 'set' command".to_string(),
                            });
                        }
                        opts.expiry = Some(Expiry::Px(ms));
                    }
                    b"EXAT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ts = parse_u64(&args[i])?;
                        opts.expiry = Some(Expiry::ExAt(ts));
                    }
                    b"PXAT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::SyntaxError);
                        }
                        let ts = parse_u64(&args[i])?;
                        opts.expiry = Some(Expiry::PxAt(ts));
                    }
                    _ => return Err(CommandError::SyntaxError),
                }
                i += 1;
            }

            // Check for conflicting options
            if opts.keep_ttl && opts.expiry.is_some() {
                return Err(CommandError::SyntaxError);
            }

            match ctx.store.set_with_options(key, Value::string(value), opts) {
                SetResult::Ok => Ok(Response::ok()),
                SetResult::OkWithOldValue(old) => {
                    match old {
                        Some(v) => {
                            if let Some(sv) = v.as_string() {
                                Ok(Response::bulk(sv.as_bytes()))
                            } else {
                                // Old value was wrong type but we replaced it anyway
                                Ok(Response::null())
                            }
                        }
                        None => Ok(Response::null()),
                    }
                }
                SetResult::NotSet => Ok(Response::null()),
            }
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
    }

    /// Parse a string as u64.
    fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
        std::str::from_utf8(arg)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(CommandError::NotInteger)
    }

    /// DEL command.
    pub struct DelCommand;

    impl Command for DelCommand {
        fn name(&self) -> &'static str {
            "DEL"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::WRITE
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            // Multi-key DEL: delete all keys and return count
            // Cross-shard routing is handled by connection handler
            let mut deleted = 0i64;
            for key in args {
                if ctx.store.delete(key) {
                    deleted += 1;
                }
            }
            Ok(Response::Integer(deleted))
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }

    /// EXISTS command.
    pub struct ExistsCommand;

    impl Command for ExistsCommand {
        fn name(&self) -> &'static str {
            "EXISTS"
        }

        fn arity(&self) -> Arity {
            Arity::AtLeast(1)
        }

        fn flags(&self) -> CommandFlags {
            CommandFlags::READONLY | CommandFlags::FAST
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, CommandError> {
            // Multi-key EXISTS: count how many keys exist
            // Note: Redis counts duplicates (EXISTS key key returns 2 if key exists)
            let mut count = 0i64;
            for key in args {
                if ctx.store.contains(key) {
                    count += 1;
                }
            }
            Ok(Response::Integer(count))
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }
}

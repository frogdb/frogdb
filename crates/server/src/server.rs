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

/// Generate a unique connection ID.
pub fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
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
    use crate::commands::*;

    registry.register(PingCommand);
    registry.register(EchoCommand);
    registry.register(QuitCommand);
    registry.register(CommandCommand);
    registry.register(GetCommand);
    registry.register(SetCommand);
    registry.register(DelCommand);
    registry.register(ExistsCommand);
}

// Commands module
pub mod commands {
    use bytes::Bytes;
    use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
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

            match ctx.store.get(key) {
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

    /// SET command.
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

            // For Phase 1, ignore options (EX, PX, NX, XX, etc.)
            ctx.store.set(key, frogdb_core::Value::string(value));

            Ok(Response::ok())
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            if args.is_empty() {
                vec![]
            } else {
                vec![&args[0]]
            }
        }
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
            // For Phase 1, only support single key
            // Multi-key DEL requires scatter-gather
            if args.len() > 1 {
                return Err(CommandError::CrossSlot);
            }

            let deleted = ctx.store.delete(&args[0]);
            Ok(Response::Integer(if deleted { 1 } else { 0 }))
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
            // For Phase 1, only support single key
            if args.len() > 1 {
                return Err(CommandError::CrossSlot);
            }

            let exists = ctx.store.contains(&args[0]);
            Ok(Response::Integer(if exists { 1 } else { 0 }))
        }

        fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
            args.iter().map(|a| a.as_ref()).collect()
        }
    }
}

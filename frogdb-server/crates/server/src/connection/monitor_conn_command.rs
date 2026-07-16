//! The connection-command seam (server-side MONITOR executor).
//!
//! MONITOR turns the connection into a monitor: it subscribes the connection to
//! the executed-command feed and replies `+OK`. The actual streaming is driven
//! by the connection run-loop (exactly like SUBSCRIBE — the command only
//! registers, the loop streams). Registration mutates a single connection-local
//! field (`ConnectionHandler::monitor_rx`), which is reachable neither through
//! [`frogdb_core::ConnStateMut`] (it lives on the handler, not the connection
//! state) nor through a `ConnCtx` field the seam could name without pulling the
//! server's [`MonitorBroadcaster`](crate::monitor::MonitorBroadcaster) type into
//! `core`.
//!
//! So — like [`frogdb_core::PubSubProvider`] — the registration runs server-side
//! behind the [`MonitorProvider`] seam: [`MonitorIo`] bundles the disjoint
//! handler borrows it needs (`&mut` the monitor receiver + `&` the broadcaster)
//! and implements [`MonitorProvider`]. The registrable [`MonitorConnCommand`]
//! executor calls `enable_monitor` and returns `+OK`; it carries the
//! [`CommandSpec`] so MONITOR is a single self-contained unit like the other
//! migrated connection commands. Dispatch builds a `ConnCtx` whose `monitor`
//! field holds `Some(&mut monitor_io)` and whose other fields are placeholders
//! (MONITOR reads none of them), mirroring
//! [`ConnectionHandler::conn_ctx_authmut`].

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, BoxFuture, CommandFlags, CommandSpec, ConnCtx, ConnectionCommand,
    ConnectionLevelOp, EventSpec, ExecutionStrategy, KeySpec, LookupSpec, MonitorProvider,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
use tokio::sync::broadcast;

use crate::connection::ConnectionHandler;
use crate::monitor::{MonitorBroadcaster, MonitorEvent};

// ============================================================================
// MonitorIo — the disjoint-borrow bundle behind the MonitorProvider seam
// ============================================================================

/// The connection-local MONITOR machinery, as a bundle of *disjoint* borrows of
/// the [`ConnectionHandler`] fields MONITOR needs. Held on [`ConnCtx::monitor`]
/// for the MONITOR executor only; see [`MonitorProvider`].
///
/// The `&mut` borrow of the receiver field and the `&` borrow of the broadcaster
/// are of distinct handler fields (`monitor_rx` vs. the observability deps), so
/// they coexist with each other and with the shared subsystem borrows the
/// surrounding [`ConnCtx`] takes.
pub(crate) struct MonitorIo<'a> {
    /// The connection's MONITOR subscription receiver (set on registration; the
    /// run-loop drains it to stream the executed-command feed).
    monitor_rx: &'a mut Option<broadcast::Receiver<Arc<MonitorEvent>>>,
    /// The server-wide monitor broadcaster; `subscribe()` yields the receiver.
    broadcaster: &'a MonitorBroadcaster,
}

impl<'a> MonitorIo<'a> {
    /// Bundle the disjoint handler borrows MONITOR needs.
    pub(crate) fn new(
        monitor_rx: &'a mut Option<broadcast::Receiver<Arc<MonitorEvent>>>,
        broadcaster: &'a MonitorBroadcaster,
    ) -> Self {
        Self {
            monitor_rx,
            broadcaster,
        }
    }
}

impl MonitorProvider for MonitorIo<'_> {
    fn enable_monitor(&mut self) {
        *self.monitor_rx = Some(self.broadcaster.subscribe());
    }
}

// ============================================================================
// MonitorConnCommand — the registrable executor
// ============================================================================

/// The `CommandSpec` for MONITOR. Declared here alongside the executor (rather
/// than in a stub `Command` impl) so the connection command is a single
/// self-contained unit. Strategy is `ConnectionLevel(Admin)`; the registry
/// validates that this agrees with the `Connection` executor variant.
static MONITOR_SPEC: CommandSpec = CommandSpec {
    name: "MONITOR",
    arity: Arity::Fixed(0),
    flags: CommandFlags::ADMIN,
    keys: KeySpec::None,
    access: AccessSpec::Uniform,
    wal: WalStrategy::NoOp,
    wakes: WaiterWake::None,
    event: EventSpec::NotApplicable,
    requires_same_slot: false,
    lookup: LookupSpec::None,
    strategy: ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin),
};

/// The registrable, `'static` MONITOR executor. Registered via
/// [`frogdb_core::CommandRegistry::register_connection`] in the server's command
/// registration (see `server/register.rs`).
pub(crate) static MONITOR_CONN_COMMAND: MonitorConnCommand = MonitorConnCommand;

/// MONITOR — subscribe this connection to the executed-command feed.
///
/// The executor registers the connection as a monitor via the
/// [`MonitorProvider`] seam and replies `+OK`; the run-loop streams the feed.
pub(crate) struct MonitorConnCommand;

impl ConnectionCommand for MonitorConnCommand {
    fn spec(&self) -> &'static CommandSpec {
        &MONITOR_SPEC
    }

    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        _args: &'a [Bytes],
    ) -> BoxFuture<'a, Response> {
        Box::pin(async move {
            let monitor = ctx
                .monitor
                .as_deref_mut()
                .expect("MONITOR executor requires ConnCtx::monitor");
            monitor.enable_monitor();
            Response::ok()
        })
    }
}

// ============================================================================
// Handler-side dispatch glue
// ============================================================================

impl ConnectionHandler {
    /// Build the MONITOR view over this handler's disjoint fields and run the
    /// executor through it, returning its single `+OK` reply.
    ///
    /// `command` is the `'static` executor already resolved from the registry,
    /// so it does not conflict with the `&mut self` borrows the [`ConnCtx`]
    /// takes. The `ConnCtx` mirrors [`conn_ctx_authmut`](Self::conn_ctx_authmut):
    /// MONITOR reads only `ConnCtx::monitor`, so the ambient view's defaults
    /// stand for everything else.
    pub(crate) async fn execute_monitor(
        &mut self,
        command: &'static dyn ConnectionCommand,
        args: &[Bytes],
    ) -> Response {
        let mut monitor_io = MonitorIo::new(
            &mut self.monitor_rx,
            self.observability.monitor_broadcaster.as_ref(),
        );
        let mut ctx = Self::base_ctx(
            &self.admin,
            &self.core,
            &self.observability,
            &self.cluster,
            &self.memory_diag,
            self.num_shards,
        )
        .with_monitor(&mut monitor_io);
        command.execute(&mut ctx, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::ExecutionStrategy;

    #[test]
    fn enable_monitor_subscribes_the_receiver() {
        let broadcaster = MonitorBroadcaster::new(16);
        let mut monitor_rx: Option<broadcast::Receiver<Arc<MonitorEvent>>> = None;
        let mut monitor_io = MonitorIo::new(&mut monitor_rx, &broadcaster);

        assert!(monitor_io.monitor_rx.is_none());
        monitor_io.enable_monitor();
        assert!(
            monitor_io.monitor_rx.is_some(),
            "MONITOR must subscribe the connection"
        );

        // Re-running MONITOR re-subscribes (idempotent, mirrors Redis).
        monitor_io.enable_monitor();
        assert!(monitor_io.monitor_rx.is_some());
    }

    #[test]
    fn enabled_monitor_receives_the_executed_command_feed() {
        let broadcaster = MonitorBroadcaster::new(16);
        let mut monitor_rx: Option<broadcast::Receiver<Arc<MonitorEvent>>> = None;
        MonitorIo::new(&mut monitor_rx, &broadcaster).enable_monitor();

        let addr: std::net::SocketAddr = "127.0.0.1:6379".parse().unwrap();
        broadcaster.send(MonitorEvent::new(
            addr,
            "PING",
            &[Bytes::from_static(b"PING")],
        ));

        let rx = monitor_rx.as_mut().expect("subscribed");
        assert!(rx.try_recv().is_ok(), "monitor should see the fed command");
    }

    #[test]
    fn monitor_spec_is_connection_level_and_valid() {
        assert!(MONITOR_CONN_COMMAND.spec().validate().is_ok());
        assert!(matches!(
            MONITOR_CONN_COMMAND.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Admin)
        ));
    }
}

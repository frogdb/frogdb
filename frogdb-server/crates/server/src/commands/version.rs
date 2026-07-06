//! Version management commands for rolling upgrades.
//!
//! - FROGDB.VERSION: Return binary, cluster, and active version info
//! - FROGDB.FINALIZE: Finalize a rolling upgrade (irreversible)

use bytes::Bytes;
use frogdb_core::{
    ExecutionStrategy,
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::{RaftClusterOp, Response};

// ============================================================================
// FROGDB.VERSION - Return version information
// ============================================================================

pub struct FrogdbVersionCommand;

impl Command for FrogdbVersionCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FROGDB.VERSION",
            arity: Arity::Fixed(0),
            flags: CommandFlags::READONLY
                .union(CommandFlags::FAST)
                .union(CommandFlags::LOADING)
                .union(CommandFlags::STALE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, _args: &[Bytes]) -> Result<Response, CommandError> {
        let binary_version = env!("CARGO_PKG_VERSION");

        let (cluster_version, active_version) = if let Some(cluster) = ctx.cluster_context() {
            let snapshot = cluster.state.snapshot();

            // Cluster version = minimum binary version across all nodes
            let min_version = snapshot
                .nodes
                .values()
                .filter(|n| !n.version.is_empty())
                .map(|n| n.version.as_str())
                .min()
                .unwrap_or(binary_version);

            (
                min_version.to_string(),
                snapshot.active_version.clone().unwrap_or_default(),
            )
        } else {
            // Standalone or replication mode: cluster version = binary version
            (binary_version.to_string(), String::new())
        };

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from("binary_version")),
            Response::bulk(Bytes::from(binary_version)),
            Response::bulk(Bytes::from("cluster_version")),
            Response::bulk(Bytes::from(cluster_version)),
            Response::bulk(Bytes::from("active_version")),
            Response::bulk(Bytes::from(active_version)),
        ]))
    }
}

// ============================================================================
// FROGDB.FINALIZE - Finalize a rolling upgrade
// ============================================================================

pub struct FrogdbFinalizeCommand;

impl Command for FrogdbFinalizeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "FROGDB.FINALIZE",
            arity: Arity::Fixed(1),
            flags: CommandFlags::ADMIN.union(CommandFlags::WRITE),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArgCount {
                command: "FROGDB.FINALIZE".to_string(),
            });
        }

        let version = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "invalid version string".to_string(),
            })?
            .to_string();

        // Validate it's a valid semver
        semver::Version::parse(&version).map_err(|e| CommandError::InvalidArgument {
            message: format!("invalid semver version '{}': {}", version, e),
        })?;

        if ctx.raft.is_some() {
            // Raft cluster mode: propose FinalizeUpgrade via Raft
            Ok(Response::RaftNeeded {
                op: RaftClusterOp::FinalizeUpgrade { version },
                register_node: None,
                unregister_node: None,
            })
        } else {
            // Standalone mode: apply directly (no coordination needed)
            // In replication mode, this would be handled differently
            // (the primary validates replicas and replicates via RESP)
            Err(CommandError::Internal {
                message: "FROGDB.FINALIZE requires cluster mode (Raft). \
                              Standalone mode does not require finalization."
                    .to_string(),
            })
        }
    }
}

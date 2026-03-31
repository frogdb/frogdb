//! Version management commands for rolling upgrades.
//!
//! - FROGDB.VERSION: Return binary, cluster, and active version info
//! - FROGDB.FINALIZE: Finalize a rolling upgrade (irreversible)

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::{RaftClusterOp, Response};

// ============================================================================
// FROGDB.VERSION - Return version information
// ============================================================================

pub struct FrogdbVersionCommand;

impl Command for FrogdbVersionCommand {
    fn name(&self) -> &'static str {
        "FROGDB.VERSION"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(0)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST | CommandFlags::LOADING | CommandFlags::STALE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

// ============================================================================
// FROGDB.FINALIZE - Finalize a rolling upgrade
// ============================================================================

pub struct FrogdbFinalizeCommand;

impl Command for FrogdbFinalizeCommand {
    fn name(&self) -> &'static str {
        "FROGDB.FINALIZE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // FROGDB.FINALIZE <version>
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::ADMIN | CommandFlags::WRITE
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

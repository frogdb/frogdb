//! MIGRATE operation state machine.
//!
//! This module implements the MIGRATE command as a state machine with
//! explicit phases. Each phase has clear inputs, outputs, and error handling.
//!
//! # Phases
//!
//! 1. **ParseArgs** - Validate and parse command arguments
//! 2. **Connecting** - Connect to destination server
//! 3. **Authenticating** - Authenticate if credentials provided
//! 4. **SelectingDb** - Select destination database
//! 5. **DumpingKeys** - Dump keys from local shards
//! 6. **RestoringKeys** - Restore keys to destination
//! 7. **DeletingLocal** - Delete local keys (unless COPY mode)
//!
//! ```text
//!   ParseArgs
//!       │
//!       ▼
//!   Connecting ──────────┐
//!       │                │
//!       ▼                │ (on error)
//!   Authenticating ──────┤
//!       │                │
//!       ▼                │
//!   SelectingDb ─────────┤
//!       │                │
//!       ▼                │
//!   DumpingKeys ─────────┤
//!       │                │
//!       ▼                ▼
//!   RestoringKeys ────> Failed
//!       │
//!       ▼
//!   DeletingLocal
//!       │
//!       ▼
//!   Complete
//! ```

use bytes::Bytes;
use frogdb_protocol::Response;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use frogdb_core::ShardMessage;

use crate::migrate::{MigrateArgs as ParsedMigrateArgs, MigrateClient, MigrateError};

use super::{Operation, PhaseResult};

/// State of the MIGRATE operation.
///
/// Note: MigrateClient doesn't implement Debug, so we use a wrapper.
pub enum MigrateState {
    /// Initial state: parse and validate arguments.
    ParseArgs {
        /// Raw command arguments.
        args: Vec<Bytes>,
    },

    /// Connecting to the destination server.
    Connecting {
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },

    /// Authenticating with the destination (if credentials provided).
    Authenticating {
        /// TCP client connection.
        client: MigrateClient,
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },

    /// Selecting the destination database.
    SelectingDb {
        /// TCP client connection.
        client: MigrateClient,
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },

    /// Dumping keys from local shards.
    DumpingKeys {
        /// TCP client connection.
        client: MigrateClient,
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },

    /// Restoring keys to destination.
    RestoringKeys {
        /// TCP client connection.
        client: MigrateClient,
        /// Keys and their serialized data.
        dumps: Vec<(Bytes, Bytes)>,
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },

    /// Deleting local keys after successful migration.
    DeletingLocal {
        /// Keys that were successfully migrated.
        migrated_keys: Vec<Bytes>,
        /// Parsed arguments.
        parsed: ParsedMigrateArgs,
    },
}

impl std::fmt::Debug for MigrateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrateState::ParseArgs { args } => f
                .debug_struct("ParseArgs")
                .field("args_len", &args.len())
                .finish(),
            MigrateState::Connecting { parsed } => f
                .debug_struct("Connecting")
                .field("host", &parsed.host)
                .finish(),
            MigrateState::Authenticating { parsed, .. } => f
                .debug_struct("Authenticating")
                .field("host", &parsed.host)
                .finish(),
            MigrateState::SelectingDb { parsed, .. } => f
                .debug_struct("SelectingDb")
                .field("db", &parsed.dest_db)
                .finish(),
            MigrateState::DumpingKeys { parsed, .. } => f
                .debug_struct("DumpingKeys")
                .field("keys_count", &parsed.keys.len())
                .finish(),
            MigrateState::RestoringKeys { dumps, .. } => f
                .debug_struct("RestoringKeys")
                .field("dumps_count", &dumps.len())
                .finish(),
            MigrateState::DeletingLocal { migrated_keys, .. } => f
                .debug_struct("DeletingLocal")
                .field("migrated_count", &migrated_keys.len())
                .finish(),
        }
    }
}

/// Context/dependencies for the MIGRATE operation.
pub struct MigrateOperation {
    /// Shard senders for dumping keys.
    #[allow(dead_code)]
    pub shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
}

impl MigrateOperation {
    /// Create a new MIGRATE operation with required dependencies.
    pub fn new(shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>) -> Self {
        Self { shard_senders }
    }
}

impl Operation for MigrateOperation {
    type State = MigrateState;
    type Output = Response;
    type Error = Response;

    async fn run_phase(
        &self,
        state: Self::State,
    ) -> PhaseResult<Self::Output, Self::State, Self::Error> {
        match state {
            MigrateState::ParseArgs { args } => {
                // Phase 1: Parse arguments
                match ParsedMigrateArgs::parse(&args) {
                    Ok(parsed) => PhaseResult::Continue(MigrateState::Connecting { parsed }),
                    Err(e) => PhaseResult::Failed(Response::error(e)),
                }
            }

            MigrateState::Connecting { parsed } => {
                // Phase 2: Connect to destination
                let timeout = Duration::from_millis(parsed.timeout_ms);
                match MigrateClient::connect(&parsed.host, parsed.port, timeout).await {
                    Ok(client) => {
                        if parsed.auth.is_some() {
                            PhaseResult::Continue(MigrateState::Authenticating { client, parsed })
                        } else {
                            PhaseResult::Continue(MigrateState::SelectingDb { client, parsed })
                        }
                    }
                    Err(e) => PhaseResult::Failed(Response::error(format!(
                        "IOERR error or timeout connecting to the target instance: {}",
                        e
                    ))),
                }
            }

            MigrateState::Authenticating { mut client, parsed } => {
                // Phase 3: Authenticate if credentials provided
                if let Some(ref auth) = parsed.auth {
                    match client.auth(&auth.password, auth.username.as_deref()).await {
                        Ok(()) => {
                            PhaseResult::Continue(MigrateState::SelectingDb { client, parsed })
                        }
                        Err(e) => PhaseResult::Failed(Response::error(format!(
                            "NOAUTH Authentication required: {}",
                            e
                        ))),
                    }
                } else {
                    PhaseResult::Continue(MigrateState::SelectingDb { client, parsed })
                }
            }

            MigrateState::SelectingDb { mut client, parsed } => {
                // Phase 4: Select destination database
                match client.select_db(parsed.dest_db).await {
                    Ok(()) => PhaseResult::Continue(MigrateState::DumpingKeys { client, parsed }),
                    Err(e) => PhaseResult::Failed(Response::error(format!(
                        "ERR Target instance returned error: {}",
                        e
                    ))),
                }
            }

            MigrateState::DumpingKeys { client, parsed } => {
                // Phase 5: Dump keys from local shards
                // NOTE: In a full implementation, this would use scatter-gather
                // to dump keys from each shard. For now, we create empty dumps
                // as a placeholder to demonstrate the state machine pattern.
                let dumps: Vec<(Bytes, Bytes)> = Vec::new();

                if dumps.is_empty() {
                    // No keys to migrate
                    return PhaseResult::Complete(Response::Simple("NOKEY".into()));
                }

                PhaseResult::Continue(MigrateState::RestoringKeys {
                    client,
                    dumps,
                    parsed,
                })
            }

            MigrateState::RestoringKeys {
                mut client,
                dumps,
                parsed,
            } => {
                // Phase 6: Restore keys to destination
                let mut migrated_keys = Vec::with_capacity(dumps.len());

                for (key, data) in dumps {
                    let ttl_ms = 0i64; // TODO: preserve TTL
                    match client.restore(&key, ttl_ms, &data, parsed.replace).await {
                        Ok(()) => {
                            migrated_keys.push(key);
                        }
                        Err(MigrateError::TargetError(msg)) if msg.contains("BUSYKEY") => {
                            // Key exists and REPLACE not specified
                            return PhaseResult::Failed(Response::error(
                                "BUSYKEY Target key name already exists",
                            ));
                        }
                        Err(e) => {
                            return PhaseResult::Failed(Response::error(format!(
                                "ERR Failed to restore key: {}",
                                e
                            )));
                        }
                    }
                }

                if parsed.copy {
                    // COPY mode: don't delete local keys
                    PhaseResult::Complete(Response::Simple("OK".into()))
                } else {
                    PhaseResult::Continue(MigrateState::DeletingLocal {
                        migrated_keys,
                        parsed,
                    })
                }
            }

            MigrateState::DeletingLocal {
                migrated_keys: _,
                parsed: _,
            } => {
                // Phase 7: Delete local keys
                // NOTE: In a full implementation, this would delete keys from
                // each shard using scatter-gather. For now, we just complete.
                PhaseResult::Complete(Response::Simple("OK".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrate_state_debug() {
        // Ensure states are Debug
        let state = MigrateState::ParseArgs { args: vec![] };
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("ParseArgs"));
    }
}

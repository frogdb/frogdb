use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, KeyspaceEventFlags, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::require_same_shard;
use crate::utils::{
    parse_i64, parse_usize, scored_array, scored_array_resp3, scored_array_with_scores_resp3,
};

// ============================================================================
// ZPOPMIN - Pop minimum score members
// ============================================================================

pub struct ZpopminCommand;

impl Command for ZpopminCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZPOPMIN",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::ZSET,
                name: "zpopmin",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();
        let (count, has_count) = if args.len() > 1 {
            let c = parse_i64(&args[1])?;
            if c < 0 {
                return Err(CommandError::InvalidArgument {
                    message: "value must be positive".to_string(),
                });
            }
            (c as usize, true)
        } else {
            (1, false)
        };

        let zset = match ctx.store.get_zset_mut(key)? {
            Some(zset) => zset,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_min(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        // In RESP3 with an explicit count argument, return an array of [member, score] pairs.
        // In RESP3 without count, return a flat [member, score] array.
        // In RESP2, always return a flat [member, score, ...] array with bulk string scores.
        if is_resp3 && has_count {
            Ok(scored_array_resp3(results, true))
        } else {
            Ok(scored_array_with_scores_resp3(results, true, is_resp3))
        }
    }
}

// ============================================================================
// ZPOPMAX - Pop maximum score members
// ============================================================================

pub struct ZpopmaxCommand;

impl Command for ZpopmaxCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZPOPMAX",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::ZSET,
                name: "zpopmax",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();
        let (count, has_count) = if args.len() > 1 {
            let c = parse_i64(&args[1])?;
            if c < 0 {
                return Err(CommandError::InvalidArgument {
                    message: "value must be positive".to_string(),
                });
            }
            (c as usize, true)
        } else {
            (1, false)
        };

        let zset = match ctx.store.get_zset_mut(key)? {
            Some(zset) => zset,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_max(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        // In RESP3 with an explicit count argument, return an array of [member, score] pairs.
        // In RESP3 without count, return a flat [member, score] array.
        // In RESP2, always return a flat [member, score, ...] array with bulk string scores.
        if is_resp3 && has_count {
            Ok(scored_array_resp3(results, true))
        } else {
            Ok(scored_array_with_scores_resp3(results, true, is_resp3))
        }
    }
}

// ============================================================================
// ZMPOP - Pop from multiple keys
// ============================================================================

pub struct ZmpopCommand;

impl Command for ZmpopCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZMPOP",
            arity: Arity::AtLeast(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::NumkeysAt {
                numkeys: 0,
                first: 1,
            },
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistOrDeleteFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Emits {
                class: KeyspaceEventFlags::ZSET,
                name: "zmpop",
            },
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0]).map_err(|_| CommandError::InvalidArgument {
            message: "numkeys can't be non-positive value".to_string(),
        })?;
        if numkeys == 0 {
            return Err(CommandError::InvalidArgument {
                message: "numkeys can't be non-positive value".to_string(),
            });
        }
        if args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        // Parse MIN|MAX and COUNT
        if remaining.is_empty() {
            return Err(CommandError::SyntaxError);
        }

        let direction = remaining[0].to_ascii_uppercase();
        let is_min = match direction.as_slice() {
            b"MIN" => true,
            b"MAX" => false,
            _ => return Err(CommandError::SyntaxError),
        };

        let mut count: usize = 1;
        let mut count_seen = false;
        let mut i = 1;
        while i < remaining.len() {
            let opt = remaining[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"COUNT" => {
                    if count_seen {
                        return Err(CommandError::SyntaxError);
                    }
                    count_seen = true;
                    if i + 1 >= remaining.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    let c = parse_i64(&remaining[i + 1]).map_err(|_| {
                        CommandError::InvalidArgument {
                            message: "count value of ZMPOP command is not an positive value"
                                .to_string(),
                        }
                    })?;
                    if c <= 0 {
                        return Err(CommandError::InvalidArgument {
                            message: "count value of ZMPOP command is not an positive value"
                                .to_string(),
                        });
                    }
                    count = c as usize;
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // Find first non-empty key (check type for all keys)
        for key in keys {
            let zset = match ctx.store.get_zset_mut(key)? {
                Some(zset) => {
                    // Must be a sorted set if key exists
                    if zset.is_empty() {
                        continue;
                    }
                    zset
                }
                None => continue,
            };

            let results = if is_min {
                zset.pop_min(count)
            } else {
                zset.pop_max(count)
            };

            // Clean up empty sorted set
            if zset.is_empty() {
                ctx.store.delete(key);
            }

            if !results.is_empty() {
                // ZMPOP always uses nested format [[member, score], ...]
                // in both RESP2 and RESP3
                let pop_result = scored_array_resp3(results, true);
                return Ok(Response::Array(vec![
                    Response::bulk(key.clone()),
                    pop_result,
                ]));
            }
        }

        Ok(Response::NullArray)
    }
}

// ============================================================================
// ZRANDMEMBER - Get random members
// ============================================================================

pub struct ZrandmemberCommand;

impl Command for ZrandmemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "ZRANDMEMBER",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();

        let Some(zset) = ctx.store.get_zset(key)? else {
            if args.len() > 1 {
                return Ok(Response::Array(vec![]));
            } else {
                return Ok(Response::null());
            }
        };

        if args.len() == 1 {
            // Single random member
            let results = zset.random_members(1);
            if let Some((member, _)) = results.first() {
                Ok(Response::bulk(member.clone()))
            } else {
                Ok(Response::null())
            }
        } else {
            let count = parse_i64(&args[1])?;
            let with_scores =
                args.len() > 2 && args[2].to_ascii_uppercase().as_slice() == b"WITHSCORES";

            // Redis rejects i64::MIN (can't negate without overflow) and counts
            // with magnitude > LONG_MAX/2 when WITHSCORES is specified.
            if count == i64::MIN {
                return Err(CommandError::InvalidArgument {
                    message: "value is out of range".to_string(),
                });
            }
            if with_scores && count.abs() > i64::MAX / 2 {
                return Err(CommandError::InvalidArgument {
                    message: "value is out of range".to_string(),
                });
            }

            let results = zset.random_members(count);

            if is_resp3 {
                Ok(scored_array_resp3(results, with_scores))
            } else {
                Ok(scored_array(results, with_scores))
            }
        }
    }
}

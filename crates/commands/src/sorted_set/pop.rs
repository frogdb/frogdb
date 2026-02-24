use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use crate::utils::require_same_shard;
use crate::utils::{parse_i64, parse_usize, scored_array, scored_array_resp3};

// ============================================================================
// ZPOPMIN - Pop minimum score members
// ============================================================================

pub struct ZpopminCommand;

impl Command for ZpopminCommand {
    fn name(&self) -> &'static str {
        "ZPOPMIN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZPOPMIN key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();
        let count = if args.len() > 1 {
            let c = parse_i64(&args[1])?;
            if c < 0 {
                return Err(CommandError::InvalidArgument {
                    message: "value must be positive".to_string(),
                });
            }
            c as usize
        } else {
            1
        };

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_min(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        if is_resp3 {
            Ok(scored_array_resp3(results, true))
        } else {
            Ok(scored_array(results, true))
        }
    }

    impl_keys_first!();
}

// ============================================================================
// ZPOPMAX - Pop maximum score members
// ============================================================================

pub struct ZpopmaxCommand;

impl Command for ZpopmaxCommand {
    fn name(&self) -> &'static str {
        "ZPOPMAX"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZPOPMAX key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();
        let count = if args.len() > 1 {
            let c = parse_i64(&args[1])?;
            if c < 0 {
                return Err(CommandError::InvalidArgument {
                    message: "value must be positive".to_string(),
                });
            }
            c as usize
        } else {
            1
        };

        let zset = match ctx.store.get_mut(key) {
            Some(value) => value.as_sorted_set_mut().ok_or(CommandError::WrongType)?,
            None => return Ok(Response::Array(vec![])),
        };

        let results = zset.pop_max(count);

        // Clean up empty sorted set
        if zset.is_empty() {
            ctx.store.delete(key);
        }

        if is_resp3 {
            Ok(scored_array_resp3(results, true))
        } else {
            Ok(scored_array(results, true))
        }
    }

    impl_keys_first!();
}

// ============================================================================
// ZMPOP - Pop from multiple keys
// ============================================================================

pub struct ZmpopCommand;

impl Command for ZmpopCommand {
    fn name(&self) -> &'static str {
        "ZMPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
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
        let is_resp3 = ctx.protocol_version.is_resp3();

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
            let zset = match ctx.store.get_mut(key) {
                Some(value) => {
                    // Must be a sorted set if key exists
                    let zset = value.as_sorted_set_mut().ok_or(CommandError::WrongType)?;
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
                let pop_result = if is_resp3 {
                    scored_array_resp3(results, true)
                } else {
                    scored_array(results, true)
                };
                return Ok(Response::Array(vec![
                    Response::bulk(key.clone()),
                    pop_result,
                ]));
            }
        }

        Ok(Response::NullArray)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZRANDMEMBER - Get random members
// ============================================================================

pub struct ZrandmemberCommand;

impl Command for ZrandmemberCommand {
    fn name(&self) -> &'static str {
        "ZRANDMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // ZRANDMEMBER key [count [WITHSCORES]]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let is_resp3 = ctx.protocol_version.is_resp3();

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => {
                if args.len() > 1 {
                    return Ok(Response::Array(vec![]));
                } else {
                    return Ok(Response::null());
                }
            }
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

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

            let results = zset.random_members(count);

            if is_resp3 {
                Ok(scored_array_resp3(results, with_scores))
            } else {
                Ok(scored_array(results, with_scores))
            }
        }
    }

    impl_keys_first!();
}

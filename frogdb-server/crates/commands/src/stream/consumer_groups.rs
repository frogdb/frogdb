use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, StreamId, Value, WalStrategy,
};
use frogdb_protocol::Response;

use super::super::utils::parse_u64;
use super::{parse_delete_ref_strategy, parse_ids_block};

// ============================================================================
// XGROUP - Consumer group management (subcommand router)
// ============================================================================

pub struct XgroupCommand;

impl Command for XgroupCommand {
    fn name(&self) -> &'static str {
        "XGROUP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XGROUP subcommand [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity { command: "xgroup" });
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"CREATE" => xgroup_create(ctx, &args[1..]),
            b"DESTROY" => xgroup_destroy(ctx, &args[1..]),
            b"CREATECONSUMER" => xgroup_createconsumer(ctx, &args[1..]),
            b"DELCONSUMER" => xgroup_delconsumer(ctx, &args[1..]),
            b"SETID" => xgroup_setid(ctx, &args[1..]),
            b"HELP" => {
                let help = vec![
                    Response::bulk(Bytes::from_static(
                        b"XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]",
                    )),
                    Response::bulk(Bytes::from_static(b"XGROUP DESTROY key group")),
                    Response::bulk(Bytes::from_static(
                        b"XGROUP CREATECONSUMER key group consumer",
                    )),
                    Response::bulk(Bytes::from_static(b"XGROUP DELCONSUMER key group consumer")),
                    Response::bulk(Bytes::from_static(
                        b"XGROUP SETID key group id|$ [ENTRIESREAD n]",
                    )),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown XGROUP subcommand '{}'. Try XGROUP HELP.",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key is second argument for most subcommands
        if args.len() >= 2 {
            vec![&args[1]]
        } else {
            vec![]
        }
    }
}

fn xgroup_create(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD n]
    if args.len() < 3 {
        return Err(CommandError::WrongArity {
            command: "xgroup|create",
        });
    }

    let key = &args[0];
    let group_name = args[1].clone();
    let id_arg = &args[2];

    let mut mkstream = false;
    let mut entries_read: Option<u64> = None;
    let mut i = 3;

    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"MKSTREAM" => {
                mkstream = true;
                i += 1;
            }
            b"ENTRIESREAD" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                entries_read = Some(parse_u64(&args[i])?);
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    // Check if stream exists
    let exists = ctx.store.get(key).is_some();
    if !exists {
        if mkstream {
            ctx.store.set(key.clone(), Value::stream());
        } else {
            return Err(CommandError::InvalidArgument {
                message: "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_string(),
            });
        }
    }

    // Parse the ID
    let last_id = if id_arg.as_ref() == b"$" {
        // $ means current last ID
        let value = ctx.store.get(key).unwrap();
        let stream = value.as_stream().ok_or(CommandError::WrongType)?;
        stream.last_id()
    } else if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
        StreamId::default()
    } else {
        StreamId::parse(id_arg)?
    };

    // Create the group
    let stream = ctx
        .store
        .get_mut(key)
        .unwrap()
        .as_stream_mut()
        .ok_or(CommandError::WrongType)?;
    stream.create_group(group_name, last_id, entries_read)?;

    Ok(Response::ok())
}

fn xgroup_destroy(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP DESTROY key group
    if args.len() < 2 {
        return Err(CommandError::WrongArity {
            command: "xgroup|destroy",
        });
    }

    let key = &args[0];
    let group_name = &args[1];

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let destroyed = stream.destroy_group(group_name);
            Ok(Response::Integer(if destroyed { 1 } else { 0 }))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_createconsumer(
    ctx: &mut CommandContext,
    args: &[Bytes],
) -> Result<Response, CommandError> {
    // XGROUP CREATECONSUMER key group consumer
    if args.len() < 3 {
        return Err(CommandError::WrongArity {
            command: "xgroup|createconsumer",
        });
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = args[2].clone();

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;
            let created = group.create_consumer(consumer_name);
            Ok(Response::Integer(if created { 1 } else { 0 }))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_delconsumer(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP DELCONSUMER key group consumer
    if args.len() < 3 {
        return Err(CommandError::WrongArity {
            command: "xgroup|delconsumer",
        });
    }

    let key = &args[0];
    let group_name = &args[1];
    let consumer_name = &args[2];

    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
            let group = stream
                .get_group_mut(group_name)
                .ok_or(CommandError::NoGroup)?;
            let pending_deleted = group.delete_consumer(consumer_name);
            Ok(Response::Integer(pending_deleted as i64))
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

fn xgroup_setid(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XGROUP SETID key group id|$ [ENTRIESREAD n]
    if args.len() < 3 {
        return Err(CommandError::WrongArity {
            command: "xgroup|setid",
        });
    }

    let key = &args[0];
    let group_name = &args[1];
    let id_arg = &args[2];

    let mut entries_read: Option<u64> = None;
    if args.len() > 3 {
        if args[3].to_ascii_uppercase().as_slice() == b"ENTRIESREAD" {
            if args.len() < 5 {
                return Err(CommandError::SyntaxError);
            }
            entries_read = Some(parse_u64(&args[4])?);
        } else {
            return Err(CommandError::SyntaxError);
        }
    }

    // Parse the ID
    match ctx.store.get_mut(key) {
        Some(value) => {
            let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;

            let new_id = if id_arg.as_ref() == b"$" {
                stream.last_id()
            } else if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
                StreamId::default()
            } else {
                StreamId::parse(id_arg)?
            };

            stream.set_group_id(group_name, new_id, entries_read)?;
            Ok(Response::ok())
        }
        None => Err(CommandError::InvalidArgument {
            message: "The XGROUP subcommand requires the key to exist.".to_string(),
        }),
    }
}

// ============================================================================
// XACK - Acknowledge entries
// ============================================================================

pub struct XackCommand;

impl Command for XackCommand {
    fn name(&self) -> &'static str {
        "XACK"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // XACK key group id [id ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];

        // Parse IDs
        let mut ids = Vec::with_capacity(args.len() - 2);
        for arg in &args[2..] {
            let id = StreamId::parse(arg)?;
            ids.push(id);
        }

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
                let group = stream
                    .get_group_mut(group_name)
                    .ok_or(CommandError::NoGroup)?;
                let acked = group.ack(&ids);
                Ok(Response::Integer(acked as i64))
            }
            None => Ok(Response::Integer(0)),
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

// ============================================================================
// XACKDEL - Atomic acknowledge + conditional delete
// ============================================================================

pub struct XackdelCommand;

impl Command for XackdelCommand {
    fn name(&self) -> &'static str {
        "XACKDEL"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // XACKDEL key group IDS numids id [id ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let group_name = &args[1];
        let mut i = 2;

        // Parse optional KEEPREF/DELREF/ACKED
        let strategy = parse_delete_ref_strategy(args, &mut i);

        // Parse IDS numids id [id ...]
        let ids = parse_ids_block(args, &mut i)?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let stream = value.as_stream_mut().ok_or(CommandError::WrongType)?;
                let results = stream.ack_and_delete(group_name, &ids, strategy)?;
                Ok(Response::Array(
                    results.into_iter().map(Response::Integer).collect(),
                ))
            }
            None => Err(CommandError::NoGroup),
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

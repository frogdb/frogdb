use bytes::Bytes;
use frogdb_core::{
    AccessSpec, ArgParser, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec,
    EventSpec, ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, StreamRangeBound,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::entry_to_response;

// ============================================================================
// XINFO - Stream/group/consumer info (subcommand router)
// ============================================================================

pub struct XinfoCommand;

impl Command for XinfoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "XINFO",
            arity: Arity::AtLeast(2),
            flags: CommandFlags::READONLY,
            keys: KeySpec::Index(1),
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.is_empty() {
            return Err(CommandError::WrongArity { command: "xinfo" });
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"STREAM" => xinfo_stream(ctx, &args[1..]),
            b"GROUPS" => xinfo_groups(ctx, &args[1..]),
            b"CONSUMERS" => xinfo_consumers(ctx, &args[1..]),
            b"HELP" => {
                let help = vec![
                    Response::bulk(Bytes::from_static(b"XINFO STREAM key [FULL [COUNT count]]")),
                    Response::bulk(Bytes::from_static(b"XINFO GROUPS key")),
                    Response::bulk(Bytes::from_static(b"XINFO CONSUMERS key group")),
                ];
                Ok(Response::Array(help))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!(
                    "Unknown XINFO subcommand '{}'. Try XINFO HELP.",
                    String::from_utf8_lossy(&subcommand)
                ),
            }),
        }
    }
}

fn xinfo_stream(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO STREAM key [FULL [COUNT count]]
    if args.is_empty() {
        return Err(CommandError::WrongArity {
            command: "xinfo|stream",
        });
    }

    let key = &args[0];
    let mut full = false;
    let mut count: usize = 10; // Default for FULL mode

    let mut parser = ArgParser::from_position(args, 1);
    while parser.has_more() {
        if parser.try_flag(b"FULL") {
            full = true;
        } else if let Some(c) = parser.try_flag_usize(b"COUNT")? {
            count = c;
        } else {
            return Err(CommandError::SyntaxError);
        }
    }

    match ctx.store.get_stream(key)? {
        Some(stream) => {
            if full {
                // Full mode - includes entries and detailed group info
                let mut result = vec![
                    Response::bulk(Bytes::from_static(b"length")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-keys")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-nodes")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"last-generated-id")),
                    Response::bulk(Bytes::from(stream.last_id().to_string())),
                    Response::bulk(Bytes::from_static(b"max-deleted-entry-id")),
                    Response::bulk(Bytes::from(
                        stream
                            .max_deleted_id()
                            .map_or_else(|| "0-0".to_string(), |id| id.to_string()),
                    )),
                    Response::bulk(Bytes::from_static(b"entries-added")),
                    Response::Integer(stream.entries_added() as i64),
                ];

                // Add entries (limited by count)
                let entries: Vec<Response> = stream
                    .to_vec()
                    .iter()
                    .take(count)
                    .map(entry_to_response)
                    .collect();
                result.push(Response::bulk(Bytes::from_static(b"entries")));
                result.push(Response::Array(entries));

                // Add detailed group info
                let groups: Vec<Response> = stream
                    .groups()
                    .map(|g| {
                        let lag = stream.compute_lag(g);

                        // Build PEL array (limited by count) via the group's query.
                        let pel: Vec<Response> = g
                            .pending_entries(
                                StreamRangeBound::Min,
                                StreamRangeBound::Max,
                                count,
                                None,
                                None,
                            )
                            .into_iter()
                            .map(|pe| {
                                Response::Array(vec![
                                    Response::bulk(Bytes::from(pe.id.to_string())),
                                    Response::bulk(pe.consumer),
                                    Response::Integer(pe.idle_ms as i64),
                                    Response::Integer(pe.delivery_count as i64),
                                ])
                            })
                            .collect();

                        // Build consumers array
                        let consumers: Vec<Response> = g
                            .consumers()
                            .map(|c| {
                                // Per-consumer PEL
                                let consumer_pel: Vec<Response> = g
                                    .pending_entries(
                                        StreamRangeBound::Min,
                                        StreamRangeBound::Max,
                                        count,
                                        None,
                                        Some(c.name()),
                                    )
                                    .into_iter()
                                    .map(|pe| {
                                        Response::Array(vec![
                                            Response::bulk(Bytes::from(pe.id.to_string())),
                                            Response::Integer(pe.idle_ms as i64),
                                            Response::Integer(pe.delivery_count as i64),
                                        ])
                                    })
                                    .collect();

                                Response::Array(vec![
                                    Response::bulk(Bytes::from_static(b"name")),
                                    Response::bulk(c.name().clone()),
                                    Response::bulk(Bytes::from_static(b"seen-time")),
                                    Response::Integer(c.idle_ms() as i64),
                                    Response::bulk(Bytes::from_static(b"active-time")),
                                    Response::Integer(c.inactive_ms()),
                                    Response::bulk(Bytes::from_static(b"pel-count")),
                                    Response::Integer(c.pending_count() as i64),
                                    Response::bulk(Bytes::from_static(b"pel")),
                                    Response::Array(consumer_pel),
                                ])
                            })
                            .collect();

                        Response::Array(vec![
                            Response::bulk(Bytes::from_static(b"name")),
                            Response::bulk(g.name.clone()),
                            Response::bulk(Bytes::from_static(b"last-delivered-id")),
                            Response::bulk(Bytes::from(g.last_delivered_id().to_string())),
                            Response::bulk(Bytes::from_static(b"entries-read")),
                            g.entries_read()
                                .map_or(Response::null(), |n| Response::Integer(n as i64)),
                            Response::bulk(Bytes::from_static(b"lag")),
                            lag.map_or(Response::null(), |n| Response::Integer(n as i64)),
                            Response::bulk(Bytes::from_static(b"pel-count")),
                            Response::Integer(g.pending_count() as i64),
                            Response::bulk(Bytes::from_static(b"pel")),
                            Response::Array(pel),
                            Response::bulk(Bytes::from_static(b"consumers")),
                            Response::Array(consumers),
                        ])
                    })
                    .collect();
                result.push(Response::bulk(Bytes::from_static(b"groups")));
                result.push(Response::Array(groups));

                Ok(Response::Array(result))
            } else {
                // Basic mode
                let first = stream.first_entry();
                let last = stream.last_entry();

                let result = vec![
                    Response::bulk(Bytes::from_static(b"length")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-keys")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"radix-tree-nodes")),
                    Response::Integer(stream.len() as i64),
                    Response::bulk(Bytes::from_static(b"last-generated-id")),
                    Response::bulk(Bytes::from(stream.last_id().to_string())),
                    Response::bulk(Bytes::from_static(b"max-deleted-entry-id")),
                    Response::bulk(Bytes::from(
                        stream
                            .max_deleted_id()
                            .map_or_else(|| "0-0".to_string(), |id| id.to_string()),
                    )),
                    Response::bulk(Bytes::from_static(b"entries-added")),
                    Response::Integer(stream.entries_added() as i64),
                    Response::bulk(Bytes::from_static(b"first-entry")),
                    first.map_or(Response::null(), |e| entry_to_response(&e)),
                    Response::bulk(Bytes::from_static(b"last-entry")),
                    last.map_or(Response::null(), |e| entry_to_response(&e)),
                    Response::bulk(Bytes::from_static(b"groups")),
                    Response::Integer(stream.group_count() as i64),
                ];

                Ok(Response::Array(result))
            }
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

fn xinfo_groups(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO GROUPS key
    if args.is_empty() {
        return Err(CommandError::WrongArity {
            command: "xinfo|groups",
        });
    }

    let key = &args[0];

    match ctx.store.get_stream(key)? {
        Some(stream) => {
            let groups: Vec<Response> = stream
                .groups()
                .map(|g| {
                    let lag = stream.compute_lag(g);
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(g.name.clone()),
                        Response::bulk(Bytes::from_static(b"consumers")),
                        Response::Integer(g.consumer_count() as i64),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(g.pending_count() as i64),
                        Response::bulk(Bytes::from_static(b"last-delivered-id")),
                        Response::bulk(Bytes::from(g.last_delivered_id().to_string())),
                        Response::bulk(Bytes::from_static(b"entries-read")),
                        g.entries_read()
                            .map_or(Response::null(), |n| Response::Integer(n as i64)),
                        Response::bulk(Bytes::from_static(b"lag")),
                        lag.map_or(Response::null(), |n| Response::Integer(n as i64)),
                    ])
                })
                .collect();

            Ok(Response::Array(groups))
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

fn xinfo_consumers(ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    // XINFO CONSUMERS key group
    if args.len() < 2 {
        return Err(CommandError::WrongArity {
            command: "xinfo|consumers",
        });
    }

    let key = &args[0];
    let group_name = &args[1];

    match ctx.store.get_stream(key)? {
        Some(stream) => {
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            let consumers: Vec<Response> = group
                .consumers()
                .map(|c| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(c.name().clone()),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(c.pending_count() as i64),
                        Response::bulk(Bytes::from_static(b"idle")),
                        Response::Integer(c.idle_ms() as i64),
                        Response::bulk(Bytes::from_static(b"inactive")),
                        Response::Integer(c.inactive_ms()),
                    ])
                })
                .collect();

            Ok(Response::Array(consumers))
        }
        None => Err(CommandError::InvalidArgument {
            message: format!("No such key '{}'", String::from_utf8_lossy(key)),
        }),
    }
}

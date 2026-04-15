use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use super::super::utils::parse_usize;
use super::entry_to_response;

// ============================================================================
// XINFO - Stream/group/consumer info (subcommand router)
// ============================================================================

pub struct XinfoCommand;

impl Command for XinfoCommand {
    fn name(&self) -> &'static str {
        "XINFO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // XINFO subcommand [args...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        // Key is second argument for most subcommands
        if args.len() >= 2 {
            vec![&args[1]]
        } else {
            vec![]
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

    let mut i = 1;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"FULL" => {
                full = true;
                i += 1;
            }
            b"COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = parse_usize(&args[i])?;
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;

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

                        // Build PEL array (limited by count)
                        let pel: Vec<Response> =
                            g.pending
                                .iter()
                                .take(count)
                                .map(|(id, pe)| {
                                    Response::Array(vec![
                                        Response::bulk(Bytes::from(id.to_string())),
                                        Response::bulk(pe.consumer.clone()),
                                        Response::Integer(
                                            pe.delivery_time.elapsed().as_millis() as i64
                                        ),
                                        Response::Integer(pe.delivery_count as i64),
                                    ])
                                })
                                .collect();

                        // Build consumers array
                        let consumers: Vec<Response> = g
                            .consumers
                            .values()
                            .map(|c| {
                                // Per-consumer PEL
                                let consumer_pel: Vec<Response> = g
                                    .pending
                                    .iter()
                                    .filter(|(_, pe)| pe.consumer == c.name)
                                    .take(count)
                                    .map(|(id, pe)| {
                                        Response::Array(vec![
                                            Response::bulk(Bytes::from(id.to_string())),
                                            Response::Integer(
                                                pe.delivery_time.elapsed().as_millis() as i64,
                                            ),
                                            Response::Integer(pe.delivery_count as i64),
                                        ])
                                    })
                                    .collect();

                                Response::Array(vec![
                                    Response::bulk(Bytes::from_static(b"name")),
                                    Response::bulk(c.name.clone()),
                                    Response::bulk(Bytes::from_static(b"seen-time")),
                                    Response::Integer(c.idle_ms() as i64),
                                    Response::bulk(Bytes::from_static(b"active-time")),
                                    Response::Integer(c.inactive_ms()),
                                    Response::bulk(Bytes::from_static(b"pel-count")),
                                    Response::Integer(c.pending_count as i64),
                                    Response::bulk(Bytes::from_static(b"pel")),
                                    Response::Array(consumer_pel),
                                ])
                            })
                            .collect();

                        Response::Array(vec![
                            Response::bulk(Bytes::from_static(b"name")),
                            Response::bulk(g.name.clone()),
                            Response::bulk(Bytes::from_static(b"last-delivered-id")),
                            Response::bulk(Bytes::from(g.last_delivered_id.to_string())),
                            Response::bulk(Bytes::from_static(b"entries-read")),
                            g.entries_read
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

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;

            let groups: Vec<Response> = stream
                .groups()
                .map(|g| {
                    let lag = stream.compute_lag(g);
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(g.name.clone()),
                        Response::bulk(Bytes::from_static(b"consumers")),
                        Response::Integer(g.consumers.len() as i64),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(g.pending_count() as i64),
                        Response::bulk(Bytes::from_static(b"last-delivered-id")),
                        Response::bulk(Bytes::from(g.last_delivered_id.to_string())),
                        Response::bulk(Bytes::from_static(b"entries-read")),
                        g.entries_read
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

    match ctx.store.get(key) {
        Some(value) => {
            let stream = value.as_stream().ok_or(CommandError::WrongType)?;
            let group = stream.get_group(group_name).ok_or(CommandError::NoGroup)?;

            let consumers: Vec<Response> = group
                .consumers
                .values()
                .map(|c| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from_static(b"name")),
                        Response::bulk(c.name.clone()),
                        Response::bulk(Bytes::from_static(b"pending")),
                        Response::Integer(c.pending_count as i64),
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

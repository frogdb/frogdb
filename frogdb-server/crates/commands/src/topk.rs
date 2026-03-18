//! Top-K commands.
//!
//! Commands for approximate top-k frequent items tracking using the HeavyKeeper algorithm.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, TopKValue, Value, WalStrategy,
};
use frogdb_protocol::Response;

/// TOPK.RESERVE - Create a new Top-K filter.
///
/// TOPK.RESERVE key topk [width depth decay]
pub struct TopkReserve;

impl Command for TopkReserve {
    fn name(&self) -> &'static str {
        "TOPK.RESERVE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 2, max: 5 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let k: u32 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid topk value".to_string(),
            })?
            .parse()
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid topk value".to_string(),
            })?;

        if k == 0 {
            return Err(CommandError::InvalidArgument {
                message: "k must be greater than 0".to_string(),
            });
        }

        let (width, depth, decay) = if args.len() >= 5 {
            let w: u32 = std::str::from_utf8(&args[2])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid width".to_string(),
                })?
                .parse()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid width".to_string(),
                })?;
            let d: u32 = std::str::from_utf8(&args[3])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid depth".to_string(),
                })?
                .parse()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid depth".to_string(),
                })?;
            let decay: f64 = std::str::from_utf8(&args[4])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid decay".to_string(),
                })?
                .parse()
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid decay".to_string(),
                })?;

            if w == 0 || d == 0 {
                return Err(CommandError::InvalidArgument {
                    message: "width and depth must be greater than 0".to_string(),
                });
            }
            if decay <= 0.0 || decay >= 1.0 {
                return Err(CommandError::InvalidArgument {
                    message: "decay must be between 0 and 1 exclusive".to_string(),
                });
            }

            (w, d, decay)
        } else if args.len() == 2 {
            // Default parameters: width=k*8, depth=7, decay=0.9
            (k.saturating_mul(8).max(8), 7, 0.9)
        } else {
            return Err(CommandError::InvalidArgument {
                message: "TOPK.RESERVE requires 2 or 5 arguments".to_string(),
            });
        };

        if ctx.store.get(key).is_some() {
            return Err(CommandError::InvalidArgument {
                message: "Key already exists".to_string(),
            });
        }

        let tk = TopKValue::new(k, width, depth, decay);
        ctx.store.set(key.clone(), Value::TopK(tk));

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

/// TOPK.ADD - Add items to the Top-K filter.
///
/// TOPK.ADD key item [item ...]
pub struct TopkAdd;

impl Command for TopkAdd {
    fn name(&self) -> &'static str {
        "TOPK.ADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        match ctx.store.get_mut(key) {
            Some(value) => {
                let tk = value.as_topk_mut().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| match tk.add(item, 1) {
                        Some(expelled) => Response::bulk(expelled),
                        None => Response::Null,
                    })
                    .collect();
                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
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

/// TOPK.INCRBY - Increment the count of items in the Top-K filter.
///
/// TOPK.INCRBY key item increment [item increment ...]
pub struct TopkIncrby;

impl Command for TopkIncrby {
    fn name(&self) -> &'static str {
        "TOPK.INCRBY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let pairs = &args[1..];

        if !pairs.len().is_multiple_of(2) {
            return Err(CommandError::InvalidArgument {
                message: "TOPK.INCRBY requires item-increment pairs".to_string(),
            });
        }

        match ctx.store.get_mut(key) {
            Some(value) => {
                let tk = value.as_topk_mut().ok_or(CommandError::WrongType)?;
                let mut results = Vec::with_capacity(pairs.len() / 2);

                for pair in pairs.chunks_exact(2) {
                    let item = &pair[0];
                    let increment: u64 = std::str::from_utf8(&pair[1])
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid increment".to_string(),
                        })?
                        .parse()
                        .map_err(|_| CommandError::InvalidArgument {
                            message: "Invalid increment".to_string(),
                        })?;

                    if increment == 0 || increment > 100000 {
                        return Err(CommandError::InvalidArgument {
                            message: "Increment must be between 1 and 100000".to_string(),
                        });
                    }

                    match tk.add(item, increment) {
                        Some(expelled) => results.push(Response::bulk(expelled)),
                        None => results.push(Response::Null),
                    }
                }

                Ok(Response::Array(results))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
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

/// TOPK.QUERY - Check if items are in the Top-K.
///
/// TOPK.QUERY key item [item ...]
pub struct TopkQuery;

impl Command for TopkQuery {
    fn name(&self) -> &'static str {
        "TOPK.QUERY"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let tk = value.as_topk().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(if tk.query(item) { 1 } else { 0 }))
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
                Ok(Response::Array(results))
            }
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

/// TOPK.COUNT - Return the count of items in the Top-K.
///
/// TOPK.COUNT key item [item ...]
pub struct TopkCount;

impl Command for TopkCount {
    fn name(&self) -> &'static str {
        "TOPK.COUNT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let items = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let tk = value.as_topk().ok_or(CommandError::WrongType)?;
                let results: Vec<Response> = items
                    .iter()
                    .map(|item| Response::Integer(tk.count(item) as i64))
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
                Ok(Response::Array(results))
            }
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

/// TOPK.LIST - List all items in the Top-K.
///
/// TOPK.LIST key [WITHCOUNT]
pub struct TopkList;

impl Command for TopkList {
    fn name(&self) -> &'static str {
        "TOPK.LIST"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 1, max: 2 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let with_count = if args.len() > 1 {
            let opt = std::str::from_utf8(&args[1])
                .map_err(|_| CommandError::InvalidArgument {
                    message: "Invalid option".to_string(),
                })?
                .to_uppercase();
            if opt == "WITHCOUNT" {
                true
            } else {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", opt),
                });
            }
        } else {
            false
        };

        match ctx.store.get(key) {
            Some(value) => {
                let tk = value.as_topk().ok_or(CommandError::WrongType)?;
                let items = tk.list();

                if with_count {
                    let mut results = Vec::with_capacity(items.len() * 2);
                    for (item, count) in items {
                        results.push(Response::bulk(item.clone()));
                        results.push(Response::Integer(count as i64));
                    }
                    Ok(Response::Array(results))
                } else {
                    let results: Vec<Response> = items
                        .into_iter()
                        .map(|(item, _)| Response::bulk(item.clone()))
                        .collect();
                    Ok(Response::Array(results))
                }
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
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

/// TOPK.INFO - Return information about the Top-K filter.
///
/// TOPK.INFO key
pub struct TopkInfo;

impl Command for TopkInfo {
    fn name(&self) -> &'static str {
        "TOPK.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        match ctx.store.get(key) {
            Some(value) => {
                let tk = value.as_topk().ok_or(CommandError::WrongType)?;
                Ok(Response::Array(vec![
                    Response::bulk(Bytes::from("k")),
                    Response::Integer(tk.k() as i64),
                    Response::bulk(Bytes::from("width")),
                    Response::Integer(tk.width() as i64),
                    Response::bulk(Bytes::from("depth")),
                    Response::Integer(tk.depth() as i64),
                    Response::bulk(Bytes::from("decay")),
                    Response::bulk(Bytes::from(format!("{}", tk.decay()))),
                ]))
            }
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
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

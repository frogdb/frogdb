use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use crate::utils::{format_float, parse_u64, parse_usize};

// ============================================================================
// ZSCAN - Cursor-based iteration
// ============================================================================

pub struct ZscanCommand;

impl Command for ZscanCommand {
    fn name(&self) -> &'static str {
        "ZSCAN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZSCAN key cursor [MATCH pattern] [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor: u64 = parse_u64(&args[1])?;

        let mut match_pattern: Option<&[u8]> = None;
        let mut count: usize = 10;

        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    match_pattern = Some(&args[i + 1]);
                    i += 2;
                }
                b"COUNT" => {
                    if i + 1 >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i + 1])?;
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => {
                return Ok(Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"0")),
                    Response::Array(vec![]),
                ]));
            }
        };
        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let (new_cursor, results) = crate::utils::hash_cursor_scan(
            zset.to_vec().into_iter(),
            cursor,
            count,
            match_pattern,
            |entry: &(Bytes, f64)| entry.0.as_ref(),
            |entry: (Bytes, f64), results: &mut Vec<Response>| {
                results.push(Response::bulk(entry.0));
                results.push(Response::bulk(Bytes::from(format_float(entry.1))));
            },
        );

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(new_cursor.to_string())),
            Response::Array(results),
        ]))
    }

    impl_keys_first!();
}

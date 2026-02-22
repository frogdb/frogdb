use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, impl_keys_first};
use frogdb_protocol::Response;

use crate::commands::utils::{format_float, parse_usize};

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
        let cursor = parse_usize(&args[1])?;

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

        // Simple cursor-based iteration (not true cursor, just offset)
        let all_members: Vec<_> = zset.to_vec();
        let total = all_members.len();

        if cursor >= total {
            return Ok(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"0")),
                Response::Array(vec![]),
            ]));
        }

        let mut result = Vec::new();
        let mut new_cursor = 0;

        for (i, (member, score)) in all_members.into_iter().enumerate().skip(cursor) {
            if result.len() >= count * 2 {
                new_cursor = i;
                break;
            }

            // Apply pattern match if specified
            if let Some(pattern) = match_pattern
                && !simple_glob_match(pattern, &member)
            {
                continue;
            }

            result.push(Response::bulk(member));
            result.push(Response::bulk(Bytes::from(format_float(score))));
        }

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from(new_cursor.to_string())),
            Response::Array(result),
        ]))
    }

    impl_keys_first!();
}

/// Simple glob pattern matching (supports * and ?)
fn simple_glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p = None;
    let mut star_t = None;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_p = Some(p);
            star_t = Some(t);
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t = Some(star_t.unwrap() + 1);
            t = star_t.unwrap();
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

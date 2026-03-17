use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

use super::versioned_entry_to_response;
use crate::utils::parse_u64;

// ============================================================================
// ES.READ — read events by version range
// ============================================================================

pub struct EsReadCommand;

impl Command for EsReadCommand {
    fn name(&self) -> &'static str {
        "ES.READ"
    }

    fn arity(&self) -> Arity {
        // ES.READ key start_version [end_version] [COUNT n]
        Arity::AtLeast(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let start_version = parse_u64(&args[1])?;

        let mut end_version: Option<u64> = None;
        let mut count: Option<usize> = None;
        let mut i = 2;

        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            if arg == b"COUNT".as_slice() {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(crate::utils::parse_usize(&args[i])?);
                i += 1;
            } else {
                // Try to parse as end_version
                if end_version.is_none() {
                    end_version = Some(parse_u64(&args[i])?);
                    i += 1;
                } else {
                    return Err(CommandError::SyntaxError);
                }
            }
        }

        // Look up the stream and collect results within the borrow scope
        let val = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let stream = val.as_stream().ok_or(CommandError::WrongType)?;
        let entries = stream.range_by_version(start_version, end_version, count);

        let results: Vec<Response> = entries
            .iter()
            .map(|(ver, entry)| versioned_entry_to_response(*ver, entry))
            .collect();

        Ok(Response::Array(results))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

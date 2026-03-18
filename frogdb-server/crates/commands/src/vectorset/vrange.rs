//! VRANGE command — iterate elements in lexicographic order.
//!
//! VRANGE key cursor COUNT count

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

pub struct VrangeCommand;

impl Command for VrangeCommand {
    fn name(&self) -> &'static str {
        "VRANGE"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 3, max: 4 }
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor = &args[1];

        // Parse COUNT.
        let count = if args.len() > 2 {
            // args[2] might be "COUNT" keyword or the count value directly.
            if args.len() == 4 && args[2].eq_ignore_ascii_case(b"COUNT") {
                std::str::from_utf8(&args[3])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
            } else {
                std::str::from_utf8(&args[2])
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
                    .parse::<usize>()
                    .map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid count".to_string(),
                    })?
            }
        } else {
            10
        };

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        let cursor_bytes = if cursor.as_ref() == b"0" || cursor.is_empty() {
            b"" as &[u8]
        } else {
            cursor.as_ref()
        };

        let elements = vs.range(cursor_bytes, count);
        let arr: Vec<Response> = elements.into_iter().map(Response::bulk).collect();
        Ok(Response::Array(arr))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

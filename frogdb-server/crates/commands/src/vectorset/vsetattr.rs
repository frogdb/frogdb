//! VSETATTR command — set JSON attributes on an element.

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, WalStrategy};
use frogdb_protocol::Response;

pub struct VsetattrCommand;

impl Command for VsetattrCommand {
    fn name(&self) -> &'static str {
        "VSETATTR"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];
        let attr_str =
            std::str::from_utf8(&args[2]).map_err(|_| CommandError::InvalidArgument {
                message: "Invalid UTF-8 in attribute value".to_string(),
            })?;

        let attr: serde_json::Value =
            serde_json::from_str(attr_str).map_err(|_| CommandError::InvalidArgument {
                message: "Invalid JSON in attribute value".to_string(),
            })?;

        match ctx.store.get_mut(key) {
            Some(value) => {
                let vs = value.as_vectorset_mut().ok_or(CommandError::WrongType)?;
                if vs.set_attr(element, attr) {
                    Ok(Response::Integer(1))
                } else {
                    // Element does not exist.
                    Ok(Response::Integer(0))
                }
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

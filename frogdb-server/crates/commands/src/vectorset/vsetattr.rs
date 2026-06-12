//! VSETATTR command — set JSON attributes on an element.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VsetattrCommand;

impl Command for VsetattrCommand {
    fn spec(&self) -> Option<&'static CommandSpec> {
        static SPEC: CommandSpec = CommandSpec {
            name: "VSETATTR",
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        Some(&SPEC)
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
}

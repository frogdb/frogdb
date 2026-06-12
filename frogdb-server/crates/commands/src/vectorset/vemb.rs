//! VEMB command — return the vector embedding of an element.
//!
//! VEMB key element [RAW]

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    KeySpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VembCommand;

impl Command for VembCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "VEMB",
            arity: Arity::Range { min: 2, max: 3 },
            flags: CommandFlags::READONLY.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let element = &args[1];
        let raw = args.len() > 2 && args[2].eq_ignore_ascii_case(b"RAW");

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        match vs.get_vector(element) {
            Some(vec) => {
                if raw {
                    // Return as FP32 binary blob.
                    let mut blob = Vec::with_capacity(vec.len() * 4);
                    for &v in vec {
                        blob.extend_from_slice(&v.to_le_bytes());
                    }
                    Ok(Response::bulk(Bytes::from(blob)))
                } else {
                    // Return as array of bulk strings.
                    let arr: Vec<Response> = vec
                        .iter()
                        .map(|&v| Response::bulk(Bytes::from(format!("{v}"))))
                        .collect();
                    Ok(Response::Array(arr))
                }
            }
            None => Ok(Response::null()),
        }
    }
}

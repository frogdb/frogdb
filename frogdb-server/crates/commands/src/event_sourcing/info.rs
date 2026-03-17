use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags};
use frogdb_protocol::Response;

// ============================================================================
// ES.INFO — event stream metadata
// ============================================================================

pub struct EsInfoCommand;

impl Command for EsInfoCommand {
    fn name(&self) -> &'static str {
        "ES.INFO"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // ES.INFO key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        let val = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::null()),
        };
        let stream = val.as_stream().ok_or(CommandError::WrongType)?;

        let version = stream.total_appended();
        let length = stream.len();
        let first_id = stream
            .first_id()
            .map(|id| Response::bulk(Bytes::from(id.to_string())))
            .unwrap_or(Response::null());
        let last_id = if stream.last_id().is_zero() && stream.is_empty() {
            Response::null()
        } else {
            Response::bulk(Bytes::from(stream.last_id().to_string()))
        };
        let idem_count = stream.idempotency().map(|s| s.len()).unwrap_or(0);

        Ok(Response::Array(vec![
            Response::bulk(Bytes::from_static(b"version")),
            Response::Integer(version as i64),
            Response::bulk(Bytes::from_static(b"entries")),
            Response::Integer(length as i64),
            Response::bulk(Bytes::from_static(b"first-id")),
            first_id,
            Response::bulk(Bytes::from_static(b"last-id")),
            last_id,
            Response::bulk(Bytes::from_static(b"idempotency-keys")),
            Response::Integer(idem_count as i64),
        ]))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

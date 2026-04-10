use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, EsAppendError, StreamIdSpec,
    StreamValue, WaiterKind, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use crate::utils::get_or_create_stream;

/// Internal key for the per-shard $all stream.
const ES_ALL_KEY: &[u8] = b"__frogdb:es:all";

/// Default MAXLEN for the per-shard $all stream.
const ES_ALL_MAXLEN: u64 = 100_000;

// ============================================================================
// ES.APPEND — append event with OCC
// ============================================================================

pub struct EsAppendCommand;

impl Command for EsAppendCommand {
    fn name(&self) -> &'static str {
        "ES.APPEND"
    }

    fn arity(&self) -> Arity {
        // ES.APPEND key expected_version event_type data [field value ...] [IF_NOT_EXISTS idem_key]
        Arity::AtLeast(4)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn wakes_waiters(&self) -> WaiterWake {
        WaiterWake::Kind(WaiterKind::Stream)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse expected_version
        let expected_version: u64 = std::str::from_utf8(&args[1])
            .map_err(|_| CommandError::NotInteger)?
            .parse()
            .map_err(|_| CommandError::NotInteger)?;

        // Parse event_type and data
        let event_type = args[2].clone();
        let data = args[3].clone();

        // Parse optional additional field-value pairs and IF_NOT_EXISTS
        let mut i = 4;
        let mut extra_fields: Vec<(Bytes, Bytes)> = Vec::new();
        let mut idempotency_key: Option<Bytes> = None;

        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            if arg == b"IF_NOT_EXISTS".as_slice() {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                idempotency_key = Some(args[i].clone());
                i += 1;
            } else if i + 1 < args.len() {
                extra_fields.push((args[i].clone(), args[i + 1].clone()));
                i += 2;
            } else {
                return Err(CommandError::SyntaxError);
            }
        }

        // Build fields: event_type + data + extra fields
        let mut fields = Vec::with_capacity(2 + extra_fields.len());
        fields.push((Bytes::from_static(b"event_type"), event_type));
        fields.push((Bytes::from_static(b"data"), data));
        fields.extend(extra_fields);

        // Get or create the event stream
        let stream = get_or_create_stream(ctx, key)?;

        // Append with version check
        match stream.add_with_version_check(
            expected_version,
            fields.clone(),
            idempotency_key.as_ref(),
        ) {
            Ok((stream_id, new_version)) => {
                // Write reference to per-shard $all stream
                append_to_all_stream(ctx, key, &stream_id, new_version);

                // Return [version, stream_id]
                Ok(Response::Array(vec![
                    Response::Integer(new_version as i64),
                    Response::bulk(Bytes::from(stream_id.to_string())),
                ]))
            }
            Err(EsAppendError::VersionMismatch { expected, actual }) => {
                Err(CommandError::VersionMismatch { expected, actual })
            }
            Err(EsAppendError::DuplicateIdempotencyKey { version }) => {
                // Idempotent: return the current version as success
                Ok(Response::Array(vec![
                    Response::Integer(version as i64),
                    Response::null(),
                ]))
            }
            Err(EsAppendError::Internal(msg)) => Err(CommandError::Internal { message: msg }),
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

/// Append a reference entry to the per-shard `__frogdb:es:all` stream.
fn append_to_all_stream(
    ctx: &mut CommandContext,
    source_key: &Bytes,
    stream_id: &frogdb_core::StreamId,
    version: u64,
) {
    use frogdb_core::StreamTrimOptions;

    let all_key = Bytes::from(ES_ALL_KEY);

    // Get or create the $all stream
    let all_stream: &mut StreamValue = match ctx.get_or_create::<StreamValue>(&all_key) {
        Ok(s) => s,
        Err(_) => return, // silently skip if we can't create
    };

    // Build reference entry: source_key, source_id, version
    let fields = vec![
        (Bytes::from_static(b"key"), source_key.clone()),
        (
            Bytes::from_static(b"id"),
            Bytes::from(stream_id.to_string()),
        ),
        (
            Bytes::from_static(b"version"),
            Bytes::from(version.to_string()),
        ),
    ];

    let _ = all_stream.add(StreamIdSpec::Auto, fields);

    // Trim to bounded size
    all_stream.trim(StreamTrimOptions {
        strategy: frogdb_core::StreamTrimStrategy::MaxLen(ES_ALL_MAXLEN),
        mode: frogdb_core::StreamTrimMode::Exact,
        limit: 0,
    });
}

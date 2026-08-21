use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

use super::{
    enforce_growth_limits, get_json_mut, json_error_to_command_error, parse_json_value_limited,
    parse_path, single_or_multi,
};

// ============================================================================
// JSON.CLEAR - Clear containers (arrays/objects) or set numbers to 0
// ============================================================================

pub struct JsonClearCommand;

impl Command for JsonClearCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.CLEAR",
            docs: frogdb_core::CommandDocs {
                summary: "Empties the containers and zeroes the numbers at a path in a JSON document.",
                since: "1.0.0",
                group: "json",
                complexity: None,
            },
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::FirstKeyOrDelete {
                kind: frogdb_core::IndexKind::Json,
            },
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json_mut!(ctx, key, Response::Integer(0));
        let cleared = json.clear(&path).map_err(json_error_to_command_error)?;
        Ok(Response::Integer(cleared as i64))
    }
}

// ============================================================================
// JSON.TOGGLE - Toggle boolean values
// ============================================================================

pub struct JsonToggleCommand;

impl Command for JsonToggleCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.TOGGLE",
            docs: frogdb_core::CommandDocs {
                summary: "Inverts the JSON boolean at a path in a document.",
                since: "1.0.0",
                group: "json",
                complexity: None,
            },
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::FirstKey {
                kind: frogdb_core::IndexKind::Json,
            },
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = parse_path(args.get(1));

        let json = get_json_mut!(ctx, key);
        let results = json.toggle(&path).map_err(json_error_to_command_error)?;

        Ok(single_or_multi(results, |b| {
            Response::Integer(if b { 1 } else { 0 })
        }))
    }
}

// ============================================================================
// JSON.MERGE - Merge a JSON value using RFC 7396 JSON Merge Patch
// ============================================================================

pub struct JsonMergeCommand;

impl Command for JsonMergeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "JSON.MERGE",
            docs: frogdb_core::CommandDocs {
                summary: "Merges a value into a JSON document at a path, following RFC 7386 semantics.",
                since: "1.0.0",
                group: "json",
                complexity: None,
            },
            arity: Arity::Fixed(3),
            flags: CommandFlags::WRITE.union(CommandFlags::DENYOOM),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: frogdb_core::ReindexSpec::FirstKey {
                kind: frogdb_core::IndexKind::Json,
            },
            lookup: LookupSpec::None,
            mutation: frogdb_core::ConnMutation::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let path = String::from_utf8_lossy(&args[1]).to_string();
        let limits = ctx.json_limits;
        let patch = parse_json_value_limited(&args[2], &limits)?;

        let json = get_json_mut!(ctx, key);
        // MERGE can grow the stored document past the caps; snapshot for rollback
        // and validate the merged result.
        let snapshot = json.clone();
        json.merge(&path, patch)
            .map_err(json_error_to_command_error)?;
        enforce_growth_limits(json, snapshot, &limits)?;

        Ok(Response::ok())
    }
}

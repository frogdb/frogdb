//! HyperLogLog commands.
//!
//! Commands for probabilistic cardinality estimation using HyperLogLog.

use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, HyperLogLogValue, KeySpec, LookupSpec, StoreTypedFamilyExt, Value,
    WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;

/// PFADD - Add elements to a HyperLogLog.
///
/// PFADD key element [element ...]
///
/// Returns 1 if the internal registers were altered, 0 otherwise.
pub struct PfaddCommand;

impl Command for PfaddCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PFADD",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE.union(CommandFlags::FAST),
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let elements = &args[1..];

        // Get or create the HyperLogLog
        let changed = match ctx.store.get_hll_mut(key)? {
            Some(hll) => {
                let mut any_changed = false;
                for element in elements {
                    if hll.add(element) {
                        any_changed = true;
                    }
                }
                if !any_changed {
                    // No register moved: declare a no-op so the shard skips WAL
                    // persist, replication, version bump, and notifications
                    // (Redis does the same for an unchanged PFADD).
                    ctx.write_was_noop = true;
                }
                any_changed
            }
            None => {
                // Create new HyperLogLog (even with no elements, like Redis)
                let mut hll = HyperLogLogValue::new();
                for element in elements {
                    hll.add(element);
                }
                ctx.store.set(key.clone(), Value::HyperLogLog(hll));
                // Redis returns 1 when creating a new HLL, even with no elements
                true
            }
        };

        Ok(Response::Integer(if changed { 1 } else { 0 }))
    }
}

/// PFCOUNT - Return the approximated cardinality of the set(s).
///
/// PFCOUNT key [key ...]
///
/// For a single key, returns the cached cardinality estimate (O(1)).
/// For multiple keys, computes the union cardinality (O(N)).
pub struct PfcountCommand;

impl Command for PfcountCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PFCOUNT",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() == 1 {
            // Single key - use cached count
            let key = &args[0];
            match ctx.store.get_hll(key)? {
                Some(hll) => Ok(Response::Integer(hll.count_no_cache() as i64)),
                None => Ok(Response::Integer(0)),
            }
        } else {
            // Multiple keys - compute union
            let mut merged = HyperLogLogValue::new();

            for key in args {
                if let Some(hll) = ctx.store.get_hll(key)? {
                    merged.merge(&hll);
                }
                // Non-existent keys are treated as empty HLLs (no-op)
            }

            Ok(Response::Integer(merged.count() as i64))
        }
    }
}

/// PFMERGE - Merge multiple HyperLogLogs into a destination key.
///
/// PFMERGE destkey sourcekey [sourcekey ...]
///
/// Creates or overwrites the destination key with the union of all source HLLs.
pub struct PfmergeCommand;

impl Command for PfmergeCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PFMERGE",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::WRITE,
            keys: KeySpec::All,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest_key = &args[0];
        let source_keys = &args[1..];

        // Fold all sources into one scratch HLL first, so the read-only borrows
        // end before the destination is borrowed mutably. Reading the sources
        // first also preserves source-type-error-before-dest ordering (a
        // wrong-typed source must surface WRONGTYPE before the destination is
        // touched).
        let mut sources = HyperLogLogValue::new();
        for src_key in source_keys {
            if let Some(src) = ctx.store.get_hll(src_key)? {
                sources.merge(&src);
            }
            // Non-existent keys are treated as empty HLLs (no-op).
        }

        // Merge into the destination in place. The destination is itself a
        // source (Redis semantics): merging into it keeps its existing
        // registers, so there is no need to fold it into the scratch HLL.
        match ctx.store.get_hll_mut(dest_key)? {
            Some(dest) => {
                if !dest.merge(&sources) {
                    // Destination already contains every source register: no
                    // register moved, so declare a no-op and let the shard skip
                    // WAL persist, replication, version bump, and notifications.
                    // (Deliberately stricter than Redis, which dirties PFMERGE
                    // unconditionally.)
                    ctx.write_was_noop = true;
                }
            }
            None => {
                // Creating the destination (even empty) is a real change, so
                // write effects must fire.
                ctx.store.set(dest_key.clone(), Value::HyperLogLog(sources));
            }
        }

        Ok(Response::ok())
    }
}

/// PFDEBUG - Internal debugging command.
///
/// PFDEBUG subcommand key
///
/// Subcommands:
/// - ENCODING: Return encoding type (sparse or dense)
/// - DECODE: Return register values
/// - GETREG: Return raw register values
pub struct PfdebugCommand;

impl Command for PfdebugCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PFDEBUG",
            arity: Arity::Fixed(2),
            flags: CommandFlags::READONLY.union(CommandFlags::ADMIN),
            keys: KeySpec::Index(1),
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid subcommand".to_string(),
            })?
            .to_uppercase();
        let key = &args[1];

        let Some(hll) = ctx.store.get_hll(key)? else {
            return Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            });
        };

        match subcommand.as_str() {
            "ENCODING" => Ok(Response::bulk(Bytes::from(hll.encoding_str()))),
            "TODENSE" => {
                // FrogDB HLL is always dense, so this is a no-op
                Ok(Response::Integer(1))
            }
            "DECODE" => {
                // Return non-zero register values
                let mut results = Vec::new();
                for i in 0..frogdb_core::HLL_REGISTERS {
                    let val = hll.get_register(i as u16);
                    if val > 0 {
                        results.push(Response::Array(vec![
                            Response::Integer(i as i64),
                            Response::Integer(val as i64),
                        ]));
                    }
                }
                Ok(Response::Array(results))
            }
            "GETREG" => {
                // Return all register values
                let results: Vec<Response> = (0..frogdb_core::HLL_REGISTERS)
                    .map(|i| Response::Integer(hll.get_register(i as u16) as i64))
                    .collect();
                Ok(Response::Array(results))
            }
            _ => Err(CommandError::InvalidArgument {
                message: format!("Unknown PFDEBUG subcommand: {}", subcommand),
            }),
        }
    }
}

/// PFSELFTEST - Run internal self-test for HyperLogLog.
///
/// PFSELFTEST
///
/// Returns OK if all tests pass.
pub struct PfselftestCommand;

impl Command for PfselftestCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "PFSELFTEST",
            arity: Arity::Fixed(1),
            flags: CommandFlags::READONLY.union(CommandFlags::ADMIN),
            keys: KeySpec::None,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
            lookup: LookupSpec::None,
            strategy: ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Run basic self-tests

        // Test 1: Empty HLL has count 0
        let mut hll = HyperLogLogValue::new();
        if hll.count() != 0 {
            return Err(CommandError::InvalidArgument {
                message: "PFSELFTEST failed: empty HLL count != 0".to_string(),
            });
        }

        // Test 2: Adding elements changes count
        hll.add(b"test1");
        hll.add(b"test2");
        hll.add(b"test3");
        let count = hll.count();
        if count == 0 {
            return Err(CommandError::InvalidArgument {
                message: "PFSELFTEST failed: count is 0 after adding elements".to_string(),
            });
        }

        // Test 3: Adding duplicate doesn't increase count
        hll.add(b"test1");
        let count2 = hll.count();
        if count2 != count {
            return Err(CommandError::InvalidArgument {
                message: "PFSELFTEST failed: count changed after adding duplicate".to_string(),
            });
        }

        // Test 4: Merge works
        let mut hll2 = HyperLogLogValue::new();
        hll2.add(b"test4");
        hll2.add(b"test5");
        hll.merge(&hll2);
        let merged_count = hll.count();
        if merged_count < count {
            return Err(CommandError::InvalidArgument {
                message: "PFSELFTEST failed: merged count is less than original".to_string(),
            });
        }

        // Test 5: Accuracy check (rough estimate for small sets)
        let mut hll3 = HyperLogLogValue::new();
        for i in 0..100 {
            hll3.add(format!("item:{}", i).as_bytes());
        }
        let estimate = hll3.count();
        if !(90..=110).contains(&estimate) {
            return Err(CommandError::InvalidArgument {
                message: format!(
                    "PFSELFTEST failed: estimate {} not in expected range [90, 110]",
                    estimate
                ),
            });
        }

        Ok(Response::ok())
    }
}

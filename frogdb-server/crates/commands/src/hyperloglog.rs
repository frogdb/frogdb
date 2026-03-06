//! HyperLogLog commands.
//!
//! Commands for probabilistic cardinality estimation using HyperLogLog.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, HyperLogLogValue, Value,
    WalStrategy,
};
use frogdb_protocol::Response;

/// PFADD - Add elements to a HyperLogLog.
///
/// PFADD key element [element ...]
///
/// Returns 1 if the internal registers were altered, 0 otherwise.
pub struct PfaddCommand;

impl Command for PfaddCommand {
    fn name(&self) -> &'static str {
        "PFADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let elements = &args[1..];

        // Get or create the HyperLogLog
        let changed = match ctx.store.get_mut(key) {
            Some(value) => {
                let hll = value.as_hyperloglog_mut().ok_or(CommandError::WrongType)?;

                let mut any_changed = false;
                for element in elements {
                    if hll.add(element) {
                        any_changed = true;
                    }
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

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
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
    fn name(&self) -> &'static str {
        "PFCOUNT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        if args.len() == 1 {
            // Single key - use cached count
            let key = &args[0];
            match ctx.store.get_mut(key) {
                Some(value) => {
                    let hll = value.as_hyperloglog_mut().ok_or(CommandError::WrongType)?;
                    Ok(Response::Integer(hll.count() as i64))
                }
                None => Ok(Response::Integer(0)),
            }
        } else {
            // Multiple keys - compute union
            let mut merged = HyperLogLogValue::new();

            for key in args {
                if let Some(value) = ctx.store.get(key) {
                    let hll = value.as_hyperloglog().ok_or(CommandError::WrongType)?;
                    merged.merge(hll);
                }
                // Non-existent keys are treated as empty HLLs (no-op)
            }

            Ok(Response::Integer(merged.count() as i64))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|b| b.as_ref()).collect()
    }
}

/// PFMERGE - Merge multiple HyperLogLogs into a destination key.
///
/// PFMERGE destkey sourcekey [sourcekey ...]
///
/// Creates or overwrites the destination key with the union of all source HLLs.
pub struct PfmergeCommand;

impl Command for PfmergeCommand {
    fn name(&self) -> &'static str {
        "PFMERGE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest_key = &args[0];
        let source_keys = &args[1..];

        // First, collect all source HLLs
        let mut merged = HyperLogLogValue::new();

        for key in source_keys {
            if let Some(value) = ctx.store.get(key) {
                let hll = value.as_hyperloglog().ok_or(CommandError::WrongType)?;
                merged.merge(hll);
            }
            // Non-existent keys are treated as empty HLLs (no-op)
        }

        // Also merge the destination if it exists and is in the source list
        // (Redis behavior: dest can also be a source)
        if let Some(value) = ctx.store.get(dest_key) {
            if let Some(hll) = value.as_hyperloglog() {
                merged.merge(hll);
            } else {
                return Err(CommandError::WrongType);
            }
        }

        // Store the merged result
        ctx.store.set(dest_key.clone(), Value::HyperLogLog(merged));

        Ok(Response::ok())
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|b| b.as_ref()).collect()
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
    fn name(&self) -> &'static str {
        "PFDEBUG"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::ADMIN
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let subcommand = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument {
                message: "Invalid subcommand".to_string(),
            })?
            .to_uppercase();
        let key = &args[1];

        match ctx.store.get(key) {
            Some(value) => {
                let hll = value.as_hyperloglog().ok_or(CommandError::WrongType)?;

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
            None => Err(CommandError::InvalidArgument {
                message: "Key does not exist".to_string(),
            }),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            vec![&args[1]]
        } else {
            vec![]
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
    fn name(&self) -> &'static str {
        "PFSELFTEST"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::ADMIN
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

    fn keys<'a>(&self, _args: &'a [Bytes]) -> Vec<&'a [u8]> {
        vec![]
    }
}

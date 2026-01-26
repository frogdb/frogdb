//! Set commands.
//!
//! Commands for set manipulation:
//! - SADD, SREM, SMEMBERS - basic operations
//! - SISMEMBER, SMISMEMBER, SCARD - inspection
//! - SUNION, SINTER, SDIFF - set operations
//! - SUNIONSTORE, SINTERSTORE, SDIFFSTORE - store results
//! - SINTERCARD - cardinality of intersection
//! - SRANDMEMBER, SPOP - random operations
//! - SMOVE, SSCAN - other operations

use bytes::Bytes;
use frogdb_core::{Arity, Command, CommandContext, CommandError, CommandFlags, SetValue, Value};
use frogdb_protocol::Response;

use super::utils::{get_or_create_set, parse_i64, parse_usize};

/// Get an existing set (cloned), returning None if key doesn't exist, Error if wrong type.
fn get_set_inline(
    ctx: &mut CommandContext,
    key: &Bytes,
) -> Result<Option<SetValue>, CommandError> {
    match ctx.store.get(key) {
        Some(value) => {
            if let Some(set) = value.as_set() {
                Ok(Some(set.clone()))
            } else {
                Err(CommandError::WrongType)
            }
        }
        None => Ok(None),
    }
}

// ============================================================================
// SADD - Add members to set
// ============================================================================

pub struct SaddCommand;

impl Command for SaddCommand {
    fn name(&self) -> &'static str {
        "SADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SADD key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let set = get_or_create_set(ctx, key)?;

        let mut added = 0i64;
        for member in &args[1..] {
            if set.add(member.clone()) {
                added += 1;
            }
        }

        Ok(Response::Integer(added))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// SREM - Remove members from set
// ============================================================================

pub struct SremCommand;

impl Command for SremCommand {
    fn name(&self) -> &'static str {
        "SREM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SREM key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        // Check if key exists
        if ctx.store.get(key).is_none() {
            return Ok(Response::Integer(0));
        }

        // Verify type
        if ctx.store.get(key).unwrap().as_set().is_none() {
            return Err(CommandError::WrongType);
        }

        let set = ctx.store.get_mut(key).unwrap().as_set_mut().unwrap();

        let mut removed = 0i64;
        for member in &args[1..] {
            if set.remove(member) {
                removed += 1;
            }
        }

        // Delete key if set is now empty
        if set.is_empty() {
            ctx.store.delete(key);
        }

        Ok(Response::Integer(removed))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// SMEMBERS - Get all members
// ============================================================================

pub struct SmembersCommand;

impl Command for SmembersCommand {
    fn name(&self) -> &'static str {
        "SMEMBERS"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // SMEMBERS key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                let results: Vec<Response> =
                    set.members().map(|m| Response::bulk(m.clone())).collect();
                if ctx.protocol_version.is_resp3() {
                    Ok(Response::Set(results))
                } else {
                    Ok(Response::Array(results))
                }
            }
            None => {
                if ctx.protocol_version.is_resp3() {
                    Ok(Response::Set(vec![]))
                } else {
                    Ok(Response::Array(vec![]))
                }
            }
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

// ============================================================================
// SISMEMBER - Check if member exists
// ============================================================================

pub struct SismemberCommand;

impl Command for SismemberCommand {
    fn name(&self) -> &'static str {
        "SISMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(2) // SISMEMBER key member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                if set.contains(member) {
                    Ok(Response::Integer(1))
                } else {
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

// ============================================================================
// SMISMEMBER - Check if multiple members exist
// ============================================================================

pub struct SmismemberCommand;

impl Command for SmismemberCommand {
    fn name(&self) -> &'static str {
        "SMISMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SMISMEMBER key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => {
                let results: Vec<Response> = args[1..]
                    .iter()
                    .map(|member| {
                        if set.contains(member) {
                            Response::Integer(1)
                        } else {
                            Response::Integer(0)
                        }
                    })
                    .collect();
                Ok(Response::Array(results))
            }
            None => {
                let results: Vec<Response> = args[1..].iter().map(|_| Response::Integer(0)).collect();
                Ok(Response::Array(results))
            }
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

// ============================================================================
// SCARD - Get set cardinality
// ============================================================================

pub struct ScardCommand;

impl Command for ScardCommand {
    fn name(&self) -> &'static str {
        "SCARD"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(1) // SCARD key
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        match get_set_inline(ctx, key)? {
            Some(set) => Ok(Response::Integer(set.len() as i64)),
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

// ============================================================================
// SUNION - Return union of sets
// ============================================================================

pub struct SunionCommand;

impl Command for SunionCommand {
    fn name(&self) -> &'static str {
        "SUNION"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SUNION key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => SetValue::new(),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s);
            }
        }

        let result = first.union(others.iter());
        let members: Vec<Response> = result
            .members()
            .map(|m| Response::bulk(m.clone()))
            .collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SINTER - Return intersection of sets
// ============================================================================

pub struct SinterCommand;

impl Command for SinterCommand {
    fn name(&self) -> &'static str {
        "SINTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SINTER key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let empty_result = if ctx.protocol_version.is_resp3() {
            Response::Set(vec![])
        } else {
            Response::Array(vec![])
        };

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => return Ok(empty_result), // Empty set intersects to empty
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s),
                None => return Ok(empty_result.clone()), // Missing key = empty intersection
            }
        }

        let result = first.intersection(others.iter());
        let members: Vec<Response> = result
            .members()
            .map(|m| Response::bulk(m.clone()))
            .collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SDIFF - Return difference of sets
// ============================================================================

pub struct SdiffCommand;

impl Command for SdiffCommand {
    fn name(&self) -> &'static str {
        "SDIFF"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SDIFF key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let empty_result = if ctx.protocol_version.is_resp3() {
            Response::Set(vec![])
        } else {
            Response::Array(vec![])
        };

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[0])? {
            Some(s) => s.clone(),
            None => return Ok(empty_result),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[1..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s);
            }
        }

        let result = first.difference(others.iter());
        let members: Vec<Response> = result
            .members()
            .map(|m| Response::bulk(m.clone()))
            .collect();
        if ctx.protocol_version.is_resp3() {
            Ok(Response::Set(members))
        } else {
            Ok(Response::Array(members))
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SUNIONSTORE - Store union of sets
// ============================================================================

pub struct SunionstoreCommand;

impl Command for SunionstoreCommand {
    fn name(&self) -> &'static str {
        "SUNIONSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SUNIONSTORE destination key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => SetValue::new(),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s.clone());
            }
        }

        let result = first.union(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SINTERSTORE - Store intersection of sets
// ============================================================================

pub struct SinterstoreCommand;

impl Command for SinterstoreCommand {
    fn name(&self) -> &'static str {
        "SINTERSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SINTERSTORE destination key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => {
                ctx.store.delete(dest);
                return Ok(Response::Integer(0));
            }
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s.clone()),
                None => {
                    ctx.store.delete(dest);
                    return Ok(Response::Integer(0));
                }
            }
        }

        let result = first.intersection(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SDIFFSTORE - Store difference of sets
// ============================================================================

pub struct SdiffstoreCommand;

impl Command for SdiffstoreCommand {
    fn name(&self) -> &'static str {
        "SDIFFSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SDIFFSTORE destination key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let dest = &args[0];

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => {
                ctx.store.delete(dest);
                return Ok(Response::Integer(0));
            }
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..] {
            if let Some(s) = get_set_inline(ctx, key)? {
                others.push(s.clone());
            }
        }

        let result = first.difference(others.iter());

        let len = result.len() as i64;

        // Store the result
        if result.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::Set(result));
        }

        Ok(Response::Integer(len))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        args.iter().map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// SINTERCARD - Return cardinality of intersection
// ============================================================================

pub struct SintercardCommand;

impl Command for SintercardCommand {
    fn name(&self) -> &'static str {
        "SINTERCARD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SINTERCARD numkeys key [key ...] [LIMIT limit]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;

        if numkeys == 0 {
            return Err(CommandError::InvalidArgument {
                message: "numkeys should be greater than 0".to_string(),
            });
        }

        if args.len() < 1 + numkeys {
            return Err(CommandError::InvalidArgument {
                message: "Number of keys can't be greater than number of args".to_string(),
            });
        }

        // Parse optional LIMIT
        let mut limit: Option<usize> = None;
        let key_end = 1 + numkeys;
        let mut i = key_end;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            if opt.as_slice() == b"LIMIT" {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                limit = Some(parse_usize(&args[i])?);
            } else {
                return Err(CommandError::SyntaxError);
            }
            i += 1;
        }

        // Get the first set (or empty)
        let first = match get_set_inline(ctx, &args[1])? {
            Some(s) => s.clone(),
            None => return Ok(Response::Integer(0)),
        };

        // Collect other sets
        let mut others = Vec::new();
        for key in &args[2..key_end] {
            match get_set_inline(ctx, key)? {
                Some(s) => others.push(s),
                None => return Ok(Response::Integer(0)),
            }
        }

        let result = first.intersection(others.iter());
        let mut count = result.len();

        if let Some(l) = limit {
            if l > 0 {
                count = count.min(l);
            }
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }
        // Parse numkeys to get the actual keys
        if let Ok(numkeys) = parse_usize(&args[0]) {
            args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
        } else {
            vec![]
        }
    }
}

// ============================================================================
// SRANDMEMBER - Get random members
// ============================================================================

pub struct SrandmemberCommand;

impl Command for SrandmemberCommand {
    fn name(&self) -> &'static str {
        "SRANDMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SRANDMEMBER key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_i64(&args[1])?)
        } else {
            None
        };

        match get_set_inline(ctx, key)? {
            Some(set) => {
                if set.is_empty() {
                    if count.is_some() {
                        return Ok(Response::Array(vec![]));
                    } else {
                        return Ok(Response::null());
                    }
                }

                match count {
                    Some(c) => {
                        let members = set.random_members(c);
                        let results: Vec<Response> =
                            members.into_iter().map(Response::bulk).collect();
                        Ok(Response::Array(results))
                    }
                    None => {
                        // Single member mode
                        let members = set.random_members(1);
                        if let Some(member) = members.into_iter().next() {
                            Ok(Response::bulk(member))
                        } else {
                            Ok(Response::null())
                        }
                    }
                }
            }
            None => {
                if count.is_some() {
                    Ok(Response::Array(vec![]))
                } else {
                    Ok(Response::null())
                }
            }
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

// ============================================================================
// SPOP - Remove and return random members
// ============================================================================

pub struct SpopCommand;

impl Command for SpopCommand {
    fn name(&self) -> &'static str {
        "SPOP"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1) // SPOP key [count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::FAST
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        let count = if args.len() > 1 {
            Some(parse_usize(&args[1])?)
        } else {
            None
        };

        // Check if key exists
        if ctx.store.get(key).is_none() {
            if count.is_some() {
                return Ok(Response::Array(vec![]));
            } else {
                return Ok(Response::null());
            }
        }

        // Verify type
        if ctx.store.get(key).unwrap().as_set().is_none() {
            return Err(CommandError::WrongType);
        }

        let set = ctx.store.get_mut(key).unwrap().as_set_mut().unwrap();

        match count {
            Some(c) => {
                let members = set.pop_many(c);
                let results: Vec<Response> = members.into_iter().map(Response::bulk).collect();

                // Delete key if set is now empty
                if set.is_empty() {
                    ctx.store.delete(key);
                }

                Ok(Response::Array(results))
            }
            None => {
                // Single member mode
                match set.pop() {
                    Some(member) => {
                        // Delete key if set is now empty
                        if set.is_empty() {
                            ctx.store.delete(key);
                        }
                        Ok(Response::bulk(member))
                    }
                    None => Ok(Response::null()),
                }
            }
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

// ============================================================================
// SMOVE - Move member from one set to another
// ============================================================================

pub struct SmoveCommand;

impl Command for SmoveCommand {
    fn name(&self) -> &'static str {
        "SMOVE"
    }

    fn arity(&self) -> Arity {
        Arity::Fixed(3) // SMOVE source destination member
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let source = &args[0];
        let dest = &args[1];
        let member = &args[2];

        // Check source exists and is correct type
        match ctx.store.get(source) {
            Some(v) => {
                if v.as_set().is_none() {
                    return Err(CommandError::WrongType);
                }
            }
            None => return Ok(Response::Integer(0)),
        }

        // Check dest is correct type if it exists
        if let Some(v) = ctx.store.get(dest) {
            if v.as_set().is_none() {
                return Err(CommandError::WrongType);
            }
        }

        // Remove from source
        let source_set = ctx.store.get_mut(source).unwrap().as_set_mut().unwrap();
        if !source_set.remove(member) {
            return Ok(Response::Integer(0));
        }

        // Delete source if empty
        if source_set.is_empty() {
            ctx.store.delete(source);
        }

        // Add to dest (create if needed)
        let dest_set = get_or_create_set(ctx, dest)?;
        dest_set.add(member.clone());

        Ok(Response::Integer(1))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            vec![]
        } else {
            vec![&args[0], &args[1]]
        }
    }
}

// ============================================================================
// SSCAN - Cursor-based iteration
// ============================================================================

pub struct SscanCommand;

impl Command for SscanCommand {
    fn name(&self) -> &'static str {
        "SSCAN"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // SSCAN key cursor [MATCH pattern] [COUNT count]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let cursor = parse_usize(&args[1])?;

        // Parse options
        let mut _match_pattern: Option<&[u8]> = None;
        let mut count: usize = 10;

        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    _match_pattern = Some(&args[i]);
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    count = parse_usize(&args[i])?;
                }
                _ => return Err(CommandError::SyntaxError),
            }
            i += 1;
        }

        match get_set_inline(ctx, key)? {
            Some(set) => {
                let members: Vec<_> = set.members().collect();
                let total = members.len();

                if cursor >= total {
                    return Ok(Response::Array(vec![
                        Response::bulk(Bytes::from("0")),
                        Response::Array(vec![]),
                    ]));
                }

                let end = (cursor + count).min(total);
                let next_cursor = if end >= total { 0 } else { end };

                let results: Vec<Response> = members
                    .into_iter()
                    .skip(cursor)
                    .take(count)
                    .map(|m| Response::bulk(m.clone()))
                    .collect();

                Ok(Response::Array(vec![
                    Response::bulk(Bytes::from(next_cursor.to_string())),
                    Response::Array(results),
                ]))
            }
            None => Ok(Response::Array(vec![
                Response::bulk(Bytes::from("0")),
                Response::Array(vec![]),
            ])),
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

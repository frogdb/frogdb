use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, SortedSetValue, Value,
    shard_for_key,
};
use frogdb_protocol::Response;
use std::collections::HashMap;

use crate::utils::require_same_shard;
use crate::utils::{parse_f64, parse_usize, scored_array};

/// Aggregate function for set operations.
#[derive(Clone, Copy)]
enum AggregateFunc {
    Sum,
    Min,
    Max,
}

/// Parse weights and aggregate options for set operations.
fn parse_set_op_options(
    args: &[Bytes],
    numkeys: usize,
) -> Result<(Vec<f64>, AggregateFunc, bool), CommandError> {
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = AggregateFunc::Sum;
    let mut with_scores = false;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"WEIGHTS" => {
                if i + numkeys >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                for j in 0..numkeys {
                    weights[j] = parse_f64(&args[i + 1 + j])?;
                }
                i += numkeys + 1;
            }
            b"AGGREGATE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let agg = args[i + 1].to_ascii_uppercase();
                aggregate = match agg.as_slice() {
                    b"SUM" => AggregateFunc::Sum,
                    b"MIN" => AggregateFunc::Min,
                    b"MAX" => AggregateFunc::Max,
                    _ => return Err(CommandError::SyntaxError),
                };
                i += 2;
            }
            b"WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    Ok((weights, aggregate, with_scores))
}

/// Apply aggregate function.
fn apply_aggregate(scores: &[f64], func: AggregateFunc) -> f64 {
    match func {
        AggregateFunc::Sum => scores.iter().sum(),
        AggregateFunc::Min => scores.iter().cloned().fold(f64::INFINITY, f64::min),
        AggregateFunc::Max => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
    }
}

// ============================================================================
// ZUNION - Union without store
// ============================================================================

pub struct ZunionCommand;

impl Command for ZunionCommand {
    fn name(&self) -> &'static str {
        "ZUNION"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let options = &args[numkeys + 1..];

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                for (member, score) in zset.iter() {
                    let weighted_score = score * weights[i];
                    result
                        .entry(member.clone())
                        .or_default()
                        .push(weighted_score);
                }
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result
            .into_iter()
            .map(|(member, scores)| (member, apply_aggregate(&scores, aggregate)))
            .collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        Ok(scored_array(members, with_scores))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZUNIONSTORE - Union and store
// ============================================================================

pub struct ZunionstoreCommand;

impl Command for ZunionstoreCommand {
    fn name(&self) -> &'static str {
        "ZUNIONSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];
        let options = &args[numkeys + 2..];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                for (member, score) in zset.iter() {
                    let weighted_score = score * weights[i];
                    result
                        .entry(member.clone())
                        .or_default()
                        .push(weighted_score);
                }
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, scores) in result {
            let score = apply_aggregate(&scores, aggregate);
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

// ============================================================================
// ZINTER - Intersection without store
// ============================================================================

pub struct ZinterCommand;

impl Command for ZinterCommand {
    fn name(&self) -> &'static str {
        "ZINTER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let options = &args[numkeys + 1..];

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_zset.iter() {
            result.insert(member.clone(), vec![score * weights[0]]);
        }

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Array(vec![])),
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            result.retain(|member, scores| {
                if let Some(score) = zset.get_score(member) {
                    scores.push(score * weights[i]);
                    true
                } else {
                    false
                }
            });

            if result.is_empty() {
                return Ok(Response::Array(vec![]));
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result
            .into_iter()
            .map(|(member, scores)| (member, apply_aggregate(&scores, aggregate)))
            .collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        Ok(scored_array(members, with_scores))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZINTERSTORE - Intersection and store
// ============================================================================

pub struct ZinterstoreCommand;

impl Command for ZinterstoreCommand {
    fn name(&self) -> &'static str {
        "ZINTERSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZINTERSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];
        let options = &args[numkeys + 2..];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_zset.iter() {
            result.insert(member.clone(), vec![score * weights[0]]);
        }

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => {
                    ctx.store.delete(&dest);
                    return Ok(Response::Integer(0));
                }
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            result.retain(|member, scores| {
                if let Some(score) = zset.get_score(member) {
                    scores.push(score * weights[i]);
                    true
                } else {
                    false
                }
            });

            if result.is_empty() {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, scores) in result {
            let score = apply_aggregate(&scores, aggregate);
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

// ============================================================================
// ZINTERCARD - Intersection cardinality
// ============================================================================

pub struct ZintercardCommand;

impl Command for ZintercardCommand {
    fn name(&self) -> &'static str {
        "ZINTERCARD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZINTERCARD numkeys key [key ...] [LIMIT limit]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        // Parse LIMIT option
        let mut limit: Option<usize> = None;
        let mut i = 0;
        while i < remaining.len() {
            let opt = remaining[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIMIT" => {
                    if i + 1 >= remaining.len() {
                        return Err(CommandError::SyntaxError);
                    }
                    limit = Some(parse_usize(&remaining[i + 1])?);
                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Integer(0)),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut members: std::collections::HashSet<Bytes> = first_zset
            .iter()
            .map(|(member, _)| member.clone())
            .collect();

        // Intersect with remaining sets
        for key in keys.iter().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Integer(0)),
            };
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            members.retain(|member| zset.contains(member));

            if members.is_empty() {
                return Ok(Response::Integer(0));
            }
        }

        let count = match limit {
            Some(l) if l > 0 => members.len().min(l),
            _ => members.len(),
        };

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZDIFF - Difference without store
// ============================================================================

pub struct ZdiffCommand;

impl Command for ZdiffCommand {
    fn name(&self) -> &'static str {
        "ZDIFF"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // ZDIFF numkeys key [key ...] [WITHSCORES]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let numkeys = parse_usize(&args[0])?;
        if numkeys == 0 || args.len() < numkeys + 1 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[1..numkeys + 1];
        let remaining = &args[numkeys + 1..];

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let with_scores =
            !remaining.is_empty() && remaining[0].to_ascii_uppercase().as_slice() == b"WITHSCORES";

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, f64> = first_zset
            .iter()
            .map(|(member, score)| (member.clone(), score))
            .collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                result.retain(|member, _| !zset.contains(member));
            }

            if result.is_empty() {
                return Ok(Response::Array(vec![]));
            }
        }

        // Build sorted result
        let mut members: Vec<(Bytes, f64)> = result.into_iter().collect();
        members.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        Ok(scored_array(members, with_scores))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        args[1..].iter().take(numkeys).map(|a| a.as_ref()).collect()
    }
}

// ============================================================================
// ZDIFFSTORE - Difference and store
// ============================================================================

pub struct ZdiffstoreCommand;

impl Command for ZdiffstoreCommand {
    fn name(&self) -> &'static str {
        "ZDIFFSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3) // ZDIFFSTORE destination numkeys key [key ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let dest = args[0].clone();
        let numkeys = parse_usize(&args[1])?;
        if numkeys == 0 || args.len() < numkeys + 2 {
            return Err(CommandError::SyntaxError);
        }

        let keys = &args[2..numkeys + 2];

        // Check all keys (including dest) are in same shard
        let first_shard = shard_for_key(&dest, ctx.num_shards);
        for key in keys {
            if shard_for_key(key, ctx.num_shards) != first_shard {
                return Err(CommandError::CrossSlot);
            }
        }

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let first_zset = first_value.as_sorted_set().ok_or(CommandError::WrongType)?;

        let mut result: HashMap<Bytes, f64> = first_zset
            .iter()
            .map(|(member, score)| (member.clone(), score))
            .collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;
                result.retain(|member, _| !zset.contains(member));
            }

            if result.is_empty() {
                break;
            }
        }

        // Build result set
        let mut new_zset = SortedSetValue::new();
        for (member, score) in result {
            new_zset.add(member, score);
        }

        let count = new_zset.len();

        if count > 0 {
            ctx.store.set(dest, Value::SortedSet(new_zset));
        } else {
            ctx.store.delete(&dest);
        }

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() < 2 {
            return vec![];
        }

        let numkeys = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        let mut keys = vec![args[0].as_ref()];
        keys.extend(args[2..].iter().take(numkeys).map(|a| a.as_ref()));
        keys
    }
}

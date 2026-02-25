use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, SortedSetValue, Value,
    shard_for_key,
};
use frogdb_protocol::Response;
use std::collections::HashMap;

use crate::utils::require_same_shard;
use crate::utils::{parse_f64, parse_usize, scored_array, scored_array_resp3};

/// Iterate members/scores from a Value that is either a sorted set or a regular set.
///
/// Redis Z*STORE operations accept both sorted sets and regular sets as inputs.
/// Regular set members are treated as having score 0.
fn iter_zset_or_set(value: &frogdb_core::Value) -> Result<Vec<(Bytes, f64)>, CommandError> {
    if let Some(zset) = value.as_sorted_set() {
        Ok(zset.iter().map(|(m, s)| (m.clone(), s)).collect())
    } else if let Some(set) = value.as_set() {
        Ok(set.members().map(|m| (m.clone(), 1.0)).collect())
    } else {
        Err(CommandError::WrongType)
    }
}

/// Check if a Value contains a member (works with sorted sets and regular sets).
fn zset_or_set_contains(value: &frogdb_core::Value, member: &Bytes) -> Result<bool, CommandError> {
    if let Some(zset) = value.as_sorted_set() {
        Ok(zset.contains(member))
    } else if let Some(set) = value.as_set() {
        Ok(set.contains(member))
    } else {
        Err(CommandError::WrongType)
    }
}

/// Get a member's score from a sorted set or regular set (score 0 for regular sets).
fn zset_or_set_get_score(
    value: &frogdb_core::Value,
    member: &Bytes,
) -> Result<Option<f64>, CommandError> {
    if let Some(zset) = value.as_sorted_set() {
        Ok(zset.get_score(member))
    } else if let Some(set) = value.as_set() {
        if set.contains(member) {
            Ok(Some(1.0))
        } else {
            Ok(None)
        }
    } else {
        Err(CommandError::WrongType)
    }
}

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
    allow_withscores: bool,
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
                    let w =
                        parse_f64(&args[i + 1 + j]).map_err(|_| CommandError::InvalidArgument {
                            message: "weight value is not a float".to_string(),
                        })?;
                    if w.is_nan() {
                        return Err(CommandError::InvalidArgument {
                            message: "weight value is not a float".to_string(),
                        });
                    }
                    weights[j] = w;
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
                if !allow_withscores {
                    return Err(CommandError::SyntaxError);
                }
                with_scores = true;
                i += 1;
            }
            _ => return Err(CommandError::SyntaxError),
        }
    }

    Ok((weights, aggregate, with_scores))
}

/// Apply aggregate function.
///
/// Returns 0.0 for NaN results (e.g. `inf + (-inf)`), matching Redis behavior.
fn apply_aggregate(scores: &[f64], func: AggregateFunc) -> f64 {
    let result = match func {
        AggregateFunc::Sum => scores.iter().sum(),
        AggregateFunc::Min => scores.iter().cloned().fold(f64::INFINITY, f64::min),
        AggregateFunc::Max => scores.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
    };
    if result.is_nan() { 0.0 } else { result }
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
        let is_resp3 = ctx.protocol_version.is_resp3();

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys, true)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                for (member, score) in iter_zset_or_set(&value)? {
                    let weighted_score = score * weights[i];
                    result.entry(member).or_default().push(weighted_score);
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

        if is_resp3 {
            Ok(scored_array_resp3(members, with_scores))
        } else {
            Ok(scored_array(members, with_scores))
        }
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

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys, false)?;

        // Collect all members with their weighted scores
        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = ctx.store.get(key) {
                for (member, score) in iter_zset_or_set(&value)? {
                    let weighted_score = score * weights[i];
                    result.entry(member).or_default().push(weighted_score);
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
        let is_resp3 = ctx.protocol_version.is_resp3();

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let (weights, aggregate, with_scores) = parse_set_op_options(options, numkeys, true)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_members = iter_zset_or_set(&first_value)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_members {
            result.insert(member, vec![score * weights[0]]);
        }

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Array(vec![])),
            };

            result.retain(
                |member, scores| match zset_or_set_get_score(&value, member) {
                    Ok(Some(score)) => {
                        scores.push(score * weights[i]);
                        true
                    }
                    _ => false,
                },
            );

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

        if is_resp3 {
            Ok(scored_array_resp3(members, with_scores))
        } else {
            Ok(scored_array(members, with_scores))
        }
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

        let (weights, aggregate, _) = parse_set_op_options(options, numkeys, false)?;

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
        };
        let first_members = iter_zset_or_set(&first_value)?;

        let mut result: HashMap<Bytes, Vec<f64>> = HashMap::new();
        for (member, score) in first_members {
            result.insert(member, vec![score * weights[0]]);
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

            result.retain(
                |member, scores| match zset_or_set_get_score(&value, member) {
                    Ok(Some(score)) => {
                        scores.push(score * weights[i]);
                        true
                    }
                    _ => false,
                },
            );

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
                    limit = Some(parse_usize(&remaining[i + 1]).map_err(|_| {
                        CommandError::InvalidArgument {
                            message: "LIMIT can't be negative".to_string(),
                        }
                    })?);
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
        let first_members = iter_zset_or_set(&first_value)?;

        let mut members: std::collections::HashSet<Bytes> = first_members
            .into_iter()
            .map(|(member, _)| member)
            .collect();

        // Intersect with remaining sets
        for key in keys.iter().skip(1) {
            let value = match ctx.store.get(key) {
                Some(v) => v,
                None => return Ok(Response::Integer(0)),
            };

            members.retain(|member| zset_or_set_contains(&value, member).unwrap_or(false));

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
        let is_resp3 = ctx.protocol_version.is_resp3();

        // Check all keys are in same shard
        require_same_shard(keys, ctx.num_shards)?;

        let with_scores =
            !remaining.is_empty() && remaining[0].to_ascii_uppercase().as_slice() == b"WITHSCORES";

        // Start with members from first set
        let first_value = match ctx.store.get(&keys[0]) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let first_members = iter_zset_or_set(&first_value)?;

        let mut result: HashMap<Bytes, f64> = first_members.into_iter().collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                result.retain(|member, _| !zset_or_set_contains(&value, member).unwrap_or(false));
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

        if is_resp3 {
            Ok(scored_array_resp3(members, with_scores))
        } else {
            Ok(scored_array(members, with_scores))
        }
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
        let first_members = iter_zset_or_set(&first_value)?;

        let mut result: HashMap<Bytes, f64> = first_members.into_iter().collect();

        // Remove members that exist in other sets
        for key in keys.iter().skip(1) {
            if let Some(value) = ctx.store.get(key) {
                result.retain(|member, _| !zset_or_set_contains(&value, member).unwrap_or(false));
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

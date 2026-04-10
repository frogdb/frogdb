//! SORT and SORT_RO command implementations.
//!
//! These commands sort elements from Lists, Sets, and Sorted Sets.
//! SORT can optionally store the result, while SORT_RO is read-only.

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, Value, WaiterKind, WaiterWake,
    shard_for_key,
};
use frogdb_protocol::Response;

use super::utils::{parse_f64, parse_i64};

/// Options parsed from SORT/SORT_RO arguments.
#[derive(Debug)]
struct SortOptions {
    /// The source key to sort.
    key: Bytes,
    /// BY pattern for external key sorting.
    by_pattern: Option<Bytes>,
    /// LIMIT offset and count.
    limit: Option<(usize, usize)>,
    /// GET patterns (can have multiple).
    get_patterns: Vec<Bytes>,
    /// Sort in ascending order (default true).
    ascending: bool,
    /// Use lexicographic sorting instead of numeric.
    alpha: bool,
    /// STORE destination key (SORT only).
    store: Option<Bytes>,
}

impl SortOptions {
    /// Parse SORT/SORT_RO arguments.
    fn parse(args: &[Bytes], allow_store: bool) -> Result<Self, CommandError> {
        if args.is_empty() {
            return Err(CommandError::SyntaxError);
        }

        let key = args[0].clone();
        let mut by_pattern = None;
        let mut limit = None;
        let mut get_patterns = Vec::new();
        let mut ascending = true;
        let mut alpha = false;
        let mut store = None;

        let mut i = 1;
        while i < args.len() {
            let arg = &args[i];
            let arg_upper = arg.to_ascii_uppercase();

            if arg_upper == b"BY" {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                by_pattern = Some(args[i].clone());
            } else if arg_upper == b"LIMIT" {
                i += 1;
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let offset = parse_i64(&args[i])?;
                i += 1;
                let count = parse_i64(&args[i])?;

                // Redis allows negative offsets (clamped to 0) and negative counts (treated as 0)
                let offset = if offset < 0 { 0 } else { offset as usize };
                let count = if count < 0 { 0 } else { count as usize };
                limit = Some((offset, count));
            } else if arg_upper == b"GET" {
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                get_patterns.push(args[i].clone());
            } else if arg_upper == b"ASC" {
                ascending = true;
            } else if arg_upper == b"DESC" {
                ascending = false;
            } else if arg_upper == b"ALPHA" {
                alpha = true;
            } else if arg_upper == b"STORE" {
                if !allow_store {
                    return Err(CommandError::SyntaxError);
                }
                i += 1;
                if i >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                store = Some(args[i].clone());
            } else {
                return Err(CommandError::SyntaxError);
            }
            i += 1;
        }

        Ok(SortOptions {
            key,
            by_pattern,
            limit,
            get_patterns,
            ascending,
            alpha,
            store,
        })
    }
}

/// Extract elements from a List, Set, or Sorted Set.
fn extract_elements(
    ctx: &mut CommandContext,
    key: &Bytes,
) -> Result<Option<Vec<Bytes>>, CommandError> {
    let value = match ctx.store.get(key) {
        Some(v) => v,
        None => return Ok(None),
    };

    match &*value {
        Value::List(list) => Ok(Some(list.iter().cloned().collect())),
        Value::Set(set) => Ok(Some(set.members().collect())),
        Value::SortedSet(zset) => Ok(Some(zset.iter().map(|(m, _)| m.clone()).collect())),
        _ => Err(CommandError::WrongType),
    }
}

/// Pattern resolution: substitute `*` with the element value and look up the key.
/// Supports two forms:
/// - `prefix_*_suffix` -> GET string key
/// - `prefix_*->field` -> GET hash key -> HGET field
fn resolve_pattern(ctx: &mut CommandContext, pattern: &[u8], element: &[u8]) -> Option<Bytes> {
    // Find the `*` in the pattern
    let star_pos = pattern.iter().position(|&b| b == b'*')?;

    // Check for hash field access with `->`
    let arrow_pos = pattern.windows(2).position(|w| w == b"->");

    if let Some(arrow_idx) = arrow_pos {
        // Hash field pattern: prefix_*->field
        // The `*` must come before `->`
        if star_pos >= arrow_idx {
            return None;
        }

        // Build the key by replacing `*` with the element
        let key_part = &pattern[..arrow_idx];
        let field = &pattern[arrow_idx + 2..];

        // If field is empty (pattern ends with "->"), treat as plain string key lookup
        // on the full expanded pattern (including the "->").
        if field.is_empty() {
            let mut key = Vec::with_capacity(pattern.len() - 1 + element.len());
            key.extend_from_slice(&pattern[..star_pos]);
            key.extend_from_slice(element);
            key.extend_from_slice(&pattern[star_pos + 1..]);
            let key_bytes = Bytes::from(key);
            if let Some(value) = ctx.store.get(&key_bytes)
                && let Value::String(s) = &*value
            {
                return Some(s.as_bytes().clone());
            }
            return None;
        }

        let mut key = Vec::with_capacity(key_part.len() - 1 + element.len());
        key.extend_from_slice(&key_part[..star_pos]);
        key.extend_from_slice(element);
        key.extend_from_slice(&key_part[star_pos + 1..]);

        // Look up hash field
        let key_bytes = Bytes::from(key);
        if let Some(value) = ctx.store.get(&key_bytes)
            && let Value::Hash(hash) = &*value
        {
            return hash.get(field);
        }
        None
    } else {
        // String pattern: prefix_*_suffix
        let mut key = Vec::with_capacity(pattern.len() - 1 + element.len());
        key.extend_from_slice(&pattern[..star_pos]);
        key.extend_from_slice(element);
        key.extend_from_slice(&pattern[star_pos + 1..]);

        let key_bytes = Bytes::from(key);
        if let Some(value) = ctx.store.get(&key_bytes)
            && let Value::String(s) = &*value
        {
            return Some(s.as_bytes().clone());
        }
        None
    }
}

/// Compute the sort key for an element.
/// Returns None if the element should be sorted as if it had score 0 (for missing external keys).
fn compute_sort_key(
    ctx: &mut CommandContext,
    element: &Bytes,
    by_pattern: &Option<Bytes>,
    alpha: bool,
) -> Result<SortKey, CommandError> {
    let value = if let Some(pattern) = by_pattern {
        // Check for "nosort" pattern - keep original order
        if pattern.to_ascii_lowercase() == b"nosort" {
            return Ok(SortKey::NoSort);
        }
        // Use external key value
        resolve_pattern(ctx, pattern, element)
    } else {
        // Use element itself
        Some(element.clone())
    };

    match value {
        Some(v) => {
            if alpha {
                Ok(SortKey::Alpha(v))
            } else {
                // Try to parse as number
                let num = parse_f64(&v).map_err(|_| CommandError::InvalidArgument {
                    message: "One or more scores can't be converted into double".to_string(),
                })?;
                Ok(SortKey::Numeric(num))
            }
        }
        None => {
            // Missing external key - sort as 0 for numeric, or empty string for alpha
            if alpha {
                Ok(SortKey::Alpha(Bytes::new()))
            } else {
                Ok(SortKey::Numeric(0.0))
            }
        }
    }
}

/// Sort key for comparison.
#[derive(Debug, Clone)]
enum SortKey {
    /// Numeric comparison.
    Numeric(f64),
    /// Lexicographic comparison.
    Alpha(Bytes),
    /// No sorting (preserve original order).
    NoSort,
}

impl SortKey {
    fn cmp(&self, other: &SortKey, ascending: bool) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let ord = match (self, other) {
            (SortKey::NoSort, _) | (_, SortKey::NoSort) => Ordering::Equal,
            (SortKey::Numeric(a), SortKey::Numeric(b)) => {
                // Handle NaN: treat as greater than everything
                if a.is_nan() && b.is_nan() {
                    Ordering::Equal
                } else if a.is_nan() {
                    Ordering::Greater
                } else if b.is_nan() {
                    Ordering::Less
                } else {
                    a.partial_cmp(b).unwrap_or(Ordering::Equal)
                }
            }
            (SortKey::Alpha(a), SortKey::Alpha(b)) => a.cmp(b),
            // Mixed types shouldn't happen due to consistent alpha flag usage
            _ => Ordering::Equal,
        };

        if ascending { ord } else { ord.reverse() }
    }
}

/// Execute the core SORT logic.
fn execute_sort(ctx: &mut CommandContext, opts: &SortOptions) -> Result<Response, CommandError> {
    // Extract elements from source key
    let elements = match extract_elements(ctx, &opts.key)? {
        Some(elems) => elems,
        None => {
            // Key doesn't exist - return empty array or store empty list
            if let Some(dest) = &opts.store {
                // Delete destination key if it exists (store empty result)
                ctx.store.delete(dest);
                return Ok(Response::Integer(0));
            }
            return Ok(Response::Array(vec![]));
        }
    };

    if elements.is_empty() {
        if let Some(dest) = &opts.store {
            ctx.store.delete(dest);
            return Ok(Response::Integer(0));
        }
        return Ok(Response::Array(vec![]));
    }

    // Compute sort keys for each element
    let mut indexed: Vec<(Bytes, SortKey, usize)> = Vec::with_capacity(elements.len());
    for (idx, elem) in elements.iter().enumerate() {
        let sort_key = compute_sort_key(ctx, elem, &opts.by_pattern, opts.alpha)?;
        indexed.push((elem.clone(), sort_key, idx));
    }

    // Sort using stable sort to preserve order for equal keys
    indexed.sort_by(|a, b| {
        let ord = a.1.cmp(&b.1, opts.ascending);
        if ord == std::cmp::Ordering::Equal {
            // For NoSort keys, preserve original insertion order
            if matches!(a.1, SortKey::NoSort) {
                a.2.cmp(&b.2)
            } else {
                // Lexicographic tiebreaker on element value (matching Redis behavior)
                if opts.ascending {
                    a.0.cmp(&b.0)
                } else {
                    b.0.cmp(&a.0)
                }
            }
        } else {
            ord
        }
    });

    // NoSort + DESC: reverse element order
    if let Some(ref pattern) = opts.by_pattern {
        let is_nosort = pattern.to_ascii_lowercase() == b"nosort";
        if is_nosort && !opts.ascending {
            indexed.reverse();
        }
    }

    // Apply LIMIT
    let sorted: Vec<Bytes> = if let Some((offset, count)) = opts.limit {
        indexed
            .into_iter()
            .skip(offset)
            .take(count)
            .map(|(elem, _, _)| elem)
            .collect()
    } else {
        indexed.into_iter().map(|(elem, _, _)| elem).collect()
    };

    // Store or return
    if let Some(dest) = &opts.store {
        // Check cross-slot
        let src_shard = shard_for_key(&opts.key, ctx.num_shards);
        let dest_shard = shard_for_key(dest, ctx.num_shards);
        if src_shard != dest_shard {
            return Err(CommandError::CrossSlot);
        }

        // Apply GET patterns for storage
        let result: Vec<Bytes> = if opts.get_patterns.is_empty() {
            sorted
        } else {
            let mut result = Vec::new();
            for elem in &sorted {
                for pattern in &opts.get_patterns {
                    if pattern.as_ref() == b"#" {
                        result.push(elem.clone());
                    } else {
                        // For storage, missing keys become empty strings
                        match resolve_pattern(ctx, pattern, elem) {
                            Some(value) => result.push(value),
                            None => result.push(Bytes::new()),
                        }
                    }
                }
            }
            result
        };

        let count = result.len() as i64;

        // Create list from result
        let mut list = frogdb_core::ListValue::new();
        for elem in result {
            list.push_back(elem);
        }

        if list.is_empty() {
            ctx.store.delete(dest);
        } else {
            ctx.store.set(dest.clone(), Value::List(list));
        }

        Ok(Response::Integer(count))
    } else {
        // Return array
        // Note: When GET patterns return nil (missing keys), Redis returns null bulk strings
        let response: Vec<Response> = if opts.get_patterns.is_empty() {
            sorted
                .into_iter()
                .map(|b| Response::Bulk(Some(b)))
                .collect()
        } else {
            let mut responses = Vec::new();
            for elem in &sorted {
                for pattern in &opts.get_patterns {
                    if pattern.as_ref() == b"#" {
                        responses.push(Response::Bulk(Some(elem.clone())));
                    } else {
                        match resolve_pattern(ctx, pattern, elem) {
                            Some(value) => responses.push(Response::Bulk(Some(value))),
                            None => responses.push(Response::Bulk(None)),
                        }
                    }
                }
            }
            responses
        };
        Ok(Response::Array(response))
    }
}

// ============================================================================
// SORT Command
// ============================================================================

pub struct SortCommand;

impl Command for SortCommand {
    fn name(&self) -> &'static str {
        "SORT"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wakes_waiters(&self) -> WaiterWake {
        // SORT..STORE always creates a list at the destination key.
        WaiterWake::Kind(WaiterKind::List)
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let opts = SortOptions::parse(args, true)?;
        execute_sort(ctx, &opts)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }

        let mut keys = vec![args[0].as_ref()];

        // Find the last STORE destination (Redis uses the last one when multiple STORE appear)
        let mut store_key: Option<&[u8]> = None;
        let mut i = 1;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            if arg_upper == b"STORE" && i + 1 < args.len() {
                store_key = Some(args[i + 1].as_ref());
                i += 2;
                continue;
            }
            // Skip arguments that take a parameter
            if arg_upper == b"BY" || arg_upper == b"GET" {
                i += 2;
                continue;
            }
            if arg_upper == b"LIMIT" && i + 2 < args.len() {
                i += 3; // LIMIT takes two parameters
                continue;
            }
            i += 1;
        }
        if let Some(dest) = store_key {
            keys.push(dest);
        }

        keys
    }
}

// ============================================================================
// SORT_RO Command (Read-Only)
// ============================================================================

pub struct SortRoCommand;

impl Command for SortRoCommand {
    fn name(&self) -> &'static str {
        "SORT_RO"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(1)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let opts = SortOptions::parse(args, false)?;
        execute_sort(ctx, &opts)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![args[0].as_ref()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::HashMapStore;
    use frogdb_protocol::ProtocolVersion;
    use std::sync::Arc;

    fn create_test_context() -> CommandContext<'static> {
        let store = Box::leak(Box::new(HashMapStore::new()));
        let shard_senders = Box::leak(Box::new(Arc::new(Vec::new())));
        CommandContext::new(store, shard_senders, 0, 1, 0, ProtocolVersion::Resp2)
    }

    #[test]
    fn test_sort_numeric_list() {
        let mut ctx = create_test_context();

        // Create list with unsorted numbers
        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("mylist")]).unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("3"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_desc() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        list.push_back(Bytes::from("3"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(&mut ctx, &[Bytes::from("mylist"), Bytes::from("DESC")])
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("3"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("1"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_alpha() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("banana"));
        list.push_back(Bytes::from("apple"));
        list.push_back(Bytes::from("cherry"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(&mut ctx, &[Bytes::from("mylist"), Bytes::from("ALPHA")])
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("apple"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("banana"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("cherry"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_limit() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        for i in 1..=5 {
            list.push_back(Bytes::from(i.to_string()));
        }
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("LIMIT"),
                    Bytes::from("1"),
                    Bytes::from("2"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("3"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_by_pattern() {
        let mut ctx = create_test_context();

        // Create list
        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        list.push_back(Bytes::from("3"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        // Create weight keys
        ctx.store.set(Bytes::from("weight_1"), Value::string("10"));
        ctx.store.set(Bytes::from("weight_2"), Value::string("5"));
        ctx.store.set(Bytes::from("weight_3"), Value::string("15"));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("BY"),
                    Bytes::from("weight_*"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            // Should be sorted by weight: 2 (5), 1 (10), 3 (15)
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("3"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_by_nosort() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("BY"),
                    Bytes::from("nosort"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            // Should preserve original order
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("3"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("2"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_get_pattern() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        ctx.store.set(Bytes::from("name_1"), Value::string("Alice"));
        ctx.store.set(Bytes::from("name_2"), Value::string("Bob"));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("GET"),
                    Bytes::from("name_*"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("Alice"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("Bob"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_get_hash_field() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        // Create hash keys
        let mut hash1 = frogdb_core::HashValue::new();
        hash1.set(
            Bytes::from("name"),
            Bytes::from("Alice"),
            frogdb_core::ListpackThresholds::DEFAULT_HASH,
        );
        ctx.store.set(Bytes::from("user_1"), Value::Hash(hash1));

        let mut hash2 = frogdb_core::HashValue::new();
        hash2.set(
            Bytes::from("name"),
            Bytes::from("Bob"),
            frogdb_core::ListpackThresholds::DEFAULT_HASH,
        );
        ctx.store.set(Bytes::from("user_2"), Value::Hash(hash2));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("GET"),
                    Bytes::from("user_*->name"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("Alice"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("Bob"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_get_hash() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[Bytes::from("mylist"), Bytes::from("GET"), Bytes::from("#")],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("2"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_multiple_get() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        ctx.store.set(Bytes::from("name_1"), Value::string("Alice"));
        ctx.store.set(Bytes::from("name_2"), Value::string("Bob"));
        ctx.store.set(Bytes::from("age_1"), Value::string("30"));
        ctx.store.set(Bytes::from("age_2"), Value::string("25"));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("GET"),
                    Bytes::from("#"),
                    Bytes::from("GET"),
                    Bytes::from("name_*"),
                    Bytes::from("GET"),
                    Bytes::from("age_*"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            // 2 elements * 3 GET patterns = 6 items
            assert_eq!(items.len(), 6);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("Alice"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("30"))));
            assert_eq!(items[3], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[4], Response::Bulk(Some(Bytes::from("Bob"))));
            assert_eq!(items[5], Response::Bulk(Some(Bytes::from("25"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_store() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("3"));
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("STORE"),
                    Bytes::from("result"),
                ],
            )
            .unwrap();

        assert_eq!(result, Response::Integer(3));

        // Verify stored list
        let stored = ctx.store.get(&Bytes::from("result")).unwrap();
        if let Value::List(list) = &*stored {
            assert_eq!(list.len(), 3);
            assert_eq!(list.get(0), Some(&Bytes::from("1")));
            assert_eq!(list.get(1), Some(&Bytes::from("2")));
            assert_eq!(list.get(2), Some(&Bytes::from("3")));
        } else {
            panic!("Expected list value");
        }
    }

    #[test]
    fn test_sort_set() {
        let mut ctx = create_test_context();

        let mut set = frogdb_core::SetValue::new();
        set.add(
            Bytes::from("3"),
            frogdb_core::ListpackThresholds::DEFAULT_SET,
        );
        set.add(
            Bytes::from("1"),
            frogdb_core::ListpackThresholds::DEFAULT_SET,
        );
        set.add(
            Bytes::from("2"),
            frogdb_core::ListpackThresholds::DEFAULT_SET,
        );
        ctx.store.set(Bytes::from("myset"), Value::Set(set));

        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("myset")]).unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("3"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_sorted_set() {
        let mut ctx = create_test_context();

        let mut zset = frogdb_core::SortedSetValue::new();
        zset.add(Bytes::from("3"), 30.0);
        zset.add(Bytes::from("1"), 10.0);
        zset.add(Bytes::from("2"), 20.0);
        ctx.store.set(Bytes::from("myzset"), Value::SortedSet(zset));

        // SORT should sort by element value, not by score
        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("myzset")]).unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("1"))));
            assert_eq!(items[1], Response::Bulk(Some(Bytes::from("2"))));
            assert_eq!(items[2], Response::Bulk(Some(Bytes::from("3"))));
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_wrong_type() {
        let mut ctx = create_test_context();

        ctx.store
            .set(Bytes::from("mystring"), Value::string("hello"));

        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("mystring")]);

        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[test]
    fn test_sort_non_numeric_error() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("not_a_number"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("mylist")]);

        // Should return the Redis-compatible error message
        assert!(matches!(result, Err(CommandError::InvalidArgument { .. })));
    }

    #[test]
    fn test_sort_ro_rejects_store() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortRoCommand;
        let result = cmd.execute(
            &mut ctx,
            &[
                Bytes::from("mylist"),
                Bytes::from("STORE"),
                Bytes::from("result"),
            ],
        );

        assert!(matches!(result, Err(CommandError::SyntaxError)));
    }

    #[test]
    fn test_sort_nonexistent_key() {
        let mut ctx = create_test_context();

        let cmd = SortCommand;
        let result = cmd
            .execute(&mut ctx, &[Bytes::from("nonexistent")])
            .unwrap();

        assert_eq!(result, Response::Array(vec![]));
    }

    #[test]
    fn test_sort_empty_list() {
        let mut ctx = create_test_context();

        let list = frogdb_core::ListValue::new();
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        let cmd = SortCommand;
        let result = cmd.execute(&mut ctx, &[Bytes::from("mylist")]).unwrap();

        assert_eq!(result, Response::Array(vec![]));
    }

    #[test]
    fn test_sort_get_missing_key() {
        let mut ctx = create_test_context();

        let mut list = frogdb_core::ListValue::new();
        list.push_back(Bytes::from("1"));
        list.push_back(Bytes::from("2"));
        ctx.store.set(Bytes::from("mylist"), Value::List(list));

        // Only set name for element 1, not 2
        ctx.store.set(Bytes::from("name_1"), Value::string("Alice"));

        let cmd = SortCommand;
        let result = cmd
            .execute(
                &mut ctx,
                &[
                    Bytes::from("mylist"),
                    Bytes::from("GET"),
                    Bytes::from("name_*"),
                ],
            )
            .unwrap();

        if let Response::Array(items) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Response::Bulk(Some(Bytes::from("Alice"))));
            assert_eq!(items[1], Response::Bulk(None)); // Missing key returns null
        } else {
            panic!("Expected array response");
        }
    }

    #[test]
    fn test_sort_keys_with_store() {
        let cmd = SortCommand;
        let args = vec![
            Bytes::from("mylist"),
            Bytes::from("STORE"),
            Bytes::from("result"),
        ];
        let keys = cmd.keys(&args);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], b"mylist");
        assert_eq!(keys[1], b"result");
    }

    #[test]
    fn test_sort_keys_without_store() {
        let cmd = SortCommand;
        let args = vec![Bytes::from("mylist"), Bytes::from("DESC")];
        let keys = cmd.keys(&args);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"mylist");
    }

    #[test]
    fn test_sort_ro_keys() {
        let cmd = SortRoCommand;
        let args = vec![Bytes::from("mylist"), Bytes::from("DESC")];
        let keys = cmd.keys(&args);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"mylist");
    }
}

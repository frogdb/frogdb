//! JSON document storage with JSONPath-based access.
//!
//! This module provides JSON document storage compatible with RedisJSON semantics,
//! supporting JSONPath queries and mutations.
//!
//! A stored document is a [`JsonTape`] — two contiguous allocations, not one boxed
//! node per value. Reads navigate it through [`TapeRef`] cursors; mutations rebuild
//! the tape in a single streaming pass with the edit applied, rather than splicing
//! it in place. `serde_json::Value` survives only at the seams: parsing input,
//! carrying the fragment a mutation inserts, and materializing values handed back
//! to callers that still speak trees.

mod path;
mod tape;

pub use tape::{Elements, JsonTape, Members, TapeRef};

use tape::TapeBuilder;

use serde_json::Value as JsonData;
use std::collections::HashMap;
use std::mem;
use thiserror::Error;

/// Default maximum JSON document depth.
pub const DEFAULT_JSON_MAX_DEPTH: usize = 128;

/// Default maximum JSON document size in bytes.
pub const DEFAULT_JSON_MAX_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// JSON document value stored in FrogDB.
#[derive(Debug, Clone)]
pub struct JsonValue {
    /// The document, encoded as a flat tape.
    tape: JsonTape,
}

/// Configuration limits for JSON documents.
#[derive(Debug, Clone, Copy)]
pub struct JsonLimits {
    /// Maximum nesting depth allowed.
    pub max_depth: usize,
    /// Maximum document size in bytes.
    pub max_size: usize,
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_JSON_MAX_DEPTH,
            max_size: DEFAULT_JSON_MAX_SIZE,
        }
    }
}

/// JSON value types as reported by JSON.TYPE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonType {
    /// JSON object.
    Object,
    /// JSON array.
    Array,
    /// JSON string.
    String,
    /// JSON integer (fits in i64).
    Integer,
    /// JSON floating-point number.
    Number,
    /// JSON boolean.
    Boolean,
    /// JSON null.
    Null,
}

impl JsonType {
    /// Get the type name as a string (for JSON.TYPE command).
    pub fn as_str(&self) -> &'static str {
        match self {
            JsonType::Object => "object",
            JsonType::Array => "array",
            JsonType::String => "string",
            JsonType::Integer => "integer",
            JsonType::Number => "number",
            JsonType::Boolean => "boolean",
            JsonType::Null => "null",
        }
    }
}

/// Errors that can occur during JSON operations.
#[derive(Debug, Error)]
pub enum JsonError {
    /// Failed to parse JSON.
    #[error("parse error: {0}")]
    ParseError(String),

    /// Path not found in document.
    #[error("path not found: {0}")]
    PathNotFound(String),

    /// Invalid JSONPath syntax.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// Document size exceeds limit.
    #[error("document size {0} exceeds maximum {1}")]
    SizeExceeded(usize, usize),

    /// Document depth exceeds limit.
    #[error("document depth {0} exceeds maximum {1}")]
    DepthExceeded(usize, usize),

    /// Type mismatch for operation.
    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch {
        expected: &'static str,
        found: &'static str,
    },

    /// Array index out of range.
    #[error("array index {0} out of range")]
    IndexOutOfRange(i64),

    /// Cannot perform operation at root.
    #[error("cannot perform operation at root path")]
    RootOperation,

    /// Path must exist for this operation.
    #[error("path must exist for this operation")]
    PathMustExist,

    /// Value cannot be incremented/multiplied.
    #[error("value is not a number")]
    NotANumber,

    /// Value cannot be toggled.
    #[error("value is not a boolean")]
    NotABoolean,

    /// Cannot append to non-string.
    #[error("value is not a string")]
    NotAString,

    /// Cannot perform array operation on non-array.
    #[error("value is not an array")]
    NotAnArray,

    /// Cannot perform object operation on non-object.
    #[error("value is not an object")]
    NotAnObject,
}

/// One pending change to a node, applied while the tape is rebuilt.
///
/// Edits are keyed by the target node's tape offset. A command resolves its path
/// once, records an edit per match, then rebuilds in a single pass — so a
/// mutation that fails partway through leaves the document untouched.
enum TapeEdit {
    /// Replace the node's subtree with this value.
    Replace(JsonData),
    /// Drop the node (and, in an object, its key) from its parent container.
    Remove,
    /// Splice `values` into this array node before element `index`; an `index` at
    /// or past the end appends.
    Insert { index: usize, values: Vec<JsonData> },
}

type EditMap = HashMap<usize, TapeEdit>;

impl JsonValue {
    /// Create a new JSON value from parsed data.
    pub fn new(data: JsonData) -> Self {
        Self {
            tape: JsonTape::from_value(&data),
        }
    }

    /// Parse JSON from bytes.
    pub fn parse(bytes: &[u8]) -> Result<Self, JsonError> {
        Self::parse_with_limits(bytes, &JsonLimits::default())
    }

    /// Parse JSON from bytes with custom limits.
    pub fn parse_with_limits(bytes: &[u8], limits: &JsonLimits) -> Result<Self, JsonError> {
        if bytes.len() > limits.max_size {
            return Err(JsonError::SizeExceeded(bytes.len(), limits.max_size));
        }

        let data: JsonData =
            serde_json::from_slice(bytes).map_err(|e| JsonError::ParseError(e.to_string()))?;

        let value = Self::new(data);
        let depth = value.root().depth();
        if depth > limits.max_depth {
            return Err(JsonError::DepthExceeded(depth, limits.max_depth));
        }

        Ok(value)
    }

    /// A cursor at the document root.
    pub fn root(&self) -> TapeRef<'_> {
        self.tape.root()
    }

    /// Materialize the whole document as a `serde_json` value.
    ///
    /// Only for seams that still speak trees (search-index field extraction,
    /// pretty-printing); stored state never holds one.
    pub fn to_json_data(&self) -> JsonData {
        self.root().to_json_data()
    }

    /// Validate this document against the configured limits.
    ///
    /// Enforces the same invariants as [`JsonValue::parse_with_limits`] but on an
    /// already-constructed value, so growth mutations (MERGE, ARRAPPEND, nested
    /// SET, ...) can reject a result that pushes the stored document past the
    /// caps. Depth is a cheap tape walk; size uses the serialized byte length so
    /// `max-size` means "max serialized size of any stored document", matching
    /// the parse-time check on input. The error variants
    /// ([`JsonError::SizeExceeded`] / [`JsonError::DepthExceeded`]) are
    /// byte-identical to the parse-time path so callers surface one error family.
    pub fn validate_limits(&self, limits: &JsonLimits) -> Result<(), JsonError> {
        let size = self.to_bytes().len();
        if size > limits.max_size {
            return Err(JsonError::SizeExceeded(size, limits.max_size));
        }
        let depth = self.root().depth();
        if depth > limits.max_depth {
            return Err(JsonError::DepthExceeded(depth, limits.max_depth));
        }
        Ok(())
    }

    /// Memory held by this document.
    ///
    /// Derived from the tape, so repeated calls on an unchanged document always
    /// agree — there is no cached total to drift.
    pub fn memory_size(&self) -> usize {
        self.tape.byte_len() + mem::size_of::<Self>()
    }

    /// Estimate memory usage of JSON values at a given path (for JSON.DEBUG MEMORY).
    ///
    /// Counts string bytes *per occurrence* ("uninterned"), so a subtree's
    /// figure answers "what would this cost on its own" — which is why
    /// `JSON.DEBUG MEMORY $` can exceed [`Self::memory_size`]: the tape's own
    /// buffer stores each repeated string once.
    pub fn debug_memory(&self, path: &str) -> Result<Vec<usize>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| self.node(at).subtree_bytes())
            .collect())
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.root().to_json_string().into_bytes()
    }

    /// Serialize to formatted string with custom formatting.
    pub fn to_formatted_string(
        &self,
        indent: Option<&str>,
        newline: Option<&str>,
        space: Option<&str>,
    ) -> String {
        if indent.is_none() && newline.is_none() && space.is_none() {
            // Compact format
            self.root().to_json_string()
        } else {
            // Custom formatting - use our own formatter
            format_node(
                self.root(),
                indent.unwrap_or(""),
                newline.unwrap_or(""),
                space.unwrap_or(""),
                0,
            )
        }
    }

    /// Query values at a JSONPath, returning cursors at the matching subtrees.
    pub fn get(&self, path: &str) -> Result<Vec<TapeRef<'_>>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| self.node(at))
            .collect())
    }

    /// Set a value at a JSONPath.
    ///
    /// Returns true if any value was set.
    pub fn set(
        &mut self,
        path: &str,
        value: JsonData,
        nx: bool,
        xx: bool,
    ) -> Result<bool, JsonError> {
        if path == "$" || path == "." {
            // Setting at root
            if nx && !self.root().is_null() {
                // NX: only set if doesn't exist - root always "exists"
                return Ok(false);
            }
            // XX: only set if exists - root always exists, so nothing to check.
            self.tape = JsonTape::from_value(&value);
            return Ok(true);
        }

        let matches = self.match_offsets(path)?;

        if matches.is_empty() {
            // Path doesn't exist - try to create it if not XX mode
            if xx {
                return Ok(false);
            }
            return self.create_path(path, value);
        }

        if nx {
            // NX mode: path exists, don't set
            return Ok(false);
        }

        let mut edits = EditMap::new();
        for at in matches {
            edits.insert(at, TapeEdit::Replace(value.clone()));
        }
        self.rebuild(&edits);
        Ok(true)
    }

    /// Delete values at a JSONPath.
    ///
    /// Returns the number of values deleted.
    pub fn delete(&mut self, path: &str) -> Result<usize, JsonError> {
        if path == "$" || path == "." {
            return Err(JsonError::RootOperation);
        }

        let matches = self.match_offsets(path)?;
        if matches.is_empty() {
            return Ok(0);
        }
        if matches.contains(&self.root().offset()) {
            return Err(JsonError::RootOperation);
        }

        let deleted = matches.len();
        let edits: EditMap = matches
            .into_iter()
            .map(|at| (at, TapeEdit::Remove))
            .collect();
        self.rebuild(&edits);
        Ok(deleted)
    }

    /// Get the JSON type at a path.
    pub fn type_at(&self, path: &str) -> Result<Vec<JsonType>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| self.node(at).json_type())
            .collect())
    }

    /// Increment a number at a path.
    pub fn num_incr_by(&mut self, path: &str, incr: f64) -> Result<Vec<f64>, JsonError> {
        self.map_numbers(path, |current| current + incr)
    }

    /// Multiply a number at a path.
    pub fn num_mult_by(&mut self, path: &str, mult: f64) -> Result<Vec<f64>, JsonError> {
        self.map_numbers(path, |current| current * mult)
    }

    /// Append a string to a string value at a path.
    pub fn str_append(&mut self, path: &str, append: &str) -> Result<Vec<usize>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let current = self.node(at).as_str().ok_or(JsonError::NotAString)?;
            let mut appended = String::with_capacity(current.len() + append.len());
            appended.push_str(current);
            appended.push_str(append);
            results.push(appended.len());
            edits.insert(at, TapeEdit::Replace(JsonData::String(appended)));
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Get the length of a string at a path.
    pub fn str_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| self.node(at).as_str().map(str::len))
            .collect())
    }

    /// Append values to an array at a path.
    pub fn arr_append(
        &mut self,
        path: &str,
        values: Vec<JsonData>,
    ) -> Result<Vec<usize>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let len = self.array_len(at)?;
            results.push(len + values.len());
            edits.insert(
                at,
                TapeEdit::Insert {
                    index: len,
                    values: values.clone(),
                },
            );
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Find the index of a value in an array at a path.
    pub fn arr_index(
        &self,
        path: &str,
        value: &JsonData,
        start: i64,
        stop: i64,
    ) -> Result<Vec<i64>, JsonError> {
        let mut results = Vec::new();

        for at in self.match_offsets(path)? {
            let node = self.node(at);
            let Some(len) = node.is_array().then(|| node.container_len().unwrap_or(0)) else {
                results.push(-1);
                continue;
            };

            let len = len as i64;
            let start_idx = normalize_array_index(start, len);
            let stop_idx = if stop == 0 {
                len
            } else {
                normalize_array_index(stop, len)
            };

            let mut found = -1i64;
            for (i, element) in node.elements().enumerate() {
                let i = i as i64;
                if i < start_idx {
                    continue;
                }
                if i >= stop_idx.min(len) {
                    break;
                }
                if element.equals_json(value) {
                    found = i;
                    break;
                }
            }
            results.push(found);
        }

        Ok(results)
    }

    /// Insert values into an array at a path at a specific index.
    pub fn arr_insert(
        &mut self,
        path: &str,
        index: i64,
        values: Vec<JsonData>,
    ) -> Result<Vec<usize>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let len = self.array_len(at)?;
            let insert_idx = if index < 0 {
                (len as i64 + index + 1).max(0) as usize
            } else {
                (index as usize).min(len)
            };
            results.push(len + values.len());
            edits.insert(
                at,
                TapeEdit::Insert {
                    index: insert_idx,
                    values: values.clone(),
                },
            );
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Get the length of an array at a path.
    pub fn arr_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| {
                let node = self.node(at);
                node.is_array().then(|| node.container_len().unwrap_or(0))
            })
            .collect())
    }

    /// Pop a value from an array at a path.
    pub fn arr_pop(
        &mut self,
        path: &str,
        index: Option<i64>,
    ) -> Result<Vec<Option<JsonData>>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let node = self.node(at);
            let Some(len) = node.is_array().then(|| node.container_len().unwrap_or(0)) else {
                results.push(None);
                continue;
            };
            if len == 0 {
                results.push(None);
                continue;
            }

            let idx = match index {
                Some(i) => {
                    let normalized = if i < 0 { len as i64 + i } else { i };
                    if normalized < 0 || normalized >= len as i64 {
                        results.push(None);
                        continue;
                    }
                    normalized as usize
                }
                None => len - 1, // Default to last element
            };

            let element = node
                .element(idx)
                .expect("index checked against array length");
            results.push(Some(element.to_json_data()));
            edits.insert(element.offset(), TapeEdit::Remove);
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Trim an array at a path to a range.
    pub fn arr_trim(&mut self, path: &str, start: i64, stop: i64) -> Result<Vec<usize>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let len = self.array_len(at)? as i64;
            let start_idx = normalize_array_index(start, len).max(0) as usize;
            let stop_idx = (normalize_array_index(stop, len) + 1).max(0) as usize;
            let kept = if start_idx >= len as usize || start_idx >= stop_idx {
                0..0
            } else {
                start_idx..stop_idx.min(len as usize)
            };

            results.push(kept.len());
            for (i, element) in self.node(at).elements().enumerate() {
                if !kept.contains(&i) {
                    edits.insert(element.offset(), TapeEdit::Remove);
                }
            }
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Get the keys of an object at a path.
    pub fn obj_keys(&self, path: &str) -> Result<Vec<Option<Vec<String>>>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| {
                let node = self.node(at);
                node.is_object()
                    .then(|| node.members().map(|(k, _)| k.to_string()).collect())
            })
            .collect())
    }

    /// Get the number of keys in an object at a path.
    pub fn obj_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        Ok(self
            .match_offsets(path)?
            .into_iter()
            .map(|at| {
                let node = self.node(at);
                node.is_object().then(|| node.container_len().unwrap_or(0))
            })
            .collect())
    }

    /// Clear a container (array/object) at a path, setting it to empty.
    /// Returns the number of values cleared.
    pub fn clear(&mut self, path: &str) -> Result<usize, JsonError> {
        let matches = self.match_offsets(path)?;
        if matches.is_empty() {
            return Ok(0);
        }

        let mut cleared = 0;
        let mut edits = EditMap::new();
        for at in matches {
            let node = self.node(at);
            let emptied = if node.is_array() {
                node.elements()
                    .next()
                    .is_some()
                    .then(|| JsonData::Array(Vec::new()))
            } else if node.is_object() {
                node.members()
                    .next()
                    .is_some()
                    .then(|| JsonData::Object(Default::default()))
            } else if node.is_number() {
                Some(JsonData::Number(0.into()))
            } else {
                None // Other types not clearable
            };

            if let Some(emptied) = emptied {
                edits.insert(at, TapeEdit::Replace(emptied));
                cleared += 1;
            }
        }

        self.rebuild(&edits);
        Ok(cleared)
    }

    /// Toggle a boolean value at a path.
    pub fn toggle(&mut self, path: &str) -> Result<Vec<bool>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let toggled = !self.node(at).as_bool().ok_or(JsonError::NotABoolean)?;
            results.push(toggled);
            edits.insert(at, TapeEdit::Replace(JsonData::Bool(toggled)));
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Merge a value at a path using RFC 7396 JSON Merge Patch.
    pub fn merge(&mut self, path: &str, patch: JsonData) -> Result<(), JsonError> {
        if path == "$" || path == "." {
            let mut merged = self.to_json_data();
            json_merge_patch(&mut merged, patch);
            self.tape = JsonTape::from_value(&merged);
            return Ok(());
        }

        let matches = self.require_matches(path)?;

        let mut edits = EditMap::new();
        for at in matches {
            let mut merged = self.node(at).to_json_data();
            json_merge_patch(&mut merged, patch.clone());
            edits.insert(at, TapeEdit::Replace(merged));
        }

        self.rebuild(&edits);
        Ok(())
    }

    // -- internals ----------------------------------------------------------

    fn node(&self, at: usize) -> TapeRef<'_> {
        self.tape.node_at(at)
    }

    fn match_offsets(&self, path: &str) -> Result<Vec<usize>, JsonError> {
        path::match_offsets(path, self.root())
    }

    /// Resolve `path`, failing with `PathNotFound` when it matches nothing — the
    /// contract every mutating op except SET, DEL and CLEAR shares.
    fn require_matches(&self, path: &str) -> Result<Vec<usize>, JsonError> {
        let matches = self.match_offsets(path)?;
        if matches.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }
        Ok(matches)
    }

    fn array_len(&self, at: usize) -> Result<usize, JsonError> {
        let node = self.node(at);
        node.is_array()
            .then(|| node.container_len().unwrap_or(0))
            .ok_or(JsonError::NotAnArray)
    }

    /// Apply `op` to every number the path matches, rejecting the whole command
    /// if any match is not a number.
    fn map_numbers(&mut self, path: &str, op: impl Fn(f64) -> f64) -> Result<Vec<f64>, JsonError> {
        let matches = self.require_matches(path)?;

        let mut results = Vec::with_capacity(matches.len());
        let mut edits = EditMap::new();
        for at in matches {
            let current = self.node(at).as_f64().ok_or(JsonError::NotANumber)?;
            let result = op(current);
            results.push(result);

            // An integral result is stored as an integer, matching the number
            // classification JSON.NUMINCRBY/NUMMULTBY have always produced.
            let number = if result.fract() == 0.0
                && result >= i64::MIN as f64
                && result <= i64::MAX as f64
            {
                serde_json::Number::from(result as i64)
            } else {
                serde_json::Number::from_f64(result).ok_or(JsonError::NotANumber)?
            };
            edits.insert(at, TapeEdit::Replace(JsonData::Number(number)));
        }

        self.rebuild(&edits);
        Ok(results)
    }

    /// Create a missing path, then re-encode the document.
    ///
    /// Path *creation* is the one mutation that invents containers along the way
    /// and, on a malformed path, keeps whatever it built before giving up. That
    /// behavior is load-bearing for JSON.SET compatibility, so this branch —
    /// reached only when the path matches nothing — runs the tree-shaped
    /// creation walk on a materialized copy and rebuilds the tape from the
    /// result, partial creations included.
    fn create_path(&mut self, path: &str, value: JsonData) -> Result<bool, JsonError> {
        let mut data = self.to_json_data();
        let created = create_path_in(&mut data, path, value)?;
        self.tape = JsonTape::from_value(&data);
        Ok(created)
    }

    /// Re-encode the document with `edits` applied, in one pass over the tape.
    fn rebuild(&mut self, edits: &EditMap) {
        if edits.is_empty() {
            return;
        }
        // `emit` honors Remove only on a container's *children*; a Remove at
        // the root would be silently ignored while the caller reports a
        // deletion. `delete` guards the root, so this keeps a future caller
        // honest rather than policing a reachable state.
        debug_assert!(
            !matches!(edits.get(&self.root().offset()), Some(TapeEdit::Remove)),
            "a root-level Remove is not representable by emit"
        );
        let mut next = TapeBuilder::new();
        emit(&mut next, self.root(), edits);
        self.tape = next.finish();
    }
}

/// Copy `node` onto `dst`, applying any edits that fall inside its subtree.
fn emit(dst: &mut TapeBuilder, node: TapeRef<'_>, edits: &EditMap) {
    if let Some(TapeEdit::Replace(value)) = edits.get(&node.offset()) {
        dst.append_value(value);
        return;
    }

    if node.is_array() {
        let insert = match edits.get(&node.offset()) {
            Some(TapeEdit::Insert { index, values }) => Some((*index, values)),
            _ => None,
        };
        let at = dst.begin_array();
        let mut spliced = false;
        for (i, element) in node.elements().enumerate() {
            if let Some((index, values)) = insert
                && i == index
            {
                append_all(dst, values);
                spliced = true;
            }
            if matches!(edits.get(&element.offset()), Some(TapeEdit::Remove)) {
                continue;
            }
            emit(dst, element, edits);
        }
        if let Some((_, values)) = insert
            && !spliced
        {
            append_all(dst, values);
        }
        dst.end_container(at);
    } else if node.is_object() {
        let at = dst.begin_object();
        for (key, value) in node.members() {
            if matches!(edits.get(&value.offset()), Some(TapeEdit::Remove)) {
                continue;
            }
            dst.push_key(key);
            emit(dst, value, edits);
        }
        dst.end_container(at);
    } else {
        dst.append_subtree(node);
    }
}

fn append_all(dst: &mut TapeBuilder, values: &[JsonData]) {
    for value in values {
        dst.append_value(value);
    }
}

/// Normalize an array index, handling negative indices.
fn normalize_array_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { (len + idx).max(0) } else { idx }
}

/// Create a path if it doesn't exist.
fn create_path_in(data: &mut JsonData, path: &str, value: JsonData) -> Result<bool, JsonError> {
    let normalized = path::normalize_path(path);
    let patterns = path::parse_path_patterns(&normalized)?;

    if patterns.is_empty() {
        return Ok(false);
    }

    // Navigate as far as we can, then create the rest
    let mut current = data;
    let mut created_any = false;

    for (i, pattern) in patterns.iter().enumerate() {
        match pattern {
            path::PathPattern::Key(key) => {
                if !current.is_object() {
                    return Ok(false);
                }
                let obj = current.as_object_mut().unwrap();
                if !obj.contains_key(key) {
                    // Need to create this key
                    if i == patterns.len() - 1 {
                        // Last segment - insert the value
                        obj.insert(key.clone(), value);
                        return Ok(true);
                    } else {
                        // Create intermediate object or array based on next segment
                        let next_value = match patterns.get(i + 1) {
                            Some(path::PathPattern::Index(_)) => JsonData::Array(vec![]),
                            _ => JsonData::Object(serde_json::Map::new()),
                        };
                        obj.insert(key.clone(), next_value);
                        created_any = true;
                    }
                }
                current = obj.get_mut(key).unwrap();
            }
            path::PathPattern::Index(idx) => {
                if !current.is_array() {
                    return Ok(false);
                }
                let arr = current.as_array_mut().unwrap();
                let actual_idx = if *idx < 0 {
                    (arr.len() as i64 + idx).max(0) as usize
                } else {
                    *idx as usize
                };

                // Extend array if needed
                while arr.len() <= actual_idx {
                    if i == patterns.len() - 1 && arr.len() == actual_idx {
                        arr.push(value);
                        return Ok(true);
                    }
                    arr.push(JsonData::Null);
                    created_any = true;
                }

                if i == patterns.len() - 1 {
                    arr[actual_idx] = value;
                    return Ok(true);
                }

                current = &mut arr[actual_idx];
            }
            path::PathPattern::Wildcard => {
                // Can't create with wildcard
                return Ok(false);
            }
        }
    }

    Ok(created_any)
}

/// Apply RFC 7396 JSON Merge Patch.
fn json_merge_patch(target: &mut JsonData, patch: JsonData) {
    match patch {
        JsonData::Object(patch_obj) => {
            if !target.is_object() {
                *target = JsonData::Object(serde_json::Map::new());
            }
            let target_obj = target.as_object_mut().unwrap();
            for (key, value) in patch_obj {
                if value.is_null() {
                    target_obj.remove(&key);
                } else {
                    let entry = target_obj.entry(key).or_insert(JsonData::Null);
                    json_merge_patch(entry, value);
                }
            }
        }
        _ => {
            *target = patch;
        }
    }
}

/// Format a tape node with JSON.GET's INDENT/NEWLINE/SPACE options.
fn format_node(
    node: TapeRef<'_>,
    indent: &str,
    newline: &str,
    space: &str,
    depth: usize,
) -> String {
    if let Some(s) = node.as_str() {
        return format!("\"{}\"", escape_json_string(s));
    }
    if node.is_array() {
        if node.elements().next().is_none() {
            return "[]".to_string();
        }
        let inner_indent = indent.repeat(depth + 1);
        let outer_indent = indent.repeat(depth);
        let items: Vec<String> = node
            .elements()
            .map(|element| {
                format!(
                    "{}{}",
                    inner_indent,
                    format_node(element, indent, newline, space, depth + 1)
                )
            })
            .collect();
        return format!(
            "[{}{}{}{}]",
            newline,
            items.join(&format!(",{}", newline)),
            newline,
            outer_indent
        );
    }
    if node.is_object() {
        if node.members().next().is_none() {
            return "{}".to_string();
        }
        let inner_indent = indent.repeat(depth + 1);
        let outer_indent = indent.repeat(depth);
        let items: Vec<String> = node
            .members()
            .map(|(key, value)| {
                format!(
                    "{}\"{}\":{}{}",
                    inner_indent,
                    escape_json_string(key),
                    space,
                    format_node(value, indent, newline, space, depth + 1)
                )
            })
            .collect();
        return format!(
            "{{{}{}{}{}}}",
            newline,
            items.join(&format!(",{}", newline)),
            newline,
            outer_indent
        );
    }
    // null, booleans and numbers render the same as in compact output.
    node.to_json_string()
}

/// Escape a string for *formatted* (INDENT/NEWLINE/SPACE) JSON output.
///
/// This deliberately differs from the compact path's `write_escaped` in
/// `tape.rs`: `char::is_control()` also escapes U+007F–U+009F, preserving the
/// pretty printer's historical output byte-for-byte, while `write_escaped`
/// mirrors `serde_json` exactly (C0 only). Reconciling the two is a behavior
/// change to formatted output — do not "fix" either one to match the other
/// in passing.
fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Render the values a path matches, so assertions read as JSON text rather
    /// than as `serde_json` node trees.
    fn get_text(json: &JsonValue, path: &str) -> Vec<String> {
        json.get(path)
            .unwrap()
            .into_iter()
            .map(|node| node.to_json_string())
            .collect()
    }

    #[test]
    fn test_parse_simple_json() {
        let json = JsonValue::parse(br#"{"name": "test", "value": 42}"#).unwrap();
        assert!(json.root().is_object());
    }

    #[test]
    fn test_parse_with_limits() {
        let limits = JsonLimits {
            max_depth: 2,
            max_size: 1000,
        };

        // Should succeed with depth 2
        let json = JsonValue::parse_with_limits(br#"{"a": {"b": 1}}"#, &limits).unwrap();
        assert!(json.root().is_object());

        // Should fail with depth 3
        let result = JsonValue::parse_with_limits(br#"{"a": {"b": {"c": 1}}}"#, &limits);
        assert!(matches!(result, Err(JsonError::DepthExceeded(_, _))));
    }

    #[test]
    fn test_validate_limits() {
        let limits = JsonLimits {
            max_depth: 2,
            max_size: 1000,
        };

        // Within both limits.
        let ok = JsonValue::parse(br#"{"a": {"b": 1}}"#).unwrap();
        assert!(ok.validate_limits(&limits).is_ok());

        // Over depth: same error variant the parse path returns.
        let deep = JsonValue::parse(br#"{"a": {"b": {"c": 1}}}"#).unwrap();
        assert!(matches!(
            deep.validate_limits(&limits),
            Err(JsonError::DepthExceeded(3, 2))
        ));

        // Over size (serialized byte length), depth is fine.
        let tiny = JsonLimits {
            max_depth: 128,
            max_size: 5,
        };
        let big = JsonValue::parse(br#"{"a": 1}"#).unwrap();
        assert!(matches!(
            big.validate_limits(&tiny),
            Err(JsonError::SizeExceeded(_, 5))
        ));
    }

    #[test]
    fn test_get_root() {
        let json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();
        let values = json.get("$").unwrap();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_get_simple_path() {
        let json = JsonValue::parse(br#"{"name": "test", "count": 42}"#).unwrap();
        assert_eq!(get_text(&json, "$.name"), vec![r#""test""#]);
        assert_eq!(get_text(&json, "$.count"), vec!["42"]);
    }

    #[test]
    fn test_get_array_index() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();
        assert_eq!(get_text(&json, "$.items[0]"), vec!["1"]);
        assert_eq!(get_text(&json, "$.items[-1]"), vec!["3"]);
    }

    #[test]
    fn test_get_wildcard() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();
        assert_eq!(get_text(&json, "$.items[*]"), vec!["1", "2", "3"]);
    }

    #[test]
    fn test_set_simple() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();
        json.set(
            "$.name",
            JsonData::String("updated".to_string()),
            false,
            false,
        )
        .unwrap();

        assert_eq!(get_text(&json, "$.name"), vec![r#""updated""#]);
    }

    #[test]
    fn test_set_nx() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();

        // Should not update existing
        let result = json
            .set(
                "$.name",
                JsonData::String("updated".to_string()),
                true,
                false,
            )
            .unwrap();
        assert!(!result);
        assert_eq!(get_text(&json, "$.name"), vec![r#""test""#]);

        // Should set new
        let result = json
            .set("$.new", JsonData::String("value".to_string()), true, false)
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_set_xx() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();

        // Should not set non-existing
        let result = json
            .set("$.new", JsonData::String("value".to_string()), false, true)
            .unwrap();
        assert!(!result);

        // Should update existing
        let result = json
            .set(
                "$.name",
                JsonData::String("updated".to_string()),
                false,
                true,
            )
            .unwrap();
        assert!(result);
    }

    #[test]
    fn test_delete() {
        let mut json = JsonValue::parse(br#"{"name": "test", "count": 42}"#).unwrap();

        let deleted = json.delete("$.name").unwrap();
        assert_eq!(deleted, 1);
        assert!(json.get("$.name").unwrap().is_empty());
        assert_eq!(json.to_bytes(), br#"{"count":42}"#);
    }

    #[test]
    fn test_type_at() {
        let json = JsonValue::parse(
            br#"{"s": "str", "n": 42, "f": 3.14, "b": true, "a": [], "o": {}, "null": null}"#,
        )
        .unwrap();

        assert_eq!(json.type_at("$.s").unwrap(), vec![JsonType::String]);
        assert_eq!(json.type_at("$.n").unwrap(), vec![JsonType::Integer]);
        assert_eq!(json.type_at("$.f").unwrap(), vec![JsonType::Number]);
        assert_eq!(json.type_at("$.b").unwrap(), vec![JsonType::Boolean]);
        assert_eq!(json.type_at("$.a").unwrap(), vec![JsonType::Array]);
        assert_eq!(json.type_at("$.o").unwrap(), vec![JsonType::Object]);
        assert_eq!(json.type_at("$.null").unwrap(), vec![JsonType::Null]);
    }

    #[test]
    fn test_num_incr_by() {
        let mut json = JsonValue::parse(br#"{"count": 10}"#).unwrap();
        let results = json.num_incr_by("$.count", 5.0).unwrap();
        assert_eq!(results, vec![15.0]);
        assert_eq!(get_text(&json, "$.count"), vec!["15"]);
    }

    #[test]
    fn test_num_mult_by() {
        let mut json = JsonValue::parse(br#"{"count": 10}"#).unwrap();
        let results = json.num_mult_by("$.count", 2.0).unwrap();
        assert_eq!(results, vec![20.0]);
    }

    #[test]
    fn test_str_append() {
        let mut json = JsonValue::parse(br#"{"name": "hello"}"#).unwrap();
        let results = json.str_append("$.name", " world").unwrap();
        assert_eq!(results, vec![11]);
        assert_eq!(get_text(&json, "$.name"), vec![r#""hello world""#]);
    }

    #[test]
    fn test_str_len() {
        let json = JsonValue::parse(br#"{"name": "hello"}"#).unwrap();
        let results = json.str_len("$.name").unwrap();
        assert_eq!(results, vec![Some(5)]);
    }

    #[test]
    fn test_arr_append() {
        let mut json = JsonValue::parse(br#"{"items": [1, 2]}"#).unwrap();
        let results = json
            .arr_append(
                "$.items",
                vec![JsonData::Number(serde_json::Number::from(3))],
            )
            .unwrap();
        assert_eq!(results, vec![3]);
        assert_eq!(get_text(&json, "$.items"), vec!["[1,2,3]"]);
    }

    #[test]
    fn test_arr_index() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3, 2]}"#).unwrap();
        let results = json
            .arr_index(
                "$.items",
                &JsonData::Number(serde_json::Number::from(2)),
                0,
                0,
            )
            .unwrap();
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_arr_insert() {
        let mut json = JsonValue::parse(br#"{"items": [1, 3]}"#).unwrap();
        let results = json
            .arr_insert(
                "$.items",
                1,
                vec![JsonData::Number(serde_json::Number::from(2))],
            )
            .unwrap();
        assert_eq!(results, vec![3]);
        assert_eq!(get_text(&json, "$.items"), vec!["[1,2,3]"]);
    }

    #[test]
    fn test_arr_len() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();
        let results = json.arr_len("$.items").unwrap();
        assert_eq!(results, vec![Some(3)]);
    }

    #[test]
    fn test_arr_pop() {
        let mut json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();
        let results = json.arr_pop("$.items", None).unwrap();
        assert_eq!(
            results,
            vec![Some(JsonData::Number(serde_json::Number::from(3)))]
        );
        assert_eq!(json.arr_len("$.items").unwrap(), vec![Some(2)]);

        // A middle index pops that element and closes the gap.
        let results = json.arr_pop("$.items", Some(0)).unwrap();
        assert_eq!(
            results,
            vec![Some(JsonData::Number(serde_json::Number::from(1)))]
        );
        assert_eq!(get_text(&json, "$.items"), vec!["[2]"]);
    }

    #[test]
    fn test_arr_trim() {
        let mut json = JsonValue::parse(br#"{"items": [0, 1, 2, 3, 4]}"#).unwrap();
        let results = json.arr_trim("$.items", 1, 3).unwrap();
        assert_eq!(results, vec![3]);
        assert_eq!(get_text(&json, "$.items"), vec!["[1,2,3]"]);
    }

    #[test]
    fn test_obj_keys() {
        let json = JsonValue::parse(br#"{"a": 1, "b": 2}"#).unwrap();
        let results = json.obj_keys("$").unwrap();
        assert_eq!(results.len(), 1);
        let keys = results[0].as_ref().unwrap();
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
    }

    #[test]
    fn test_obj_len() {
        let json = JsonValue::parse(br#"{"a": 1, "b": 2, "c": 3}"#).unwrap();
        let results = json.obj_len("$").unwrap();
        assert_eq!(results, vec![Some(3)]);
    }

    #[test]
    fn test_clear() {
        let mut json = JsonValue::parse(br#"{"items": [1, 2, 3], "obj": {"a": 1}}"#).unwrap();

        json.clear("$.items").unwrap();
        assert_eq!(json.arr_len("$.items").unwrap(), vec![Some(0)]);

        json.clear("$.obj").unwrap();
        assert_eq!(json.obj_len("$.obj").unwrap(), vec![Some(0)]);
    }

    #[test]
    fn test_toggle() {
        let mut json = JsonValue::parse(br#"{"flag": true}"#).unwrap();
        let results = json.toggle("$.flag").unwrap();
        assert_eq!(results, vec![false]);

        let results = json.toggle("$.flag").unwrap();
        assert_eq!(results, vec![true]);
    }

    #[test]
    fn test_merge() {
        let mut json = JsonValue::parse(br#"{"a": 1, "b": 2}"#).unwrap();
        let patch: JsonData = serde_json::from_str(r#"{"b": 3, "c": 4}"#).unwrap();
        json.merge("$", patch).unwrap();

        assert_eq!(get_text(&json, "$.b"), vec!["3"]);
        assert_eq!(get_text(&json, "$.c"), vec!["4"]);
    }

    #[test]
    fn test_merge_delete() {
        let mut json = JsonValue::parse(br#"{"a": 1, "b": 2}"#).unwrap();
        let patch: JsonData = serde_json::from_str(r#"{"b": null}"#).unwrap();
        json.merge("$", patch).unwrap();

        assert!(json.get("$.b").unwrap().is_empty());
    }

    #[test]
    fn test_memory_size() {
        let json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();
        assert!(json.memory_size() > 0);
    }

    #[test]
    fn test_to_bytes() {
        let json = JsonValue::parse(br#"{"name":"test"}"#).unwrap();
        let bytes = json.to_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_formatted_output() {
        let json = JsonValue::parse(br#"{"a":1}"#).unwrap();
        let formatted = json.to_formatted_string(Some("  "), Some("\n"), Some(" "));
        assert!(formatted.contains('\n'));
        assert!(formatted.contains("  "));
    }

    #[test]
    fn test_json_type_as_str() {
        assert_eq!(JsonType::Object.as_str(), "object");
        assert_eq!(JsonType::Array.as_str(), "array");
        assert_eq!(JsonType::String.as_str(), "string");
        assert_eq!(JsonType::Integer.as_str(), "integer");
        assert_eq!(JsonType::Number.as_str(), "number");
        assert_eq!(JsonType::Boolean.as_str(), "boolean");
        assert_eq!(JsonType::Null.as_str(), "null");
    }

    /// Every mutation re-encodes the whole document, so `memory_size()` must be
    /// a pure function of the current content — no residue from what the
    /// document used to hold.
    #[test]
    fn memory_size_is_run_stable_across_mutations() {
        let source = br#"{"items":[1,2,3],"name":"frog","nested":{"a":[true,null]}}"#;
        let baseline = JsonValue::parse(source).unwrap();

        let mut mutated = JsonValue::parse(source).unwrap();
        mutated
            .arr_append(
                "$.items",
                vec![JsonData::Number(serde_json::Number::from(4))],
            )
            .unwrap();
        mutated.arr_pop("$.items", None).unwrap();
        mutated.str_append("$.name", "gy").unwrap();
        mutated
            .set("$.name", JsonData::String("frog".into()), false, false)
            .unwrap();

        assert_eq!(mutated.to_bytes(), baseline.to_bytes());
        assert_eq!(mutated.memory_size(), baseline.memory_size());
        assert_eq!(baseline.memory_size(), baseline.memory_size());
    }

    /// The tape's whole reason to exist: a stored document should cost about
    /// what its text costs, not the 3-5x a node-per-value tree costs.
    #[test]
    fn stored_size_stays_near_serialized_size() {
        // A representative ~10KB document: an array of records mixing strings,
        // numbers, booleans and nesting. The 60 records are structurally
        // identical, which is maximally favourable to string interning (each
        // key and repeated tag is stored once) — the measured ratio is not a
        // general bound; a key-diverse document lands materially higher.
        let records: Vec<JsonData> = (0..60)
            .map(|i| {
                serde_json::json!({
                    "id": i,
                    "name": format!("user-{i:04}"),
                    "email": format!("user{i}@example.com"),
                    "active": i % 3 == 0,
                    "score": i as f64 * 1.5,
                    "tags": ["alpha", "beta", "gamma"],
                    "meta": {"region": "us-east-1", "tier": i % 4},
                })
            })
            .collect();
        let document = JsonData::Array(records);

        let serialized = serde_json::to_vec(&document).unwrap();
        assert!(
            (8 * 1024..16 * 1024).contains(&serialized.len()),
            "fixture drifted from ~10KB: {} bytes",
            serialized.len()
        );

        let stored = JsonValue::new(document).memory_size();
        let ratio = stored as f64 / serialized.len() as f64;
        println!(
            "tape stored {stored} bytes for {} serialized bytes (ratio {ratio:.2})",
            serialized.len()
        );
        assert!(
            ratio <= 1.5,
            "stored {stored} bytes for {} serialized bytes (ratio {ratio:.2}) exceeds 1.5x",
            serialized.len()
        );
    }
}

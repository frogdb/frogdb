//! JSON document storage with JSONPath-based access.
//!
//! This module provides JSON document storage compatible with RedisJSON semantics,
//! supporting JSONPath queries and mutations.

use serde_json::Value as JsonData;
use std::mem;
use thiserror::Error;

/// Default maximum JSON document depth.
pub const DEFAULT_JSON_MAX_DEPTH: usize = 128;

/// Default maximum JSON document size in bytes.
pub const DEFAULT_JSON_MAX_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// JSON document value stored in FrogDB.
#[derive(Debug, Clone)]
pub struct JsonValue {
    /// The underlying JSON data.
    data: JsonData,
    /// Cached serialized size for memory tracking.
    cached_size: usize,
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

impl JsonValue {
    /// Create a new JSON value from parsed data.
    pub fn new(data: JsonData) -> Self {
        let cached_size = estimate_json_size(&data);
        Self { data, cached_size }
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

        // Check depth
        let depth = json_depth(&data);
        if depth > limits.max_depth {
            return Err(JsonError::DepthExceeded(depth, limits.max_depth));
        }

        Ok(Self::new(data))
    }

    /// Get the underlying JSON data.
    pub fn data(&self) -> &JsonData {
        &self.data
    }

    /// Get mutable reference to the underlying JSON data.
    pub fn data_mut(&mut self) -> &mut JsonData {
        &mut self.data
    }

    /// Get the memory size estimate.
    pub fn memory_size(&self) -> usize {
        self.cached_size + mem::size_of::<Self>()
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.data).unwrap_or_default()
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
            serde_json::to_string(&self.data).unwrap_or_default()
        } else {
            // Custom formatting - use our own formatter
            format_json_value(
                &self.data,
                indent.unwrap_or(""),
                newline.unwrap_or(""),
                space.unwrap_or(""),
                0,
            )
        }
    }

    /// Query values at a JSONPath, returning matching values.
    pub fn get(&self, path: &str) -> Result<Vec<&JsonData>, JsonError> {
        if path == "$" || path == "." {
            return Ok(vec![&self.data]);
        }

        // Use our own path parsing and navigation
        let paths = extract_concrete_paths(path, &self.data)?;
        let mut refs = Vec::new();
        for p in paths {
            if let Some(v) = navigate_to_path(&self.data, &p) {
                refs.push(v);
            }
        }
        Ok(refs)
    }

    /// Set a value at a JSONPath.
    ///
    /// Returns true if any value was set.
    pub fn set(&mut self, path: &str, value: JsonData, nx: bool, xx: bool) -> Result<bool, JsonError> {
        if path == "$" || path == "." {
            // Setting at root
            if nx && !self.data.is_null() {
                // NX: only set if doesn't exist - root always "exists"
                return Ok(false);
            }
            if xx {
                // XX: only set if exists - root always exists
            }
            self.data = value;
            self.update_cached_size();
            return Ok(true);
        }

        let paths = extract_concrete_paths(path, &self.data)?;

        if paths.is_empty() {
            // Path doesn't exist - try to create it if not XX mode
            if xx {
                return Ok(false);
            }
            // Try to create the path
            if create_path(&mut self.data, path, value.clone())? {
                self.update_cached_size();
                return Ok(true);
            }
            return Ok(false);
        }

        if nx {
            // NX mode: path exists, don't set
            return Ok(false);
        }

        let mut any_set = false;
        for p in paths {
            if set_at_path(&mut self.data, &p, value.clone())? {
                any_set = true;
            }
        }

        if any_set {
            self.update_cached_size();
        }
        Ok(any_set)
    }

    /// Delete values at a JSONPath.
    ///
    /// Returns the number of values deleted.
    pub fn delete(&mut self, path: &str) -> Result<usize, JsonError> {
        if path == "$" || path == "." {
            return Err(JsonError::RootOperation);
        }

        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Ok(0);
        }

        // Delete in reverse order to handle array indices correctly
        let mut sorted_paths = paths;
        sorted_paths.sort_by(|a, b| b.cmp(a));

        let mut deleted = 0;
        for p in sorted_paths {
            if delete_at_path(&mut self.data, &p)? {
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.update_cached_size();
        }
        Ok(deleted)
    }

    /// Get the JSON type at a path.
    pub fn type_at(&self, path: &str) -> Result<Vec<JsonType>, JsonError> {
        let values = self.get(path)?;
        Ok(values.iter().map(|v| get_json_type(v)).collect())
    }

    /// Increment a number at a path.
    pub fn num_incr_by(&mut self, path: &str, incr: f64) -> Result<Vec<f64>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let value = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            let new_val = match value {
                JsonData::Number(n) => {
                    let current = n.as_f64().ok_or(JsonError::NotANumber)?;
                    let result = current + incr;
                    results.push(result);
                    if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
                        JsonData::Number(serde_json::Number::from(result as i64))
                    } else {
                        JsonData::Number(serde_json::Number::from_f64(result).ok_or(JsonError::NotANumber)?)
                    }
                }
                _ => return Err(JsonError::NotANumber),
            };
            *value = new_val;
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Multiply a number at a path.
    pub fn num_mult_by(&mut self, path: &str, mult: f64) -> Result<Vec<f64>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let value = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            let new_val = match value {
                JsonData::Number(n) => {
                    let current = n.as_f64().ok_or(JsonError::NotANumber)?;
                    let result = current * mult;
                    results.push(result);
                    if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
                        JsonData::Number(serde_json::Number::from(result as i64))
                    } else {
                        JsonData::Number(serde_json::Number::from_f64(result).ok_or(JsonError::NotANumber)?)
                    }
                }
                _ => return Err(JsonError::NotANumber),
            };
            *value = new_val;
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Append a string to a string value at a path.
    pub fn str_append(&mut self, path: &str, append: &str) -> Result<Vec<usize>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let value = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match value {
                JsonData::String(s) => {
                    s.push_str(append);
                    results.push(s.len());
                }
                _ => return Err(JsonError::NotAString),
            }
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Get the length of a string at a path.
    pub fn str_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        let values = self.get(path)?;
        Ok(values
            .iter()
            .map(|v| match v {
                JsonData::String(s) => Some(s.len()),
                _ => None,
            })
            .collect())
    }

    /// Append values to an array at a path.
    pub fn arr_append(&mut self, path: &str, values: Vec<JsonData>) -> Result<Vec<usize>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Array(arr) => {
                    arr.extend(values.clone());
                    results.push(arr.len());
                }
                _ => return Err(JsonError::NotAnArray),
            }
        }

        self.update_cached_size();
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
        let values = self.get(path)?;
        let mut results = Vec::new();

        for v in values {
            match v {
                JsonData::Array(arr) => {
                    let len = arr.len() as i64;
                    let start_idx = normalize_array_index(start, len);
                    let stop_idx = if stop == 0 { len } else { normalize_array_index(stop, len) };

                    let mut found = -1i64;
                    for i in start_idx..stop_idx.min(len) {
                        if i >= 0 && (i as usize) < arr.len() && &arr[i as usize] == value {
                            found = i;
                            break;
                        }
                    }
                    results.push(found);
                }
                _ => results.push(-1),
            }
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
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Array(arr) => {
                    let len = arr.len() as i64;
                    let insert_idx = if index < 0 {
                        (len + index + 1).max(0) as usize
                    } else {
                        (index as usize).min(arr.len())
                    };

                    // Insert in reverse order to maintain order
                    for (i, v) in values.iter().enumerate() {
                        arr.insert(insert_idx + i, v.clone());
                    }
                    results.push(arr.len());
                }
                _ => return Err(JsonError::NotAnArray),
            }
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Get the length of an array at a path.
    pub fn arr_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        let values = self.get(path)?;
        Ok(values
            .iter()
            .map(|v| match v {
                JsonData::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .collect())
    }

    /// Pop a value from an array at a path.
    pub fn arr_pop(&mut self, path: &str, index: Option<i64>) -> Result<Vec<Option<JsonData>>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Array(arr) => {
                    if arr.is_empty() {
                        results.push(None);
                        continue;
                    }

                    let idx = match index {
                        Some(i) => {
                            let len = arr.len() as i64;
                            let normalized = if i < 0 { len + i } else { i };
                            if normalized < 0 || normalized >= len {
                                results.push(None);
                                continue;
                            }
                            normalized as usize
                        }
                        None => arr.len() - 1, // Default to last element
                    };

                    if idx < arr.len() {
                        results.push(Some(arr.remove(idx)));
                    } else {
                        results.push(None);
                    }
                }
                _ => results.push(None),
            }
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Trim an array at a path to a range.
    pub fn arr_trim(&mut self, path: &str, start: i64, stop: i64) -> Result<Vec<usize>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Array(arr) => {
                    let len = arr.len() as i64;
                    let start_idx = normalize_array_index(start, len).max(0) as usize;
                    let stop_idx = (normalize_array_index(stop, len) + 1).max(0) as usize;

                    if start_idx >= arr.len() || start_idx >= stop_idx {
                        arr.clear();
                    } else {
                        let stop_idx = stop_idx.min(arr.len());
                        *arr = arr[start_idx..stop_idx].to_vec();
                    }
                    results.push(arr.len());
                }
                _ => return Err(JsonError::NotAnArray),
            }
        }

        self.update_cached_size();
        Ok(results)
    }

    /// Get the keys of an object at a path.
    pub fn obj_keys(&self, path: &str) -> Result<Vec<Option<Vec<String>>>, JsonError> {
        let values = self.get(path)?;
        Ok(values
            .iter()
            .map(|v| match v {
                JsonData::Object(obj) => Some(obj.keys().cloned().collect()),
                _ => None,
            })
            .collect())
    }

    /// Get the number of keys in an object at a path.
    pub fn obj_len(&self, path: &str) -> Result<Vec<Option<usize>>, JsonError> {
        let values = self.get(path)?;
        Ok(values
            .iter()
            .map(|v| match v {
                JsonData::Object(obj) => Some(obj.len()),
                _ => None,
            })
            .collect())
    }

    /// Clear a container (array/object) at a path, setting it to empty.
    /// Returns the number of values cleared.
    pub fn clear(&mut self, path: &str) -> Result<usize, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Ok(0);
        }

        let mut cleared = 0;
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Array(arr) => {
                    if !arr.is_empty() {
                        arr.clear();
                        cleared += 1;
                    }
                }
                JsonData::Object(obj) => {
                    if !obj.is_empty() {
                        obj.clear();
                        cleared += 1;
                    }
                }
                JsonData::Number(_) => {
                    *target = JsonData::Number(serde_json::Number::from(0));
                    cleared += 1;
                }
                _ => {} // Other types not clearable
            }
        }

        if cleared > 0 {
            self.update_cached_size();
        }
        Ok(cleared)
    }

    /// Toggle a boolean value at a path.
    pub fn toggle(&mut self, path: &str) -> Result<Vec<bool>, JsonError> {
        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        let mut results = Vec::new();
        for p in &paths {
            let target = navigate_to_path_mut(&mut self.data, p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;

            match target {
                JsonData::Bool(b) => {
                    *b = !*b;
                    results.push(*b);
                }
                _ => return Err(JsonError::NotABoolean),
            }
        }

        Ok(results)
    }

    /// Merge a value at a path using RFC 7396 JSON Merge Patch.
    pub fn merge(&mut self, path: &str, patch: JsonData) -> Result<(), JsonError> {
        if path == "$" || path == "." {
            json_merge_patch(&mut self.data, patch);
            self.update_cached_size();
            return Ok(());
        }

        let paths = extract_concrete_paths(path, &self.data)?;
        if paths.is_empty() {
            return Err(JsonError::PathNotFound(path.to_string()));
        }

        for p in paths {
            let target = navigate_to_path_mut(&mut self.data, &p)
                .ok_or_else(|| JsonError::PathNotFound(path.to_string()))?;
            json_merge_patch(target, patch.clone());
        }

        self.update_cached_size();
        Ok(())
    }

    /// Update the cached size.
    fn update_cached_size(&mut self) {
        self.cached_size = estimate_json_size(&self.data);
    }
}

/// Estimate the memory size of a JSON value.
fn estimate_json_size(data: &JsonData) -> usize {
    match data {
        JsonData::Null => 1,
        JsonData::Bool(_) => 1,
        JsonData::Number(_) => 8,
        JsonData::String(s) => s.len() + mem::size_of::<String>(),
        JsonData::Array(arr) => {
            mem::size_of::<Vec<JsonData>>()
                + arr.iter().map(estimate_json_size).sum::<usize>()
        }
        JsonData::Object(obj) => {
            mem::size_of::<serde_json::Map<String, JsonData>>()
                + obj
                    .iter()
                    .map(|(k, v)| k.len() + mem::size_of::<String>() + estimate_json_size(v))
                    .sum::<usize>()
        }
    }
}

/// Calculate the depth of a JSON value.
fn json_depth(data: &JsonData) -> usize {
    match data {
        JsonData::Array(arr) => 1 + arr.iter().map(json_depth).max().unwrap_or(0),
        JsonData::Object(obj) => 1 + obj.values().map(json_depth).max().unwrap_or(0),
        _ => 0, // Primitives don't add to depth
    }
}

/// Get the JSON type of a value.
fn get_json_type(data: &JsonData) -> JsonType {
    match data {
        JsonData::Null => JsonType::Null,
        JsonData::Bool(_) => JsonType::Boolean,
        JsonData::Number(n) => {
            if n.is_i64() || n.is_u64() {
                JsonType::Integer
            } else {
                JsonType::Number
            }
        }
        JsonData::String(_) => JsonType::String,
        JsonData::Array(_) => JsonType::Array,
        JsonData::Object(_) => JsonType::Object,
    }
}

/// Normalize a JSONPath to standard format.
fn normalize_path(path: &str) -> String {
    let path = path.trim();
    if path.is_empty() || path == "$" || path == "." {
        return "$".to_string();
    }

    // Handle legacy dot notation
    if path.starts_with('.') && !path.starts_with("..") {
        // Convert .foo.bar to $.foo.bar
        return format!("${}", path);
    }

    if !path.starts_with('$') {
        return format!("$.{}", path);
    }

    path.to_string()
}

/// Path segment for navigation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PathSegment {
    Key(String),
    Index(usize),
}

/// Extract concrete paths that match a JSONPath pattern.
fn extract_concrete_paths(path: &str, data: &JsonData) -> Result<Vec<Vec<PathSegment>>, JsonError> {
    let normalized = normalize_path(path);

    if normalized == "$" {
        return Ok(vec![vec![]]);
    }

    // Parse the path and expand wildcards
    let segments = parse_path_segments(&normalized)?;
    expand_path_segments(&segments, data, &[])
}

/// Parse a JSONPath into segments.
fn parse_path_segments(path: &str) -> Result<Vec<PathPattern>, JsonError> {
    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();

    // Skip the leading $
    if chars.peek() == Some(&'$') {
        chars.next();
    }

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                if chars.peek() == Some(&'.') {
                    // Recursive descent - not fully supported, treat as error for now
                    return Err(JsonError::InvalidPath("recursive descent not supported".to_string()));
                }
                // Read key name
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(chars.next().unwrap());
                }
                if key == "*" {
                    segments.push(PathPattern::Wildcard);
                } else if !key.is_empty() {
                    segments.push(PathPattern::Key(key));
                }
            }
            '[' => {
                let mut bracket_content = String::new();
                let mut depth = 1;
                while let Some(c) = chars.next() {
                    if c == '[' {
                        depth += 1;
                    } else if c == ']' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    bracket_content.push(c);
                }

                let content = bracket_content.trim();
                if content == "*" {
                    segments.push(PathPattern::Wildcard);
                } else if content.starts_with('\'') || content.starts_with('"') {
                    // String key
                    let key = content.trim_matches(|c| c == '\'' || c == '"');
                    segments.push(PathPattern::Key(key.to_string()));
                } else if let Ok(idx) = content.parse::<i64>() {
                    segments.push(PathPattern::Index(idx));
                } else {
                    return Err(JsonError::InvalidPath(format!("invalid bracket content: {}", content)));
                }
            }
            _ => {
                return Err(JsonError::InvalidPath(format!("unexpected character: {}", c)));
            }
        }
    }

    Ok(segments)
}

/// Pattern segment for path parsing.
#[derive(Debug, Clone)]
enum PathPattern {
    Key(String),
    Index(i64),
    Wildcard,
}

/// Expand path patterns to concrete paths.
fn expand_path_segments(
    patterns: &[PathPattern],
    data: &JsonData,
    current_path: &[PathSegment],
) -> Result<Vec<Vec<PathSegment>>, JsonError> {
    if patterns.is_empty() {
        return Ok(vec![current_path.to_vec()]);
    }

    let pattern = &patterns[0];
    let rest = &patterns[1..];

    match pattern {
        PathPattern::Key(key) => {
            if let JsonData::Object(obj) = data {
                if let Some(value) = obj.get(key) {
                    let mut new_path = current_path.to_vec();
                    new_path.push(PathSegment::Key(key.clone()));
                    return expand_path_segments(rest, value, &new_path);
                }
            }
            Ok(vec![])
        }
        PathPattern::Index(idx) => {
            if let JsonData::Array(arr) = data {
                let len = arr.len() as i64;
                let actual_idx = if *idx < 0 { len + idx } else { *idx };
                if actual_idx >= 0 && actual_idx < len {
                    let mut new_path = current_path.to_vec();
                    new_path.push(PathSegment::Index(actual_idx as usize));
                    return expand_path_segments(rest, &arr[actual_idx as usize], &new_path);
                }
            }
            Ok(vec![])
        }
        PathPattern::Wildcard => {
            let mut all_paths = Vec::new();
            match data {
                JsonData::Object(obj) => {
                    for (key, value) in obj.iter() {
                        let mut new_path = current_path.to_vec();
                        new_path.push(PathSegment::Key(key.clone()));
                        let expanded = expand_path_segments(rest, value, &new_path)?;
                        all_paths.extend(expanded);
                    }
                }
                JsonData::Array(arr) => {
                    for (idx, value) in arr.iter().enumerate() {
                        let mut new_path = current_path.to_vec();
                        new_path.push(PathSegment::Index(idx));
                        let expanded = expand_path_segments(rest, value, &new_path)?;
                        all_paths.extend(expanded);
                    }
                }
                _ => {}
            }
            Ok(all_paths)
        }
    }
}

/// Navigate to a path in the JSON data.
fn navigate_to_path<'a>(data: &'a JsonData, path: &[PathSegment]) -> Option<&'a JsonData> {
    let mut current = data;
    for segment in path {
        match segment {
            PathSegment::Key(key) => {
                current = current.as_object()?.get(key)?;
            }
            PathSegment::Index(idx) => {
                current = current.as_array()?.get(*idx)?;
            }
        }
    }
    Some(current)
}

/// Navigate to a path in the JSON data mutably.
fn navigate_to_path_mut<'a>(data: &'a mut JsonData, path: &[PathSegment]) -> Option<&'a mut JsonData> {
    let mut current = data;
    for segment in path {
        match segment {
            PathSegment::Key(key) => {
                current = current.as_object_mut()?.get_mut(key)?;
            }
            PathSegment::Index(idx) => {
                current = current.as_array_mut()?.get_mut(*idx)?;
            }
        }
    }
    Some(current)
}

/// Set a value at a concrete path.
fn set_at_path(data: &mut JsonData, path: &[PathSegment], value: JsonData) -> Result<bool, JsonError> {
    if path.is_empty() {
        *data = value;
        return Ok(true);
    }

    let parent_path = &path[..path.len() - 1];
    let last_segment = &path[path.len() - 1];

    let parent = navigate_to_path_mut(data, parent_path)
        .ok_or_else(|| JsonError::PathNotFound("parent path not found".to_string()))?;

    match last_segment {
        PathSegment::Key(key) => {
            if let JsonData::Object(obj) = parent {
                obj.insert(key.clone(), value);
                Ok(true)
            } else {
                Err(JsonError::NotAnObject)
            }
        }
        PathSegment::Index(idx) => {
            if let JsonData::Array(arr) = parent {
                if *idx < arr.len() {
                    arr[*idx] = value;
                    Ok(true)
                } else {
                    Err(JsonError::IndexOutOfRange(*idx as i64))
                }
            } else {
                Err(JsonError::NotAnArray)
            }
        }
    }
}

/// Delete a value at a concrete path.
fn delete_at_path(data: &mut JsonData, path: &[PathSegment]) -> Result<bool, JsonError> {
    if path.is_empty() {
        return Err(JsonError::RootOperation);
    }

    let parent_path = &path[..path.len() - 1];
    let last_segment = &path[path.len() - 1];

    let parent = navigate_to_path_mut(data, parent_path)
        .ok_or_else(|| JsonError::PathNotFound("parent path not found".to_string()))?;

    match last_segment {
        PathSegment::Key(key) => {
            if let JsonData::Object(obj) = parent {
                Ok(obj.remove(key).is_some())
            } else {
                Ok(false)
            }
        }
        PathSegment::Index(idx) => {
            if let JsonData::Array(arr) = parent {
                if *idx < arr.len() {
                    arr.remove(*idx);
                    Ok(true)
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        }
    }
}

/// Create a path if it doesn't exist.
fn create_path(data: &mut JsonData, path: &str, value: JsonData) -> Result<bool, JsonError> {
    let normalized = normalize_path(path);
    let segments = parse_path_segments(&normalized)?;

    if segments.is_empty() {
        return Ok(false);
    }

    // Navigate as far as we can, then create the rest
    let mut current = data;
    let mut created_any = false;

    for (i, segment) in segments.iter().enumerate() {
        match segment {
            PathPattern::Key(key) => {
                if !current.is_object() {
                    return Ok(false);
                }
                let obj = current.as_object_mut().unwrap();
                if !obj.contains_key(key) {
                    // Need to create this key
                    if i == segments.len() - 1 {
                        // Last segment - insert the value
                        obj.insert(key.clone(), value);
                        return Ok(true);
                    } else {
                        // Create intermediate object or array based on next segment
                        let next_value = match segments.get(i + 1) {
                            Some(PathPattern::Index(_)) => JsonData::Array(vec![]),
                            _ => JsonData::Object(serde_json::Map::new()),
                        };
                        obj.insert(key.clone(), next_value);
                        created_any = true;
                    }
                }
                current = obj.get_mut(key).unwrap();
            }
            PathPattern::Index(idx) => {
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
                    if i == segments.len() - 1 && arr.len() == actual_idx {
                        arr.push(value);
                        return Ok(true);
                    }
                    arr.push(JsonData::Null);
                    created_any = true;
                }

                if i == segments.len() - 1 {
                    arr[actual_idx] = value;
                    return Ok(true);
                }

                current = &mut arr[actual_idx];
            }
            PathPattern::Wildcard => {
                // Can't create with wildcard
                return Ok(false);
            }
        }
    }

    Ok(created_any)
}

/// Normalize an array index, handling negative indices.
fn normalize_array_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx
    }
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

/// Format a JSON value with custom formatting.
fn format_json_value(
    data: &JsonData,
    indent: &str,
    newline: &str,
    space: &str,
    depth: usize,
) -> String {
    match data {
        JsonData::Null => "null".to_string(),
        JsonData::Bool(b) => b.to_string(),
        JsonData::Number(n) => n.to_string(),
        JsonData::String(s) => format!("\"{}\"", escape_json_string(s)),
        JsonData::Array(arr) => {
            if arr.is_empty() {
                "[]".to_string()
            } else {
                let inner_indent = indent.repeat(depth + 1);
                let outer_indent = indent.repeat(depth);
                let items: Vec<String> = arr
                    .iter()
                    .map(|v| {
                        format!(
                            "{}{}",
                            inner_indent,
                            format_json_value(v, indent, newline, space, depth + 1)
                        )
                    })
                    .collect();
                format!(
                    "[{}{}{}{}]",
                    newline,
                    items.join(&format!(",{}", newline)),
                    newline,
                    outer_indent
                )
            }
        }
        JsonData::Object(obj) => {
            if obj.is_empty() {
                "{}".to_string()
            } else {
                let inner_indent = indent.repeat(depth + 1);
                let outer_indent = indent.repeat(depth);
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}\"{}\":{}{}",
                            inner_indent,
                            escape_json_string(k),
                            space,
                            format_json_value(v, indent, newline, space, depth + 1)
                        )
                    })
                    .collect();
                format!(
                    "{{{}{}{}{}}}",
                    newline,
                    items.join(&format!(",{}", newline)),
                    newline,
                    outer_indent
                )
            }
        }
    }
}

/// Escape a string for JSON output.
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

    #[test]
    fn test_parse_simple_json() {
        let json = JsonValue::parse(br#"{"name": "test", "value": 42}"#).unwrap();
        assert!(json.data().is_object());
    }

    #[test]
    fn test_parse_with_limits() {
        let limits = JsonLimits {
            max_depth: 2,
            max_size: 1000,
        };

        // Should succeed with depth 2
        let json = JsonValue::parse_with_limits(br#"{"a": {"b": 1}}"#, &limits).unwrap();
        assert!(json.data().is_object());

        // Should fail with depth 3
        let result = JsonValue::parse_with_limits(br#"{"a": {"b": {"c": 1}}}"#, &limits);
        assert!(matches!(result, Err(JsonError::DepthExceeded(_, _))));
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

        let values = json.get("$.name").unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], &JsonData::String("test".to_string()));

        let values = json.get("$.count").unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(42)));
    }

    #[test]
    fn test_get_array_index() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();

        let values = json.get("$.items[0]").unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(1)));

        let values = json.get("$.items[-1]").unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(3)));
    }

    #[test]
    fn test_get_wildcard() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3]}"#).unwrap();
        let values = json.get("$.items[*]").unwrap();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_set_simple() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();
        json.set("$.name", JsonData::String("updated".to_string()), false, false).unwrap();

        let values = json.get("$.name").unwrap();
        assert_eq!(values[0], &JsonData::String("updated".to_string()));
    }

    #[test]
    fn test_set_nx() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();

        // Should not update existing
        let result = json.set("$.name", JsonData::String("updated".to_string()), true, false).unwrap();
        assert!(!result);

        let values = json.get("$.name").unwrap();
        assert_eq!(values[0], &JsonData::String("test".to_string()));

        // Should set new
        let result = json.set("$.new", JsonData::String("value".to_string()), true, false).unwrap();
        assert!(result);
    }

    #[test]
    fn test_set_xx() {
        let mut json = JsonValue::parse(br#"{"name": "test"}"#).unwrap();

        // Should not set non-existing
        let result = json.set("$.new", JsonData::String("value".to_string()), false, true).unwrap();
        assert!(!result);

        // Should update existing
        let result = json.set("$.name", JsonData::String("updated".to_string()), false, true).unwrap();
        assert!(result);
    }

    #[test]
    fn test_delete() {
        let mut json = JsonValue::parse(br#"{"name": "test", "count": 42}"#).unwrap();

        let deleted = json.delete("$.name").unwrap();
        assert_eq!(deleted, 1);

        let values = json.get("$.name").unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn test_type_at() {
        let json = JsonValue::parse(br#"{"s": "str", "n": 42, "f": 3.14, "b": true, "a": [], "o": {}, "null": null}"#).unwrap();

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

        let values = json.get("$.count").unwrap();
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(15)));
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

        let values = json.get("$.name").unwrap();
        assert_eq!(values[0], &JsonData::String("hello world".to_string()));
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
        let results = json.arr_append("$.items", vec![JsonData::Number(serde_json::Number::from(3))]).unwrap();
        assert_eq!(results, vec![3]);
    }

    #[test]
    fn test_arr_index() {
        let json = JsonValue::parse(br#"{"items": [1, 2, 3, 2]}"#).unwrap();
        let results = json.arr_index("$.items", &JsonData::Number(serde_json::Number::from(2)), 0, 0).unwrap();
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn test_arr_insert() {
        let mut json = JsonValue::parse(br#"{"items": [1, 3]}"#).unwrap();
        let results = json.arr_insert("$.items", 1, vec![JsonData::Number(serde_json::Number::from(2))]).unwrap();
        assert_eq!(results, vec![3]);

        let values = json.get("$.items").unwrap();
        let arr = values[0].as_array().unwrap();
        assert_eq!(arr[1], JsonData::Number(serde_json::Number::from(2)));
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
        assert_eq!(results, vec![Some(JsonData::Number(serde_json::Number::from(3)))]);

        let results = json.arr_len("$.items").unwrap();
        assert_eq!(results, vec![Some(2)]);
    }

    #[test]
    fn test_arr_trim() {
        let mut json = JsonValue::parse(br#"{"items": [0, 1, 2, 3, 4]}"#).unwrap();
        let results = json.arr_trim("$.items", 1, 3).unwrap();
        assert_eq!(results, vec![3]);

        let values = json.get("$.items").unwrap();
        let arr = values[0].as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], JsonData::Number(serde_json::Number::from(1)));
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
        let results = json.arr_len("$.items").unwrap();
        assert_eq!(results, vec![Some(0)]);

        json.clear("$.obj").unwrap();
        let results = json.obj_len("$.obj").unwrap();
        assert_eq!(results, vec![Some(0)]);
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

        let values = json.get("$.b").unwrap();
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(3)));

        let values = json.get("$.c").unwrap();
        assert_eq!(values[0], &JsonData::Number(serde_json::Number::from(4)));
    }

    #[test]
    fn test_merge_delete() {
        let mut json = JsonValue::parse(br#"{"a": 1, "b": 2}"#).unwrap();
        let patch: JsonData = serde_json::from_str(r#"{"b": null}"#).unwrap();
        json.merge("$", patch).unwrap();

        let values = json.get("$.b").unwrap();
        assert!(values.is_empty());
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
}

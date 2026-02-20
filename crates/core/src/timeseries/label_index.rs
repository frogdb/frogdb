//! Label index for efficient time series FILTER queries.
//!
//! Provides O(1) lookup of time series keys by label-value pairs.
//! Each shard maintains its own label index.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// Filter types for label-based queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelFilter {
    /// label=value - exact match
    Equals(String, String),
    /// label!=value - not equal
    NotEquals(String, String),
    /// label= - label exists (any value)
    Exists(String),
    /// label!= - label does not exist
    NotExists(String),
    /// label=(v1,v2,...) - value in set
    In(String, Vec<String>),
    /// label!=(v1,v2,...) - value not in set
    NotIn(String, Vec<String>),
}

impl LabelFilter {
    /// Parse a filter expression.
    ///
    /// Formats:
    /// - `label=value` - equals
    /// - `label!=value` - not equals
    /// - `label=` - exists
    /// - `label!=` - not exists
    /// - `label=(v1,v2)` - in set
    /// - `label!=(v1,v2)` - not in set
    pub fn parse(expr: &str) -> Option<Self> {
        // Check for != first (before =)
        if let Some(pos) = expr.find("!=") {
            let label = expr[..pos].to_string();
            let value_part = &expr[pos + 2..];

            if value_part.is_empty() {
                return Some(LabelFilter::NotExists(label));
            }

            if value_part.starts_with('(') && value_part.ends_with(')') {
                let inner = &value_part[1..value_part.len() - 1];
                let values: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
                return Some(LabelFilter::NotIn(label, values));
            }

            return Some(LabelFilter::NotEquals(label, value_part.to_string()));
        }

        // Check for =
        if let Some(pos) = expr.find('=') {
            let label = expr[..pos].to_string();
            let value_part = &expr[pos + 1..];

            if value_part.is_empty() {
                return Some(LabelFilter::Exists(label));
            }

            if value_part.starts_with('(') && value_part.ends_with(')') {
                let inner = &value_part[1..value_part.len() - 1];
                let values: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
                return Some(LabelFilter::In(label, values));
            }

            return Some(LabelFilter::Equals(label, value_part.to_string()));
        }

        None
    }
}

/// Index for efficient label-based lookups.
///
/// Structure: label_name -> label_value -> set of keys
#[derive(Debug, Clone, Default)]
pub struct LabelIndex {
    /// Main index: label -> value -> keys
    index: HashMap<String, HashMap<String, HashSet<Bytes>>>,
    /// Reverse index: key -> labels (for efficient removal)
    key_labels: HashMap<Bytes, Vec<(String, String)>>,
}

impl LabelIndex {
    /// Create a new empty label index.
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            key_labels: HashMap::new(),
        }
    }

    /// Add a key with its labels to the index.
    pub fn add(&mut self, key: Bytes, labels: &[(String, String)]) {
        // Remove existing labels for this key if any
        self.remove(&key);

        // Add to forward index
        for (label, value) in labels {
            self.index
                .entry(label.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(key.clone());
        }

        // Add to reverse index
        if !labels.is_empty() {
            self.key_labels.insert(key, labels.to_vec());
        }
    }

    /// Remove a key from the index.
    pub fn remove(&mut self, key: &[u8]) {
        if let Some(labels) = self.key_labels.remove(key) {
            for (label, value) in labels {
                if let Some(values) = self.index.get_mut(&label) {
                    if let Some(keys) = values.get_mut(&value) {
                        keys.remove(key);
                        if keys.is_empty() {
                            values.remove(&value);
                        }
                    }
                    if values.is_empty() {
                        self.index.remove(&label);
                    }
                }
            }
        }
    }

    /// Update labels for a key.
    pub fn update(&mut self, key: Bytes, labels: &[(String, String)]) {
        self.add(key, labels);
    }

    /// Query keys matching all filters (AND logic).
    pub fn query(&self, filters: &[LabelFilter]) -> HashSet<Bytes> {
        if filters.is_empty() {
            // Return all indexed keys
            return self.key_labels.keys().cloned().collect();
        }

        let mut result: Option<HashSet<Bytes>> = None;

        for filter in filters {
            let matching = self.query_single(filter);

            result = match result {
                None => Some(matching),
                Some(current) => Some(current.intersection(&matching).cloned().collect()),
            };

            // Early exit if result is empty
            if result.as_ref().is_some_and(|r| r.is_empty()) {
                return HashSet::new();
            }
        }

        result.unwrap_or_default()
    }

    /// Query keys matching a single filter.
    fn query_single(&self, filter: &LabelFilter) -> HashSet<Bytes> {
        match filter {
            LabelFilter::Equals(label, value) => self
                .index
                .get(label)
                .and_then(|values| values.get(value))
                .cloned()
                .unwrap_or_default(),

            LabelFilter::NotEquals(label, value) => {
                // All keys that have this label but not with this value
                let all_keys: HashSet<Bytes> = self.key_labels.keys().cloned().collect();
                let matching = self
                    .index
                    .get(label)
                    .and_then(|values| values.get(value))
                    .cloned()
                    .unwrap_or_default();
                all_keys.difference(&matching).cloned().collect()
            }

            LabelFilter::Exists(label) => {
                // All keys that have this label (any value)
                self.index
                    .get(label)
                    .map(|values| {
                        values
                            .values()
                            .flat_map(|keys| keys.iter().cloned())
                            .collect()
                    })
                    .unwrap_or_default()
            }

            LabelFilter::NotExists(label) => {
                // All keys that don't have this label
                let all_keys: HashSet<Bytes> = self.key_labels.keys().cloned().collect();
                let has_label: HashSet<Bytes> = self
                    .index
                    .get(label)
                    .map(|values| {
                        values
                            .values()
                            .flat_map(|keys| keys.iter().cloned())
                            .collect()
                    })
                    .unwrap_or_default();
                all_keys.difference(&has_label).cloned().collect()
            }

            LabelFilter::In(label, values) => {
                let mut result = HashSet::new();
                if let Some(label_values) = self.index.get(label) {
                    for value in values {
                        if let Some(keys) = label_values.get(value) {
                            result.extend(keys.iter().cloned());
                        }
                    }
                }
                result
            }

            LabelFilter::NotIn(label, values) => {
                let all_keys: HashSet<Bytes> = self.key_labels.keys().cloned().collect();
                let mut matching = HashSet::new();
                if let Some(label_values) = self.index.get(label) {
                    for value in values {
                        if let Some(keys) = label_values.get(value) {
                            matching.extend(keys.iter().cloned());
                        }
                    }
                }
                all_keys.difference(&matching).cloned().collect()
            }
        }
    }

    /// Get all labels for a key.
    pub fn get_labels(&self, key: &[u8]) -> Option<&Vec<(String, String)>> {
        self.key_labels.get(key)
    }

    /// Get the number of indexed keys.
    pub fn len(&self) -> usize {
        self.key_labels.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.key_labels.is_empty()
    }

    /// Get all unique label names.
    pub fn label_names(&self) -> Vec<&str> {
        self.index.keys().map(|s| s.as_str()).collect()
    }

    /// Get all values for a label.
    pub fn label_values(&self, label: &str) -> Vec<&str> {
        self.index
            .get(label)
            .map(|values| values.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_equals() {
        let filter = LabelFilter::parse("location=kitchen").unwrap();
        assert_eq!(
            filter,
            LabelFilter::Equals("location".to_string(), "kitchen".to_string())
        );
    }

    #[test]
    fn test_parse_filter_not_equals() {
        let filter = LabelFilter::parse("location!=kitchen").unwrap();
        assert_eq!(
            filter,
            LabelFilter::NotEquals("location".to_string(), "kitchen".to_string())
        );
    }

    #[test]
    fn test_parse_filter_exists() {
        let filter = LabelFilter::parse("location=").unwrap();
        assert_eq!(filter, LabelFilter::Exists("location".to_string()));
    }

    #[test]
    fn test_parse_filter_not_exists() {
        let filter = LabelFilter::parse("location!=").unwrap();
        assert_eq!(filter, LabelFilter::NotExists("location".to_string()));
    }

    #[test]
    fn test_parse_filter_in() {
        let filter = LabelFilter::parse("location=(kitchen,bedroom)").unwrap();
        assert_eq!(
            filter,
            LabelFilter::In(
                "location".to_string(),
                vec!["kitchen".to_string(), "bedroom".to_string()]
            )
        );
    }

    #[test]
    fn test_parse_filter_not_in() {
        let filter = LabelFilter::parse("location!=(kitchen,bedroom)").unwrap();
        assert_eq!(
            filter,
            LabelFilter::NotIn(
                "location".to_string(),
                vec!["kitchen".to_string(), "bedroom".to_string()]
            )
        );
    }

    #[test]
    fn test_add_and_query() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("temp:kitchen"),
            &[
                ("location".to_string(), "kitchen".to_string()),
                ("type".to_string(), "temp".to_string()),
            ],
        );
        index.add(
            Bytes::from("temp:bedroom"),
            &[
                ("location".to_string(), "bedroom".to_string()),
                ("type".to_string(), "temp".to_string()),
            ],
        );
        index.add(
            Bytes::from("humidity:kitchen"),
            &[
                ("location".to_string(), "kitchen".to_string()),
                ("type".to_string(), "humidity".to_string()),
            ],
        );

        // Query by location
        let result = index.query(&[LabelFilter::Equals(
            "location".to_string(),
            "kitchen".to_string(),
        )]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Bytes::from("temp:kitchen")));
        assert!(result.contains(&Bytes::from("humidity:kitchen")));

        // Query by type
        let result = index.query(&[LabelFilter::Equals("type".to_string(), "temp".to_string())]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Bytes::from("temp:kitchen")));
        assert!(result.contains(&Bytes::from("temp:bedroom")));

        // Query by both (AND)
        let result = index.query(&[
            LabelFilter::Equals("location".to_string(), "kitchen".to_string()),
            LabelFilter::Equals("type".to_string(), "temp".to_string()),
        ]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Bytes::from("temp:kitchen")));
    }

    #[test]
    fn test_remove() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("key1"),
            &[("label".to_string(), "value".to_string())],
        );
        index.add(
            Bytes::from("key2"),
            &[("label".to_string(), "value".to_string())],
        );

        assert_eq!(index.len(), 2);

        index.remove(b"key1");
        assert_eq!(index.len(), 1);

        let result = index.query(&[LabelFilter::Equals(
            "label".to_string(),
            "value".to_string(),
        )]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Bytes::from("key2")));
    }

    #[test]
    fn test_query_not_equals() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("a"),
            &[("color".to_string(), "red".to_string())],
        );
        index.add(
            Bytes::from("b"),
            &[("color".to_string(), "blue".to_string())],
        );
        index.add(
            Bytes::from("c"),
            &[("color".to_string(), "red".to_string())],
        );

        let result = index.query(&[LabelFilter::NotEquals(
            "color".to_string(),
            "red".to_string(),
        )]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Bytes::from("b")));
    }

    #[test]
    fn test_query_exists() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("a"),
            &[("color".to_string(), "red".to_string())],
        );
        index.add(
            Bytes::from("b"),
            &[("size".to_string(), "large".to_string())],
        );

        let result = index.query(&[LabelFilter::Exists("color".to_string())]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Bytes::from("a")));
    }

    #[test]
    fn test_query_not_exists() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("a"),
            &[("color".to_string(), "red".to_string())],
        );
        index.add(
            Bytes::from("b"),
            &[("size".to_string(), "large".to_string())],
        );

        let result = index.query(&[LabelFilter::NotExists("color".to_string())]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&Bytes::from("b")));
    }

    #[test]
    fn test_query_in() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("a"),
            &[("color".to_string(), "red".to_string())],
        );
        index.add(
            Bytes::from("b"),
            &[("color".to_string(), "blue".to_string())],
        );
        index.add(
            Bytes::from("c"),
            &[("color".to_string(), "green".to_string())],
        );

        let result = index.query(&[LabelFilter::In(
            "color".to_string(),
            vec!["red".to_string(), "blue".to_string()],
        )]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Bytes::from("a")));
        assert!(result.contains(&Bytes::from("b")));
    }

    #[test]
    fn test_update_labels() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("key"),
            &[("color".to_string(), "red".to_string())],
        );

        let result = index.query(&[LabelFilter::Equals("color".to_string(), "red".to_string())]);
        assert!(result.contains(&Bytes::from("key")));

        // Update to new labels
        index.update(
            Bytes::from("key"),
            &[("color".to_string(), "blue".to_string())],
        );

        let result = index.query(&[LabelFilter::Equals("color".to_string(), "red".to_string())]);
        assert!(result.is_empty());

        let result = index.query(&[LabelFilter::Equals("color".to_string(), "blue".to_string())]);
        assert!(result.contains(&Bytes::from("key")));
    }

    #[test]
    fn test_empty_query_returns_all() {
        let mut index = LabelIndex::new();

        index.add(Bytes::from("a"), &[("x".to_string(), "1".to_string())]);
        index.add(Bytes::from("b"), &[("y".to_string(), "2".to_string())]);

        let result = index.query(&[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_label_names_and_values() {
        let mut index = LabelIndex::new();

        index.add(
            Bytes::from("a"),
            &[
                ("color".to_string(), "red".to_string()),
                ("size".to_string(), "large".to_string()),
            ],
        );
        index.add(
            Bytes::from("b"),
            &[("color".to_string(), "blue".to_string())],
        );

        let names = index.label_names();
        assert!(names.contains(&"color"));
        assert!(names.contains(&"size"));

        let values = index.label_values("color");
        assert!(values.contains(&"red"));
        assert!(values.contains(&"blue"));
    }
}

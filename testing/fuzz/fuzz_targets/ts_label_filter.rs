#![no_main]
use bytes::Bytes;
use frogdb_types::timeseries::{LabelFilter, LabelIndex};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
struct LabelFilterInput {
    /// Raw filter expression strings to parse.
    filters: Vec<String>,
    /// Label-value pairs to populate the index with.
    entries: Vec<Vec<(String, String)>>,
}

fuzz_target!(|input: LabelFilterInput| {
    // Cap sizes to keep execution bounded.
    if input.filters.len() > 32 || input.entries.len() > 32 {
        return;
    }

    // Parse all filter expressions — no crash = pass.
    let parsed: Vec<LabelFilter> = input
        .filters
        .iter()
        .filter(|f| f.len() <= 256)
        .filter_map(|f| LabelFilter::parse(f))
        .collect();

    // Build a label index with fuzz entries.
    let mut index = LabelIndex::new();
    for (i, labels) in input.entries.iter().enumerate() {
        if labels.len() > 16 {
            continue;
        }
        // Skip entries with oversized strings.
        if labels.iter().any(|(k, v)| k.len() > 64 || v.len() > 64) {
            continue;
        }
        let key = Bytes::from(format!("key:{i}"));
        index.add(key, labels);
    }

    // Query with each parsed filter individually.
    for filter in &parsed {
        let _ = index.query(std::slice::from_ref(filter));
    }

    // Query with all filters combined (AND logic).
    if !parsed.is_empty() {
        let result = index.query(&parsed);
        // Result size must not exceed total indexed keys.
        assert!(
            result.len() <= index.len(),
            "query returned {} results but index only has {} keys",
            result.len(),
            index.len()
        );
    }
});

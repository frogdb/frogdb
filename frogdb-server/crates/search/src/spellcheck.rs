//! Spell checking for FT.SPELLCHECK command.

use std::collections::HashSet;

use crate::index::ShardSearchIndex;
use crate::schema::FieldType;
use crate::suggest::levenshtein_distance;

/// Run spellcheck on a query string against a search index.
///
/// Returns a list of (misspelled_term, suggestions) where suggestions
/// are (score, suggested_term) pairs sorted by score descending.
pub fn spellcheck_query(
    index: &ShardSearchIndex,
    query: &str,
    max_distance: usize,
    include_dicts: &[&HashSet<String>],
    exclude_dicts: &[&HashSet<String>],
) -> Vec<(String, Vec<(f64, String)>)> {
    // Tokenize query into words (simple whitespace split, lowercase)
    let terms: Vec<String> = query
        .split_whitespace()
        .map(|t| t.to_lowercase())
        .filter(|t| !t.is_empty() && t != "*")
        .collect();

    let searcher = index.reader().searcher();
    let def = index.definition();

    // Collect all TEXT field tantivy field handles
    let text_fields: Vec<tantivy::schema::Field> = def
        .fields
        .iter()
        .filter(|f| matches!(f.field_type, FieldType::Text { .. }))
        .filter_map(|f| index.field_map().get(&f.name).copied())
        .collect();

    let mut results = Vec::new();

    for term in &terms {
        // Check if the term exists in any TEXT field's term dictionary
        let exists = term_exists_in_index(&searcher, &text_fields, term);
        if exists {
            continue; // Term is spelled correctly
        }

        // Collect suggestions from index term dictionaries
        let mut suggestions: Vec<(f64, String)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();

        // Excluded terms
        let excluded: HashSet<&str> = exclude_dicts
            .iter()
            .flat_map(|d| d.iter().map(|s| s.as_str()))
            .collect();

        // Check index term dictionaries
        for &field in &text_fields {
            for segment_reader in searcher.segment_readers() {
                if let Ok(inv_index) = segment_reader.inverted_index(field) {
                    let mut stream = inv_index.terms().stream().unwrap();
                    while let Some((term_bytes, _)) = stream.next() {
                        if let Ok(candidate) = std::str::from_utf8(term_bytes) {
                            let candidate_lower = candidate.to_lowercase();
                            if excluded.contains(candidate_lower.as_str()) {
                                continue;
                            }
                            if seen.contains(&candidate_lower) {
                                continue;
                            }
                            let dist = levenshtein_distance(term, &candidate_lower);
                            if dist > 0 && dist <= max_distance {
                                let score = 1.0 / (1.0 + dist as f64);
                                suggestions.push((score, candidate_lower.clone()));
                                seen.insert(candidate_lower);
                            }
                        }
                    }
                }
            }
        }

        // Check INCLUDE dictionaries
        for dict in include_dicts {
            for candidate in dict.iter() {
                let candidate_lower = candidate.to_lowercase();
                if excluded.contains(candidate_lower.as_str()) {
                    continue;
                }
                if seen.contains(&candidate_lower) {
                    continue;
                }
                let dist = levenshtein_distance(term, &candidate_lower);
                if dist <= max_distance {
                    let score = 1.0 / (1.0 + dist as f64);
                    suggestions.push((score, candidate_lower.clone()));
                    seen.insert(candidate_lower);
                }
            }
        }

        // Sort by score descending
        suggestions.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        if !suggestions.is_empty() {
            results.push((term.clone(), suggestions));
        }
    }

    results
}

/// Check if a term exists in any of the given fields' term dictionaries.
fn term_exists_in_index(
    searcher: &tantivy::Searcher,
    fields: &[tantivy::schema::Field],
    term: &str,
) -> bool {
    for &field in fields {
        for segment_reader in searcher.segment_readers() {
            if let Ok(inv_index) = segment_reader.inverted_index(field) {
                let term_bytes = term.as_bytes();
                if inv_index.terms().get(term_bytes).ok().flatten().is_some() {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spellcheck_basic() {
        // Create a simple in-memory index
        use crate::schema::{FieldDef, IndexSource, SearchIndexDef};

        let def = SearchIndexDef {
            name: "test_idx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "title".to_string(),
                field_type: FieldType::Text { weight: 1.0 },
                sortable: false,
                noindex: false,
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            synonym_groups: std::collections::HashMap::new(),
            version: 1,
            source: IndexSource::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        };

        let mut idx = ShardSearchIndex::open_in_ram(def).unwrap();

        // Index some documents
        idx.index_document("doc1", &[("title".to_string(), "hello world".to_string())]);
        idx.index_document("doc2", &[("title".to_string(), "help wanted".to_string())]);
        idx.index_document("doc3", &[("title".to_string(), "world peace".to_string())]);
        idx.commit().unwrap();

        // Check a misspelled term
        let results = spellcheck_query(&idx, "helo", 1, &[], &[]);
        assert!(!results.is_empty());
        let (term, suggestions) = &results[0];
        assert_eq!(term, "helo");
        // "hello" should be suggested (distance 1)
        assert!(suggestions.iter().any(|(_, s)| s == "hello"));
    }

    #[test]
    fn test_spellcheck_correct_term() {
        use crate::schema::{FieldDef, IndexSource, SearchIndexDef};

        let def = SearchIndexDef {
            name: "test_idx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "title".to_string(),
                field_type: FieldType::Text { weight: 1.0 },
                sortable: false,
                noindex: false,
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            synonym_groups: std::collections::HashMap::new(),
            version: 1,
            source: IndexSource::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        };

        let mut idx = ShardSearchIndex::open_in_ram(def).unwrap();
        idx.index_document("doc1", &[("title".to_string(), "hello world".to_string())]);
        idx.commit().unwrap();

        // Correctly spelled term should return no suggestions
        let results = spellcheck_query(&idx, "hello", 1, &[], &[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_spellcheck_include_dict() {
        use crate::schema::{FieldDef, IndexSource, SearchIndexDef};

        let def = SearchIndexDef {
            name: "test_idx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "title".to_string(),
                field_type: FieldType::Text { weight: 1.0 },
                sortable: false,
                noindex: false,
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            synonym_groups: std::collections::HashMap::new(),
            version: 1,
            source: IndexSource::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        };

        let mut idx = ShardSearchIndex::open_in_ram(def).unwrap();
        idx.index_document("doc1", &[("title".to_string(), "hello".to_string())]);
        idx.commit().unwrap();

        // Add custom dictionary with extra term
        let mut custom_dict = HashSet::new();
        custom_dict.insert("helm".to_string());

        let results = spellcheck_query(&idx, "helo", 1, &[&custom_dict], &[]);
        assert!(!results.is_empty());
        let (_, suggestions) = &results[0];
        // Both "hello" (from index) and "helm" (from dict) should appear
        assert!(suggestions.iter().any(|(_, s)| s == "hello"));
        assert!(suggestions.iter().any(|(_, s)| s == "helm"));
    }

    #[test]
    fn test_spellcheck_exclude_dict() {
        use crate::schema::{FieldDef, IndexSource, SearchIndexDef};

        let def = SearchIndexDef {
            name: "test_idx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "title".to_string(),
                field_type: FieldType::Text { weight: 1.0 },
                sortable: false,
                noindex: false,
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            synonym_groups: std::collections::HashMap::new(),
            version: 1,
            source: IndexSource::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        };

        let mut idx = ShardSearchIndex::open_in_ram(def).unwrap();
        idx.index_document(
            "doc1",
            &[("title".to_string(), "hello world help".to_string())],
        );
        idx.commit().unwrap();

        // Exclude "hello" from suggestions
        let mut exclude = HashSet::new();
        exclude.insert("hello".to_string());

        let results = spellcheck_query(&idx, "helo", 1, &[], &[&exclude]);
        // hello should be excluded but help should still appear if within distance
        if !results.is_empty() {
            let (_, suggestions) = &results[0];
            assert!(!suggestions.iter().any(|(_, s)| s == "hello"));
        }
    }
}

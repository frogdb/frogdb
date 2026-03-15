//! Tantivy index wrapper for per-shard search.

use std::collections::HashMap;
use std::path::Path;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{Field, NumericOptions, STORED, STRING, Schema, TEXT, Value};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

use crate::error::SearchError;
use crate::query::QueryParser;
use crate::schema::{FieldType, SearchIndexDef};

/// A search result entry from a shard.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// The Redis key of the matching document.
    pub key: String,
    /// BM25 score.
    pub score: f32,
    /// Hash fields (field_name -> value).
    pub fields: Vec<(String, String)>,
}

/// Result of a search query including total count and hits.
#[derive(Debug)]
pub struct SearchResult {
    /// Total number of matching documents (before pagination).
    pub total: usize,
    /// The hits within the requested offset/limit window.
    pub hits: Vec<SearchHit>,
}

/// Per-shard tantivy index wrapper.
pub struct ShardSearchIndex {
    /// The index definition.
    pub def: SearchIndexDef,
    /// Tantivy index.
    _index: Index,
    /// Tantivy index writer.
    writer: IndexWriter,
    /// Tantivy index reader.
    reader: IndexReader,
    /// Whether there are uncommitted changes.
    dirty: bool,
    /// Tantivy schema.
    tantivy_schema: Schema,
    /// Map from field name to tantivy Field handle.
    field_map: HashMap<String, Field>,
    /// The special __key field.
    key_field: Field,
}

impl ShardSearchIndex {
    /// Open or create a search index at the given path.
    pub fn open(def: SearchIndexDef, path: &Path) -> Result<Self, SearchError> {
        std::fs::create_dir_all(path)?;
        let (tantivy_schema, field_map, key_field) = build_tantivy_schema(&def);

        let dir = MmapDirectory::open(path)?;
        let index = Index::open_or_create(dir, tantivy_schema.clone())?;

        let writer = index.writer(50_000_000)?; // 50MB heap
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        Ok(Self {
            def,
            _index: index,
            writer,
            reader,
            dirty: false,
            tantivy_schema,
            field_map,
            key_field,
        })
    }

    /// Open a search index using a RAM directory (for testing).
    pub fn open_in_ram(def: SearchIndexDef) -> Result<Self, SearchError> {
        let (tantivy_schema, field_map, key_field) = build_tantivy_schema(&def);
        let index = Index::create_in_ram(tantivy_schema.clone());

        let writer = index.writer(15_000_000)?; // 15MB heap for RAM
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        Ok(Self {
            def,
            _index: index,
            writer,
            reader,
            dirty: false,
            tantivy_schema,
            field_map,
            key_field,
        })
    }

    /// Build a tantivy document from hash field-value pairs.
    pub fn build_document(&self, key: &str, hash_fields: &[(String, String)]) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        doc.add_text(self.key_field, key);

        for (field_name, value) in hash_fields {
            if let Some(&tantivy_field) = self.field_map.get(field_name.as_str())
                && let Some(field_def) = self.def.fields.iter().find(|f| f.name == *field_name)
            {
                match &field_def.field_type {
                    FieldType::Text { .. } | FieldType::Tag { .. } => {
                        doc.add_text(tantivy_field, value);
                    }
                    FieldType::Numeric => {
                        if let Ok(v) = value.parse::<f64>() {
                            doc.add_f64(tantivy_field, v);
                        }
                    }
                }
            }
        }
        doc
    }

    /// Index a document (add or replace).
    pub fn index_document(&mut self, key: &str, hash_fields: &[(String, String)]) {
        // Delete existing document with this key first
        let key_term = tantivy::Term::from_field_text(self.key_field, key);
        self.writer.delete_term(key_term);

        let doc = self.build_document(key, hash_fields);
        let _ = self.writer.add_document(doc);
        self.dirty = true;
    }

    /// Delete a document by key.
    pub fn delete_document(&mut self, key: &str) {
        let key_term = tantivy::Term::from_field_text(self.key_field, key);
        self.writer.delete_term(key_term);
        self.dirty = true;
    }

    /// Commit pending changes to the index.
    pub fn commit(&mut self) -> Result<(), SearchError> {
        if self.dirty {
            self.writer.commit()?;
            self.reader.reload()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Whether there are uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Check if a key matches any of this index's prefixes.
    pub fn matches_prefix(&self, key: &str) -> bool {
        if self.def.prefix.is_empty() {
            return true;
        }
        self.def.prefix.iter().any(|p| key.starts_with(p))
    }

    /// Search the index. Returns hits and total count.
    pub fn search(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
    ) -> Result<SearchResult, SearchError> {
        let parser = QueryParser::new(
            &self.tantivy_schema,
            &self.field_map,
            &self.def,
            self.key_field,
        );
        let tantivy_query = parser.parse(query_str)?;

        let searcher = self.reader.searcher();

        // Get the total count of matching documents
        let total = searcher.search(&tantivy_query, &tantivy::collector::Count)?;

        // Get the paginated results
        let top_docs = searcher.search(
            &tantivy_query,
            &TopDocs::with_limit(offset + limit).and_offset(offset),
        )?;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            // Extract key
            let key = doc
                .get_first(self.key_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Extract all stored fields
            let mut fields = Vec::new();
            for field_def in &self.def.fields {
                if let Some(&tantivy_field) = self.field_map.get(&field_def.name)
                    && let Some(value) = doc.get_first(tantivy_field)
                {
                    let val_str = match &field_def.field_type {
                        FieldType::Text { .. } | FieldType::Tag { .. } => {
                            value.as_str().unwrap_or("").to_string()
                        }
                        FieldType::Numeric => match value.as_f64() {
                            Some(v) => v.to_string(),
                            None => String::new(),
                        },
                    };
                    fields.push((field_def.name.clone(), val_str));
                }
            }

            hits.push(SearchHit { key, score, fields });
        }

        Ok(SearchResult { total, hits })
    }

    /// Get the number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// Destroy the index (remove files).
    pub fn destroy(self, path: &Path) -> Result<(), SearchError> {
        drop(self);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    /// Get the index definition.
    pub fn definition(&self) -> &SearchIndexDef {
        &self.def
    }
}

/// Build a tantivy Schema from our SearchIndexDef.
fn build_tantivy_schema(def: &SearchIndexDef) -> (Schema, HashMap<String, Field>, Field) {
    let mut builder = Schema::builder();

    // Always add __key as STRING | STORED | INDEXED
    let key_field = builder.add_text_field("__key", STRING | STORED);

    let mut field_map = HashMap::new();
    for field_def in &def.fields {
        let field = match &field_def.field_type {
            FieldType::Text { .. } => {
                if field_def.noindex {
                    builder.add_text_field(&field_def.name, STORED)
                } else {
                    builder.add_text_field(&field_def.name, TEXT | STORED)
                }
            }
            FieldType::Tag { .. } => {
                if field_def.noindex {
                    builder.add_text_field(&field_def.name, STORED)
                } else {
                    builder.add_text_field(&field_def.name, STRING | STORED)
                }
            }
            FieldType::Numeric => {
                let opts: NumericOptions = if field_def.noindex {
                    NumericOptions::default().set_stored()
                } else {
                    NumericOptions::default().set_indexed().set_stored()
                };
                builder.add_f64_field(&field_def.name, opts)
            }
        };
        field_map.insert(field_def.name.clone(), field);
    }

    (builder.build(), field_map, key_field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldDef, FieldType, SearchIndexDef};

    fn test_def() -> SearchIndexDef {
        SearchIndexDef {
            name: "test_idx".to_string(),
            prefix: vec!["doc:".to_string()],
            fields: vec![
                FieldDef {
                    name: "title".to_string(),
                    field_type: FieldType::Text { weight: 1.0 },
                    sortable: false,
                    noindex: false,
                },
                FieldDef {
                    name: "tags".to_string(),
                    field_type: FieldType::Tag { separator: ',' },
                    sortable: false,
                    noindex: false,
                },
                FieldDef {
                    name: "price".to_string(),
                    field_type: FieldType::Numeric,
                    sortable: true,
                    noindex: false,
                },
            ],
            version: 1,
        }
    }

    #[test]
    fn test_create_and_search() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document(
            "doc:1",
            &[
                ("title".to_string(), "hello world".to_string()),
                ("tags".to_string(), "redis".to_string()),
                ("price".to_string(), "9.99".to_string()),
            ],
        );
        index.commit().unwrap();

        let result = index.search("hello", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].key, "doc:1");
    }

    #[test]
    fn test_delete_document() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document("doc:1", &[("title".to_string(), "hello world".to_string())]);
        index.commit().unwrap();
        assert_eq!(index.search("hello", 0, 10).unwrap().hits.len(), 1);

        index.delete_document("doc:1");
        index.commit().unwrap();
        assert_eq!(index.search("hello", 0, 10).unwrap().hits.len(), 0);
    }

    #[test]
    fn test_reindex_document() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document("doc:1", &[("title".to_string(), "hello world".to_string())]);
        index.commit().unwrap();

        index.index_document(
            "doc:1",
            &[("title".to_string(), "goodbye world".to_string())],
        );
        index.commit().unwrap();

        assert_eq!(index.search("hello", 0, 10).unwrap().hits.len(), 0);
        assert_eq!(index.search("goodbye", 0, 10).unwrap().hits.len(), 1);
    }

    #[test]
    fn test_prefix_matching() {
        let def = test_def();
        let index = ShardSearchIndex::open_in_ram(def).unwrap();

        assert!(index.matches_prefix("doc:1"));
        assert!(index.matches_prefix("doc:foo"));
        assert!(!index.matches_prefix("user:1"));
    }

    #[test]
    fn test_empty_prefix_matches_all() {
        let def = SearchIndexDef {
            name: "idx".to_string(),
            prefix: vec![],
            fields: vec![FieldDef {
                name: "title".to_string(),
                field_type: FieldType::Text { weight: 1.0 },
                sortable: false,
                noindex: false,
            }],
            version: 1,
        };
        let index = ShardSearchIndex::open_in_ram(def).unwrap();
        assert!(index.matches_prefix("anything"));
    }

    #[test]
    fn test_num_docs() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();
        assert_eq!(index.num_docs(), 0);

        index.index_document("doc:1", &[("title".to_string(), "hello".to_string())]);
        index.index_document("doc:2", &[("title".to_string(), "world".to_string())]);
        index.commit().unwrap();
        assert_eq!(index.num_docs(), 2);
    }

    #[test]
    fn test_search_with_limit() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        for i in 0..5 {
            index.index_document(
                &format!("doc:{}", i),
                &[("title".to_string(), "common term".to_string())],
            );
        }
        index.commit().unwrap();

        let result = index.search("common", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 5);

        let result = index.search("common", 0, 2).unwrap();
        assert_eq!(result.hits.len(), 2);
    }

    #[test]
    fn test_multi_field_document() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document(
            "doc:1",
            &[
                ("title".to_string(), "redis database".to_string()),
                ("tags".to_string(), "fast".to_string()),
                ("price".to_string(), "29.99".to_string()),
            ],
        );
        index.commit().unwrap();

        let result = index.search("redis", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].fields.len(), 3);
    }

    #[test]
    fn test_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_idx");

        let def = test_def();

        {
            let mut index = ShardSearchIndex::open(def.clone(), &path).unwrap();
            index.index_document(
                "doc:1",
                &[("title".to_string(), "persistent data".to_string())],
            );
            index.commit().unwrap();
        }

        {
            let index = ShardSearchIndex::open(def, &path).unwrap();
            let result = index.search("persistent", 0, 10).unwrap();
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].key, "doc:1");
        }
    }
}

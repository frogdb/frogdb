//! Tantivy index wrapper for per-shard search.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, RangeQuery, TermSetQuery};
use tantivy::schema::{
    FAST, Field, NumericOptions, STORED, STRING, Schema, TEXT, TextFieldIndexing, TextOptions,
    Value,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::{
    Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer,
};
use tantivy::{Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument};

use crate::error::SearchError;
use crate::query::{GeoFilter, QueryParser};
use crate::schema::{FieldType, SearchIndexDef, SortOrder, VectorDistanceMetric};

/// A sort value for cross-shard merge-sorting.
#[derive(Debug, Clone)]
pub enum SortValue {
    F64(f64),
    Str(String),
}

/// Options for HIGHLIGHT in FT.SEARCH.
#[derive(Debug, Clone, Default)]
pub struct HighlightOptions {
    /// Fields to highlight (empty = all TEXT fields).
    pub fields: Vec<String>,
    /// Opening tag for highlights (default: "<b>").
    pub open_tag: Option<String>,
    /// Closing tag for highlights (default: "</b>").
    pub close_tag: Option<String>,
}

/// Options for SUMMARIZE in FT.SEARCH.
#[derive(Debug, Clone)]
pub struct SummarizeOptions {
    /// Fields to summarize (empty = all TEXT fields).
    pub fields: Vec<String>,
    /// Maximum number of fragments (default 3).
    pub num_frags: usize,
    /// Fragment length in words (default 20).
    pub frag_len: usize,
    /// Separator between fragments (default "... ").
    pub separator: String,
}

impl Default for SummarizeOptions {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            num_frags: 3,
            frag_len: 20,
            separator: "... ".to_string(),
        }
    }
}

/// Options for the text search portion of a hybrid search.
#[derive(Debug, Clone, Default)]
pub struct HybridTextOptions {
    /// Restrict search to specific fields.
    pub infields: Option<Vec<String>>,
    /// Slop for phrase queries.
    pub slop: Option<u32>,
    /// Whether to skip stemming.
    pub verbatim: bool,
    /// Numeric range filters.
    pub extra_filters: Vec<(String, f64, f64)>,
    /// Geo filters.
    pub extra_geo_filters: Vec<GeoFilter>,
}

/// Extract field values from a JSON document according to a search index definition.
///
/// For each field in the index definition that has a `json_path`, evaluates the
/// JSONPath against the JSON document and converts matching values to strings.
/// Returns `(field_name, string_value)` pairs suitable for `index_document()`.
pub fn extract_json_fields(
    def: &SearchIndexDef,
    json_data: &serde_json::Value,
) -> Vec<(String, String)> {
    use frogdb_types::JsonValue;

    // Wrap in a temporary JsonValue to use the JSONPath evaluator
    let jv = JsonValue::new(json_data.clone());
    let mut fields = Vec::new();

    for field_def in &def.fields {
        let path = match &field_def.json_path {
            Some(p) => p.as_str(),
            None => continue,
        };

        if let Ok(matches) = jv.get(path) {
            for val in matches {
                let s = json_value_to_string(val);
                if !s.is_empty() {
                    fields.push((field_def.name.clone(), s));
                }
            }
        }
    }

    fields
}

/// Convert a serde_json::Value to a string for indexing.
fn json_value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => {
            // For TAG fields: join array elements with commas
            arr.iter()
                .filter_map(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(",")
        }
        serde_json::Value::Null => String::new(),
        serde_json::Value::Object(_) => serde_json::to_string(val).unwrap_or_default(),
    }
}

/// A search result entry from a shard.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// The Redis key of the matching document.
    pub key: String,
    /// BM25 score.
    pub score: f32,
    /// Hash fields (field_name -> value).
    pub fields: Vec<(String, String)>,
    /// Sort value when SORTBY is active (for cross-shard merging).
    pub sort_value: Option<SortValue>,
}

/// Result of a search query including total count and hits.
#[derive(Debug)]
pub struct SearchResult {
    /// Total number of matching documents (before pagination).
    pub total: usize,
    /// The hits within the requested offset/limit window.
    pub hits: Vec<SearchHit>,
}

/// Companion tantivy fields for a GEO field.
pub struct GeoCompanionFields {
    pub hash_field: Field,
    pub lon_field: Field,
    pub lat_field: Field,
}

/// A KNN search result entry.
#[derive(Debug, Clone)]
pub struct KnnHit {
    /// The Redis key of the matching document.
    pub key: String,
    /// Distance from the query vector.
    pub distance: f32,
    /// Hash fields (field_name -> value).
    pub fields: Vec<(String, String)>,
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
    /// Map from field name to companion sort field (__sort_<name>) for sortable TEXT fields.
    sort_field_map: HashMap<String, Field>,
    /// Map from field name to GEO companion fields.
    geo_field_map: HashMap<String, GeoCompanionFields>,
    /// The special __key field.
    key_field: Field,
    /// Path for disk-based indexes (None for RAM indexes).
    path: Option<PathBuf>,
    /// Sidecar usearch indexes for VECTOR fields (field_name -> usearch::Index).
    vector_indexes: HashMap<String, usearch::Index>,
    /// Mapping from usearch key (u64) to Redis key string, for vector fields.
    vector_key_map: HashMap<String, HashMap<u64, String>>,
    /// Next available usearch key ID per vector field.
    vector_next_id: HashMap<String, u64>,
    /// Reverse map: Redis key -> usearch key, for delete/update.
    vector_reverse_map: HashMap<String, HashMap<String, u64>>,
}

impl ShardSearchIndex {
    /// Open or create a search index at the given path.
    pub fn open(def: SearchIndexDef, path: &Path) -> Result<Self, SearchError> {
        std::fs::create_dir_all(path)?;
        let (tantivy_schema, field_map, sort_field_map, geo_field_map, key_field) =
            build_tantivy_schema(&def);

        let dir = MmapDirectory::open(path)?;
        let index = Index::open_or_create(dir, tantivy_schema.clone())?;
        register_custom_tokenizers(&index, &def);

        let writer = index.writer(50_000_000)?; // 50MB heap
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        // Create usearch indexes for VECTOR fields, loading from disk if available
        let (vector_indexes, vector_key_map, vector_next_id, vector_reverse_map) =
            create_vector_indexes(&def, Some(path))?;

        Ok(Self {
            def,
            _index: index,
            writer,
            reader,
            dirty: false,
            tantivy_schema,
            field_map,
            sort_field_map,
            geo_field_map,
            key_field,
            path: Some(path.to_path_buf()),
            vector_indexes,
            vector_key_map,
            vector_next_id,
            vector_reverse_map,
        })
    }

    /// Open a search index using a RAM directory (for testing).
    pub fn open_in_ram(def: SearchIndexDef) -> Result<Self, SearchError> {
        let (tantivy_schema, field_map, sort_field_map, geo_field_map, key_field) =
            build_tantivy_schema(&def);
        let index = Index::create_in_ram(tantivy_schema.clone());
        register_custom_tokenizers(&index, &def);

        let writer = index.writer(15_000_000)?; // 15MB heap for RAM
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let (vector_indexes, vector_key_map, vector_next_id, vector_reverse_map) =
            create_vector_indexes(&def, None)?;

        Ok(Self {
            def,
            _index: index,
            writer,
            reader,
            dirty: false,
            tantivy_schema,
            field_map,
            sort_field_map,
            geo_field_map,
            key_field,
            path: None,
            vector_indexes,
            vector_key_map,
            vector_next_id,
            vector_reverse_map,
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
                    FieldType::Text { .. } => {
                        doc.add_text(tantivy_field, value);
                        // Populate companion sort field for sortable TEXT fields
                        if let Some(&sort_field) = self.sort_field_map.get(field_name.as_str()) {
                            doc.add_text(sort_field, value);
                        }
                    }
                    FieldType::Tag { separator } => {
                        // Split by separator and add each tag as a separate term
                        // so that TermQuery can match individual tag values.
                        for part in value.split(*separator) {
                            let part = part.trim();
                            if part.is_empty() {
                                continue;
                            }
                            if field_def.casesensitive {
                                doc.add_text(tantivy_field, part);
                            } else {
                                doc.add_text(tantivy_field, &part.to_lowercase());
                            }
                        }
                    }
                    FieldType::Numeric => {
                        if let Ok(v) = value.parse::<f64>() {
                            doc.add_f64(tantivy_field, v);
                        }
                    }
                    FieldType::Geo => {
                        // Parse "lon,lat" and populate companion fields
                        if let Some(geo) = self.geo_field_map.get(field_name.as_str())
                            && let Some((lon, lat)) = parse_geo_value(value)
                        {
                            // Store raw value in main field
                            doc.add_text(tantivy_field, value);
                            // Geohash for prefix queries (12 chars = ~3.7cm precision)
                            if let Ok(hash) = geohash::encode(geohash::Coord { x: lon, y: lat }, 12)
                            {
                                doc.add_text(geo.hash_field, &hash);
                            }
                            doc.add_f64(geo.lon_field, lon);
                            doc.add_f64(geo.lat_field, lat);
                        }
                    }
                    FieldType::Vector { .. } => {
                        // Vectors are handled in usearch sidecar, not tantivy.
                        // The raw blob is stored separately via index_vector().
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

        // Index vector fields into usearch sidecar
        for (field_name, value) in hash_fields {
            if let Some(field_def) = self.def.fields.iter().find(|f| f.name == *field_name)
                && matches!(field_def.field_type, FieldType::Vector { .. })
            {
                self.index_vector(field_name, key, value.as_bytes());
            }
        }

        self.dirty = true;
    }

    /// Delete a document by key.
    pub fn delete_document(&mut self, key: &str) {
        let key_term = tantivy::Term::from_field_text(self.key_field, key);
        self.writer.delete_term(key_term);

        // Remove from vector indexes
        self.delete_vector(key);

        self.dirty = true;
    }

    /// Commit pending changes to the index.
    pub fn commit(&mut self) -> Result<(), SearchError> {
        if self.dirty {
            self.writer.commit()?;
            self.reader.reload()?;
            self.save_vectors();
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

    /// Search the index by BM25 score. Returns hits and total count.
    pub fn search(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
    ) -> Result<SearchResult, SearchError> {
        self.search_inner(
            query_str,
            offset,
            limit,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            Vec::new(),
            Vec::new(),
        )
    }

    /// Search the index with SORTBY. Returns hits sorted by the specified field.
    pub fn search_sorted(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
        sort_field: &str,
        sort_order: SortOrder,
    ) -> Result<SearchResult, SearchError> {
        self.search_inner(
            query_str,
            offset,
            limit,
            Some(sort_field),
            Some(sort_order),
            None,
            None,
            None,
            None,
            false,
            None,
            Vec::new(),
            Vec::new(),
        )
    }

    /// Search with optional SORTBY, INFIELDS, HIGHLIGHT, SUMMARIZE, SLOP, VERBATIM,
    /// INKEYS, numeric FILTER, and GEOFILTER options.
    #[allow(clippy::too_many_arguments)]
    pub fn search_with_options(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
        sort_by: Option<(&str, SortOrder)>,
        infields: Option<Vec<String>>,
        highlight: Option<HighlightOptions>,
        slop: Option<u32>,
        summarize: Option<SummarizeOptions>,
        verbatim: bool,
        inkeys: Option<Vec<String>>,
        extra_filters: Vec<(String, f64, f64)>,
        extra_geo_filters: Vec<GeoFilter>,
    ) -> Result<SearchResult, SearchError> {
        self.search_inner(
            query_str,
            offset,
            limit,
            sort_by.map(|(f, _)| f),
            sort_by.map(|(_, o)| o),
            infields,
            highlight,
            slop,
            summarize,
            verbatim,
            inkeys,
            extra_filters,
            extra_geo_filters,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn search_inner(
        &self,
        query_str: &str,
        offset: usize,
        limit: usize,
        sort_field: Option<&str>,
        sort_order: Option<SortOrder>,
        infields: Option<Vec<String>>,
        highlight: Option<HighlightOptions>,
        slop: Option<u32>,
        summarize: Option<SummarizeOptions>,
        verbatim: bool,
        inkeys: Option<Vec<String>>,
        extra_filters: Vec<(String, f64, f64)>,
        extra_geo_filters: Vec<GeoFilter>,
    ) -> Result<SearchResult, SearchError> {
        let mut parser = QueryParser::new(
            &self.tantivy_schema,
            &self.field_map,
            &self.def,
            self.key_field,
        );
        if let Some(inf) = infields {
            parser = parser.with_infields(inf);
        }
        if slop.is_some() {
            parser = parser.with_slop(slop);
        }
        if verbatim {
            parser = parser.with_verbatim(true);
        }
        let (mut tantivy_query, mut geo_filters) = parser.parse_with_geo_filters(query_str)?;

        // Compose INKEYS filter at the tantivy level
        if let Some(keys) = inkeys {
            let inkeys_query = TermSetQuery::new(
                keys.iter()
                    .map(|k| tantivy::Term::from_field_text(self.key_field, k)),
            );
            tantivy_query = Box::new(BooleanQuery::new(vec![
                (Occur::Must, tantivy_query),
                (Occur::Must, Box::new(inkeys_query)),
            ]));
        }

        // Compose extra numeric FILTER clauses at the tantivy level
        for (field_name, min, max) in &extra_filters {
            let range_query = RangeQuery::new_f64_bounds(
                field_name.clone(),
                std::ops::Bound::Included(*min),
                std::ops::Bound::Included(*max),
            );
            tantivy_query = Box::new(BooleanQuery::new(vec![
                (Occur::Must, tantivy_query),
                (Occur::Must, Box::new(range_query)),
            ]));
        }

        // Merge extra GEOFILTER clauses into geo post-filters
        geo_filters.extend(extra_geo_filters);

        let tantivy_query = tantivy_query;
        let searcher = self.reader.searcher();

        let has_geo = !geo_filters.is_empty();

        // When geo-filtering, we must over-fetch (fetch all matches) because
        // geo post-filtering changes the true total and offset/limit math.
        let raw_total = searcher.search(&tantivy_query, &tantivy::collector::Count)?;
        let (fetch_offset, fetch_limit) = if has_geo {
            (0, raw_total.max(1))
        } else {
            (offset, offset + limit)
        };

        // Build snippet generators for highlighted fields
        let snippet_gens = if let Some(ref hl) = highlight {
            self.build_snippet_generators(&searcher, &tantivy_query, hl)
        } else {
            HashMap::new()
        };

        if let Some(sf) = sort_field {
            // Determine which tantivy field name to sort by
            let field_def = self.def.fields.iter().find(|f| f.name == sf);
            let order = match sort_order.unwrap_or(SortOrder::Asc) {
                SortOrder::Asc => Order::Asc,
                SortOrder::Desc => Order::Desc,
            };

            let is_numeric =
                field_def.is_some_and(|fd| matches!(fd.field_type, FieldType::Numeric));

            if is_numeric {
                // Sort by numeric fast field
                let collector = TopDocs::with_limit(fetch_limit)
                    .and_offset(fetch_offset)
                    .order_by_fast_field::<f64>(sf, order);
                let top_docs = searcher.search(&tantivy_query, &collector)?;
                let mut hits = Vec::with_capacity(top_docs.len());
                for (sort_val, doc_address) in top_docs {
                    let doc: TantivyDocument = searcher.doc(doc_address)?;
                    if has_geo && !self.doc_passes_geo_filters(&doc, &geo_filters) {
                        continue;
                    }
                    let (key, fields) = self.extract_hit_fields(&doc);
                    let fields = self.apply_highlights(fields, &doc, &snippet_gens, &highlight);
                    let fields = if let Some(ref summ) = summarize {
                        self.apply_summarize(fields, summ)
                    } else {
                        fields
                    };
                    hits.push(SearchHit {
                        key,
                        score: 0.0,
                        fields,
                        sort_value: Some(SortValue::F64(sort_val)),
                    });
                }
                if has_geo {
                    let geo_total = hits.len();
                    let hits = hits.into_iter().skip(offset).take(limit).collect();
                    return Ok(SearchResult {
                        total: geo_total,
                        hits,
                    });
                }
                Ok(SearchResult {
                    total: raw_total,
                    hits,
                })
            } else {
                // String sort: retrieve by score, extract sort value, sort in Rust
                let top_docs = searcher.search(
                    &tantivy_query,
                    &TopDocs::with_limit(fetch_limit).and_offset(fetch_offset),
                )?;
                let sort_tantivy_field = self
                    .sort_field_map
                    .get(sf)
                    .or_else(|| self.field_map.get(sf))
                    .copied();
                let mut hits = Vec::with_capacity(top_docs.len());
                for (_score, doc_address) in top_docs {
                    let doc: TantivyDocument = searcher.doc(doc_address)?;
                    if has_geo && !self.doc_passes_geo_filters(&doc, &geo_filters) {
                        continue;
                    }
                    let (key, fields) = self.extract_hit_fields(&doc);
                    let fields = self.apply_highlights(fields, &doc, &snippet_gens, &highlight);
                    let fields = if let Some(ref summ) = summarize {
                        self.apply_summarize(fields, summ)
                    } else {
                        fields
                    };
                    let sort_val = sort_tantivy_field
                        .and_then(|f| doc.get_first(f))
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    hits.push(SearchHit {
                        key,
                        score: 0.0,
                        fields,
                        sort_value: Some(SortValue::Str(sort_val)),
                    });
                }
                // Sort in Rust
                hits.sort_by(|a, b| {
                    let va = match &a.sort_value {
                        Some(SortValue::Str(s)) => s.as_str(),
                        _ => "",
                    };
                    let vb = match &b.sort_value {
                        Some(SortValue::Str(s)) => s.as_str(),
                        _ => "",
                    };
                    match order {
                        Order::Asc => va.cmp(vb),
                        Order::Desc => vb.cmp(va),
                    }
                });
                if has_geo {
                    let geo_total = hits.len();
                    let hits = hits.into_iter().skip(offset).take(limit).collect();
                    return Ok(SearchResult {
                        total: geo_total,
                        hits,
                    });
                }
                Ok(SearchResult {
                    total: raw_total,
                    hits,
                })
            }
        } else {
            // Default: sort by BM25 score
            let top_docs = searcher.search(
                &tantivy_query,
                &TopDocs::with_limit(fetch_limit).and_offset(fetch_offset),
            )?;
            let mut hits = Vec::with_capacity(top_docs.len());
            for (score, doc_address) in top_docs {
                let doc: TantivyDocument = searcher.doc(doc_address)?;
                if has_geo && !self.doc_passes_geo_filters(&doc, &geo_filters) {
                    continue;
                }
                let (key, fields) = self.extract_hit_fields(&doc);
                let fields = self.apply_highlights(fields, &doc, &snippet_gens, &highlight);
                let fields = if let Some(ref summ) = summarize {
                    self.apply_summarize(fields, summ)
                } else {
                    fields
                };
                hits.push(SearchHit {
                    key,
                    score,
                    fields,
                    sort_value: None,
                });
            }
            if has_geo {
                let geo_total = hits.len();
                let hits = hits.into_iter().skip(offset).take(limit).collect();
                return Ok(SearchResult {
                    total: geo_total,
                    hits,
                });
            }
            Ok(SearchResult {
                total: raw_total,
                hits,
            })
        }
    }

    /// Build SnippetGenerators for each highlighted TEXT field.
    fn build_snippet_generators(
        &self,
        searcher: &tantivy::Searcher,
        query: &dyn tantivy::query::Query,
        hl: &HighlightOptions,
    ) -> HashMap<String, SnippetGenerator> {
        let mut generators = HashMap::new();
        for field_def in &self.def.fields {
            if !matches!(field_def.field_type, FieldType::Text { .. }) {
                continue;
            }
            if !hl.fields.is_empty() && !hl.fields.contains(&field_def.name) {
                continue;
            }
            if let Some(&tantivy_field) = self.field_map.get(&field_def.name)
                && let Ok(sg) = SnippetGenerator::create(searcher, query, tantivy_field)
            {
                generators.insert(field_def.name.clone(), sg);
            }
        }
        generators
    }

    /// Apply highlights to field values, replacing raw values with highlighted snippets.
    fn apply_highlights(
        &self,
        fields: Vec<(String, String)>,
        doc: &TantivyDocument,
        snippet_generators: &HashMap<String, SnippetGenerator>,
        highlight: &Option<HighlightOptions>,
    ) -> Vec<(String, String)> {
        if snippet_generators.is_empty() {
            return fields;
        }
        let open_tag = highlight
            .as_ref()
            .and_then(|hl| hl.open_tag.as_deref())
            .unwrap_or("<b>");
        let close_tag = highlight
            .as_ref()
            .and_then(|hl| hl.close_tag.as_deref())
            .unwrap_or("</b>");
        fields
            .into_iter()
            .map(|(name, value)| {
                if let Some(sg) = snippet_generators.get(&name) {
                    let mut snippet = sg.snippet_from_doc(doc);
                    if !snippet.is_empty() {
                        snippet.set_snippet_prefix_postfix(open_tag, close_tag);
                        return (name, snippet.to_html());
                    }
                }
                (name, value)
            })
            .collect()
    }

    /// Apply summarization to field values, truncating TEXT fields to snippet fragments.
    fn apply_summarize(
        &self,
        fields: Vec<(String, String)>,
        opts: &SummarizeOptions,
    ) -> Vec<(String, String)> {
        fields
            .into_iter()
            .map(|(name, value)| {
                // Only summarize TEXT fields
                let is_text = self
                    .def
                    .fields
                    .iter()
                    .any(|f| f.name == name && matches!(f.field_type, FieldType::Text { .. }));
                if !is_text {
                    return (name, value);
                }
                // Skip if FIELDS was specified and this field is not in the list
                if !opts.fields.is_empty() && !opts.fields.contains(&name) {
                    return (name, value);
                }
                let words: Vec<&str> = value.split_whitespace().collect();
                if words.len() <= opts.frag_len {
                    return (name, value);
                }
                // Split into fragments of frag_len words
                let mut frags = Vec::new();
                for chunk in words.chunks(opts.frag_len) {
                    frags.push(chunk.join(" "));
                    if frags.len() >= opts.num_frags {
                        break;
                    }
                }
                (name, frags.join(&opts.separator))
            })
            .collect()
    }

    /// Check if a document passes all geo radius filters.
    fn doc_passes_geo_filters(&self, doc: &TantivyDocument, filters: &[GeoFilter]) -> bool {
        for filter in filters {
            if let Some(geo) = self.geo_field_map.get(&filter.field) {
                let lon = doc.get_first(geo.lon_field).and_then(|v| v.as_f64());
                let lat = doc.get_first(geo.lat_field).and_then(|v| v.as_f64());
                match (lon, lat) {
                    (Some(lon), Some(lat)) => {
                        let dist = haversine_distance(filter.lon, filter.lat, lon, lat);
                        if dist > filter.radius_m {
                            return false;
                        }
                    }
                    _ => return false,
                }
            } else {
                return false;
            }
        }
        true
    }

    fn extract_hit_fields(&self, doc: &TantivyDocument) -> (String, Vec<(String, String)>) {
        let key = doc
            .get_first(self.key_field)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut fields = Vec::new();
        for field_def in &self.def.fields {
            if let Some(&tantivy_field) = self.field_map.get(&field_def.name)
                && let Some(value) = doc.get_first(tantivy_field)
            {
                let val_str = match &field_def.field_type {
                    FieldType::Text { .. } | FieldType::Tag { .. } | FieldType::Geo => {
                        value.as_str().unwrap_or("").to_string()
                    }
                    FieldType::Numeric => match value.as_f64() {
                        Some(v) => v.to_string(),
                        None => String::new(),
                    },
                    FieldType::Vector { .. } => {
                        // Vectors are not stored in tantivy
                        continue;
                    }
                };
                fields.push((field_def.name.clone(), val_str));
            }
        }
        (key, fields)
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

    /// Return the tantivy schema.
    pub fn tantivy_schema(&self) -> &Schema {
        &self.tantivy_schema
    }

    /// Return the key field handle.
    pub fn key_field(&self) -> Field {
        self.key_field
    }

    /// Return all distinct values for a TAG field by scanning the tantivy index.
    pub fn tag_values(&self, field_name: &str) -> Result<Vec<String>, SearchError> {
        // Find the field definition and verify it's a TAG type
        let field_def = self
            .def
            .fields
            .iter()
            .find(|f| f.name == field_name)
            .ok_or_else(|| SearchError::FieldNotFound(field_name.to_string()))?;

        let separator = match &field_def.field_type {
            FieldType::Tag { separator } => *separator,
            _ => {
                return Err(SearchError::Other(format!(
                    "{} is not a TAG field",
                    field_name
                )));
            }
        };

        let tantivy_field = match self.field_map.get(field_name) {
            Some(&f) => f,
            None => return Ok(vec![]),
        };

        let searcher = self.reader.searcher();
        let mut values = std::collections::HashSet::new();

        // Iterate all docs across all segments using DocAddress
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            for doc_id in 0..segment_reader.max_doc() {
                if segment_reader.is_deleted(doc_id) {
                    continue;
                }
                let doc_address = tantivy::DocAddress::new(segment_ord as u32, doc_id);
                if let Ok(doc) = searcher.doc::<TantivyDocument>(doc_address) {
                    for field_value in doc.get_all(tantivy_field) {
                        if let Some(text) = field_value.as_str() {
                            for part in text.split(separator) {
                                let trimmed = part.trim();
                                if !trimmed.is_empty() {
                                    values.insert(trimmed.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Return the tantivy reader (for term enumeration in spellcheck).
    pub fn reader(&self) -> &IndexReader {
        &self.reader
    }

    /// Return the field map (for spellcheck term enumeration).
    pub fn field_map(&self) -> &HashMap<String, Field> {
        &self.field_map
    }

    /// Update the definition and recreate the index with the expanded schema.
    ///
    /// Used by FT.ALTER to add new fields. Tantivy does not support altering
    /// an existing schema, so we destroy the old index and create a fresh one.
    /// The caller must re-index all matching documents after this call.
    pub fn reopen_with_def(&mut self, new_def: SearchIndexDef) -> Result<(), SearchError> {
        let (tantivy_schema, field_map, sort_field_map, geo_field_map, key_field) =
            build_tantivy_schema(&new_def);

        let index = if let Some(ref path) = self.path {
            // Disk-based: we need to create a fresh index in a temp dir,
            // then swap. We can't delete the existing dir while mmap handles
            // are still open. Instead, create a RAM-based temp index first,
            // which drops the old fields, then create the real one.

            // First, create a temporary RAM index to replace self's fields
            // so that the old writer/reader/index get dropped.
            let temp = Index::create_in_ram(tantivy_schema.clone());
            let temp_writer = temp.writer(15_000_000)?;
            let temp_reader = temp
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            self._index = temp;
            self.writer = temp_writer;
            self.reader = temp_reader;

            // Now the old mmap handles are dropped; safe to delete and recreate
            if path.exists() {
                std::fs::remove_dir_all(path)?;
            }
            std::fs::create_dir_all(path)?;
            let dir = MmapDirectory::open(path)?;
            Index::open_or_create(dir, tantivy_schema.clone())?
        } else {
            // RAM-based: create fresh
            Index::create_in_ram(tantivy_schema.clone())
        };
        register_custom_tokenizers(&index, &new_def);

        let heap_size = if self.path.is_some() {
            50_000_000
        } else {
            15_000_000
        };
        let writer = index.writer(heap_size)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        // Recreate vector indexes for new definition
        let (vector_indexes, vector_key_map, vector_next_id, vector_reverse_map) =
            create_vector_indexes(&new_def, self.path.as_deref())?;

        self.def = new_def;
        self._index = index;
        self.writer = writer;
        self.reader = reader;
        self.dirty = false;
        self.tantivy_schema = tantivy_schema;
        self.field_map = field_map;
        self.sort_field_map = sort_field_map;
        self.geo_field_map = geo_field_map;
        self.key_field = key_field;
        self.vector_indexes = vector_indexes;
        self.vector_key_map = vector_key_map;
        self.vector_next_id = vector_next_id;
        self.vector_reverse_map = vector_reverse_map;

        Ok(())
    }

    /// Index a vector for a specific field. The blob is raw f32 bytes (little-endian).
    pub fn index_vector(&mut self, field_name: &str, key: &str, blob: &[u8]) {
        let field_def = self.def.fields.iter().find(|f| f.name == field_name);
        let dim = match field_def {
            Some(f) => match &f.field_type {
                FieldType::Vector { dim, .. } => *dim,
                _ => return,
            },
            None => return,
        };

        let expected_len = dim * 4; // f32 = 4 bytes
        if blob.len() != expected_len {
            tracing::warn!(
                field = field_name,
                expected = expected_len,
                got = blob.len(),
                "Vector blob size mismatch"
            );
            return;
        }

        let vec_idx = match self.vector_indexes.get(field_name) {
            Some(idx) => idx,
            None => return,
        };

        // Remove old vector if key was previously indexed
        if let Some(rev_map) = self.vector_reverse_map.get(field_name)
            && let Some(&old_id) = rev_map.get(key)
        {
            let _ = vec_idx.remove(old_id);
        }

        // Allocate new ID
        let id = self
            .vector_next_id
            .entry(field_name.to_string())
            .or_insert(0);
        let vec_id = *id;
        *id += 1;

        // Reserve capacity if needed
        let size = vec_idx.size();
        let capacity = vec_idx.capacity();
        if size >= capacity {
            let _ = vec_idx.reserve(capacity.max(64) * 2);
        }

        // Convert bytes to f32 slice
        let floats = bytemuck_cast_f32(blob);
        let _ = vec_idx.add(vec_id, &floats);

        // Update maps
        self.vector_key_map
            .entry(field_name.to_string())
            .or_default()
            .insert(vec_id, key.to_string());
        self.vector_reverse_map
            .entry(field_name.to_string())
            .or_default()
            .insert(key.to_string(), vec_id);
    }

    /// Remove all vectors for a given Redis key from all vector fields.
    pub fn delete_vector(&mut self, key: &str) {
        for field_def in &self.def.fields {
            if !matches!(field_def.field_type, FieldType::Vector { .. }) {
                continue;
            }
            let fname = &field_def.name;
            if let Some(rev_map) = self.vector_reverse_map.get_mut(fname)
                && let Some(id) = rev_map.remove(key)
            {
                if let Some(vec_idx) = self.vector_indexes.get(fname) {
                    let _ = vec_idx.remove(id);
                }
                if let Some(key_map) = self.vector_key_map.get_mut(fname) {
                    key_map.remove(&id);
                }
            }
        }
    }

    /// Save vector indexes to disk.
    fn save_vectors(&self) {
        let base_path = match &self.path {
            Some(p) => p,
            None => return,
        };
        for (field_name, vec_idx) in &self.vector_indexes {
            let vec_path = base_path.join(format!("__vec_{}.usearch", field_name));
            if let Err(e) = vec_idx.save(vec_path.to_str().unwrap_or("")) {
                tracing::error!(error = %e, field = field_name, "Failed to save vector index");
            }
            // Save key maps as JSON
            let map_path = base_path.join(format!("__vec_{}_map.json", field_name));
            if let Some(key_map) = self.vector_key_map.get(field_name) {
                let map_data = serde_json::json!({
                    "key_map": key_map.iter().map(|(id, key)| (id.to_string(), key)).collect::<HashMap<String, &String>>(),
                    "next_id": self.vector_next_id.get(field_name).copied().unwrap_or(0),
                });
                if let Ok(json) = serde_json::to_vec(&map_data) {
                    let _ = std::fs::write(&map_path, json);
                }
            }
        }
    }

    /// Perform a KNN search on a vector field.
    pub fn knn_search(
        &self,
        field_name: &str,
        query_vector: &[f32],
        k: usize,
    ) -> Result<Vec<KnnHit>, SearchError> {
        let vec_idx = self.vector_indexes.get(field_name).ok_or_else(|| {
            SearchError::SchemaError(format!("No vector index for field: {}", field_name))
        })?;
        let key_map = self.vector_key_map.get(field_name).ok_or_else(|| {
            SearchError::SchemaError(format!("No key map for field: {}", field_name))
        })?;

        let results = vec_idx
            .search(query_vector, k)
            .map_err(|e| SearchError::SchemaError(format!("Vector search failed: {}", e)))?;

        let mut hits = Vec::with_capacity(results.keys.len());
        for i in 0..results.keys.len() {
            let usearch_key = results.keys[i];
            let distance = results.distances[i];
            if let Some(redis_key) = key_map.get(&usearch_key) {
                hits.push(KnnHit {
                    key: redis_key.clone(),
                    distance,
                    fields: Vec::new(), // Fields are populated by the caller from the hash
                });
            }
        }

        Ok(hits)
    }

    /// Options for the text search portion of a hybrid search.
    #[allow(clippy::too_many_arguments)]
    pub fn hybrid_search(
        &self,
        text_query: &str,
        vector_field: &str,
        vector_data: &[f32],
        strategy: &crate::hybrid::FusionStrategy,
        window: usize,
        count: usize,
        text_opts: HybridTextOptions,
    ) -> Result<Vec<crate::hybrid::HybridHit>, SearchError> {
        let fetch_count = window * count;

        // Run text search
        let text_result = self.search_with_options(
            text_query,
            0,
            fetch_count,
            None,
            text_opts.infields,
            None,
            text_opts.slop,
            None,
            text_opts.verbatim,
            None,
            text_opts.extra_filters,
            text_opts.extra_geo_filters,
        )?;

        // Run vector search
        let knn_hits = self.knn_search(vector_field, vector_data, fetch_count)?;

        // Get distance metric for the vector field
        let distance_metric = self
            .def
            .fields
            .iter()
            .find_map(|f| match (f.name == vector_field, &f.field_type) {
                (
                    true,
                    FieldType::Vector {
                        distance_metric, ..
                    },
                ) => Some(*distance_metric),
                _ => None,
            })
            .unwrap_or(VectorDistanceMetric::Cosine);

        Ok(crate::hybrid::hybrid_fuse(
            &text_result.hits,
            &knn_hits,
            strategy,
            distance_metric,
            count,
        ))
    }

    /// Check if this index has any vector fields.
    pub fn has_vector_fields(&self) -> bool {
        self.def
            .fields
            .iter()
            .any(|f| matches!(f.field_type, FieldType::Vector { .. }))
    }
}

/// Cast raw bytes to f32 slice (assumes little-endian, which is standard for x86/ARM).
fn bytemuck_cast_f32(bytes: &[u8]) -> Vec<f32> {
    assert!(bytes.len() % 4 == 0);
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Create usearch indexes for all VECTOR fields in the definition.
#[allow(clippy::type_complexity)]
fn create_vector_indexes(
    def: &SearchIndexDef,
    base_path: Option<&Path>,
) -> Result<
    (
        HashMap<String, usearch::Index>,
        HashMap<String, HashMap<u64, String>>,
        HashMap<String, u64>,
        HashMap<String, HashMap<String, u64>>,
    ),
    SearchError,
> {
    let mut vector_indexes = HashMap::new();
    let mut vector_key_map: HashMap<String, HashMap<u64, String>> = HashMap::new();
    let mut vector_next_id: HashMap<String, u64> = HashMap::new();
    let mut vector_reverse_map: HashMap<String, HashMap<String, u64>> = HashMap::new();

    for field_def in &def.fields {
        if let FieldType::Vector {
            dim,
            distance_metric,
        } = &field_def.field_type
        {
            let metric = match distance_metric {
                VectorDistanceMetric::Cosine => usearch::MetricKind::Cos,
                VectorDistanceMetric::L2 => usearch::MetricKind::L2sq,
                VectorDistanceMetric::IP => usearch::MetricKind::IP,
            };

            let opts = usearch::IndexOptions {
                dimensions: *dim,
                metric,
                quantization: usearch::ScalarKind::F32,
                ..Default::default()
            };

            let vec_idx = usearch::Index::new(&opts).map_err(|e| {
                SearchError::SchemaError(format!(
                    "Failed to create vector index for {}: {}",
                    field_def.name, e
                ))
            })?;

            // Try to load from disk
            if let Some(base) = base_path {
                let vec_path = base.join(format!("__vec_{}.usearch", field_def.name));
                let map_path = base.join(format!("__vec_{}_map.json", field_def.name));
                if vec_path.exists()
                    && let Err(e) = vec_idx.load(vec_path.to_str().unwrap_or(""))
                {
                    tracing::warn!(error = %e, "Failed to load vector index, starting fresh");
                }
                if map_path.exists()
                    && let Ok(data) = std::fs::read(&map_path)
                    && let Ok(map_data) = serde_json::from_slice::<serde_json::Value>(&data)
                {
                    let mut km = HashMap::new();
                    let mut rm = HashMap::new();
                    if let Some(obj) = map_data.get("key_map").and_then(|v| v.as_object()) {
                        for (id_str, key_val) in obj {
                            if let Ok(id) = id_str.parse::<u64>()
                                && let Some(key) = key_val.as_str()
                            {
                                km.insert(id, key.to_string());
                                rm.insert(key.to_string(), id);
                            }
                        }
                    }
                    let next_id = map_data
                        .get("next_id")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    vector_key_map.insert(field_def.name.clone(), km);
                    vector_reverse_map.insert(field_def.name.clone(), rm);
                    vector_next_id.insert(field_def.name.clone(), next_id);
                }
            }

            // Reserve initial capacity
            let _ = vec_idx.reserve(1024);

            vector_indexes.insert(field_def.name.clone(), vec_idx);
            vector_key_map.entry(field_def.name.clone()).or_default();
            vector_reverse_map
                .entry(field_def.name.clone())
                .or_default();
            vector_next_id.entry(field_def.name.clone()).or_insert(0);
        }
    }

    Ok((
        vector_indexes,
        vector_key_map,
        vector_next_id,
        vector_reverse_map,
    ))
}

/// Register custom tokenizers that aren't built into tantivy by default.
///
/// Map a language string to a tantivy `Language` enum value.
fn parse_language(lang: &str) -> Language {
    match lang {
        "arabic" => Language::Arabic,
        "danish" => Language::Danish,
        "dutch" => Language::Dutch,
        "finnish" => Language::Finnish,
        "french" => Language::French,
        "german" => Language::German,
        "greek" => Language::Greek,
        "hungarian" => Language::Hungarian,
        "italian" => Language::Italian,
        "norwegian" => Language::Norwegian,
        "portuguese" => Language::Portuguese,
        "romanian" => Language::Romanian,
        "russian" => Language::Russian,
        "spanish" => Language::Spanish,
        "swedish" => Language::Swedish,
        "tamil" => Language::Tamil,
        "turkish" => Language::Turkish,
        _ => Language::English,
    }
}

/// Register custom tokenizers on a tantivy index.
///
/// Always registers:
/// - "simple": lowercase + remove-long only (no stemming, no stopwords) — for NOSTEM fields
/// - "no_stopwords": lowercase + stemming, no stop-word filtering — for STOPWORDS 0
///
/// Conditionally registers:
/// - "lang": like default but with a language-specific stemmer — when LANGUAGE is set
/// - "custom_sw": like default but with custom stopword list — when STOPWORDS has entries
/// - "lang_custom_sw": language stemmer + custom stopwords — when both are set
pub(crate) fn register_custom_tokenizers(index: &Index, def: &SearchIndexDef) {
    let stemmer = def
        .language
        .as_deref()
        .map(|l| Stemmer::new(parse_language(l)))
        .unwrap_or_default();

    let simple = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build();
    index.tokenizers().register("simple", simple);

    let no_stopwords = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(stemmer.clone())
        .build();
    index.tokenizers().register("no_stopwords", no_stopwords);

    // Register language-specific tokenizer (stemmer only, default stopwords)
    if def.language.is_some() {
        let lang_analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(stemmer.clone())
            .build();
        index.tokenizers().register("lang", lang_analyzer);
    }

    // Register custom stopword tokenizer
    if let Some(ref words) = def.stopwords
        && !words.is_empty()
    {
        let custom_sw = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(40))
            .filter(LowerCaser)
            .filter(stemmer)
            .filter(StopWordFilter::remove(words.iter().cloned()))
            .build();
        index.tokenizers().register("custom_sw", custom_sw);
    }
}

/// Build a tantivy Schema from our SearchIndexDef.
///
/// Returns (schema, field_map, sort_field_map, geo_field_map, key_field).
#[allow(clippy::type_complexity)]
fn build_tantivy_schema(
    def: &SearchIndexDef,
) -> (
    Schema,
    HashMap<String, Field>,
    HashMap<String, Field>,
    HashMap<String, GeoCompanionFields>,
    Field,
) {
    let mut builder = Schema::builder();

    // Always add __key as STRING | STORED | INDEXED
    let key_field = builder.add_text_field("__key", STRING | STORED);

    let mut field_map = HashMap::new();
    let mut sort_field_map = HashMap::new();
    let mut geo_field_map = HashMap::new();

    for field_def in &def.fields {
        let field = match &field_def.field_type {
            FieldType::Text { .. } => {
                if field_def.noindex {
                    builder.add_text_field(&field_def.name, STORED)
                } else if field_def.nostem {
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("simple")
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            ),
                    );
                    builder.add_text_field(&field_def.name, opts)
                } else if matches!(&def.stopwords, Some(sw) if sw.is_empty()) {
                    // STOPWORDS 0: use tokenizer with stemming but no stop-word filtering
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("no_stopwords")
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            ),
                    );
                    builder.add_text_field(&field_def.name, opts)
                } else if matches!(&def.stopwords, Some(sw) if !sw.is_empty()) {
                    // Custom stopword list: use tokenizer with stemming + custom stopwords
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("custom_sw")
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            ),
                    );
                    builder.add_text_field(&field_def.name, opts)
                } else if def.language.is_some() {
                    // Language-specific stemmer (default stopwords)
                    let opts = TextOptions::default().set_stored().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("lang")
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            ),
                    );
                    builder.add_text_field(&field_def.name, opts)
                } else {
                    builder.add_text_field(&field_def.name, TEXT | STORED)
                }
            }
            FieldType::Tag { .. } => {
                if field_def.noindex {
                    builder.add_text_field(&field_def.name, STORED)
                } else if field_def.sortable {
                    builder.add_text_field(&field_def.name, STRING | STORED | FAST)
                } else {
                    builder.add_text_field(&field_def.name, STRING | STORED)
                }
            }
            FieldType::Numeric => {
                let opts: NumericOptions = if field_def.noindex {
                    NumericOptions::default().set_stored()
                } else if field_def.sortable {
                    NumericOptions::default()
                        .set_indexed()
                        .set_stored()
                        .set_fast()
                } else {
                    NumericOptions::default().set_indexed().set_stored()
                };
                builder.add_f64_field(&field_def.name, opts)
            }
            FieldType::Geo => {
                // Main field stores raw "lon,lat" string
                let main = builder.add_text_field(&field_def.name, STRING | STORED);
                // Companion fields for geo queries
                let hash_name = format!("__geo_hash_{}", field_def.name);
                let lon_name = format!("__geo_lon_{}", field_def.name);
                let lat_name = format!("__geo_lat_{}", field_def.name);
                let hash_field = builder.add_text_field(&hash_name, TEXT | STORED);
                let lon_field = builder.add_f64_field(
                    &lon_name,
                    NumericOptions::default()
                        .set_indexed()
                        .set_stored()
                        .set_fast(),
                );
                let lat_field = builder.add_f64_field(
                    &lat_name,
                    NumericOptions::default()
                        .set_indexed()
                        .set_stored()
                        .set_fast(),
                );
                geo_field_map.insert(
                    field_def.name.clone(),
                    GeoCompanionFields {
                        hash_field,
                        lon_field,
                        lat_field,
                    },
                );
                main
            }
            FieldType::Vector { .. } => {
                // Vectors are stored in usearch sidecar, not tantivy.
                // Skip adding to tantivy schema; no tantivy field needed.
                continue;
            }
        };
        field_map.insert(field_def.name.clone(), field);

        // For sortable TEXT fields, add a companion __sort_ field (STRING | STORED | FAST)
        if field_def.sortable && matches!(field_def.field_type, FieldType::Text { .. }) {
            let sort_name = format!("__sort_{}", field_def.name);
            let sort_field = builder.add_text_field(&sort_name, STRING | STORED | FAST);
            sort_field_map.insert(field_def.name.clone(), sort_field);
        }
    }

    (
        builder.build(),
        field_map,
        sort_field_map,
        geo_field_map,
        key_field,
    )
}

/// Parse a "lon,lat" geo value string.
fn parse_geo_value(value: &str) -> Option<(f64, f64)> {
    let parts: Vec<&str> = value.split(',').collect();
    if parts.len() == 2 {
        let lon = parts[0].trim().parse::<f64>().ok()?;
        let lat = parts[1].trim().parse::<f64>().ok()?;
        Some((lon, lat))
    } else {
        None
    }
}

/// Haversine distance in meters between two (lon, lat) points.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const R: f64 = 6_371_000.0; // Earth's radius in meters
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    R * c
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
                    nostem: false,
                    casesensitive: false,
                    json_path: None,
                },
                FieldDef {
                    name: "tags".to_string(),
                    field_type: FieldType::Tag { separator: ',' },
                    sortable: false,
                    noindex: false,
                    nostem: false,
                    casesensitive: false,
                    json_path: None,
                },
                FieldDef {
                    name: "price".to_string(),
                    field_type: FieldType::Numeric,
                    sortable: true,
                    noindex: false,
                    nostem: false,
                    casesensitive: false,
                    json_path: None,
                },
            ],
            version: 1,
            synonym_groups: HashMap::new(),
            source: Default::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
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
                nostem: false,
                casesensitive: false,
                json_path: None,
            }],
            version: 1,
            synonym_groups: HashMap::new(),
            source: Default::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
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

    #[test]
    fn test_reopen_with_def_adds_field() {
        let def = test_def();
        let mut index = ShardSearchIndex::open_in_ram(def.clone()).unwrap();

        // Index a document with original fields
        index.index_document(
            "doc:1",
            &[
                ("title".to_string(), "hello world".to_string()),
                ("category".to_string(), "books".to_string()),
            ],
        );
        index.commit().unwrap();
        assert_eq!(index.search("hello", 0, 10).unwrap().hits.len(), 1);

        // Expand schema with new field
        let mut new_def = def;
        new_def.fields.push(FieldDef {
            name: "category".to_string(),
            field_type: FieldType::Tag { separator: ',' },
            sortable: false,
            noindex: false,
            nostem: false,
            casesensitive: false,
            json_path: None,
        });
        index.reopen_with_def(new_def).unwrap();

        // Re-index document with new field populated
        index.index_document(
            "doc:1",
            &[
                ("title".to_string(), "hello world".to_string()),
                ("category".to_string(), "books".to_string()),
            ],
        );
        index.commit().unwrap();

        // Search by new field
        let result = index.search("@category:{books}", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].key, "doc:1");
    }

    #[test]
    fn test_synonym_expansion() {
        let mut def = test_def();
        def.synonym_groups.insert(
            "vehicles".to_string(),
            vec![
                "car".to_string(),
                "automobile".to_string(),
                "vehicle".to_string(),
            ],
        );
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document(
            "doc:1",
            &[("title".to_string(), "buy a new car today".to_string())],
        );
        index.index_document(
            "doc:2",
            &[(
                "title".to_string(),
                "automobile insurance rates".to_string(),
            )],
        );
        index.index_document(
            "doc:3",
            &[("title".to_string(), "vehicle maintenance tips".to_string())],
        );
        index.commit().unwrap();

        // Searching for "car" should find all three docs via synonym expansion
        let result = index.search("car", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 3);

        // Searching for "automobile" should also find all three
        let result = index.search("automobile", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 3);

        // Searching for a non-synonym term should work normally
        let result = index.search("insurance", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 1);
    }

    fn geo_def() -> SearchIndexDef {
        SearchIndexDef {
            name: "geo_idx".to_string(),
            prefix: vec!["place:".to_string()],
            fields: vec![
                FieldDef {
                    name: "name".to_string(),
                    field_type: FieldType::Text { weight: 1.0 },
                    sortable: false,
                    noindex: false,
                    nostem: false,
                    casesensitive: false,
                    json_path: None,
                },
                FieldDef {
                    name: "location".to_string(),
                    field_type: FieldType::Geo,
                    sortable: false,
                    noindex: false,
                    nostem: false,
                    casesensitive: false,
                    json_path: None,
                },
            ],
            version: 2,
            synonym_groups: HashMap::new(),
            source: Default::default(),
            stopwords: None,
            skip_initial_scan: false,
            language: None,
        }
    }

    #[test]
    fn test_geo_indexing_and_extraction() {
        let def = geo_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document(
            "place:1",
            &[
                ("name".to_string(), "Central Park".to_string()),
                ("location".to_string(), "-73.9654,40.7829".to_string()),
            ],
        );
        index.commit().unwrap();

        // Search for all docs — geo field should be returned as "lon,lat"
        let result = index.search("*", 0, 10).unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].key, "place:1");
        let loc = result.hits[0]
            .fields
            .iter()
            .find(|(k, _)| k == "location")
            .map(|(_, v)| v.as_str());
        assert_eq!(loc, Some("-73.9654,40.7829"));
    }

    #[test]
    fn test_geo_radius_query() {
        let def = geo_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        // Central Park, NYC
        index.index_document(
            "place:1",
            &[
                ("name".to_string(), "Central Park".to_string()),
                ("location".to_string(), "-73.9654,40.7829".to_string()),
            ],
        );
        // Times Square, NYC (~1.5km from Central Park)
        index.index_document(
            "place:2",
            &[
                ("name".to_string(), "Times Square".to_string()),
                ("location".to_string(), "-73.9855,40.7580".to_string()),
            ],
        );
        // Statue of Liberty (~8km from Central Park)
        index.index_document(
            "place:3",
            &[
                ("name".to_string(), "Statue of Liberty".to_string()),
                ("location".to_string(), "-74.0445,40.6892".to_string()),
            ],
        );
        index.commit().unwrap();

        // 5km radius from Central Park: should find Central Park + Times Square (~3.25km away)
        let result = index
            .search("@location:[-73.9654 40.7829 5 km]", 0, 10)
            .unwrap();
        assert_eq!(result.total, 2);
        let keys: Vec<&str> = result.hits.iter().map(|h| h.key.as_str()).collect();
        assert!(keys.contains(&"place:1"));
        assert!(keys.contains(&"place:2"));

        // 500m radius from Central Park: should find only Central Park
        let result = index
            .search("@location:[-73.9654 40.7829 500 m]", 0, 10)
            .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.hits[0].key, "place:1");

        // 20km radius: should find all three
        let result = index
            .search("@location:[-73.9654 40.7829 20 km]", 0, 10)
            .unwrap();
        assert_eq!(result.total, 3);
    }

    #[test]
    fn test_geo_with_text_query() {
        let def = geo_def();
        let mut index = ShardSearchIndex::open_in_ram(def).unwrap();

        index.index_document(
            "place:1",
            &[
                ("name".to_string(), "Central Park".to_string()),
                ("location".to_string(), "-73.9654,40.7829".to_string()),
            ],
        );
        index.index_document(
            "place:2",
            &[
                ("name".to_string(), "Times Square".to_string()),
                ("location".to_string(), "-73.9855,40.7580".to_string()),
            ],
        );
        index.commit().unwrap();

        // Combine text and geo: "park" within 3km radius
        let result = index
            .search("park @location:[-73.9654 40.7829 3 km]", 0, 10)
            .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.hits[0].key, "place:1");
    }

    #[test]
    fn test_haversine_distance_known_values() {
        // NYC to London: ~5,570 km
        let nyc_lon = -74.006;
        let nyc_lat = 40.7128;
        let london_lon = -0.1278;
        let london_lat = 51.5074;
        let dist = haversine_distance(nyc_lon, nyc_lat, london_lon, london_lat);
        // Should be roughly 5,570 km (allow 50km tolerance)
        assert!((dist - 5_570_000.0).abs() < 50_000.0, "dist={}", dist);

        // Same point: distance = 0
        let dist = haversine_distance(0.0, 0.0, 0.0, 0.0);
        assert!(dist < 0.01);
    }
}

//! Full-text search engine for FrogDB.
//!
//! Provides RediSearch-compatible FT.* command support using tantivy
//! as the underlying inverted index engine.

pub mod aggregate;
pub mod error;
pub mod expression;
pub mod hybrid;
pub mod index;
pub mod query;
pub mod schema;
pub mod spellcheck;
pub mod suggest;

pub use error::SearchError;
pub use hybrid::{FusionStrategy, HybridHit, hybrid_fuse};
pub use index::{
    HighlightOptions, HybridTextOptions, KnnHit, SearchResult, ShardSearchIndex, SortValue,
    SummarizeOptions, extract_json_fields,
};
pub use query::{GeoFilter, QueryParser};
pub use schema::{
    FieldDef, FieldType, IndexSource, SearchIndexDef, SortOrder, VectorDistanceMetric,
    parse_ft_alter_args, parse_ft_create_args,
};

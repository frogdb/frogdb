//! Full-text search engine for FrogDB.
//!
//! Provides RediSearch-compatible FT.* command support using tantivy
//! as the underlying inverted index engine.

pub mod error;
pub mod index;
pub mod query;
pub mod schema;
pub mod suggest;

pub use error::SearchError;
pub use index::{HighlightOptions, SearchResult, ShardSearchIndex, SortValue};
pub use query::{GeoFilter, QueryParser};
pub use schema::{
    FieldDef, FieldType, SearchIndexDef, SortOrder, parse_ft_alter_args, parse_ft_create_args,
};

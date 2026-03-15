//! Full-text search engine for FrogDB.
//!
//! Provides RediSearch-compatible FT.* command support using tantivy
//! as the underlying inverted index engine.

pub mod error;
pub mod index;
pub mod query;
pub mod schema;

pub use error::SearchError;
pub use index::{SearchResult, ShardSearchIndex, SortValue};
pub use query::QueryParser;
pub use schema::{FieldDef, FieldType, SearchIndexDef, SortOrder, parse_ft_create_args};

//! Search error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Query parse error: {0}")]
    QueryParseError(String),

    #[error("Tantivy error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Directory error: {0}")]
    DirectoryError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

impl From<tantivy::directory::error::OpenDirectoryError> for SearchError {
    fn from(e: tantivy::directory::error::OpenDirectoryError) -> Self {
        SearchError::DirectoryError(e.to_string())
    }
}

//! TimeSeries data type with Gorilla compression.
//!
//! Implements Redis TimeSeries compatible data structures with:
//! - Delta-of-delta timestamp encoding
//! - XOR-based value compression
//! - Label indexing for efficient FILTER queries
//! - Configurable retention and duplicate policies

mod aggregation;
mod chunk;
mod compression;
mod downsample;
mod label_index;
mod value;

pub use aggregation::Aggregation;
pub use chunk::CompressedChunk;
pub use compression::{decode_samples, encode_samples};
pub use downsample::{DownsampleError, DownsampleManager, DownsampleRule, NoopDownsampleManager};
pub use label_index::{LabelFilter, LabelIndex};
pub use value::{DuplicatePolicy, TimeSeriesValue};

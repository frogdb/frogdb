//! FrogDB Types
//!
//! Shared type foundation for FrogDB — value types, errors, traits, and utilities.
//! This crate provides the foundational types used across all FrogDB crates.

pub mod args;
pub mod bitmap;
pub mod bloom;
pub mod cms;
pub mod cuckoo;
pub mod error;
pub mod geo;
pub mod glob;
pub mod hyperloglog;
pub mod json;
pub mod skiplist;
pub mod sync;
pub mod tdigest;
pub mod timeseries;
pub mod topk;
pub mod traits;
pub mod types;
pub mod vectorset;

pub use args::{
    ArgParser, CompareCondition, ExpiryOption, ScanOptions, parse_f64, parse_from_bytes, parse_i64,
    parse_u64, parse_usize,
};
pub use bitmap::{
    BitOp, BitfieldEncoding, BitfieldOffset, BitfieldSubCommand, OverflowMode, bitcount,
    bitfield_get, bitfield_incrby, bitfield_set, bitop, bitpos, getbit, setbit,
};
pub use bloom::{BloomFilterValue, BloomLayer};
pub use cms::CountMinSketchValue;
pub use cuckoo::{CuckooFilterValue, CuckooLayer};
pub use error::{CommandError, RespError};
pub use geo::{
    BoundingBox, Coordinates, DistanceUnit, EARTH_RADIUS_M, GEOHASH_BITS, GeoHashBits, LAT_MAX,
    LAT_MIN, LON_MAX, LON_MIN, geohash_calculate_areas, geohash_decode, geohash_encode,
    geohash_range_for_bbox, geohash_score_range, geohash_to_score, geohash_to_string,
    haversine_distance, is_within_box, is_within_radius, score_to_geohash,
};
pub use glob::glob_match;
pub use hyperloglog::{HLL_DENSE_SIZE, HLL_REGISTERS, HyperLogLogValue};
pub use json::{
    DEFAULT_JSON_MAX_DEPTH, DEFAULT_JSON_MAX_SIZE, JsonError, JsonLimits, JsonType, JsonValue,
    estimate_json_size,
};
pub use sync::{LockError, MutexExt, RwLockExt};
pub use tdigest::TDigestValue;
pub use timeseries::{
    Aggregation, CompressedChunk, DownsampleError, DownsampleManager, DownsampleRule,
    DuplicatePolicy, LabelFilter, LabelIndex, NoopDownsampleManager, TimeSeriesValue,
};
pub use topk::TopKValue;
pub use traits::{
    MetricsRecorder, NoopMetricsRecorder, NoopReplicationTracker, NoopSpan, NoopTracer,
    NoopWalWriter, ReplicationConfig, ReplicationTracker, Span, Tracer, WalOperation, WalWriter,
};
pub use types::{
    BlockingOp, Consumer, ConsumerGroup, DeleteRefStrategy, Direction, EsAppendError, Expiry,
    HashValue, IdempotencyState, IncrementError, KeyMetadata, KeyType, LexBound, ListValue,
    ListpackThresholds, PendingEntry, ScoreBound, ScoreIndexBackend, SetCondition, SetOptions,
    SetResult, SetValue, SortedSetValue, StreamAddError, StreamEntry, StreamGroupError, StreamId,
    StreamIdParseError, StreamIdSpec, StreamRangeBound, StreamTrimMode, StreamTrimOptions,
    StreamTrimStrategy, StreamValue, StringValue, Value, ZAddResult, set_default_score_index,
};
pub use vectorset::{
    FilterExpr, VectorDistanceMetric, VectorQuantization, VectorSearchResult, VectorSetInfo,
    VectorSetValue,
};

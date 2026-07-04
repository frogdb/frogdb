//! Typed FT.* request and shard-reply wire types.
//!
//! The FT.SEARCH and FT.AGGREGATE grammars are parsed exactly once, at the
//! coordinator, into [`FtSearchRequest`] / [`FtAggregateRequest`]; shards
//! receive the parsed request through the scatter path and reply with typed
//! [`ShardSearchReply`] hits or a typed [`PartialAggregate`]. Both ends of the
//! shard boundary consume this one module, so the option grammar and the hit
//! layout cannot drift apart — the former sentinel-keyed positional protocol
//! (`__ft_total__` + score-by-index) is gone.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;

use crate::aggregate::{AggregateStep, PartialAggregate, parse_aggregate_pipeline};
use crate::index::{HighlightOptions, SearchOptions, SortValue, SummarizeOptions};
use crate::query::GeoFilter;
use crate::schema::SortOrder;

/// The full FT.SEARCH grammar, parsed once at the coordinator and carried to
/// every shard typed.
///
/// `offset` / `limit` are the *global* paging window: the coordinator applies
/// them after the cross-shard merge, while each shard overfetches the window
/// `0..offset+limit` (see [`FtSearchRequest::index_options`]).
#[derive(Debug, Clone)]
pub struct FtSearchRequest {
    /// The query string (`args[0]`), before PARAMS substitution.
    pub query: String,
    /// LIMIT offset (default 0).
    pub offset: usize,
    /// LIMIT count (default 10).
    pub limit: usize,
    /// NOCONTENT: suppress document fields in the reply.
    pub nocontent: bool,
    /// WITHSCORES: include the score per hit in the reply.
    pub withscores: bool,
    /// VERBATIM: skip stemming.
    pub verbatim: bool,
    /// RETURN: restrict reply fields to these names.
    pub return_fields: Option<Vec<String>>,
    /// SORTBY field and direction.
    pub sortby: Option<(String, SortOrder)>,
    /// INFIELDS: restrict text matching to these fields.
    pub infields: Option<Vec<String>>,
    /// INKEYS: restrict matching to these document keys.
    pub inkeys: Option<Vec<String>>,
    /// Numeric FILTER clauses as `(field, min, max)`.
    pub filters: Vec<(String, f64, f64)>,
    /// GEOFILTER clauses.
    pub geofilters: Vec<GeoFilter>,
    /// HIGHLIGHT options.
    pub highlight: Option<HighlightOptions>,
    /// SLOP for phrase queries.
    pub slop: Option<u32>,
    /// SUMMARIZE options.
    pub summarize: Option<SummarizeOptions>,
    /// PARAMS name/value pairs ($param substitution, KNN vector blobs).
    pub params: HashMap<String, Bytes>,
    /// TIMEOUT override — consumed by the coordinator, ignored by shards.
    pub timeout: Option<Duration>,
}

impl Default for FtSearchRequest {
    fn default() -> Self {
        Self {
            query: "*".to_string(),
            offset: 0,
            limit: 10,
            nocontent: false,
            withscores: false,
            verbatim: false,
            return_fields: None,
            sortby: None,
            infields: None,
            inkeys: None,
            filters: Vec::new(),
            geofilters: Vec::new(),
            highlight: None,
            slop: None,
            summarize: None,
            params: HashMap::new(),
            timeout: None,
        }
    }
}

impl FtSearchRequest {
    /// Parse `FT.SEARCH` arguments (`args[0]` = query string, rest = options).
    ///
    /// Lenient like the grammar it replaces: unknown tokens and malformed
    /// option values fall back to defaults rather than erroring.
    pub fn parse(args: &[Bytes]) -> Self {
        let mut req = Self::default();
        if let Some(q) = args.first() {
            req.query = String::from_utf8_lossy(q).to_string();
        }

        let mut i = 1;
        while i < args.len() {
            let arg_upper = args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"PARAMS" => {
                    if i + 1 < args.len() {
                        let count: usize = parse_num(&args[i + 1]).unwrap_or(0);
                        i += 2;
                        for _ in 0..(count / 2) {
                            if i + 1 < args.len() {
                                let name = String::from_utf8_lossy(&args[i]).to_string();
                                req.params.insert(name, args[i + 1].clone());
                                i += 2;
                            }
                        }
                    } else {
                        i += 1;
                    }
                }
                b"LIMIT" => {
                    if i + 2 < args.len() {
                        req.offset = parse_num(&args[i + 1]).unwrap_or(0);
                        req.limit = parse_num(&args[i + 2]).unwrap_or(10);
                        i += 3;
                    } else {
                        i += 1;
                    }
                }
                b"NOCONTENT" => {
                    req.nocontent = true;
                    i += 1;
                }
                b"WITHSCORES" => {
                    req.withscores = true;
                    i += 1;
                }
                b"VERBATIM" => {
                    req.verbatim = true;
                    i += 1;
                }
                b"RETURN" => {
                    if i + 1 < args.len() {
                        let (fields, consumed) = parse_counted_list(args, i + 1);
                        req.return_fields = Some(fields);
                        i += 1 + consumed;
                    } else {
                        i += 1;
                    }
                }
                b"INFIELDS" => {
                    if i + 1 < args.len() {
                        let (fields, consumed) = parse_counted_list(args, i + 1);
                        req.infields = Some(fields);
                        i += 1 + consumed;
                    } else {
                        i += 1;
                    }
                }
                b"INKEYS" => {
                    if i + 1 < args.len() {
                        let (keys, consumed) = parse_counted_list(args, i + 1);
                        req.inkeys = Some(keys);
                        i += 1 + consumed;
                    } else {
                        i += 1;
                    }
                }
                b"SORTBY" => {
                    if i + 1 < args.len() {
                        let field = String::from_utf8_lossy(&args[i + 1]).to_string();
                        let order = if i + 2 < args.len() {
                            let dir = args[i + 2].to_ascii_uppercase();
                            if dir.as_slice() == b"DESC" {
                                i += 3;
                                SortOrder::Desc
                            } else if dir.as_slice() == b"ASC" {
                                i += 3;
                                SortOrder::Asc
                            } else {
                                i += 2;
                                SortOrder::Asc
                            }
                        } else {
                            i += 2;
                            SortOrder::Asc
                        };
                        req.sortby = Some((field, order));
                    } else {
                        i += 1;
                    }
                }
                b"SLOP" => {
                    if i + 1 < args.len() {
                        req.slop = parse_num(&args[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"FILTER" => {
                    if i + 3 < args.len() {
                        let field = String::from_utf8_lossy(&args[i + 1]).to_string();
                        let min = parse_range_bound(&args[i + 2]).unwrap_or(f64::NEG_INFINITY);
                        let max = parse_range_bound(&args[i + 3]).unwrap_or(f64::INFINITY);
                        req.filters.push((field, min, max));
                        i += 4;
                    } else {
                        i += 1;
                    }
                }
                b"GEOFILTER" => {
                    if i + 5 < args.len() {
                        let field = String::from_utf8_lossy(&args[i + 1]).to_string();
                        let lon: f64 = parse_num(&args[i + 2]).unwrap_or(0.0);
                        let lat: f64 = parse_num(&args[i + 3]).unwrap_or(0.0);
                        let radius: f64 = parse_num(&args[i + 4]).unwrap_or(0.0);
                        let unit = String::from_utf8_lossy(&args[i + 5]).to_lowercase();
                        let radius_m = match unit.as_str() {
                            "km" => radius * 1000.0,
                            "mi" => radius * 1609.344,
                            "ft" => radius * 0.3048,
                            _ => radius, // default meters
                        };
                        req.geofilters.push(GeoFilter {
                            field,
                            lon,
                            lat,
                            radius_m,
                        });
                        i += 6;
                    } else {
                        i += 1;
                    }
                }
                b"TIMEOUT" => {
                    // Consumed by the coordinator (gather deadline); shards
                    // never see a per-query timeout.
                    if i + 1 < args.len() {
                        if let Some(ms) = parse_num::<u64>(&args[i + 1])
                            && ms > 0
                        {
                            req.timeout = Some(Duration::from_millis(ms));
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"DIALECT" => {
                    // Accept and ignore — always dialect 2.
                    if i + 1 < args.len() {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"SUMMARIZE" => {
                    i += 1;
                    let mut summ = SummarizeOptions::default();
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"FIELDS") {
                        i += 1;
                        if i < args.len() {
                            let count: usize = parse_num(&args[i]).unwrap_or(0);
                            i += 1;
                            for _ in 0..count {
                                if i < args.len() {
                                    summ.fields
                                        .push(String::from_utf8_lossy(&args[i]).to_string());
                                    i += 1;
                                }
                            }
                        }
                    }
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"FRAGS") {
                        i += 1;
                        if i < args.len() {
                            summ.num_frags = parse_num(&args[i]).unwrap_or(3);
                            i += 1;
                        }
                    }
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"LEN") {
                        i += 1;
                        if i < args.len() {
                            summ.frag_len = parse_num(&args[i]).unwrap_or(20);
                            i += 1;
                        }
                    }
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"SEPARATOR") {
                        i += 1;
                        if i < args.len() {
                            summ.separator = String::from_utf8_lossy(&args[i]).to_string();
                            i += 1;
                        }
                    }
                    req.summarize = Some(summ);
                }
                b"HIGHLIGHT" => {
                    i += 1;
                    let mut hl = HighlightOptions::default();
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"FIELDS") {
                        i += 1;
                        if i < args.len() {
                            let count: usize = parse_num(&args[i]).unwrap_or(0);
                            i += 1;
                            for _ in 0..count {
                                if i < args.len() {
                                    hl.fields
                                        .push(String::from_utf8_lossy(&args[i]).to_string());
                                    i += 1;
                                }
                            }
                        }
                    }
                    if i < args.len() && args[i].eq_ignore_ascii_case(b"TAGS") {
                        i += 1;
                        if i + 1 < args.len() {
                            hl.open_tag = Some(String::from_utf8_lossy(&args[i]).to_string());
                            i += 1;
                            hl.close_tag = Some(String::from_utf8_lossy(&args[i]).to_string());
                            i += 1;
                        }
                    }
                    req.highlight = Some(hl);
                }
                _ => {
                    i += 1;
                }
            }
        }

        req
    }

    /// The index-level options a shard executes this request with.
    ///
    /// The paging window becomes `0..offset+limit`: every shard overfetches so
    /// the coordinator can apply the global offset+limit after the merge.
    pub fn index_options(&self) -> SearchOptions {
        SearchOptions {
            offset: 0,
            limit: self.offset + self.limit,
            sort_by: self.sortby.clone(),
            infields: self.infields.clone(),
            highlight: self.highlight.clone(),
            slop: self.slop,
            summarize: self.summarize.clone(),
            verbatim: self.verbatim,
            inkeys: self.inkeys.clone(),
            filters: self.filters.clone(),
            geofilters: self.geofilters.clone(),
        }
    }

    /// Whether the query is a KNN vector query (`... =>[KNN ...]`), which
    /// merges by ascending distance instead of descending BM25 score.
    pub fn is_knn(&self) -> bool {
        self.query.to_ascii_uppercase().contains("=>[KNN")
    }
}

/// The full FT.AGGREGATE request, parsed once at the coordinator.
///
/// The pipeline is parsed into typed [`AggregateStep`]s exactly once; shards
/// run the shard-local prefix and the coordinator runs the merge steps from
/// the same `steps` vector.
#[derive(Debug, Clone)]
pub struct FtAggregateRequest {
    /// The query string (`args[0]`).
    pub query: String,
    /// The parsed pipeline (LOAD / GROUPBY / REDUCE / SORTBY / APPLY / LIMIT / FILTER).
    pub steps: Vec<AggregateStep>,
    /// TIMEOUT override — consumed by the coordinator, ignored by shards.
    pub timeout: Option<Duration>,
    /// WITHCURSOR: stash the tail in the coordinator cursor store.
    pub withcursor: bool,
    /// WITHCURSOR COUNT (0 = return all rows in the first batch).
    pub cursor_count: usize,
    /// WITHCURSOR MAXIDLE in milliseconds.
    pub cursor_maxidle_ms: u64,
}

impl FtAggregateRequest {
    /// Parse `FT.AGGREGATE` arguments (`args[0]` = query string, rest =
    /// coordinator options + pipeline). Fails only on a malformed pipeline.
    pub fn parse(args: &[Bytes]) -> Result<Self, String> {
        let query = args
            .first()
            .map(|q| String::from_utf8_lossy(q).to_string())
            .unwrap_or_else(|| "*".to_string());

        let mut timeout = None;
        let mut withcursor = false;
        let mut cursor_count: usize = 0; // 0 = return all
        let mut cursor_maxidle_ms: u64 = 300_000; // default 300s
        let mut pipeline_args: Vec<&Bytes> = Vec::with_capacity(args.len());

        let mut i = 1;
        while i < args.len() {
            let upper = args[i].to_ascii_uppercase();
            match upper.as_slice() {
                b"TIMEOUT" => {
                    if i + 1 < args.len() {
                        if let Some(ms) = parse_num::<u64>(&args[i + 1])
                            && ms > 0
                        {
                            timeout = Some(Duration::from_millis(ms));
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"DIALECT" => {
                    // Accept and ignore.
                    if i + 1 < args.len() {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"WITHCURSOR" => {
                    withcursor = true;
                    i += 1;
                    // Parse optional COUNT and MAXIDLE
                    while i < args.len() {
                        let sub = args[i].to_ascii_uppercase();
                        if sub.as_slice() == b"COUNT" && i + 1 < args.len() {
                            cursor_count = parse_num(&args[i + 1]).unwrap_or(0);
                            i += 2;
                        } else if sub.as_slice() == b"MAXIDLE" && i + 1 < args.len() {
                            cursor_maxidle_ms = parse_num(&args[i + 1]).unwrap_or(300_000);
                            i += 2;
                        } else {
                            break;
                        }
                    }
                }
                _ => {
                    pipeline_args.push(&args[i]);
                    i += 1;
                }
            }
        }

        let pipeline_strs: Vec<&str> = pipeline_args
            .iter()
            .filter_map(|b| std::str::from_utf8(b).ok())
            .collect();
        let steps = parse_aggregate_pipeline(&pipeline_strs)?;

        Ok(Self {
            query,
            steps,
            timeout,
            withcursor,
            cursor_count,
            cursor_maxidle_ms,
        })
    }
}

/// One FT.SEARCH / FT.HYBRID hit crossing the shard→coordinator boundary.
#[derive(Debug, Clone)]
pub struct ShardSearchHit {
    /// Document key.
    pub key: String,
    /// BM25 score, KNN distance, or hybrid fused score depending on the op.
    pub score: f32,
    /// SORTBY value when active (used for the cross-shard merge sort).
    pub sort_value: Option<SortValue>,
    /// Reply field/value pairs (RETURN filter, HIGHLIGHT, SUMMARIZE already
    /// applied). `None` when the hit carries no content — NOCONTENT, or a KNN /
    /// hybrid hit whose document was missing from the store.
    pub fields: Option<Vec<(String, String)>>,
}

impl ShardSearchHit {
    /// The merge-sort key: the string form of the sort value, exactly as the
    /// pre-typed wire carried it (F64 via `to_string`, missing as empty).
    pub fn sort_key(&self) -> String {
        match &self.sort_value {
            Some(SortValue::F64(v)) => v.to_string(),
            Some(SortValue::Str(s)) => s.clone(),
            None => String::new(),
        }
    }
}

/// One shard's FT.SEARCH / FT.HYBRID reply: its local total plus its hits.
#[derive(Debug, Clone, Default)]
pub struct ShardSearchReply {
    /// Matching documents on this shard (before pagination).
    pub total: usize,
    /// The hits within the overfetch window.
    pub hits: Vec<ShardSearchHit>,
}

/// The typed payload a shard attaches to its scatter reply for the FT.* query
/// fan-outs. `Err` carries the exact client-facing error message.
#[derive(Debug, Clone)]
pub enum FtShardReply {
    /// FT.SEARCH / FT.HYBRID: hits + shard-local total.
    Search(Result<ShardSearchReply, String>),
    /// FT.AGGREGATE: the shard-local partial aggregate.
    Aggregate(Result<PartialAggregate, String>),
}

/// Parse a numeric token, `None` on malformed input.
fn parse_num<T: std::str::FromStr>(arg: &Bytes) -> Option<T> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse().ok())
}

/// Parse a FILTER range bound, accepting `-inf` / `+inf` / `inf`.
fn parse_range_bound(arg: &Bytes) -> Option<f64> {
    let s = std::str::from_utf8(arg).ok()?;
    match s {
        "-inf" => Some(f64::NEG_INFINITY),
        "+inf" | "inf" => Some(f64::INFINITY),
        _ => s.parse().ok(),
    }
}

/// Parse a `<count> <item>...` list starting at `start`; returns the items and
/// the number of tokens consumed (count token + items).
fn parse_counted_list(args: &[Bytes], start: usize) -> (Vec<String>, usize) {
    let count: usize = parse_num(&args[start]).unwrap_or(0);
    let mut items = Vec::new();
    for j in 0..count {
        if start + 1 + j < args.len()
            && let Ok(s) = std::str::from_utf8(&args[start + 1 + j])
        {
            items.push(s.to_string());
        }
    }
    (items, 1 + count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    #[test]
    fn test_search_request_defaults() {
        let req = FtSearchRequest::parse(&args(&["hello"]));
        assert_eq!(req.query, "hello");
        assert_eq!(req.offset, 0);
        assert_eq!(req.limit, 10);
        assert!(!req.nocontent);
        assert!(!req.withscores);
        assert!(!req.verbatim);
        assert!(req.sortby.is_none());
        assert!(req.return_fields.is_none());
        assert!(req.timeout.is_none());
        assert!(!req.is_knn());
    }

    #[test]
    fn test_search_request_full_grammar() {
        let req = FtSearchRequest::parse(&args(&[
            "@title:hello", // query
            "NOCONTENT",
            "WITHSCORES",
            "VERBATIM",
            "RETURN",
            "2",
            "title",
            "body",
            "SORTBY",
            "price",
            "DESC",
            "LIMIT",
            "5",
            "20",
            "INFIELDS",
            "1",
            "title",
            "INKEYS",
            "2",
            "doc:1",
            "doc:2",
            "SLOP",
            "2",
            "FILTER",
            "price",
            "-inf",
            "100",
            "GEOFILTER",
            "loc",
            "1.5",
            "2.5",
            "3",
            "km",
            "HIGHLIGHT",
            "FIELDS",
            "1",
            "title",
            "TAGS",
            "<em>",
            "</em>",
            "SUMMARIZE",
            "FIELDS",
            "1",
            "body",
            "FRAGS",
            "2",
            "LEN",
            "10",
            "SEPARATOR",
            "|",
            "PARAMS",
            "2",
            "vec",
            "blob",
            "TIMEOUT",
            "150",
            "DIALECT",
            "2",
        ]));

        assert_eq!(req.query, "@title:hello");
        assert!(req.nocontent);
        assert!(req.withscores);
        assert!(req.verbatim);
        assert_eq!(
            req.return_fields,
            Some(vec!["title".to_string(), "body".to_string()])
        );
        assert_eq!(req.sortby, Some(("price".to_string(), SortOrder::Desc)));
        assert_eq!(req.offset, 5);
        assert_eq!(req.limit, 20);
        assert_eq!(req.infields, Some(vec!["title".to_string()]));
        assert_eq!(
            req.inkeys,
            Some(vec!["doc:1".to_string(), "doc:2".to_string()])
        );
        assert_eq!(req.slop, Some(2));
        assert_eq!(req.filters.len(), 1);
        assert_eq!(req.filters[0].0, "price");
        assert_eq!(req.filters[0].1, f64::NEG_INFINITY);
        assert_eq!(req.filters[0].2, 100.0);
        assert_eq!(req.geofilters.len(), 1);
        assert_eq!(req.geofilters[0].field, "loc");
        assert_eq!(req.geofilters[0].lon, 1.5);
        assert_eq!(req.geofilters[0].lat, 2.5);
        assert_eq!(req.geofilters[0].radius_m, 3000.0);
        let hl = req.highlight.as_ref().unwrap();
        assert_eq!(hl.fields, vec!["title".to_string()]);
        assert_eq!(hl.open_tag.as_deref(), Some("<em>"));
        assert_eq!(hl.close_tag.as_deref(), Some("</em>"));
        let summ = req.summarize.as_ref().unwrap();
        assert_eq!(summ.fields, vec!["body".to_string()]);
        assert_eq!(summ.num_frags, 2);
        assert_eq!(summ.frag_len, 10);
        assert_eq!(summ.separator, "|");
        assert_eq!(req.params.get("vec"), Some(&Bytes::from("blob")));
        assert_eq!(req.timeout, Some(Duration::from_millis(150)));
    }

    #[test]
    fn test_search_request_index_options_overfetch_window() {
        let req = FtSearchRequest::parse(&args(&["*", "LIMIT", "5", "20", "SORTBY", "price"]));
        let opts = req.index_options();
        // The shard overfetches 0..offset+limit; the coordinator applies the
        // global window after the merge.
        assert_eq!(opts.offset, 0);
        assert_eq!(opts.limit, 25);
        assert_eq!(opts.sort_by, Some(("price".to_string(), SortOrder::Asc)));
    }

    #[test]
    fn test_search_request_knn_detection() {
        let req = FtSearchRequest::parse(&args(&["*=>[KNN 10 @vec $blob]"]));
        assert!(req.is_knn());
    }

    #[test]
    fn test_search_request_timeout_zero_ignored() {
        let req = FtSearchRequest::parse(&args(&["*", "TIMEOUT", "0"]));
        assert!(req.timeout.is_none());
    }

    #[test]
    fn test_search_request_params_not_misread_as_options() {
        // A PARAMS *value* that collides with an option keyword must not be
        // parsed as that option — the pre-typed coordinator scanner got this
        // wrong because it re-scanned the raw args without PARAMS awareness.
        let req = FtSearchRequest::parse(&args(&["*", "PARAMS", "2", "p", "NOCONTENT"]));
        assert!(!req.nocontent);
        assert_eq!(req.params.get("p"), Some(&Bytes::from("NOCONTENT")));
    }

    #[test]
    fn test_aggregate_request_parse() {
        let req = FtAggregateRequest::parse(&args(&[
            "*",
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "count",
            "SORTBY",
            "2",
            "@count",
            "DESC",
            "WITHCURSOR",
            "COUNT",
            "5",
            "MAXIDLE",
            "10000",
            "TIMEOUT",
            "50",
            "DIALECT",
            "2",
        ]))
        .unwrap();

        assert_eq!(req.query, "*");
        assert!(req.withcursor);
        assert_eq!(req.cursor_count, 5);
        assert_eq!(req.cursor_maxidle_ms, 10_000);
        assert_eq!(req.timeout, Some(Duration::from_millis(50)));
        // GROUPBY + SORTBY parsed as pipeline steps; the coordinator-only
        // options never reach the pipeline parser.
        assert_eq!(req.steps.len(), 2);
        assert!(matches!(req.steps[0], AggregateStep::GroupBy { .. }));
        assert!(matches!(req.steps[1], AggregateStep::SortBy { .. }));
    }

    #[test]
    fn test_aggregate_request_bad_pipeline_errors() {
        let err = FtAggregateRequest::parse(&args(&["*", "GROUPBY"])).unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn test_shard_hit_sort_key() {
        let mut hit = ShardSearchHit {
            key: "k".to_string(),
            score: 1.0,
            sort_value: None,
            fields: None,
        };
        assert_eq!(hit.sort_key(), "");
        hit.sort_value = Some(SortValue::F64(2.5));
        assert_eq!(hit.sort_key(), "2.5");
        hit.sort_value = Some(SortValue::Str("abc".to_string()));
        assert_eq!(hit.sort_key(), "abc");
    }
}

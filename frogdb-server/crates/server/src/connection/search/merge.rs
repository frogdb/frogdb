//! Bespoke [`MergeStrategy`] implementations for the search fan-out commands.
//!
//! These keep the per-command merge logic (FT.SEARCH overfetch + sort,
//! FT.AGGREGATE partial-aggregate reduction, FT.SPELLCHECK suggestion union, …)
//! but express it behind the [`MergeStrategy`] seam so the fan-out, the shared
//! gather timeout, and the send-failure / drop / timeout error mapping live in
//! [`ScatterGather::run`](crate::scatter::ScatterGather::run) — not re-typed in
//! each handler.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{PartialResult, StreamId};
use frogdb_protocol::Response;
use frogdb_search::FtSearchRequest;
use frogdb_search::aggregate::{self, AggregateStep, PartialAggregate};
use frogdb_search::wire::{FtShardReply, ShardSearchHit};

use crate::cursor_store::AggregateCursorStore;
use crate::scatter::MergeStrategy;

/// Turn a reply a search merge did not expect into an error to abort on, rather
/// than silently dropping it (issue #15 item 4).
///
/// A search fan-out (FT.SEARCH / FT.HYBRID / FT.AGGREGATE) only ever expects a
/// typed [`PartialResult::Ft`] reply. The one other shape it can legitimately
/// receive is the Continuation-Lock conflict fallback: a shard whose keys are
/// held by an in-flight MULTI/EXEC or Lua script rejects the scatter part with
/// the conflict error carried inside a [`PartialResult::Keyed`] reply (see
/// `dispatch_core::scatter_conflict_reply`). Surface that embedded error so the
/// merge aborts and the client sees the failure — never a truncated result set
/// returned as success. Any other shape is a dispatch bug and is surfaced
/// loudly instead of swallowed.
fn shard_conflict_or_bug(reply: PartialResult, cmd: &str) -> Response {
    if let Some((_, resp)) = reply
        .keyed_slice()
        .iter()
        .find(|(_, resp)| matches!(resp, Response::Error(_)))
    {
        return resp.clone();
    }
    Response::error(format!(
        "ERR {cmd}: shard returned an unmergeable reply shape"
    ))
}

/// Return the first embedded error from any shard, else `OK`.
///
/// Shared by the FT admin broadcasts (FT.CREATE / FT.ALTER / FT.DROPINDEX /
/// FT.SYNUPDATE): every shard applies the same schema mutation, so success is
/// just "no shard errored".
#[derive(Default)]
pub(crate) struct OkOrFirstError {
    error: Option<Response>,
}

impl MergeStrategy for OkOrFirstError {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_OK_OR_ERROR"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        for (_, resp) in reply.keyed_slice() {
            if let Response::Error(_) = resp {
                self.error = Some(resp.clone());
                break;
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        self.error.unwrap_or_else(Response::ok)
    }
}

/// Union tag values across shards (dedup + sort), short-circuiting on the first
/// embedded error. Used by FT.TAGVALS.
#[derive(Default)]
pub(crate) struct TagValsUnion {
    error: Option<Response>,
    values: HashSet<String>,
}

impl MergeStrategy for TagValsUnion {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_TAGVALS"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        for (_, resp) in reply.into_keyed_results() {
            match resp {
                Response::Error(_) => {
                    self.error = Some(resp);
                    return;
                }
                Response::Array(items) => {
                    for item in items {
                        if let Response::Bulk(Some(b)) = item
                            && let Ok(s) = std::str::from_utf8(&b)
                        {
                            self.values.insert(s.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }
        let mut sorted: Vec<String> = self.values.into_iter().collect();
        sorted.sort();
        Response::Array(
            sorted
                .into_iter()
                .map(|v| Response::bulk(Bytes::from(v)))
                .collect(),
        )
    }
}

/// Merge ES.ALL entries by `StreamId` ordering, applying COUNT after the merge.
pub(crate) struct EsAllMerge {
    count: Option<usize>,
    error: Option<Response>,
    entries: Vec<(StreamId, Response)>,
}

impl EsAllMerge {
    pub(crate) fn new(count: Option<usize>) -> Self {
        Self {
            count,
            error: None,
            entries: Vec::new(),
        }
    }
}

impl MergeStrategy for EsAllMerge {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "ES_ALL"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        for (_, resp) in reply.into_keyed_results() {
            // An embedded error is a real shard failure — surface it and abort,
            // like every other merge, rather than sorting it into the stream as
            // an unparseable data row (`StreamId::max()`) and returning it as a
            // spurious ES.ALL entry.
            if let Response::Error(_) = &resp {
                self.error = Some(resp);
                return;
            }
            // Each entry is [stream_id_string, [fields...]]; parse the id to sort.
            if let Response::Array(ref parts) = resp
                && let Some(Response::Bulk(Some(id_bytes))) = parts.first()
                && let Ok(id) = StreamId::parse(id_bytes)
            {
                self.entries.push((id, resp));
                continue;
            }
            // Fallback: sort unparseable entries to the end.
            self.entries.push((StreamId::max(), resp));
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }
        self.entries.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some(limit) = self.count {
            self.entries.truncate(limit);
        }
        Response::Array(self.entries.into_iter().map(|(_, r)| r).collect())
    }
}

/// Merge FT.SPELLCHECK suggestions by unioning per-term suggestions and keeping
/// the highest score per word, then re-sorting.
#[derive(Default)]
pub(crate) struct SpellcheckMerge {
    error: Option<Response>,
    /// term -> (suggestion word -> best score)
    term_map: HashMap<String, HashMap<String, f64>>,
}

impl MergeStrategy for SpellcheckMerge {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_SPELLCHECK"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        for (_, resp) in reply.into_keyed_results() {
            if let Response::Error(_) = &resp {
                self.error = Some(resp);
                return;
            }
            // resp: Array([ Array(["TERM", term, Array([Array([score, suggestion]), ...])]), ... ])
            if let Response::Array(term_entries) = resp {
                for entry in term_entries {
                    if let Response::Array(parts) = entry
                        && parts.len() >= 3
                    {
                        let term = match &parts[1] {
                            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
                            _ => continue,
                        };
                        if let Response::Array(suggestions) = &parts[2] {
                            let suggestions_map = self.term_map.entry(term).or_default();
                            for sugg in suggestions {
                                if let Response::Array(pair) = sugg
                                    && pair.len() >= 2
                                {
                                    let score = match &pair[0] {
                                        Response::Bulk(Some(b)) => std::str::from_utf8(b)
                                            .ok()
                                            .and_then(|s| s.parse().ok())
                                            .unwrap_or(0.0),
                                        _ => 0.0,
                                    };
                                    let word = match &pair[1] {
                                        Response::Bulk(Some(b)) => {
                                            String::from_utf8_lossy(b).to_string()
                                        }
                                        _ => continue,
                                    };
                                    let e = suggestions_map.entry(word).or_insert(0.0);
                                    if score > *e {
                                        *e = score;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }
        let mut term_entries: Vec<Response> = self
            .term_map
            .into_iter()
            .map(|(term, suggestions)| {
                let mut sorted: Vec<(String, f64)> = suggestions.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                let suggestion_items: Vec<Response> = sorted
                    .into_iter()
                    .map(|(word, score)| {
                        Response::Array(vec![
                            Response::bulk(Bytes::from(format!("{}", score))),
                            Response::bulk(Bytes::from(word)),
                        ])
                    })
                    .collect();
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"TERM")),
                    Response::bulk(Bytes::from(term)),
                    Response::Array(suggestion_items),
                ])
            })
            .collect();
        // Sort term entries for deterministic output.
        term_entries.sort_by_key(spellcheck_term_key);
        Response::Array(term_entries)
    }
}

/// Extract the misspelled term (element 1) from a spellcheck entry for sorting.
fn spellcheck_term_key(entry: &Response) -> String {
    if let Response::Array(parts) = entry
        && let Some(Response::Bulk(Some(b))) = parts.get(1)
    {
        return String::from_utf8_lossy(b).to_string();
    }
    String::new()
}

/// Encode one typed hit's fields as the flat `[name, value, …]` reply array.
fn fields_to_response(fields: Vec<(String, String)>) -> Response {
    let mut field_array = Vec::with_capacity(fields.len() * 2);
    for (name, value) in fields {
        field_array.push(Response::bulk(Bytes::from(name)));
        field_array.push(Response::bulk(Bytes::from(value)));
    }
    Response::Array(field_array)
}

/// Sort `(sort_key, hit)` pairs by SORTBY key, numerically when every
/// observed key parses as a number, lexically otherwise.
fn sort_by_key(hits: &mut [(String, ShardSearchHit)], numeric: bool, desc: bool) {
    hits.sort_by(|a, b| {
        let cmp = if numeric {
            let va: f64 = a.0.parse().unwrap_or(0.0);
            let vb: f64 = b.0.parse().unwrap_or(0.0);
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.0.cmp(&b.0)
        };
        if desc { cmp.reverse() } else { cmp }
    });
}

/// Merge FT.SEARCH hits: overfetch per shard, sort globally (SORTBY / KNN
/// ascending / BM25 descending), then apply the global OFFSET + LIMIT.
///
/// Operates on typed [`ShardSearchHit`]s from the shard's `FtShardReply` —
/// there is no positional decode; NOCONTENT / RETURN are already reflected in
/// each hit's `fields`.
pub(crate) struct FtSearchMerge {
    pub(crate) sortby_active: bool,
    pub(crate) sortby_desc: bool,
    pub(crate) sortby_numeric: bool,
    pub(crate) withscores: bool,
    pub(crate) is_knn: bool,
    pub(crate) global_offset: usize,
    pub(crate) global_limit: usize,
    pub(crate) error: Option<Response>,
    /// (SORTBY key, hit) — the key is precomputed during absorb so numeric
    /// detection sees every shard's values.
    pub(crate) all_hits: Vec<(String, ShardSearchHit)>,
    pub(crate) total: usize,
}

impl FtSearchMerge {
    /// Build the merge from the parsed request — the same struct the shards
    /// execute, so the merge cannot disagree with the shard grammar.
    pub(crate) fn from_request(request: &FtSearchRequest) -> Self {
        Self {
            sortby_active: request.sortby.is_some(),
            sortby_desc: matches!(request.sortby, Some((_, frogdb_search::SortOrder::Desc))),
            // Numeric-sort detection happens during the merge as sort values
            // arrive; it starts false here.
            sortby_numeric: false,
            withscores: request.withscores,
            is_knn: request.is_knn(),
            global_offset: request.offset,
            global_limit: request.limit,
            error: None,
            all_hits: Vec::new(),
            total: 0,
        }
    }
}

impl MergeStrategy for FtSearchMerge {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_SEARCH"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        match reply {
            PartialResult::Ft(FtShardReply::Search(Ok(shard_reply))) => {
                self.total += shard_reply.total;
                for hit in shard_reply.hits {
                    let sort_key = if self.sortby_active {
                        hit.sort_key()
                    } else {
                        String::new()
                    };
                    if self.sortby_active && !self.sortby_numeric && sort_key.parse::<f64>().is_ok()
                    {
                        self.sortby_numeric = true;
                    }
                    self.all_hits.push((sort_key, hit));
                }
            }
            PartialResult::Ft(FtShardReply::Search(Err(msg))) => {
                self.error = Some(Response::error(msg));
            }
            other => {
                self.error = Some(shard_conflict_or_bug(other, "FT.SEARCH"));
            }
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }

        if self.sortby_active {
            sort_by_key(&mut self.all_hits, self.sortby_numeric, self.sortby_desc);
        } else if self.is_knn {
            // KNN: sort by distance ascending (lower = more similar).
            self.all_hits.sort_by(|a, b| {
                a.1.score
                    .partial_cmp(&b.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            // BM25: sort by score descending (higher = more relevant).
            self.all_hits.sort_by(|a, b| {
                b.1.score
                    .partial_cmp(&a.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        let hits: Vec<_> = self
            .all_hits
            .into_iter()
            .skip(self.global_offset)
            .take(self.global_limit)
            .collect();

        // RediSearch-format response: [total, key1, [fields...], key2, [fields...], ...]
        let mut response_items = Vec::new();
        response_items.push(Response::Integer(self.total as i64));

        for (_sort_key, hit) in hits {
            response_items.push(Response::bulk(Bytes::from(hit.key)));
            if self.withscores {
                response_items.push(Response::bulk(Bytes::from(hit.score.to_string())));
            }
            if let Some(fields) = hit.fields {
                response_items.push(fields_to_response(fields));
            }
        }

        Response::Array(response_items)
    }
}

/// Merge FT.HYBRID hits: collect fused-score hits, sort (NOSORT / SORTBY /
/// fused-score descending), then apply the global OFFSET + LIMIT.
///
/// Shares the typed [`ShardSearchHit`] wire with FT.SEARCH; the hit's `score`
/// is the fused score.
pub(crate) struct FtHybridMerge {
    pub(crate) sortby_active: bool,
    pub(crate) sortby_desc: bool,
    pub(crate) sortby_numeric: bool,
    pub(crate) nosort: bool,
    pub(crate) global_offset: usize,
    pub(crate) global_limit: usize,
    pub(crate) error: Option<Response>,
    /// (SORTBY key, hit).
    pub(crate) all_hits: Vec<(String, ShardSearchHit)>,
    pub(crate) total: usize,
}

impl MergeStrategy for FtHybridMerge {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_HYBRID"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        match reply {
            PartialResult::Ft(FtShardReply::Search(Ok(shard_reply))) => {
                self.total += shard_reply.total;
                for hit in shard_reply.hits {
                    let sort_key = if self.sortby_active {
                        hit.sort_key()
                    } else {
                        String::new()
                    };
                    self.all_hits.push((sort_key, hit));
                }
            }
            PartialResult::Ft(FtShardReply::Search(Err(msg))) => {
                self.error = Some(Response::error(msg));
            }
            other => {
                self.error = Some(shard_conflict_or_bug(other, "FT.HYBRID"));
            }
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }

        if self.nosort {
            // No sorting.
        } else if self.sortby_active {
            sort_by_key(&mut self.all_hits, self.sortby_numeric, self.sortby_desc);
        } else {
            // Default: sort by fused score descending (higher = better).
            self.all_hits.sort_by(|a, b| {
                b.1.score
                    .partial_cmp(&a.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        let hits: Vec<_> = self
            .all_hits
            .into_iter()
            .skip(self.global_offset)
            .take(self.global_limit)
            .collect();

        // RediSearch-format response: [total, key1, [fields...], key2, [fields...], ...]
        let mut response_items = Vec::new();
        response_items.push(Response::Integer(self.total as i64));

        for (_sort_key, hit) in hits {
            response_items.push(Response::bulk(Bytes::from(hit.key)));
            if let Some(fields) = hit.fields {
                response_items.push(fields_to_response(fields));
            }
        }

        Response::Array(response_items)
    }
}

/// Merge FT.AGGREGATE partial aggregates across shards, run the merge pipeline,
/// and (optionally) stash the tail in the cursor store for FT.CURSOR READ.
pub(crate) struct FtAggregateMerge {
    pub(crate) steps: Vec<AggregateStep>,
    pub(crate) withcursor: bool,
    pub(crate) cursor_count: usize,
    pub(crate) cursor_maxidle_ms: u64,
    pub(crate) index_name: Bytes,
    pub(crate) cursor_store: Arc<AggregateCursorStore>,
    pub(crate) error: Option<Response>,
    pub(crate) partials: Vec<PartialAggregate>,
}

impl MergeStrategy for FtAggregateMerge {
    type Reply = PartialResult;

    fn name(&self) -> &'static str {
        "FT_AGGREGATE"
    }

    fn absorb(&mut self, _shard_id: usize, reply: PartialResult) {
        if self.error.is_some() {
            return;
        }
        match reply {
            // The typed partial aggregate crosses the shard boundary as-is —
            // no reducer-state decode.
            PartialResult::Ft(FtShardReply::Aggregate(Ok(partial))) => self.partials.push(partial),
            PartialResult::Ft(FtShardReply::Aggregate(Err(msg))) => {
                self.error = Some(Response::error(msg));
            }
            other => {
                self.error = Some(shard_conflict_or_bug(other, "FT.AGGREGATE"));
            }
        }
    }

    fn finish(self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }

        let FtAggregateMerge {
            steps,
            withcursor,
            cursor_count,
            cursor_maxidle_ms,
            index_name,
            cursor_store,
            partials,
            ..
        } = *self;

        // Merge all partials and apply SORTBY + LIMIT.
        let rows = aggregate::merge_partials(partials, &steps);

        if withcursor && cursor_count > 0 && rows.len() > cursor_count {
            // Return the first batch and stash the remainder in the cursor store.
            let (first_batch, remaining) = rows.split_at(cursor_count);

            let mut result_items = Vec::new();
            result_items.push(Response::Integer(first_batch.len() as i64));
            for row in first_batch {
                result_items.push(row_to_response(row));
            }

            let cursor_id = {
                let idx_name = std::str::from_utf8(&index_name).unwrap_or("").to_string();
                cursor_store.create_cursor(
                    remaining.to_vec(),
                    cursor_count,
                    idx_name,
                    Duration::from_millis(cursor_maxidle_ms),
                )
            };

            Response::Array(vec![
                Response::Array(result_items),
                Response::Integer(cursor_id as i64),
            ])
        } else if withcursor {
            // All rows fit in one batch — cursor_id 0 (done).
            let mut result_items = Vec::new();
            result_items.push(Response::Integer(rows.len() as i64));
            for row in &rows {
                result_items.push(row_to_response(row));
            }
            Response::Array(vec![Response::Array(result_items), Response::Integer(0)])
        } else {
            // Standard non-cursor response.
            let mut response_items = Vec::new();
            response_items.push(Response::Integer(rows.len() as i64));
            for row in &rows {
                response_items.push(row_to_response(row));
            }
            Response::Array(response_items)
        }
    }
}

/// Encode one aggregate row as a flat `[field, value, …]` array.
fn row_to_response(row: &aggregate::Row) -> Response {
    let mut field_array = Vec::with_capacity(row.len() * 2);
    for (k, v) in row {
        field_array.push(Response::bulk(Bytes::from(k.clone())));
        field_array.push(Response::bulk(Bytes::from(v.clone())));
    }
    Response::Array(field_array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_search::SortValue;
    use frogdb_search::wire::ShardSearchReply;

    fn hit(
        key: &str,
        score: f32,
        sort_value: Option<SortValue>,
        fields: Option<Vec<(&str, &str)>>,
    ) -> ShardSearchHit {
        ShardSearchHit {
            key: key.to_string(),
            score,
            sort_value,
            fields: fields.map(|fs| {
                fs.into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            }),
        }
    }

    fn search_reply(total: usize, hits: Vec<ShardSearchHit>) -> PartialResult {
        PartialResult::ft(FtShardReply::Search(Ok(ShardSearchReply { total, hits })))
    }

    fn parse_search(parts: &[&str]) -> FtSearchRequest {
        let args: Vec<Bytes> = parts.iter().map(|s| Bytes::from(s.to_string())).collect();
        FtSearchRequest::parse(&args)
    }

    /// Unwrap the reply array and return (total, tail items).
    fn unwrap_reply(resp: Response) -> (i64, Vec<Response>) {
        match resp {
            Response::Array(items) => {
                let mut it = items.into_iter();
                let total = match it.next() {
                    Some(Response::Integer(n)) => n,
                    other => panic!("expected total integer, got {other:?}"),
                };
                (total, it.collect())
            }
            other => panic!("expected array reply, got {other:?}"),
        }
    }

    fn key_at(items: &[Response], idx: usize) -> String {
        match &items[idx] {
            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
            other => panic!("expected bulk key at {idx}, got {other:?}"),
        }
    }

    #[test]
    fn test_search_merge_bm25_descending_across_shards() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&["hello"])));
        merge.absorb(
            0,
            search_reply(
                2,
                vec![
                    hit("a", 1.0, None, Some(vec![("t", "x")])),
                    hit("b", 3.0, None, Some(vec![("t", "y")])),
                ],
            ),
        );
        merge.absorb(1, search_reply(1, vec![hit("c", 2.0, None, Some(vec![]))]));

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 3);
        // key + fields array per hit
        assert_eq!(items.len(), 6);
        assert_eq!(key_at(&items, 0), "b");
        assert_eq!(key_at(&items, 2), "c");
        assert_eq!(key_at(&items, 4), "a");
        assert_eq!(
            items[1],
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"t")),
                Response::bulk(Bytes::from_static(b"y")),
            ])
        );
    }

    #[test]
    fn test_search_merge_sortby_numeric_ascending() {
        // Numeric detection must fire even though the values arrive as strings
        // ("10" < "2" lexically but 2 < 10 numerically).
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*", "SORTBY", "price", "ASC",
        ])));
        merge.absorb(
            0,
            search_reply(
                1,
                vec![hit("a", 0.0, Some(SortValue::F64(10.0)), Some(vec![]))],
            ),
        );
        merge.absorb(
            1,
            search_reply(
                1,
                vec![hit("b", 0.0, Some(SortValue::F64(2.0)), Some(vec![]))],
            ),
        );

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 2);
        assert_eq!(key_at(&items, 0), "b");
        assert_eq!(key_at(&items, 2), "a");
    }

    #[test]
    fn test_search_merge_sortby_string_desc() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*", "SORTBY", "name", "DESC",
        ])));
        merge.absorb(
            0,
            search_reply(
                2,
                vec![
                    hit("a", 0.0, Some(SortValue::Str("apple".into())), Some(vec![])),
                    hit("z", 0.0, Some(SortValue::Str("zebra".into())), Some(vec![])),
                ],
            ),
        );

        let (_, items) = unwrap_reply(merge.finish());
        assert_eq!(key_at(&items, 0), "z");
        assert_eq!(key_at(&items, 2), "a");
    }

    #[test]
    fn test_search_merge_withscores_and_nocontent() {
        // NOCONTENT hits carry fields=None (the shard already suppressed
        // content); WITHSCORES adds the score element after each key.
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*",
            "WITHSCORES",
            "NOCONTENT",
        ])));
        merge.absorb(0, search_reply(1, vec![hit("a", 1.5, None, None)]));

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 1);
        assert_eq!(items.len(), 2); // key + score, no fields array
        assert_eq!(key_at(&items, 0), "a");
        assert_eq!(items[1], Response::bulk(Bytes::from("1.5")));
    }

    #[test]
    fn test_search_merge_global_offset_limit() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*",
            "LIMIT",
            "1",
            "2",
            "NOCONTENT",
        ])));
        merge.absorb(
            0,
            search_reply(
                4,
                vec![
                    hit("s4", 4.0, None, None),
                    hit("s2", 2.0, None, None),
                    hit("s3", 3.0, None, None),
                    hit("s1", 1.0, None, None),
                ],
            ),
        );

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 4); // total is pre-pagination
        assert_eq!(items.len(), 2);
        assert_eq!(key_at(&items, 0), "s3");
        assert_eq!(key_at(&items, 1), "s2");
    }

    #[test]
    fn test_search_merge_knn_ascending_distance() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*=>[KNN 10 @v $b]",
            "NOCONTENT",
        ])));
        assert!(merge.is_knn);
        merge.absorb(0, search_reply(1, vec![hit("far", 9.0, None, None)]));
        merge.absorb(1, search_reply(1, vec![hit("near", 0.5, None, None)]));

        let (_, items) = unwrap_reply(merge.finish());
        assert_eq!(key_at(&items, 0), "near");
        assert_eq!(key_at(&items, 1), "far");
    }

    #[test]
    fn test_search_merge_shard_error_wins() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&["*"])));
        merge.absorb(0, search_reply(1, vec![hit("a", 1.0, None, Some(vec![]))]));
        merge.absorb(
            1,
            PartialResult::ft(FtShardReply::Search(Err("idx: no such index".to_string()))),
        );

        match merge.finish() {
            Response::Error(msg) => {
                assert_eq!(&msg[..], b"idx: no such index");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// Regression for issue #15 item 4: a shard whose keys are held by a
    /// Continuation Lock (MULTI/EXEC or Lua in flight) rejects the scatter part
    /// with the conflict error carried inside a `Keyed` reply. The merge must
    /// surface that error and abort — never silently drop it and return the
    /// surviving shards' hits as a (truncated) success, which is silent data
    /// loss to the client.
    #[test]
    fn test_search_merge_continuation_lock_error_not_swallowed() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&["*"])));
        merge.absorb(0, search_reply(1, vec![hit("a", 1.0, None, Some(vec![]))]));
        merge.absorb(
            1,
            PartialResult::keyed(vec![(
                Bytes::from_static(b"k"),
                Response::error("ERR shard busy with continuation lock"),
            )]),
        );

        match merge.finish() {
            Response::Error(msg) => {
                assert_eq!(&msg[..], b"ERR shard busy with continuation lock");
            }
            other => panic!("expected the conflict error to abort the merge, got {other:?}"),
        }
    }

    /// A search merge must not silently swallow a wholly-unexpected reply shape
    /// either: an empty/errorless non-`Ft` reply is a dispatch bug, and
    /// returning a truncated result set as success would hide it. It must error
    /// loudly instead.
    #[test]
    fn test_search_merge_unexpected_reply_shape_errors_loudly() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&["*"])));
        merge.absorb(0, search_reply(1, vec![hit("a", 1.0, None, Some(vec![]))]));
        merge.absorb(1, PartialResult::Count(3));

        assert!(
            matches!(merge.finish(), Response::Error(_)),
            "an unmergeable reply shape must surface as an error, not silent truncation"
        );
    }

    /// A clean two-shard FT.SEARCH still merges to a success array (guards the
    /// swallow fix from over-firing on the happy path).
    #[test]
    fn test_search_merge_clean_two_shards_regression() {
        let mut merge = Box::new(FtSearchMerge::from_request(&parse_search(&[
            "*",
            "NOCONTENT",
        ])));
        merge.absorb(0, search_reply(1, vec![hit("a", 2.0, None, None)]));
        merge.absorb(1, search_reply(1, vec![hit("b", 1.0, None, None)]));

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 2);
        assert_eq!(key_at(&items, 0), "a");
        assert_eq!(key_at(&items, 1), "b");
    }

    #[test]
    fn test_hybrid_merge_continuation_lock_error_not_swallowed() {
        let mut merge = Box::new(FtHybridMerge {
            sortby_active: false,
            sortby_desc: false,
            sortby_numeric: false,
            nosort: false,
            global_offset: 0,
            global_limit: 10,
            error: None,
            all_hits: Vec::new(),
            total: 0,
        });
        merge.absorb(0, search_reply(1, vec![hit("low", 0.2, None, None)]));
        merge.absorb(
            1,
            PartialResult::keyed(vec![(
                Bytes::from_static(b"k"),
                Response::error("ERR shard busy with continuation lock"),
            )]),
        );

        match merge.finish() {
            Response::Error(msg) => assert_eq!(&msg[..], b"ERR shard busy with continuation lock"),
            other => panic!("expected the conflict error to abort the merge, got {other:?}"),
        }
    }

    #[test]
    fn test_aggregate_merge_continuation_lock_error_not_swallowed() {
        let mut merge = Box::new(FtAggregateMerge {
            steps: Vec::new(),
            withcursor: false,
            cursor_count: 0,
            cursor_maxidle_ms: 300_000,
            index_name: Bytes::from_static(b"idx"),
            cursor_store: Arc::new(AggregateCursorStore::new()),
            error: None,
            partials: Vec::new(),
        });
        merge.absorb(
            0,
            PartialResult::keyed(vec![(
                Bytes::from_static(b"k"),
                Response::error("ERR shard busy with continuation lock"),
            )]),
        );

        match merge.finish() {
            Response::Error(msg) => assert_eq!(&msg[..], b"ERR shard busy with continuation lock"),
            other => panic!("expected the conflict error to abort the merge, got {other:?}"),
        }
    }

    #[test]
    fn test_search_merge_full_grammar_highlight_sortby_limit() {
        // A full-grammar query: HIGHLIGHT (applied shard-side, visible as
        // rewritten field values) + SORTBY + LIMIT merged across two shards.
        // Option agreement between coordinator and shard is by construction:
        // this merge is built from the same request the shards execute.
        let request = parse_search(&[
            "hello",
            "HIGHLIGHT",
            "FIELDS",
            "1",
            "t",
            "TAGS",
            "<em>",
            "</em>",
            "SORTBY",
            "rank",
            "DESC",
            "LIMIT",
            "0",
            "2",
        ]);
        assert!(request.highlight.is_some());
        let mut merge = Box::new(FtSearchMerge::from_request(&request));
        merge.absorb(
            0,
            search_reply(
                2,
                vec![
                    hit(
                        "a",
                        0.0,
                        Some(SortValue::F64(1.0)),
                        Some(vec![("t", "<em>hello</em> a")]),
                    ),
                    hit(
                        "c",
                        0.0,
                        Some(SortValue::F64(3.0)),
                        Some(vec![("t", "<em>hello</em> c")]),
                    ),
                ],
            ),
        );
        merge.absorb(
            1,
            search_reply(
                1,
                vec![hit(
                    "b",
                    0.0,
                    Some(SortValue::F64(2.0)),
                    Some(vec![("t", "<em>hello</em> b")]),
                )],
            ),
        );

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 3);
        assert_eq!(items.len(), 4); // LIMIT 0 2 → two hits, key + fields each
        assert_eq!(key_at(&items, 0), "c");
        assert_eq!(
            items[1],
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"t")),
                Response::bulk(Bytes::from_static(b"<em>hello</em> c")),
            ])
        );
        assert_eq!(key_at(&items, 2), "b");
    }

    #[test]
    fn test_hybrid_merge_fused_score_descending() {
        let mut merge = Box::new(FtHybridMerge {
            sortby_active: false,
            sortby_desc: false,
            sortby_numeric: false,
            nosort: false,
            global_offset: 0,
            global_limit: 10,
            error: None,
            all_hits: Vec::new(),
            total: 0,
        });
        merge.absorb(
            0,
            search_reply(1, vec![hit("low", 0.2, None, Some(vec![("t", "x")]))]),
        );
        merge.absorb(1, search_reply(1, vec![hit("high", 0.9, None, None)]));

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 2);
        // "high" has no content (missing doc) → key only; "low" has fields.
        assert_eq!(items.len(), 3);
        assert_eq!(key_at(&items, 0), "high");
        assert_eq!(key_at(&items, 1), "low");
        assert!(matches!(items[2], Response::Array(_)));
    }

    #[test]
    fn test_aggregate_merge_typed_partials() {
        use frogdb_search::FtAggregateRequest;
        use frogdb_search::aggregate::execute_shard_local;

        let args: Vec<Bytes> = [
            "*", "GROUPBY", "1", "@color", "REDUCE", "COUNT", "0", "AS", "count",
        ]
        .iter()
        .map(|s| Bytes::from(s.to_string()))
        .collect();
        let request = FtAggregateRequest::parse(&args).unwrap();

        // Two shards produce typed partials from disjoint rows; the merge must
        // sum the per-group counts.
        let rows1 = vec![
            vec![("color".to_string(), "red".to_string())],
            vec![("color".to_string(), "blue".to_string())],
        ];
        let rows2 = vec![vec![("color".to_string(), "red".to_string())]];
        let partial1 = execute_shard_local(&rows1, &request.steps);
        let partial2 = execute_shard_local(&rows2, &request.steps);

        let mut merge = Box::new(FtAggregateMerge {
            steps: request.steps.clone(),
            withcursor: false,
            cursor_count: 0,
            cursor_maxidle_ms: 300_000,
            index_name: Bytes::from_static(b"idx"),
            cursor_store: Arc::new(AggregateCursorStore::new()),
            error: None,
            partials: Vec::new(),
        });
        merge.absorb(0, PartialResult::ft(FtShardReply::Aggregate(Ok(partial1))));
        merge.absorb(1, PartialResult::ft(FtShardReply::Aggregate(Ok(partial2))));

        let (total, items) = unwrap_reply(merge.finish());
        assert_eq!(total, 2); // two groups
        let mut groups: Vec<(String, String)> = items
            .iter()
            .map(|row| match row {
                Response::Array(kv) => {
                    let mut color = String::new();
                    let mut count = String::new();
                    for pair in kv.chunks(2) {
                        if let (Response::Bulk(Some(k)), Response::Bulk(Some(v))) =
                            (&pair[0], &pair[1])
                        {
                            match k.as_ref() {
                                b"color" => color = String::from_utf8_lossy(v).to_string(),
                                b"count" => count = String::from_utf8_lossy(v).to_string(),
                                _ => {}
                            }
                        }
                    }
                    (color, count)
                }
                other => panic!("expected row array, got {other:?}"),
            })
            .collect();
        groups.sort();
        assert_eq!(
            groups,
            vec![
                ("blue".to_string(), "1".to_string()),
                ("red".to_string(), "2".to_string()),
            ]
        );
    }

    /// Finding 2: `EsAllMerge` must surface an embedded `Response::Error` rather
    /// than pushing it as an unparseable data row sorted via `StreamId::max()`
    /// (which would return the error string as a spurious ES.ALL stream entry).
    #[test]
    fn test_es_all_merge_surfaces_embedded_error() {
        let mut merge = Box::new(EsAllMerge::new(None));
        // A healthy shard contributes a well-formed entry...
        merge.absorb(
            0,
            PartialResult::keyed(vec![(
                Bytes::from_static(b"k"),
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"1-0")),
                    Response::Array(vec![]),
                ]),
            )]),
        );
        // ...another shard reports an error.
        merge.absorb(
            1,
            PartialResult::keyed(vec![(
                Bytes::from_static(b"k"),
                Response::error("ERR es.all failed on shard"),
            )]),
        );

        match merge.finish() {
            Response::Error(msg) => assert_eq!(&msg[..], b"ERR es.all failed on shard"),
            other => panic!("expected the embedded error to surface, got {other:?}"),
        }
    }

    #[test]
    fn test_aggregate_merge_shard_error_wins() {
        let mut merge = Box::new(FtAggregateMerge {
            steps: Vec::new(),
            withcursor: false,
            cursor_count: 0,
            cursor_maxidle_ms: 300_000,
            index_name: Bytes::from_static(b"idx"),
            cursor_store: Arc::new(AggregateCursorStore::new()),
            error: None,
            partials: Vec::new(),
        });
        merge.absorb(
            0,
            PartialResult::ft(FtShardReply::Aggregate(Err("ERR bad pipeline".to_string()))),
        );
        match merge.finish() {
            Response::Error(msg) => assert_eq!(&msg[..], b"ERR bad pipeline"),
            other => panic!("expected error, got {other:?}"),
        }
    }
}

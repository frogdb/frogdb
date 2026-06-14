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
use frogdb_search::aggregate::{self, AggregateStep, PartialAggregate, PartialReducerState};

use crate::cursor_store::AggregateCursorStore;
use crate::scatter::MergeStrategy;

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
        for (_, resp) in &reply.results {
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
        for (_, resp) in reply.results {
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
    entries: Vec<(StreamId, Response)>,
}

impl EsAllMerge {
    pub(crate) fn new(count: Option<usize>) -> Self {
        Self {
            count,
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
        for (_, resp) in reply.results {
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
        for (_, resp) in reply.results {
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

/// Merge FT.SEARCH hits: overfetch per shard, sort globally (SORTBY / KNN
/// ascending / BM25 descending), then apply the global OFFSET + LIMIT.
pub(crate) struct FtSearchMerge {
    pub(crate) sortby_active: bool,
    pub(crate) sortby_desc: bool,
    pub(crate) sortby_numeric: bool,
    pub(crate) nocontent: bool,
    pub(crate) withscores: bool,
    pub(crate) is_knn: bool,
    pub(crate) global_offset: usize,
    pub(crate) global_limit: usize,
    pub(crate) error: Option<Response>,
    /// (key, score, sort_value, raw response)
    pub(crate) all_hits: Vec<(Bytes, f32, String, Response)>,
    pub(crate) total: usize,
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
        for (key, resp) in reply.results {
            if let Response::Error(_) = &resp {
                self.error = Some(resp);
                return;
            }
            if key.as_ref() == b"__ft_total__" {
                if let Response::Integer(n) = &resp {
                    self.total += *n as usize;
                }
                continue;
            }
            if let Response::Array(ref items) = resp
                && !items.is_empty()
                && let Response::Bulk(Some(ref score_bytes)) = items[0]
                && let Ok(s) = std::str::from_utf8(score_bytes)
                && let Ok(score) = s.parse::<f32>()
            {
                // Extract sort value if SORTBY is active (second element).
                let sort_val = if self.sortby_active && items.len() > 1 {
                    if let Response::Bulk(Some(ref sv_bytes)) = items[1] {
                        let sv = std::str::from_utf8(sv_bytes).unwrap_or("").to_string();
                        if !self.sortby_numeric && sv.parse::<f64>().is_ok() {
                            self.sortby_numeric = true;
                        }
                        sv
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                self.all_hits.push((key, score, sort_val, resp));
                continue;
            }
            self.all_hits.push((key, 0.0, String::new(), resp));
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }

        if self.sortby_active {
            let numeric = self.sortby_numeric;
            let desc = self.sortby_desc;
            self.all_hits.sort_by(|a, b| {
                let cmp = if numeric {
                    let va: f64 = a.2.parse().unwrap_or(0.0);
                    let vb: f64 = b.2.parse().unwrap_or(0.0);
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a.2.cmp(&b.2)
                };
                if desc { cmp.reverse() } else { cmp }
            });
        } else if self.is_knn {
            // KNN: sort by distance ascending (lower = more similar).
            self.all_hits
                .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            // BM25: sort by score descending (higher = more relevant).
            self.all_hits
                .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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

        for (key, _score, _sort_val, resp) in hits {
            response_items.push(Response::bulk(key));

            if let Response::Array(items) = resp {
                let mut idx = 1; // skip internal score

                // Skip sort value element if SORTBY was active.
                if self.sortby_active && idx < items.len() {
                    idx += 1;
                }

                if self.withscores && idx < items.len() {
                    response_items.push(items[idx].clone());
                    idx += 1;
                }

                if !self.nocontent && idx < items.len() {
                    response_items.push(items[idx].clone());
                }
            }
        }

        Response::Array(response_items)
    }
}

/// Merge FT.HYBRID hits: collect fused-score hits, sort (NOSORT / SORTBY /
/// fused-score descending), then apply the global OFFSET + LIMIT.
pub(crate) struct FtHybridMerge {
    pub(crate) sortby_active: bool,
    pub(crate) sortby_desc: bool,
    pub(crate) sortby_numeric: bool,
    pub(crate) nosort: bool,
    pub(crate) global_offset: usize,
    pub(crate) global_limit: usize,
    pub(crate) error: Option<Response>,
    /// (key, fused_score, sort_value, raw response)
    pub(crate) all_hits: Vec<(Bytes, f32, String, Response)>,
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
        for (key, resp) in reply.results {
            if let Response::Error(_) = &resp {
                self.error = Some(resp);
                return;
            }
            if key.as_ref() == b"__ft_total__" {
                if let Response::Integer(n) = &resp {
                    self.total += *n as usize;
                }
                continue;
            }
            if let Response::Array(ref items) = resp
                && !items.is_empty()
                && let Response::Bulk(Some(ref score_bytes)) = items[0]
                && let Ok(s) = std::str::from_utf8(score_bytes)
                && let Ok(score) = s.parse::<f32>()
            {
                self.all_hits.push((key, score, String::new(), resp));
                continue;
            }
            self.all_hits.push((key, 0.0, String::new(), resp));
        }
    }

    fn finish(mut self: Box<Self>) -> Response {
        if let Some(err) = self.error {
            return err;
        }

        if self.nosort {
            // No sorting.
        } else if self.sortby_active {
            let numeric = self.sortby_numeric;
            let desc = self.sortby_desc;
            self.all_hits.sort_by(|a, b| {
                let cmp = if numeric {
                    let va: f64 = a.2.parse().unwrap_or(0.0);
                    let vb: f64 = b.2.parse().unwrap_or(0.0);
                    va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a.2.cmp(&b.2)
                };
                if desc { cmp.reverse() } else { cmp }
            });
        } else {
            // Default: sort by fused score descending (higher = better).
            self.all_hits
                .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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

        for (key, _score, _sort_val, resp) in hits {
            response_items.push(Response::bulk(key));

            if let Response::Array(items) = resp {
                let idx = 1; // skip internal fused score
                if idx < items.len() {
                    response_items.push(items[idx].clone());
                }
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
        // Surface a shard-side aggregate error verbatim.
        for (key, resp) in &reply.results {
            if key.as_ref() == b"__ft_error__" {
                self.error = Some(resp.clone());
                return;
            }
        }

        // Deserialize partial results back into a PartialAggregate.
        let mut groups = Vec::new();
        for (_key, resp) in reply.results {
            if let Response::Array(entry) = resp {
                // Entry format: [field1, val1, field2, val2, ..., Array([states])]
                let mut key_fields = Vec::new();
                let mut state_items = Vec::new();

                for item in &entry {
                    if let Response::Array(states) = item {
                        state_items = states.clone();
                    }
                }

                // Parse key fields (all items except the trailing Array).
                let mut idx = 0;
                while idx + 1 < entry.len() {
                    if matches!(&entry[idx + 1], Response::Array(_)) {
                        break;
                    }
                    if let (Response::Bulk(Some(k)), Response::Bulk(Some(v))) =
                        (&entry[idx], &entry[idx + 1])
                    {
                        let k = String::from_utf8_lossy(k).to_string();
                        let v = String::from_utf8_lossy(v).to_string();
                        key_fields.push((k, v));
                    }
                    idx += 2;
                }

                let states = parse_partial_reducer_states(&state_items);
                groups.push((key_fields, states));
            }
        }

        self.partials.push(PartialAggregate { groups });
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

/// Decode the serialized partial reducer state array a shard returns for one
/// aggregate group.
fn parse_partial_reducer_states(state_items: &[Response]) -> Vec<PartialReducerState> {
    let mut states = Vec::new();
    let mut si = 0;
    while si < state_items.len() {
        if let Response::Bulk(Some(ref tag)) = state_items[si] {
            match tag.as_ref() {
                b"COUNT" => {
                    si += 1;
                    if let Response::Integer(c) = &state_items[si] {
                        states.push(PartialReducerState::Count(*c));
                    }
                    si += 1;
                }
                b"SUM" => {
                    si += 1;
                    if let Response::Bulk(Some(ref v)) = state_items[si] {
                        let val: f64 = String::from_utf8_lossy(v).parse().unwrap_or(0.0);
                        states.push(PartialReducerState::Sum(val));
                    }
                    si += 1;
                }
                b"MIN" => {
                    si += 1;
                    if let Response::Bulk(Some(ref v)) = state_items[si] {
                        let val: f64 = String::from_utf8_lossy(v).parse().unwrap_or(f64::INFINITY);
                        states.push(PartialReducerState::Min(val));
                    }
                    si += 1;
                }
                b"MAX" => {
                    si += 1;
                    if let Response::Bulk(Some(ref v)) = state_items[si] {
                        let val: f64 = String::from_utf8_lossy(v)
                            .parse()
                            .unwrap_or(f64::NEG_INFINITY);
                        states.push(PartialReducerState::Max(val));
                    }
                    si += 1;
                }
                b"AVG" => {
                    si += 1;
                    let sum = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    si += 1;
                    let count = if let Response::Integer(c) = &state_items[si] {
                        *c
                    } else {
                        0
                    };
                    si += 1;
                    states.push(PartialReducerState::Avg(sum, count));
                }
                b"COUNT_DISTINCT" => {
                    si += 1;
                    let num = if let Response::Integer(n) = &state_items[si] {
                        *n as usize
                    } else {
                        0
                    };
                    si += 1;
                    let mut set = HashSet::new();
                    for _ in 0..num {
                        if si < state_items.len() {
                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                set.insert(String::from_utf8_lossy(v).to_string());
                            }
                            si += 1;
                        }
                    }
                    states.push(PartialReducerState::CountDistinct(set));
                }
                b"COUNT_DISTINCTISH" => {
                    si += 1;
                    let regs = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        v.to_vec()
                    } else {
                        vec![0u8; 256]
                    };
                    si += 1;
                    states.push(PartialReducerState::CountDistinctish(regs));
                }
                b"TOLIST" => {
                    si += 1;
                    let num = if let Response::Integer(n) = &state_items[si] {
                        *n as usize
                    } else {
                        0
                    };
                    si += 1;
                    let mut list = Vec::with_capacity(num);
                    for _ in 0..num {
                        if si < state_items.len() {
                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                list.push(String::from_utf8_lossy(v).to_string());
                            }
                            si += 1;
                        }
                    }
                    states.push(PartialReducerState::Tolist(list));
                }
                b"FIRST_VALUE" => {
                    si += 1;
                    let value = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        let s = String::from_utf8_lossy(v).to_string();
                        if s.is_empty() { None } else { Some(s) }
                    } else {
                        None
                    };
                    si += 1;
                    let sort_key = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        let s = String::from_utf8_lossy(v).to_string();
                        if s.is_empty() { None } else { Some(s) }
                    } else {
                        None
                    };
                    si += 1;
                    let sort_asc = if let Response::Integer(a) = &state_items[si] {
                        *a != 0
                    } else {
                        true
                    };
                    si += 1;
                    states.push(PartialReducerState::FirstValue {
                        value,
                        sort_key,
                        sort_asc,
                    });
                }
                b"STDDEV" => {
                    si += 1;
                    let sum = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    si += 1;
                    let sum_sq = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        String::from_utf8_lossy(v).parse().unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    si += 1;
                    let count = if let Response::Integer(c) = &state_items[si] {
                        *c
                    } else {
                        0
                    };
                    si += 1;
                    states.push(PartialReducerState::Stddev { sum, sum_sq, count });
                }
                b"QUANTILE" => {
                    si += 1;
                    let quantile: f64 = if let Response::Bulk(Some(ref v)) = state_items[si] {
                        String::from_utf8_lossy(v).parse().unwrap_or(0.5)
                    } else {
                        0.5
                    };
                    si += 1;
                    let num = if let Response::Integer(n) = &state_items[si] {
                        *n as usize
                    } else {
                        0
                    };
                    si += 1;
                    let mut values = Vec::with_capacity(num);
                    for _ in 0..num {
                        if si < state_items.len()
                            && let Response::Bulk(Some(ref v)) = state_items[si]
                            && let Ok(f) = String::from_utf8_lossy(v).parse::<f64>()
                        {
                            values.push(f);
                        }
                        if si < state_items.len() {
                            si += 1;
                        }
                    }
                    states.push(PartialReducerState::Quantile { values, quantile });
                }
                b"RANDOM_SAMPLE" => {
                    si += 1;
                    let count = if let Response::Integer(c) = &state_items[si] {
                        *c as usize
                    } else {
                        0
                    };
                    si += 1;
                    let seen = if let Response::Integer(s) = &state_items[si] {
                        *s as usize
                    } else {
                        0
                    };
                    si += 1;
                    let num = if let Response::Integer(n) = &state_items[si] {
                        *n as usize
                    } else {
                        0
                    };
                    si += 1;
                    let mut reservoir = Vec::with_capacity(num);
                    for _ in 0..num {
                        if si < state_items.len() {
                            if let Response::Bulk(Some(ref v)) = state_items[si] {
                                reservoir.push(String::from_utf8_lossy(v).to_string());
                            }
                            si += 1;
                        }
                    }
                    states.push(PartialReducerState::RandomSample {
                        reservoir,
                        count,
                        seen,
                    });
                }
                _ => {
                    si += 1;
                }
            }
        } else {
            si += 1;
        }
    }
    states
}

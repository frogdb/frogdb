use bytes::Bytes;
use frogdb_search::SearchOptions;
use frogdb_search::wire::{FtAggregateRequest, FtSearchRequest, ShardSearchHit, ShardSearchReply};

use super::super::worker::ShardWorker;
use super::{parse_knn_query, substitute_params};
use crate::store::Store;

/// Convert an index-level search result into the typed shard reply, applying
/// the request's RETURN filter and NOCONTENT.
fn to_shard_reply(
    search_result: frogdb_search::SearchResult,
    request: &FtSearchRequest,
) -> ShardSearchReply {
    let hits = search_result
        .hits
        .into_iter()
        .map(|hit| {
            let fields = if request.nocontent {
                None
            } else {
                let fields: Vec<(String, String)> = match &request.return_fields {
                    Some(rf) => hit
                        .fields
                        .into_iter()
                        .filter(|(name, _)| rf.contains(name))
                        .collect(),
                    None => hit.fields,
                };
                Some(fields)
            };
            ShardSearchHit {
                key: hit.key,
                score: hit.score,
                sort_value: if request.sortby.is_some() {
                    hit.sort_value
                } else {
                    None
                },
                fields,
            }
        })
        .collect();

    ShardSearchReply {
        total: search_result.total,
        hits,
    }
}

impl ShardWorker {
    pub(crate) fn execute_ft_search(
        &mut self,
        index_name: &Bytes,
        request: &FtSearchRequest,
    ) -> Result<ShardSearchReply, String> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.search.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => return Err(format!("{}: no such index", name)),
        };

        // Check for KNN vector query BEFORE param substitution, since substitution
        // replaces $BLOB with raw binary bytes which corrupts the query string.
        // The KNN path fetches the blob directly from params.get().
        if let Some((knn_k, knn_field, knn_param)) = parse_knn_query(&request.query) {
            let blob = match request.params.get(&knn_param) {
                Some(b) => b.clone(),
                None => return Err(format!("ERR No such parameter '{}'", knn_param)),
            };
            return execute_ft_knn_search(&mut self.store, idx, request, &knn_field, &blob, knn_k);
        }

        // Substitute $param references in the query string (only for non-KNN queries)
        let query_str = if !request.params.is_empty() {
            substitute_params(&request.query, &request.params)
        } else {
            request.query.clone()
        };

        let search_result = idx
            .search(&query_str, &request.index_options())
            .map_err(|e| format!("ERR {}", e))?;

        Ok(to_shard_reply(search_result, request))
    }
}

fn execute_ft_knn_search(
    store: &mut dyn Store,
    idx: &frogdb_search::ShardSearchIndex,
    request: &FtSearchRequest,
    knn_field: &str,
    blob: &Bytes,
    knn_k: usize,
) -> Result<ShardSearchReply, String> {
    let floats: Vec<f32> = blob
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let knn_hits = idx
        .knn_search(knn_field, &floats, knn_k)
        .map_err(|e| format!("ERR {}", e))?;

    let total = knn_hits.len();
    let mut hits = Vec::with_capacity(total);
    for hit in knn_hits {
        // Populate fields from the store (hash or JSON); a hit whose document
        // is missing from the store carries no content.
        let fields = if request.nocontent {
            None
        } else {
            store.get(&Bytes::from(hit.key.clone())).map(|value| {
                let mut fields = Vec::new();
                let is_json = idx.definition().source == frogdb_search::IndexSource::Json;
                if is_json {
                    if let Some(json_val) = value.as_json() {
                        let json_fields =
                            frogdb_search::extract_json_fields(idx.definition(), json_val.data());
                        for (k, v) in json_fields {
                            let include = match &request.return_fields {
                                Some(rf) => rf.contains(&k),
                                None => true,
                            };
                            if include {
                                fields.push((k, v));
                            }
                        }
                    }
                } else if let Some(hash) = value.as_hash() {
                    for (k, v) in hash.iter() {
                        let key_str = std::str::from_utf8(&k).unwrap_or("").to_string();
                        let include = match &request.return_fields {
                            Some(rf) => rf.contains(&key_str),
                            None => true,
                        };
                        if include {
                            fields
                                .push((key_str, std::str::from_utf8(&v).unwrap_or("").to_string()));
                        }
                    }
                }
                // Add __vec_score field
                fields.push(("__vec_score".to_string(), hit.distance.to_string()));
                fields
            })
        };

        hits.push(ShardSearchHit {
            // Distance as "score" (lower = better); the coordinator merges
            // KNN hits ascending.
            key: hit.key,
            score: hit.distance,
            sort_value: None,
            fields,
        });
    }

    Ok(ShardSearchReply { total, hits })
}

impl ShardWorker {
    pub(crate) fn execute_ft_aggregate(
        &mut self,
        index_name: &Bytes,
        request: &FtAggregateRequest,
    ) -> Result<frogdb_search::aggregate::PartialAggregate, String> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.search.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => return Err(format!("{name}: no such index")),
        };

        // Execute search to get ALL matching rows (no limit, offset 0)
        let search_result = idx
            .search(&request.query, &SearchOptions::page(0, 100_000))
            .map_err(|e| format!("ERR {e}"))?;

        // Convert SearchHit fields into Row format, preserving keys for LOAD
        let mut rows: Vec<frogdb_search::aggregate::Row> = search_result
            .hits
            .iter()
            .map(|h| {
                let mut fields = h.fields.clone();
                // Stash the document key as __key for LOAD lookups
                fields.push(("__key".to_string(), h.key.clone()));
                fields
            })
            .collect();

        // Process LOAD steps: enrich rows from the store before aggregation
        let is_json_index = idx.definition().source == frogdb_search::IndexSource::Json;
        let idx_def = idx.definition().clone();
        for step in &request.steps {
            if let frogdb_search::aggregate::AggregateStep::Load { fields } = step {
                for row in &mut rows {
                    let doc_key = row
                        .iter()
                        .find(|(k, _)| k == "__key")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    if doc_key.is_empty() {
                        continue;
                    }
                    if let Some(val) = self.store.get(&Bytes::from(doc_key)) {
                        if is_json_index {
                            if let Some(json_val) = val.as_json() {
                                let all_fields =
                                    frogdb_search::extract_json_fields(&idx_def, json_val.data());
                                for field_name in fields {
                                    if row.iter().any(|(k, _)| k == field_name) {
                                        continue;
                                    }
                                    if let Some((_, v)) =
                                        all_fields.iter().find(|(k, _)| k == field_name)
                                    {
                                        row.push((field_name.clone(), v.clone()));
                                    }
                                }
                            }
                        } else if let frogdb_types::Value::Hash(ref hash) = *val {
                            for field_name in fields {
                                if row.iter().any(|(k, _)| k == field_name) {
                                    continue;
                                }
                                if let Some(v) = hash.get(field_name.as_bytes()) {
                                    row.push((
                                        field_name.clone(),
                                        String::from_utf8_lossy(&v).to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Remove __key field before aggregation
        for row in &mut rows {
            row.retain(|(k, _)| k != "__key");
        }

        // Run shard-local aggregation; the typed partial crosses the shard
        // boundary as-is — no reducer-state serialization.
        Ok(frogdb_search::aggregate::execute_shard_local(
            &rows,
            &request.steps,
        ))
    }

    pub(crate) fn execute_ft_hybrid(
        &mut self,
        index_name: &Bytes,
        query_args: &[Bytes],
    ) -> Result<ShardSearchReply, String> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.search.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => return Err(format!("{}: no such index", name)),
        };

        // Parse hybrid query args:
        //   SEARCH query [SCORER ...] [YIELD_SCORE_AS name]
        //   VSIM @field $param [KNN count K] [RANGE count RADIUS r] [EF_RUNTIME ef]
        //        [YIELD_SCORE_AS name] [FILTER "expr"]
        //   COMBINE RRF|LINEAR count [CONSTANT c] [ALPHA a] [BETA b] [WINDOW w] [YIELD_SCORE_AS name]
        //   PARAMS nargs key value [...]
        let mut search_query = String::new();
        let mut search_yield_as: Option<String> = None;
        let mut vsim_field = String::new();
        let mut vsim_param = String::new();
        let mut knn_k: Option<usize> = None;
        let mut range_radius: Option<f32> = None;
        let mut _ef_runtime: Option<usize> = None;
        let mut vsim_yield_as: Option<String> = None;
        let mut combine_strategy: Option<String> = None; // "RRF" or "LINEAR"
        let mut combine_count: usize = 10;
        let mut rrf_constant: f32 = 60.0;
        let mut linear_alpha: f32 = 0.5;
        let mut linear_beta: f32 = 0.5;
        let mut window: usize = 3;
        let mut combine_yield_as: Option<String> = None;
        let mut params: std::collections::HashMap<String, Bytes> = std::collections::HashMap::new();
        let mut nocontent = false;
        let mut return_fields: Option<Vec<String>> = None;
        let mut verbatim = false;
        let mut infields: Option<Vec<String>> = None;
        let mut slop: Option<u32> = None;
        let mut filters: Vec<(String, f64, f64)> = Vec::new();
        let mut geofilters: Vec<frogdb_search::GeoFilter> = Vec::new();

        let mut i = 0;
        while i < query_args.len() {
            let arg_upper = query_args[i].to_ascii_uppercase();
            match arg_upper.as_slice() {
                b"SEARCH" => {
                    i += 1;
                    if i < query_args.len() {
                        search_query = std::str::from_utf8(&query_args[i])
                            .unwrap_or("*")
                            .to_string();
                        i += 1;
                        // Parse optional SCORER, YIELD_SCORE_AS
                        while i < query_args.len() {
                            let sub = query_args[i].to_ascii_uppercase();
                            if sub.as_slice() == b"SCORER" {
                                // Skip SCORER and its args (algorithm + params)
                                i += 1;
                                if i < query_args.len() {
                                    i += 1; // skip algorithm name
                                }
                            } else if sub.as_slice() == b"YIELD_SCORE_AS" {
                                i += 1;
                                if i < query_args.len() {
                                    search_yield_as = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .map(|s| s.to_string());
                                    i += 1;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                b"VSIM" => {
                    i += 1;
                    if i < query_args.len() {
                        let field_str = std::str::from_utf8(&query_args[i]).unwrap_or("");
                        vsim_field = field_str.trim_start_matches('@').to_string();
                        i += 1;
                    }
                    if i < query_args.len() {
                        let param_str = std::str::from_utf8(&query_args[i]).unwrap_or("");
                        vsim_param = param_str.trim_start_matches('$').to_string();
                        i += 1;
                    }
                    // Parse VSIM options
                    while i < query_args.len() {
                        let sub = query_args[i].to_ascii_uppercase();
                        match sub.as_slice() {
                            b"KNN" => {
                                i += 1; // skip "count" token
                                if i < query_args.len() {
                                    i += 1;
                                }
                                if i < query_args.len() {
                                    knn_k = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok());
                                    i += 1;
                                }
                                // Optional EF_RUNTIME
                                if i < query_args.len()
                                    && query_args[i].to_ascii_uppercase().as_slice()
                                        == b"EF_RUNTIME"
                                {
                                    i += 1;
                                    if i < query_args.len() {
                                        _ef_runtime = std::str::from_utf8(&query_args[i])
                                            .ok()
                                            .and_then(|s| s.parse().ok());
                                        i += 1;
                                    }
                                }
                            }
                            b"RANGE" => {
                                i += 1; // skip "count" token
                                if i < query_args.len() {
                                    i += 1;
                                }
                                if i + 1 < query_args.len()
                                    && query_args[i].to_ascii_uppercase().as_slice() == b"RADIUS"
                                {
                                    i += 1;
                                    range_radius = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok());
                                    i += 1;
                                }
                                // Optional EPSILON
                                if i < query_args.len()
                                    && query_args[i].to_ascii_uppercase().as_slice() == b"EPSILON"
                                {
                                    i += 2; // skip EPSILON and value
                                }
                            }
                            b"EF_RUNTIME" => {
                                i += 1;
                                if i < query_args.len() {
                                    _ef_runtime = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok());
                                    i += 1;
                                }
                            }
                            b"YIELD_SCORE_AS" => {
                                i += 1;
                                if i < query_args.len() {
                                    vsim_yield_as = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .map(|s| s.to_string());
                                    i += 1;
                                }
                            }
                            b"FILTER" => {
                                i += 1;
                                // VSIM FILTER is a filter expression string, skip it
                                if i < query_args.len() {
                                    i += 1;
                                }
                            }
                            _ => break,
                        }
                    }
                }
                b"COMBINE" => {
                    i += 1;
                    if i < query_args.len() {
                        let strategy_upper = query_args[i].to_ascii_uppercase();
                        combine_strategy = std::str::from_utf8(&strategy_upper)
                            .ok()
                            .map(|s| s.to_string());
                        i += 1;
                    }
                    // count
                    if i < query_args.len()
                        && let Some(c) = std::str::from_utf8(&query_args[i])
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                    {
                        combine_count = c;
                        i += 1;
                    }
                    // Parse options: CONSTANT, ALPHA, BETA, WINDOW, YIELD_SCORE_AS
                    while i < query_args.len() {
                        let sub = query_args[i].to_ascii_uppercase();
                        match sub.as_slice() {
                            b"CONSTANT" => {
                                i += 1;
                                if i < query_args.len() {
                                    rrf_constant = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(60.0);
                                    i += 1;
                                }
                            }
                            b"ALPHA" => {
                                i += 1;
                                if i < query_args.len() {
                                    linear_alpha = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(0.5);
                                    i += 1;
                                }
                            }
                            b"BETA" => {
                                i += 1;
                                if i < query_args.len() {
                                    linear_beta = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(0.5);
                                    i += 1;
                                }
                            }
                            b"WINDOW" => {
                                i += 1;
                                if i < query_args.len() {
                                    window = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(3);
                                    i += 1;
                                }
                            }
                            b"YIELD_SCORE_AS" => {
                                i += 1;
                                if i < query_args.len() {
                                    combine_yield_as = std::str::from_utf8(&query_args[i])
                                        .ok()
                                        .map(|s| s.to_string());
                                    i += 1;
                                }
                            }
                            _ => break,
                        }
                    }
                }
                b"PARAMS" => {
                    if i + 1 < query_args.len() {
                        let count: usize = std::str::from_utf8(&query_args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        i += 2;
                        for _ in 0..(count / 2) {
                            if i + 1 < query_args.len() {
                                let param_name = std::str::from_utf8(&query_args[i])
                                    .unwrap_or("")
                                    .to_string();
                                let param_val = query_args[i + 1].clone();
                                params.insert(param_name, param_val);
                                i += 2;
                            }
                        }
                    } else {
                        i += 1;
                    }
                }
                b"NOCONTENT" => {
                    nocontent = true;
                    i += 1;
                }
                b"RETURN" => {
                    if i + 1 < query_args.len() {
                        let count: usize = std::str::from_utf8(&query_args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let mut fields = Vec::new();
                        for j in 0..count {
                            if i + 2 + j < query_args.len()
                                && let Ok(f) = std::str::from_utf8(&query_args[i + 2 + j])
                            {
                                fields.push(f.to_string());
                            }
                        }
                        return_fields = Some(fields);
                        i += 2 + count;
                    } else {
                        i += 1;
                    }
                }
                b"VERBATIM" => {
                    verbatim = true;
                    i += 1;
                }
                b"INFIELDS" => {
                    if i + 1 < query_args.len() {
                        let count: usize = std::str::from_utf8(&query_args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let mut fields = Vec::new();
                        for j in 0..count {
                            if i + 2 + j < query_args.len()
                                && let Ok(f) = std::str::from_utf8(&query_args[i + 2 + j])
                            {
                                fields.push(f.to_string());
                            }
                        }
                        infields = Some(fields);
                        i += 2 + count;
                    } else {
                        i += 1;
                    }
                }
                b"SLOP" => {
                    if i + 1 < query_args.len() {
                        slop = std::str::from_utf8(&query_args[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok());
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                b"FILTER" => {
                    if i + 3 < query_args.len() {
                        let field = std::str::from_utf8(&query_args[i + 1])
                            .unwrap_or("")
                            .to_string();
                        let min: f64 = std::str::from_utf8(&query_args[i + 2])
                            .ok()
                            .and_then(|s| {
                                if s == "-inf" {
                                    Some(f64::NEG_INFINITY)
                                } else if s == "+inf" || s == "inf" {
                                    Some(f64::INFINITY)
                                } else {
                                    s.parse().ok()
                                }
                            })
                            .unwrap_or(f64::NEG_INFINITY);
                        let max: f64 = std::str::from_utf8(&query_args[i + 3])
                            .ok()
                            .and_then(|s| {
                                if s == "-inf" {
                                    Some(f64::NEG_INFINITY)
                                } else if s == "+inf" || s == "inf" {
                                    Some(f64::INFINITY)
                                } else {
                                    s.parse().ok()
                                }
                            })
                            .unwrap_or(f64::INFINITY);
                        filters.push((field, min, max));
                        i += 4;
                    } else {
                        i += 1;
                    }
                }
                b"GEOFILTER" => {
                    if i + 5 < query_args.len() {
                        let field = std::str::from_utf8(&query_args[i + 1])
                            .unwrap_or("")
                            .to_string();
                        let lon: f64 = std::str::from_utf8(&query_args[i + 2])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
                        let lat: f64 = std::str::from_utf8(&query_args[i + 3])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
                        let radius: f64 = std::str::from_utf8(&query_args[i + 4])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
                        let unit = std::str::from_utf8(&query_args[i + 5]).unwrap_or("m");
                        let radius_m = match unit.to_lowercase().as_str() {
                            "km" => radius * 1000.0,
                            "mi" => radius * 1609.344,
                            "ft" => radius * 0.3048,
                            _ => radius,
                        };
                        geofilters.push(frogdb_search::GeoFilter {
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
                b"DIALECT" | b"TIMEOUT" | b"LIMIT" | b"SORTBY" | b"NOSORT" | b"LOAD"
                | b"GROUPBY" | b"APPLY" => {
                    // These are coordinator-level options, skip at shard level
                    i += 1;
                    while i < query_args.len() {
                        // Skip until next known top-level keyword
                        let peek = query_args[i].to_ascii_uppercase();
                        if matches!(
                            peek.as_slice(),
                            b"SEARCH"
                                | b"VSIM"
                                | b"COMBINE"
                                | b"PARAMS"
                                | b"LIMIT"
                                | b"SORTBY"
                                | b"NOSORT"
                                | b"LOAD"
                                | b"GROUPBY"
                                | b"APPLY"
                                | b"FILTER"
                                | b"NOCONTENT"
                                | b"RETURN"
                                | b"TIMEOUT"
                                | b"DIALECT"
                        ) {
                            break;
                        }
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        // Validate required fields
        if search_query.is_empty() {
            return Err("ERR SEARCH clause is required".to_string());
        }
        if vsim_field.is_empty() || vsim_param.is_empty() {
            return Err("ERR VSIM clause with @field and $param is required".to_string());
        }

        // Resolve vector bytes from PARAMS
        let blob = match params.get(&vsim_param) {
            Some(b) => b.clone(),
            None => return Err(format!("ERR No such parameter '{}'", vsim_param)),
        };
        let floats: Vec<f32> = blob
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        // Determine count: KNN k, or combine_count
        let count = knn_k.unwrap_or(combine_count);
        // Cap window to prevent excessive over-fetching
        let window = window.min(10000 / count.max(1));

        // Build fusion strategy
        let strategy = match combine_strategy.as_deref() {
            Some("LINEAR") => frogdb_search::FusionStrategy::Linear {
                alpha: linear_alpha,
                beta: linear_beta,
            },
            _ => {
                // Default to RRF
                frogdb_search::FusionStrategy::Rrf {
                    constant: rrf_constant,
                }
            }
        };

        // Substitute $param references in the search query
        let search_query = if !params.is_empty() {
            substitute_params(&search_query, &params)
        } else {
            search_query
        };

        // Run hybrid search
        let text_opts = frogdb_search::HybridTextOptions {
            infields,
            slop,
            verbatim,
            extra_filters: filters,
            extra_geo_filters: geofilters,
        };
        let hybrid_hits = idx
            .hybrid_search(
                &search_query,
                &vsim_field,
                &floats,
                &strategy,
                window,
                count,
                text_opts,
            )
            .map_err(|e| format!("ERR {}", e))?;

        // Build the typed reply
        let is_json = idx.definition().source == frogdb_search::IndexSource::Json;
        let _ = range_radius; // RANGE mode: for future use

        let total = hybrid_hits.len();
        let mut hits = Vec::with_capacity(total);
        for hit in hybrid_hits {
            let fields = if nocontent {
                None
            } else {
                self.store.get(&Bytes::from(hit.key.clone())).map(|value| {
                    let mut fields = Vec::new();
                    if is_json {
                        if let Some(json_val) = value.as_json() {
                            let json_fields = frogdb_search::extract_json_fields(
                                idx.definition(),
                                json_val.data(),
                            );
                            for (k, v) in json_fields {
                                let include = match &return_fields {
                                    Some(rf) => rf.contains(&k),
                                    None => true,
                                };
                                if include {
                                    fields.push((k, v));
                                }
                            }
                        }
                    } else if let Some(hash) = value.as_hash() {
                        for (k, v) in hash.iter() {
                            let key_str = std::str::from_utf8(&k).unwrap_or("").to_string();
                            let include = match &return_fields {
                                Some(rf) => rf.contains(&key_str),
                                None => true,
                            };
                            if include {
                                fields.push((
                                    key_str,
                                    std::str::from_utf8(&v).unwrap_or("").to_string(),
                                ));
                            }
                        }
                    }

                    // Add YIELD_SCORE_AS named scores
                    if let Some(ref name) = search_yield_as {
                        fields.push((name.clone(), hit.text_score.unwrap_or(0.0).to_string()));
                    }
                    if let Some(ref name) = vsim_yield_as {
                        fields.push((name.clone(), hit.vector_distance.unwrap_or(0.0).to_string()));
                    }
                    if let Some(ref name) = combine_yield_as {
                        fields.push((name.clone(), hit.fused_score.to_string()));
                    }

                    fields
                })
            };

            hits.push(ShardSearchHit {
                key: hit.key,
                score: hit.fused_score,
                sort_value: None,
                fields,
            });
        }

        Ok(ShardSearchReply { total, hits })
    }

    pub(crate) fn execute_ft_explain(
        &self,
        index_name: &Bytes,
        query_str: &Bytes,
    ) -> Vec<(Bytes, frogdb_protocol::Response)> {
        use frogdb_protocol::Response;

        let name = std::str::from_utf8(index_name).unwrap_or("");
        let resolved = self.search.resolve_index_name(name);
        let idx = match self.search.indexes.get(resolved) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_explain__"),
                    Response::error(format!("{}: no such index", name)),
                )];
            }
        };

        let q = std::str::from_utf8(query_str).unwrap_or("*");
        let parser = frogdb_search::QueryParser::new(
            idx.tantivy_schema(),
            idx.field_map(),
            idx.definition(),
            idx.key_field(),
        );

        match parser.explain(q) {
            Ok(plan) => {
                vec![(
                    Bytes::from_static(b"__ft_explain__"),
                    Response::bulk(Bytes::from(plan)),
                )]
            }
            Err(e) => {
                vec![(
                    Bytes::from_static(b"__ft_explain__"),
                    Response::error(format!("ERR {}", e)),
                )]
            }
        }
    }
}

use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use super::{parse_knn_query, substitute_params};
use crate::store::Store;

/// Parsed options for FT.SEARCH.
struct FtSearchOptions {
    query_str: String,
    offset: usize,
    limit: usize,
    nocontent: bool,
    withscores: bool,
    verbatim: bool,
    return_fields: Option<Vec<String>>,
    sortby: Option<(String, frogdb_search::SortOrder)>,
    infields: Option<Vec<String>>,
    inkeys: Option<Vec<String>>,
    filters: Vec<(String, f64, f64)>,
    geofilters: Vec<frogdb_search::GeoFilter>,
    highlight: Option<frogdb_search::HighlightOptions>,
    slop: Option<u32>,
    summarize: Option<frogdb_search::SummarizeOptions>,
    params: std::collections::HashMap<String, Bytes>,
}

fn parse_ft_search_options(query_args: &[Bytes]) -> FtSearchOptions {
    // Parse query string from args[0], options from rest
    let query_str = if !query_args.is_empty() {
        std::str::from_utf8(&query_args[0])
            .unwrap_or("*")
            .to_string()
    } else {
        "*".to_string()
    };

    // Parse LIMIT offset num (default 0 10)
    let mut offset = 0usize;
    let mut limit = 10usize;
    let mut nocontent = false;
    let mut withscores = false;
    let mut verbatim = false;
    let mut return_fields: Option<Vec<String>> = None;
    let mut sortby: Option<(String, frogdb_search::SortOrder)> = None;
    let mut infields: Option<Vec<String>> = None;
    let mut inkeys: Option<Vec<String>> = None;
    let mut filters: Vec<(String, f64, f64)> = Vec::new();
    let mut geofilters: Vec<frogdb_search::GeoFilter> = Vec::new();
    let mut highlight: Option<frogdb_search::HighlightOptions> = None;
    let mut slop: Option<u32> = None;
    let mut summarize: Option<frogdb_search::SummarizeOptions> = None;

    // Collect PARAMS key-value pairs (for KNN vector queries)
    let mut params: std::collections::HashMap<String, Bytes> = std::collections::HashMap::new();

    let mut i = 1;
    while i < query_args.len() {
        let arg_upper = query_args[i].to_ascii_uppercase();
        match arg_upper.as_slice() {
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
            b"LIMIT" => {
                if i + 2 < query_args.len() {
                    offset = std::str::from_utf8(&query_args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    limit = std::str::from_utf8(&query_args[i + 2])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10);
                    i += 3;
                } else {
                    i += 1;
                }
            }
            b"NOCONTENT" => {
                nocontent = true;
                i += 1;
            }
            b"WITHSCORES" => {
                withscores = true;
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
            b"SORTBY" => {
                if i + 1 < query_args.len() {
                    let field_name = std::str::from_utf8(&query_args[i + 1])
                        .unwrap_or("")
                        .to_string();
                    let order = if i + 2 < query_args.len() {
                        let dir = query_args[i + 2].to_ascii_uppercase();
                        if dir.as_slice() == b"DESC" {
                            i += 3;
                            frogdb_search::SortOrder::Desc
                        } else if dir.as_slice() == b"ASC" {
                            i += 3;
                            frogdb_search::SortOrder::Asc
                        } else {
                            i += 2;
                            frogdb_search::SortOrder::Asc
                        }
                    } else {
                        i += 2;
                        frogdb_search::SortOrder::Asc
                    };
                    sortby = Some((field_name, order));
                } else {
                    i += 1;
                }
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
            b"VERBATIM" => {
                verbatim = true;
                i += 1;
            }
            b"INKEYS" => {
                if i + 1 < query_args.len() {
                    let count: usize = std::str::from_utf8(&query_args[i + 1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    let mut keys = Vec::new();
                    for j in 0..count {
                        if i + 2 + j < query_args.len()
                            && let Ok(k) = std::str::from_utf8(&query_args[i + 2 + j])
                        {
                            keys.push(k.to_string());
                        }
                    }
                    inkeys = Some(keys);
                    i += 2 + count;
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
                        _ => radius, // default meters
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
            b"DIALECT" | b"TIMEOUT" => {
                // Accept and ignore — DIALECT is always dialect 2; TIMEOUT
                // is handled at the coordinator level (scatter.rs), not per-shard.
                if i + 1 < query_args.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
            b"SUMMARIZE" => {
                i += 1;
                let mut summ = frogdb_search::SummarizeOptions::default();
                // Parse optional FIELDS count field...
                if i < query_args.len()
                    && query_args[i].to_ascii_uppercase().as_slice() == b"FIELDS"
                {
                    i += 1;
                    if i < query_args.len() {
                        let count: usize = std::str::from_utf8(&query_args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        i += 1;
                        for _ in 0..count {
                            if i < query_args.len()
                                && let Ok(f) = std::str::from_utf8(&query_args[i])
                            {
                                summ.fields.push(f.to_string());
                                i += 1;
                            }
                        }
                    }
                }
                // Parse optional FRAGS num
                if i < query_args.len() && query_args[i].to_ascii_uppercase().as_slice() == b"FRAGS"
                {
                    i += 1;
                    if i < query_args.len() {
                        summ.num_frags = std::str::from_utf8(&query_args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(3);
                        i += 1;
                    }
                }
                // Parse optional LEN num
                if i < query_args.len() && query_args[i].to_ascii_uppercase().as_slice() == b"LEN" {
                    i += 1;
                    if i < query_args.len() {
                        summ.frag_len = std::str::from_utf8(&query_args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(20);
                        i += 1;
                    }
                }
                // Parse optional SEPARATOR sep
                if i < query_args.len()
                    && query_args[i].to_ascii_uppercase().as_slice() == b"SEPARATOR"
                {
                    i += 1;
                    if i < query_args.len() {
                        summ.separator = std::str::from_utf8(&query_args[i])
                            .unwrap_or("... ")
                            .to_string();
                        i += 1;
                    }
                }
                summarize = Some(summ);
            }
            b"HIGHLIGHT" => {
                i += 1;
                let mut hl = frogdb_search::HighlightOptions::default();
                // Parse optional FIELDS count field...
                if i < query_args.len()
                    && query_args[i].to_ascii_uppercase().as_slice() == b"FIELDS"
                {
                    i += 1;
                    if i < query_args.len() {
                        let count: usize = std::str::from_utf8(&query_args[i])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        i += 1;
                        for _ in 0..count {
                            if i < query_args.len()
                                && let Ok(f) = std::str::from_utf8(&query_args[i])
                            {
                                hl.fields.push(f.to_string());
                                i += 1;
                            }
                        }
                    }
                }
                // Parse optional TAGS open close
                if i < query_args.len() && query_args[i].to_ascii_uppercase().as_slice() == b"TAGS"
                {
                    i += 1;
                    if i + 1 < query_args.len() {
                        hl.open_tag = std::str::from_utf8(&query_args[i])
                            .ok()
                            .map(|s| s.to_string());
                        i += 1;
                        hl.close_tag = std::str::from_utf8(&query_args[i])
                            .ok()
                            .map(|s| s.to_string());
                        i += 1;
                    }
                }
                highlight = Some(hl);
            }
            _ => {
                i += 1;
            }
        }
    }

    FtSearchOptions {
        query_str,
        offset,
        limit,
        nocontent,
        withscores,
        verbatim,
        return_fields,
        sortby,
        infields,
        inkeys,
        filters,
        geofilters,
        highlight,
        slop,
        summarize,
        params,
    }
}

fn format_ft_search_results(
    search_result: frogdb_search::SearchResult,
    opts: &FtSearchOptions,
) -> Vec<(Bytes, Response)> {
    // First result is the total count for this shard
    let mut results = Vec::with_capacity(search_result.hits.len() + 1);
    results.push((
        Bytes::from_static(b"__ft_total__"),
        Response::Integer(search_result.total as i64),
    ));

    for hit in search_result.hits {
        let mut entry = Vec::new();

        // Score as first element (for merge sorting)
        entry.push(Response::bulk(Bytes::from(hit.score.to_string())));

        // Sort value as second element when SORTBY is active
        if opts.sortby.is_some() {
            let sv = match &hit.sort_value {
                Some(frogdb_search::SortValue::F64(v)) => v.to_string(),
                Some(frogdb_search::SortValue::Str(s)) => s.clone(),
                None => String::new(),
            };
            entry.push(Response::bulk(Bytes::from(sv)));
        }

        if opts.withscores {
            entry.push(Response::bulk(Bytes::from(hit.score.to_string())));
        }

        if !opts.nocontent {
            let fields_to_include: Vec<(String, String)> = match &opts.return_fields {
                Some(rf) => hit
                    .fields
                    .into_iter()
                    .filter(|(name, _)| rf.contains(name))
                    .collect(),
                None => hit.fields,
            };

            let mut field_array = Vec::new();
            for (name, value) in fields_to_include {
                field_array.push(Response::bulk(Bytes::from(name)));
                field_array.push(Response::bulk(Bytes::from(value)));
            }
            entry.push(Response::Array(field_array));
        }

        results.push((Bytes::from(hit.key), Response::Array(entry)));
    }

    results
}

impl ShardWorker {
    pub(crate) fn execute_ft_search(
        &mut self,
        index_name: &Bytes,
        query_args: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_search__"),
                    Response::error(format!("{}: no such index", name)),
                )];
            }
        };

        let opts = parse_ft_search_options(query_args);

        // Check for KNN vector query BEFORE param substitution, since substitution
        // replaces $BLOB with raw binary bytes which corrupts the query string.
        // The KNN path fetches the blob directly from params.get().
        if let Some((knn_k, knn_field, knn_param)) = parse_knn_query(&opts.query_str) {
            let blob = match opts.params.get(&knn_param) {
                Some(b) => b.clone(),
                None => {
                    return vec![(
                        Bytes::from_static(b"__ft_search__"),
                        Response::error(format!("ERR No such parameter '{}'", knn_param)),
                    )];
                }
            };
            return execute_ft_knn_search(&mut self.store, idx, &opts, &knn_field, &blob, knn_k);
        }

        // Substitute $param references in the query string (only for non-KNN queries)
        let query_str = if !opts.params.is_empty() {
            substitute_params(&opts.query_str, &opts.params)
        } else {
            opts.query_str.clone()
        };

        let sort_opt = opts.sortby.as_ref().map(|(f, o)| (f.as_str(), *o));
        let search_result = match idx.search_with_options(
            &query_str,
            0,
            opts.offset + opts.limit,
            sort_opt,
            opts.infields.clone(),
            opts.highlight.clone(),
            opts.slop,
            opts.summarize.clone(),
            opts.verbatim,
            opts.inkeys.clone(),
            opts.filters.clone(),
            opts.geofilters.clone(),
        ) {
            Ok(r) => r,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_search__"),
                    Response::error(format!("ERR {}", e)),
                )];
            }
        };

        format_ft_search_results(search_result, &opts)
    }
}

fn execute_ft_knn_search(
    store: &mut dyn Store,
    idx: &frogdb_search::ShardSearchIndex,
    opts: &FtSearchOptions,
    knn_field: &str,
    blob: &Bytes,
    knn_k: usize,
) -> Vec<(Bytes, Response)> {
    let floats: Vec<f32> = blob
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let knn_hits = match idx.knn_search(knn_field, &floats, knn_k) {
        Ok(hits) => hits,
        Err(e) => {
            return vec![(
                Bytes::from_static(b"__ft_search__"),
                Response::error(format!("ERR {}", e)),
            )];
        }
    };

    // Build response in same format as regular FT.SEARCH
    let mut results = Vec::with_capacity(knn_hits.len() + 1);
    results.push((
        Bytes::from_static(b"__ft_total__"),
        Response::Integer(knn_hits.len() as i64),
    ));

    for hit in knn_hits {
        let mut entry = Vec::new();
        // Use distance as "score" (lower = better)
        entry.push(Response::bulk(Bytes::from(hit.distance.to_string())));

        if opts.withscores {
            entry.push(Response::bulk(Bytes::from(hit.distance.to_string())));
        }

        if !opts.nocontent {
            // Populate fields from the store (hash or JSON)
            if let Some(value) = store.get(&Bytes::from(hit.key.clone())) {
                let mut field_array = Vec::new();
                let is_json = idx.definition().source == frogdb_search::IndexSource::Json;
                if is_json {
                    if let Some(json_val) = value.as_json() {
                        let json_fields =
                            frogdb_search::extract_json_fields(idx.definition(), json_val.data());
                        for (k, v) in &json_fields {
                            let include = match &opts.return_fields {
                                Some(rf) => rf.iter().any(|f| f == k),
                                None => true,
                            };
                            if include {
                                field_array.push(Response::bulk(Bytes::from(k.clone())));
                                field_array.push(Response::bulk(Bytes::from(v.clone())));
                            }
                        }
                    }
                } else if let Some(hash) = value.as_hash() {
                    for (k, v) in hash.iter() {
                        let include = match &opts.return_fields {
                            Some(rf) => {
                                let key_str = std::str::from_utf8(&k).unwrap_or("");
                                rf.iter().any(|f| f == key_str)
                            }
                            None => true,
                        };
                        if include {
                            field_array.push(Response::bulk(Bytes::from(
                                std::str::from_utf8(&k).unwrap_or("").to_string(),
                            )));
                            field_array.push(Response::bulk(Bytes::from(
                                std::str::from_utf8(&v).unwrap_or("").to_string(),
                            )));
                        }
                    }
                }
                // Add __vec_score field
                field_array.push(Response::bulk(Bytes::from_static(b"__vec_score")));
                field_array.push(Response::bulk(Bytes::from(hit.distance.to_string())));
                entry.push(Response::Array(field_array));
            }
        }

        results.push((Bytes::from(hit.key), Response::Array(entry)));
    }

    results
}

impl ShardWorker {
    pub(crate) fn execute_ft_aggregate(
        &mut self,
        index_name: &Bytes,
        query_args: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_error__"),
                    Response::error(format!("{name}: no such index")),
                )];
            }
        };

        // query_args[0] = query string, rest = pipeline args
        let query_str = if !query_args.is_empty() {
            std::str::from_utf8(&query_args[0]).unwrap_or("*")
        } else {
            "*"
        };

        // Parse pipeline args (everything after the query string)
        let pipeline_strs: Vec<&str> = query_args[1..]
            .iter()
            .filter_map(|b| std::str::from_utf8(b).ok())
            .collect();
        let steps = match frogdb_search::aggregate::parse_aggregate_pipeline(&pipeline_strs) {
            Ok(s) => s,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_error__"),
                    Response::error(format!("ERR {e}")),
                )];
            }
        };

        // Execute search to get ALL matching rows (no limit, offset 0)
        let search_result = match idx.search(query_str, 0, 100_000) {
            Ok(r) => r,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_error__"),
                    Response::error(format!("ERR {e}")),
                )];
            }
        };

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
        for step in &steps {
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

        // Run shard-local aggregation
        let partial = frogdb_search::aggregate::execute_shard_local(&rows, &steps);

        // Serialize partial aggregate as a JSON blob
        let mut results = Vec::new();
        for (key_fields, states) in &partial.groups {
            let mut parts: Vec<String> = Vec::new();

            // Serialize group key
            for (k, v) in key_fields {
                parts.push(format!("{k}={v}"));
            }
            let group_key = parts.join(",");

            // Serialize partial states as a response array
            let mut state_items: Vec<Response> = Vec::new();
            for state in states {
                match state {
                    frogdb_search::aggregate::PartialReducerState::Count(c) => {
                        state_items.push(Response::bulk(Bytes::from("COUNT")));
                        state_items.push(Response::Integer(*c));
                    }
                    frogdb_search::aggregate::PartialReducerState::Sum(s) => {
                        state_items.push(Response::bulk(Bytes::from("SUM")));
                        state_items.push(Response::bulk(Bytes::from(s.to_string())));
                    }
                    frogdb_search::aggregate::PartialReducerState::Min(m) => {
                        state_items.push(Response::bulk(Bytes::from("MIN")));
                        state_items.push(Response::bulk(Bytes::from(m.to_string())));
                    }
                    frogdb_search::aggregate::PartialReducerState::Max(m) => {
                        state_items.push(Response::bulk(Bytes::from("MAX")));
                        state_items.push(Response::bulk(Bytes::from(m.to_string())));
                    }
                    frogdb_search::aggregate::PartialReducerState::Avg(sum, count) => {
                        state_items.push(Response::bulk(Bytes::from("AVG")));
                        state_items.push(Response::bulk(Bytes::from(sum.to_string())));
                        state_items.push(Response::Integer(*count));
                    }
                    frogdb_search::aggregate::PartialReducerState::CountDistinct(set) => {
                        state_items.push(Response::bulk(Bytes::from("COUNT_DISTINCT")));
                        // Serialize as count of items, then each item
                        state_items.push(Response::Integer(set.len() as i64));
                        for val in set {
                            state_items.push(Response::bulk(Bytes::from(val.clone())));
                        }
                    }
                    frogdb_search::aggregate::PartialReducerState::CountDistinctish(regs) => {
                        state_items.push(Response::bulk(Bytes::from("COUNT_DISTINCTISH")));
                        // Serialize 256 registers as a single bulk string
                        state_items.push(Response::bulk(Bytes::from(regs.clone())));
                    }
                    frogdb_search::aggregate::PartialReducerState::Tolist(list) => {
                        state_items.push(Response::bulk(Bytes::from("TOLIST")));
                        state_items.push(Response::Integer(list.len() as i64));
                        for val in list {
                            state_items.push(Response::bulk(Bytes::from(val.clone())));
                        }
                    }
                    frogdb_search::aggregate::PartialReducerState::FirstValue {
                        value,
                        sort_key,
                        sort_asc,
                    } => {
                        state_items.push(Response::bulk(Bytes::from("FIRST_VALUE")));
                        state_items.push(Response::bulk(Bytes::from(
                            value.clone().unwrap_or_default(),
                        )));
                        state_items.push(Response::bulk(Bytes::from(
                            sort_key.clone().unwrap_or_default(),
                        )));
                        state_items.push(Response::Integer(if *sort_asc { 1 } else { 0 }));
                    }
                    frogdb_search::aggregate::PartialReducerState::Stddev {
                        sum,
                        sum_sq,
                        count,
                    } => {
                        state_items.push(Response::bulk(Bytes::from("STDDEV")));
                        state_items.push(Response::bulk(Bytes::from(sum.to_string())));
                        state_items.push(Response::bulk(Bytes::from(sum_sq.to_string())));
                        state_items.push(Response::Integer(*count));
                    }
                    frogdb_search::aggregate::PartialReducerState::Quantile {
                        values,
                        quantile,
                    } => {
                        state_items.push(Response::bulk(Bytes::from("QUANTILE")));
                        state_items.push(Response::bulk(Bytes::from(quantile.to_string())));
                        state_items.push(Response::Integer(values.len() as i64));
                        for val in values {
                            state_items.push(Response::bulk(Bytes::from(val.to_string())));
                        }
                    }
                    frogdb_search::aggregate::PartialReducerState::RandomSample {
                        reservoir,
                        count,
                        seen,
                    } => {
                        state_items.push(Response::bulk(Bytes::from("RANDOM_SAMPLE")));
                        state_items.push(Response::Integer(*count as i64));
                        state_items.push(Response::Integer(*seen as i64));
                        state_items.push(Response::Integer(reservoir.len() as i64));
                        for val in reservoir {
                            state_items.push(Response::bulk(Bytes::from(val.clone())));
                        }
                    }
                }
            }

            // Encode key fields as first part, states as second
            let mut entry = Vec::new();
            for (k, v) in key_fields {
                entry.push(Response::bulk(Bytes::from(k.clone())));
                entry.push(Response::bulk(Bytes::from(v.clone())));
            }
            entry.push(Response::Array(state_items));

            results.push((Bytes::from(group_key), Response::Array(entry)));
        }

        results
    }

    pub(crate) fn execute_ft_hybrid(
        &mut self,
        index_name: &Bytes,
        query_args: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_hybrid__"),
                    Response::error(format!("{}: no such index", name)),
                )];
            }
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
            return vec![(
                Bytes::from_static(b"__ft_hybrid__"),
                Response::error("ERR SEARCH clause is required"),
            )];
        }
        if vsim_field.is_empty() || vsim_param.is_empty() {
            return vec![(
                Bytes::from_static(b"__ft_hybrid__"),
                Response::error("ERR VSIM clause with @field and $param is required"),
            )];
        }

        // Resolve vector bytes from PARAMS
        let blob = match params.get(&vsim_param) {
            Some(b) => b.clone(),
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_hybrid__"),
                    Response::error(format!("ERR No such parameter '{}'", vsim_param)),
                )];
            }
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
        let hybrid_hits = match idx.hybrid_search(
            &search_query,
            &vsim_field,
            &floats,
            &strategy,
            window,
            count,
            text_opts,
        ) {
            Ok(hits) => hits,
            Err(e) => {
                return vec![(
                    Bytes::from_static(b"__ft_hybrid__"),
                    Response::error(format!("ERR {}", e)),
                )];
            }
        };

        // Build response
        let mut results = Vec::with_capacity(hybrid_hits.len() + 1);
        results.push((
            Bytes::from_static(b"__ft_total__"),
            Response::Integer(hybrid_hits.len() as i64),
        ));

        let is_json = idx.definition().source == frogdb_search::IndexSource::Json;
        let _ = range_radius; // RANGE mode: for future use

        for hit in hybrid_hits {
            let mut entry = Vec::new();

            // Fused score as first element (for merge sorting)
            entry.push(Response::bulk(Bytes::from(hit.fused_score.to_string())));

            if !nocontent && let Some(value) = self.store.get(&Bytes::from(hit.key.clone())) {
                let mut field_array = Vec::new();
                if is_json {
                    if let Some(json_val) = value.as_json() {
                        let json_fields =
                            frogdb_search::extract_json_fields(idx.definition(), json_val.data());
                        for (k, v) in &json_fields {
                            let include = match &return_fields {
                                Some(rf) => rf.iter().any(|f| f == k),
                                None => true,
                            };
                            if include {
                                field_array.push(Response::bulk(Bytes::from(k.clone())));
                                field_array.push(Response::bulk(Bytes::from(v.clone())));
                            }
                        }
                    }
                } else if let Some(hash) = value.as_hash() {
                    for (k, v) in hash.iter() {
                        let include = match &return_fields {
                            Some(rf) => {
                                let key_str = std::str::from_utf8(&k).unwrap_or("");
                                rf.iter().any(|f| f == key_str)
                            }
                            None => true,
                        };
                        if include {
                            field_array.push(Response::bulk(Bytes::from(
                                std::str::from_utf8(&k).unwrap_or("").to_string(),
                            )));
                            field_array.push(Response::bulk(Bytes::from(
                                std::str::from_utf8(&v).unwrap_or("").to_string(),
                            )));
                        }
                    }
                }

                // Add YIELD_SCORE_AS named scores
                if let Some(ref name) = search_yield_as {
                    field_array.push(Response::bulk(Bytes::from(name.clone())));
                    field_array.push(Response::bulk(Bytes::from(
                        hit.text_score.unwrap_or(0.0).to_string(),
                    )));
                }
                if let Some(ref name) = vsim_yield_as {
                    field_array.push(Response::bulk(Bytes::from(name.clone())));
                    field_array.push(Response::bulk(Bytes::from(
                        hit.vector_distance.unwrap_or(0.0).to_string(),
                    )));
                }
                if let Some(ref name) = combine_yield_as {
                    field_array.push(Response::bulk(Bytes::from(name.clone())));
                    field_array.push(Response::bulk(Bytes::from(hit.fused_score.to_string())));
                }

                entry.push(Response::Array(field_array));
            }

            results.push((Bytes::from(hit.key), Response::Array(entry)));
        }

        results
    }

    pub(crate) fn execute_ft_explain(
        &self,
        index_name: &Bytes,
        query_str: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let resolved = self.resolve_index_name(name);
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

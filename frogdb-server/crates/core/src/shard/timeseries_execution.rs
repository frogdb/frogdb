use bytes::Bytes;
use frogdb_protocol::Response;

use crate::store::Store;
use crate::{Aggregation, LabelFilter, TimeSeriesValue};

use super::worker::ShardWorker;

impl ShardWorker {
    pub(super) fn execute_ts_queryindex(&mut self, args: &[Bytes]) -> Vec<(Bytes, Response)> {
        let label_index = match self.store.ts_label_index() {
            Some(idx) => idx,
            None => return vec![],
        };
        let filters = parse_ts_filters(args);
        let matching = label_index.query(&filters);
        matching
            .into_iter()
            .map(|key| (key.clone(), Response::bulk(key)))
            .collect()
    }

    pub(super) fn execute_ts_mget(&mut self, args: &[Bytes]) -> Vec<(Bytes, Response)> {
        let label_index = match self.store.ts_label_index() {
            Some(idx) => idx,
            None => return vec![],
        };
        let (label_mode, filters) = parse_mget_args(args);
        let matching = label_index.query(&filters);
        let mut results = Vec::new();
        for key in matching {
            if let Some(val) = self.store.get(&key)
                && let Some(ts) = val.as_timeseries()
            {
                let sample = match ts.get_last() {
                    Some((t, v)) => Response::Array(vec![
                        Response::Integer(t),
                        Response::bulk(Bytes::from(format_float(v))),
                    ]),
                    None => Response::Array(vec![]),
                };
                let labels_resp = build_labels_response(ts, &label_mode);
                let entry = Response::Array(vec![Response::bulk(key.clone()), labels_resp, sample]);
                results.push((key, entry));
            }
        }
        results
    }

    pub(super) fn execute_ts_mrange(
        &mut self,
        args: &[Bytes],
        reverse: bool,
    ) -> Vec<(Bytes, Response)> {
        let label_index = match self.store.ts_label_index() {
            Some(idx) => idx,
            None => return vec![],
        };
        let params = parse_mrange_args(args);
        let matching = label_index.query(&params.filters);
        let mut results = Vec::new();
        for key in matching {
            if let Some(val) = self.store.get(&key)
                && let Some(ts) = val.as_timeseries()
            {
                let mut samples = if let Some((agg, bucket)) = params.aggregation {
                    ts.range_aggregated(params.from, params.to, bucket, agg)
                } else if reverse {
                    ts.revrange(params.from, params.to)
                } else {
                    ts.range(params.from, params.to)
                };

                // Apply FILTER_BY_TS
                if let Some(ref allowed_ts) = params.filter_by_ts {
                    samples.retain(|(t, _)| allowed_ts.contains(t));
                }
                // Apply FILTER_BY_VALUE
                if let Some((min, max)) = params.filter_by_value {
                    samples.retain(|(_, v)| *v >= min && *v <= max);
                }
                // Apply COUNT
                if let Some(limit) = params.count {
                    samples.truncate(limit);
                }

                let sample_responses: Vec<Response> = samples
                    .iter()
                    .map(|(t, v)| {
                        Response::Array(vec![
                            Response::Integer(*t),
                            Response::bulk(Bytes::from(format_float(*v))),
                        ])
                    })
                    .collect();

                let labels_resp = build_labels_response(ts, &params.label_mode);
                let entry = Response::Array(vec![
                    Response::bulk(key.clone()),
                    labels_resp,
                    Response::Array(sample_responses),
                ]);
                results.push((key, entry));
            }
        }
        results
    }
}

/// Label output mode for MGET/MRANGE.
enum LabelMode {
    /// Don't include labels (default).
    None,
    /// Include all labels (WITHLABELS).
    WithLabels,
    /// Include selected labels (SELECTED_LABELS l1 l2 ...).
    SelectedLabels(Vec<String>),
}

/// Parsed parameters for MRANGE/MREVRANGE.
struct MrangeParams {
    from: i64,
    to: i64,
    filters: Vec<LabelFilter>,
    count: Option<usize>,
    aggregation: Option<(Aggregation, i64)>,
    label_mode: LabelMode,
    filter_by_ts: Option<Vec<i64>>,
    filter_by_value: Option<(f64, f64)>,
}

fn parse_ts_filters(args: &[Bytes]) -> Vec<LabelFilter> {
    args.iter()
        .filter_map(|arg| std::str::from_utf8(arg).ok().and_then(LabelFilter::parse))
        .collect()
}

fn parse_mget_args(args: &[Bytes]) -> (LabelMode, Vec<LabelFilter>) {
    let mut label_mode = LabelMode::None;
    let mut filter_args = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"WITHLABELS" => {
                label_mode = LabelMode::WithLabels;
            }
            b"SELECTED_LABELS" => {
                let mut selected = Vec::new();
                i += 1;
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    if u == b"FILTER" {
                        break;
                    }
                    if let Ok(s) = std::str::from_utf8(&args[i]) {
                        selected.push(s.to_string());
                    }
                    i += 1;
                }
                label_mode = LabelMode::SelectedLabels(selected);
                continue; // Don't increment again
            }
            b"FILTER" => {
                i += 1;
                while i < args.len() {
                    filter_args.push(args[i].clone());
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }

    let filters = parse_ts_filters(&filter_args);
    (label_mode, filters)
}

fn parse_mrange_args(args: &[Bytes]) -> MrangeParams {
    let from = parse_range_bound_or_default(&args[0], i64::MIN);
    let to = parse_range_bound_or_default(&args[1], i64::MAX);

    let mut filters = Vec::new();
    let mut count = None;
    let mut aggregation = None;
    let mut label_mode = LabelMode::None;
    let mut filter_by_ts = None;
    let mut filter_by_value = None;

    let mut i = 2;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"FILTER" => {
                i += 1;
                let mut filter_args = Vec::new();
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    // Stop at known keywords
                    if matches!(
                        u.as_slice(),
                        b"COUNT"
                            | b"AGGREGATION"
                            | b"WITHLABELS"
                            | b"SELECTED_LABELS"
                            | b"FILTER_BY_TS"
                            | b"FILTER_BY_VALUE"
                    ) {
                        break;
                    }
                    filter_args.push(args[i].clone());
                    i += 1;
                }
                filters = parse_ts_filters(&filter_args);
                continue;
            }
            b"COUNT" => {
                i += 1;
                if i < args.len()
                    && let Ok(s) = std::str::from_utf8(&args[i])
                {
                    count = s.parse().ok();
                }
            }
            b"AGGREGATION" => {
                i += 1;
                if i + 1 < args.len()
                    && let Ok(agg_str) = std::str::from_utf8(&args[i])
                    && let Some(agg) = Aggregation::parse(agg_str)
                {
                    i += 1;
                    if let Ok(bucket_str) = std::str::from_utf8(&args[i])
                        && let Ok(bucket) = bucket_str.parse::<i64>()
                    {
                        aggregation = Some((agg, bucket));
                    }
                }
            }
            b"WITHLABELS" => {
                label_mode = LabelMode::WithLabels;
            }
            b"SELECTED_LABELS" => {
                let mut selected = Vec::new();
                i += 1;
                while i < args.len() {
                    let u = args[i].to_ascii_uppercase();
                    if matches!(
                        u.as_slice(),
                        b"FILTER"
                            | b"COUNT"
                            | b"AGGREGATION"
                            | b"FILTER_BY_TS"
                            | b"FILTER_BY_VALUE"
                    ) {
                        break;
                    }
                    if let Ok(s) = std::str::from_utf8(&args[i]) {
                        selected.push(s.to_string());
                    }
                    i += 1;
                }
                label_mode = LabelMode::SelectedLabels(selected);
                continue;
            }
            b"FILTER_BY_TS" => {
                i += 1;
                let mut timestamps = Vec::new();
                while i < args.len()
                    && let Ok(s) = std::str::from_utf8(&args[i])
                    && let Ok(ts) = s.parse::<i64>()
                {
                    timestamps.push(ts);
                    i += 1;
                }
                filter_by_ts = Some(timestamps);
                continue;
            }
            b"FILTER_BY_VALUE" => {
                i += 1;
                if i + 1 < args.len()
                    && let (Ok(min_s), Ok(max_s)) = (
                        std::str::from_utf8(&args[i]),
                        std::str::from_utf8(&args[i + 1]),
                    )
                    && let (Ok(min), Ok(max)) = (min_s.parse::<f64>(), max_s.parse::<f64>())
                {
                    filter_by_value = Some((min, max));
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    MrangeParams {
        from,
        to,
        filters,
        count,
        aggregation,
        label_mode,
        filter_by_ts,
        filter_by_value,
    }
}

fn parse_range_bound_or_default(arg: &[u8], default: i64) -> i64 {
    match std::str::from_utf8(arg) {
        Ok("-") => i64::MIN,
        Ok("+") => i64::MAX,
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}

fn build_labels_response(ts: &TimeSeriesValue, mode: &LabelMode) -> Response {
    match mode {
        LabelMode::None => Response::Array(vec![]),
        LabelMode::WithLabels => {
            let labels: Vec<Response> = ts
                .labels()
                .iter()
                .map(|(k, v)| {
                    Response::Array(vec![
                        Response::bulk(Bytes::from(k.clone())),
                        Response::bulk(Bytes::from(v.clone())),
                    ])
                })
                .collect();
            Response::Array(labels)
        }
        LabelMode::SelectedLabels(selected) => {
            let labels: Vec<Response> = selected
                .iter()
                .map(|name| {
                    let value = ts.get_label(name).unwrap_or("");
                    Response::Array(vec![
                        Response::bulk(Bytes::from(name.clone())),
                        Response::bulk(Bytes::from(value.to_string())),
                    ])
                })
                .collect();
            Response::Array(labels)
        }
    }
}

fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        format!("{}", f)
    }
}

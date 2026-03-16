//! FT.AGGREGATE pipeline: parser, AST, and executor.
//!
//! Supports: GROUPBY + reducers (COUNT, SUM, MIN, MAX, AVG) + SORTBY + LIMIT.

use std::collections::HashMap;

/// A single step in the aggregation pipeline.
#[derive(Debug, Clone)]
pub enum AggregateStep {
    GroupBy {
        fields: Vec<String>,
        reducers: Vec<ReducerDef>,
    },
    SortBy {
        fields: Vec<(String, SortDir)>,
    },
    Limit {
        offset: usize,
        count: usize,
    },
}

/// Sort direction for SORTBY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A reducer definition within a GROUPBY step.
#[derive(Debug, Clone)]
pub struct ReducerDef {
    pub function: ReducerFn,
    pub alias: String,
}

/// Supported reducer functions.
#[derive(Debug, Clone)]
pub enum ReducerFn {
    Count,
    Sum { field: String },
    Min { field: String },
    Max { field: String },
    Avg { field: String },
}

/// A row of field name-value pairs (the unit of data flowing through the pipeline).
pub type Row = Vec<(String, String)>;

/// Partial aggregate result from a single shard (mergeable across shards).
#[derive(Debug, Clone)]
pub struct PartialAggregate {
    /// Grouped partial results: group_key → partial reducer states.
    #[allow(clippy::type_complexity)]
    pub groups: Vec<(Vec<(String, String)>, Vec<PartialReducerState>)>,
}

/// Partial state for a single reducer (designed for cross-shard merging).
#[derive(Debug, Clone)]
pub enum PartialReducerState {
    Count(i64),
    Sum(f64),
    Min(f64),
    Max(f64),
    /// Avg stores (sum, count) so it can be merged then divided.
    Avg(f64, i64),
}

// =============================================================================
// Pipeline parser
// =============================================================================

/// Parse FT.AGGREGATE arguments (after the query string) into pipeline steps.
///
/// ```text
/// FT.AGGREGATE idx query
///   GROUPBY 1 @field REDUCE COUNT 0 AS count
///   SORTBY 2 @count DESC
///   LIMIT 0 10
/// ```
pub fn parse_aggregate_pipeline(args: &[&str]) -> Result<Vec<AggregateStep>, String> {
    let mut steps = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let kw = args[i].to_ascii_uppercase();
        match kw.as_str() {
            "GROUPBY" => {
                i += 1;
                if i >= args.len() {
                    return Err("GROUPBY requires nargs".into());
                }
                let nargs: usize = args[i]
                    .parse()
                    .map_err(|_| "GROUPBY nargs must be integer")?;
                i += 1;

                let mut fields = Vec::with_capacity(nargs);
                for _ in 0..nargs {
                    if i >= args.len() {
                        return Err("not enough fields for GROUPBY".into());
                    }
                    fields.push(strip_at(args[i]));
                    i += 1;
                }

                let mut reducers = Vec::new();
                while i < args.len() && args[i].eq_ignore_ascii_case("REDUCE") {
                    i += 1; // skip REDUCE
                    if i >= args.len() {
                        return Err("REDUCE requires function name".into());
                    }
                    let func_name = args[i].to_ascii_uppercase();
                    i += 1;

                    if i >= args.len() {
                        return Err("REDUCE requires nargs".into());
                    }
                    let reducer_nargs: usize = args[i]
                        .parse()
                        .map_err(|_| "REDUCE nargs must be integer")?;
                    i += 1;

                    let function = match func_name.as_str() {
                        "COUNT" => ReducerFn::Count,
                        "SUM" => {
                            if reducer_nargs < 1 || i >= args.len() {
                                return Err("SUM requires 1 argument".into());
                            }
                            let field = strip_at(args[i]);
                            i += 1;
                            ReducerFn::Sum { field }
                        }
                        "MIN" => {
                            if reducer_nargs < 1 || i >= args.len() {
                                return Err("MIN requires 1 argument".into());
                            }
                            let field = strip_at(args[i]);
                            i += 1;
                            ReducerFn::Min { field }
                        }
                        "MAX" => {
                            if reducer_nargs < 1 || i >= args.len() {
                                return Err("MAX requires 1 argument".into());
                            }
                            let field = strip_at(args[i]);
                            i += 1;
                            ReducerFn::Max { field }
                        }
                        "AVG" => {
                            if reducer_nargs < 1 || i >= args.len() {
                                return Err("AVG requires 1 argument".into());
                            }
                            let field = strip_at(args[i]);
                            i += 1;
                            ReducerFn::Avg { field }
                        }
                        other => return Err(format!("unknown reducer '{other}'")),
                    };

                    // Parse optional AS alias
                    let alias = if i + 1 < args.len() && args[i].eq_ignore_ascii_case("AS") {
                        i += 1; // skip AS
                        let a = args[i].to_string();
                        i += 1;
                        a
                    } else {
                        // Default alias: function name lowercased
                        match &function {
                            ReducerFn::Count => "count".into(),
                            ReducerFn::Sum { field } => format!("sum_{field}"),
                            ReducerFn::Min { field } => format!("min_{field}"),
                            ReducerFn::Max { field } => format!("max_{field}"),
                            ReducerFn::Avg { field } => format!("avg_{field}"),
                        }
                    };

                    reducers.push(ReducerDef { function, alias });
                }

                steps.push(AggregateStep::GroupBy { fields, reducers });
            }
            "SORTBY" => {
                i += 1;
                if i >= args.len() {
                    return Err("SORTBY requires nargs".into());
                }
                let nargs: usize = args[i]
                    .parse()
                    .map_err(|_| "SORTBY nargs must be integer")?;
                i += 1;

                let mut fields = Vec::new();
                let mut consumed = 0;
                while consumed < nargs && i < args.len() {
                    let field = strip_at(args[i]);
                    i += 1;
                    consumed += 1;

                    let dir =
                        if consumed < nargs && i < args.len() {
                            let maybe_dir = args[i].to_ascii_uppercase();
                            if maybe_dir == "ASC" || maybe_dir == "DESC" {
                                i += 1;
                                consumed += 1;
                                if maybe_dir == "DESC" {
                                    SortDir::Desc
                                } else {
                                    SortDir::Asc
                                }
                            } else {
                                SortDir::Asc
                            }
                        } else {
                            SortDir::Asc
                        };
                    fields.push((field, dir));
                }

                steps.push(AggregateStep::SortBy { fields });
            }
            "LIMIT" => {
                i += 1;
                if i + 1 >= args.len() {
                    return Err("LIMIT requires offset and count".into());
                }
                let offset: usize = args[i]
                    .parse()
                    .map_err(|_| "LIMIT offset must be integer")?;
                i += 1;
                let count: usize = args[i]
                    .parse()
                    .map_err(|_| "LIMIT count must be integer")?;
                i += 1;
                steps.push(AggregateStep::Limit { offset, count });
            }
            _ => {
                return Err(format!("unknown pipeline step '{}'", args[i]));
            }
        }
    }

    Ok(steps)
}

/// Strip leading '@' from a field name.
fn strip_at(s: &str) -> String {
    s.strip_prefix('@').unwrap_or(s).to_string()
}

// =============================================================================
// Shard-local execution
// =============================================================================

/// Execute the aggregation pipeline on shard-local rows, producing partial aggregates.
///
/// For GROUPBY+REDUCE, returns partial reducer states that can be merged across shards.
/// For steps that don't involve GROUPBY (or after GROUPBY), returns fully realized rows.
pub fn execute_shard_local(
    rows: &[Row],
    steps: &[AggregateStep],
) -> PartialAggregate {
    // Find the GROUPBY step (we only support one GROUPBY for now)
    let groupby = steps.iter().find_map(|s| {
        if let AggregateStep::GroupBy { fields, reducers } = s {
            Some((fields, reducers))
        } else {
            None
        }
    });

    match groupby {
        Some((group_fields, reducers)) => {
            // Group rows by the groupby fields
            let mut groups: HashMap<Vec<(String, String)>, Vec<PartialReducerState>> =
                HashMap::new();

            for row in rows {
                let key: Vec<(String, String)> = group_fields
                    .iter()
                    .map(|f| {
                        let val = row
                            .iter()
                            .find(|(k, _)| k == f)
                            .map(|(_, v)| v.clone())
                            .unwrap_or_default();
                        (f.clone(), val)
                    })
                    .collect();

                let states = groups
                    .entry(key)
                    .or_insert_with(|| init_reducer_states(reducers));

                update_reducer_states(states, reducers, row);
            }

            PartialAggregate {
                groups: groups.into_iter().collect(),
            }
        }
        None => {
            // No GROUPBY — just return all rows as individual groups (pass-through)
            PartialAggregate {
                groups: rows
                    .iter()
                    .map(|row| (row.clone(), Vec::new()))
                    .collect(),
            }
        }
    }
}

/// Initialize reducer states to identity values.
fn init_reducer_states(reducers: &[ReducerDef]) -> Vec<PartialReducerState> {
    reducers
        .iter()
        .map(|r| match &r.function {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum { .. } => PartialReducerState::Sum(0.0),
            ReducerFn::Min { .. } => PartialReducerState::Min(f64::INFINITY),
            ReducerFn::Max { .. } => PartialReducerState::Max(f64::NEG_INFINITY),
            ReducerFn::Avg { .. } => PartialReducerState::Avg(0.0, 0),
        })
        .collect()
}

/// Update reducer states with one row's values.
fn update_reducer_states(
    states: &mut [PartialReducerState],
    reducers: &[ReducerDef],
    row: &Row,
) {
    for (state, def) in states.iter_mut().zip(reducers.iter()) {
        match (&mut *state, &def.function) {
            (PartialReducerState::Count(c), ReducerFn::Count) => {
                *c += 1;
            }
            (PartialReducerState::Sum(s), ReducerFn::Sum { field }) => {
                if let Some(val) = get_field_f64(row, field) {
                    *s += val;
                }
            }
            (PartialReducerState::Min(m), ReducerFn::Min { field }) => {
                if let Some(val) = get_field_f64(row, field)
                    && val < *m
                {
                    *m = val;
                }
            }
            (PartialReducerState::Max(m), ReducerFn::Max { field }) => {
                if let Some(val) = get_field_f64(row, field)
                    && val > *m
                {
                    *m = val;
                }
            }
            (PartialReducerState::Avg(sum, count), ReducerFn::Avg { field }) => {
                if let Some(val) = get_field_f64(row, field) {
                    *sum += val;
                    *count += 1;
                }
            }
            _ => {}
        }
    }
}

fn get_field_f64(row: &Row, field: &str) -> Option<f64> {
    row.iter()
        .find(|(k, _)| k == field)
        .and_then(|(_, v)| v.parse().ok())
}

// =============================================================================
// Cross-shard merge
// =============================================================================

/// Merge partial aggregates from multiple shards into final rows.
pub fn merge_partials(
    partials: Vec<PartialAggregate>,
    steps: &[AggregateStep],
) -> Vec<Row> {
    let has_groupby = steps
        .iter()
        .any(|s| matches!(s, AggregateStep::GroupBy { .. }));

    let reducers: Vec<&ReducerDef> = steps
        .iter()
        .find_map(|s| {
            if let AggregateStep::GroupBy { reducers, .. } = s {
                Some(reducers.iter().collect())
            } else {
                None
            }
        })
        .unwrap_or_default();

    let mut rows = if has_groupby {
        // Merge grouped results across shards
        let mut merged: HashMap<Vec<(String, String)>, Vec<PartialReducerState>> = HashMap::new();

        for partial in partials {
            for (key, states) in partial.groups {
                let entry = merged
                    .entry(key)
                    .or_insert_with(|| init_reducer_states_from_refs(&reducers));
                merge_states(entry, &states);
            }
        }

        // Finalize: convert group key + reducer states into rows
        let mut rows: Vec<Row> = Vec::with_capacity(merged.len());
        for (key, states) in merged {
            let mut row = key;
            for (state, def) in states.iter().zip(reducers.iter()) {
                let val = finalize_state(state);
                row.push((def.alias.clone(), val));
            }
            rows.push(row);
        }
        rows
    } else {
        // No GROUPBY — just concatenate all rows
        partials
            .into_iter()
            .flat_map(|p| p.groups.into_iter().map(|(row, _)| row))
            .collect()
    };

    // Apply post-merge pipeline steps: SORTBY, LIMIT
    for step in steps {
        match step {
            AggregateStep::SortBy { fields } => {
                rows.sort_by(|a, b| {
                    for (field, dir) in fields {
                        let va = a.iter().find(|(k, _)| k == field).map(|(_, v)| v.as_str());
                        let vb = b.iter().find(|(k, _)| k == field).map(|(_, v)| v.as_str());

                        // Try numeric comparison first
                        let cmp = match (
                            va.and_then(|v| v.parse::<f64>().ok()),
                            vb.and_then(|v| v.parse::<f64>().ok()),
                        ) {
                            (Some(fa), Some(fb)) => {
                                fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                            }
                            _ => va.cmp(&vb),
                        };

                        let cmp = if *dir == SortDir::Desc {
                            cmp.reverse()
                        } else {
                            cmp
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            AggregateStep::Limit { offset, count } => {
                let start = (*offset).min(rows.len());
                let end = (start + *count).min(rows.len());
                rows = rows[start..end].to_vec();
            }
            AggregateStep::GroupBy { .. } => {
                // Already handled above
            }
        }
    }

    rows
}

fn init_reducer_states_from_refs(reducers: &[&ReducerDef]) -> Vec<PartialReducerState> {
    reducers
        .iter()
        .map(|r| match &r.function {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum { .. } => PartialReducerState::Sum(0.0),
            ReducerFn::Min { .. } => PartialReducerState::Min(f64::INFINITY),
            ReducerFn::Max { .. } => PartialReducerState::Max(f64::NEG_INFINITY),
            ReducerFn::Avg { .. } => PartialReducerState::Avg(0.0, 0),
        })
        .collect()
}

/// Merge source states into destination states.
fn merge_states(dst: &mut [PartialReducerState], src: &[PartialReducerState]) {
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        match (d, s) {
            (PartialReducerState::Count(dc), PartialReducerState::Count(sc)) => *dc += sc,
            (PartialReducerState::Sum(ds), PartialReducerState::Sum(ss)) => *ds += ss,
            (PartialReducerState::Min(dm), PartialReducerState::Min(sm)) => {
                if *sm < *dm {
                    *dm = *sm;
                }
            }
            (PartialReducerState::Max(dm), PartialReducerState::Max(sm)) => {
                if *sm > *dm {
                    *dm = *sm;
                }
            }
            (
                PartialReducerState::Avg(dsum, dcount),
                PartialReducerState::Avg(ssum, scount),
            ) => {
                *dsum += ssum;
                *dcount += scount;
            }
            _ => {}
        }
    }
}

/// Finalize a partial reducer state into a string value.
fn finalize_state(state: &PartialReducerState) -> String {
    match state {
        PartialReducerState::Count(c) => c.to_string(),
        PartialReducerState::Sum(s) => format_f64(*s),
        PartialReducerState::Min(m) => {
            if *m == f64::INFINITY {
                "0".to_string()
            } else {
                format_f64(*m)
            }
        }
        PartialReducerState::Max(m) => {
            if *m == f64::NEG_INFINITY {
                "0".to_string()
            } else {
                format_f64(*m)
            }
        }
        PartialReducerState::Avg(sum, count) => {
            if *count == 0 {
                "0".to_string()
            } else {
                format_f64(*sum / *count as f64)
            }
        }
    }
}

/// Format f64, omitting trailing ".0" for whole numbers.
fn format_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_groupby_count() {
        let args = ["GROUPBY", "1", "@city", "REDUCE", "COUNT", "0", "AS", "num"];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        assert_eq!(steps.len(), 1);
        if let AggregateStep::GroupBy { fields, reducers } = &steps[0] {
            assert_eq!(fields, &["city"]);
            assert_eq!(reducers.len(), 1);
            assert_eq!(reducers[0].alias, "num");
            assert!(matches!(reducers[0].function, ReducerFn::Count));
        } else {
            panic!("expected GroupBy");
        }
    }

    #[test]
    fn test_parse_groupby_sum_with_sortby_limit() {
        let args = [
            "GROUPBY", "1", "@category",
            "REDUCE", "SUM", "1", "@price", "AS", "total",
            "SORTBY", "2", "@total", "DESC",
            "LIMIT", "0", "5",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        assert_eq!(steps.len(), 3);
        assert!(matches!(&steps[0], AggregateStep::GroupBy { .. }));
        assert!(matches!(&steps[1], AggregateStep::SortBy { .. }));
        if let AggregateStep::Limit { offset, count } = &steps[2] {
            assert_eq!(*offset, 0);
            assert_eq!(*count, 5);
        }
    }

    #[test]
    fn test_shard_local_groupby_count() {
        let rows = vec![
            vec![("city".into(), "NYC".into()), ("name".into(), "Alice".into())],
            vec![("city".into(), "NYC".into()), ("name".into(), "Bob".into())],
            vec![("city".into(), "LA".into()), ("name".into(), "Carol".into())],
        ];
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["city".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Count,
                alias: "count".into(),
            }],
        }];
        let partial = execute_shard_local(&rows, &steps);
        assert_eq!(partial.groups.len(), 2);

        // Find NYC group
        let nyc = partial.groups.iter().find(|(k, _)| k[0].1 == "NYC").unwrap();
        assert!(matches!(nyc.1[0], PartialReducerState::Count(2)));
    }

    #[test]
    fn test_merge_partials_sum() {
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["city".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Sum {
                    field: "sales".into(),
                },
                alias: "total_sales".into(),
            }],
        }];

        let p1 = PartialAggregate {
            groups: vec![(
                vec![("city".into(), "NYC".into())],
                vec![PartialReducerState::Sum(100.0)],
            )],
        };
        let p2 = PartialAggregate {
            groups: vec![(
                vec![("city".into(), "NYC".into())],
                vec![PartialReducerState::Sum(250.0)],
            )],
        };

        let rows = merge_partials(vec![p1, p2], &steps);
        assert_eq!(rows.len(), 1);
        let total = rows[0].iter().find(|(k, _)| k == "total_sales").unwrap();
        assert_eq!(total.1, "350");
    }

    #[test]
    fn test_merge_partials_avg() {
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["dept".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Avg {
                    field: "salary".into(),
                },
                alias: "avg_salary".into(),
            }],
        }];

        let p1 = PartialAggregate {
            groups: vec![(
                vec![("dept".into(), "eng".into())],
                vec![PartialReducerState::Avg(300.0, 3)],
            )],
        };
        let p2 = PartialAggregate {
            groups: vec![(
                vec![("dept".into(), "eng".into())],
                vec![PartialReducerState::Avg(200.0, 2)],
            )],
        };

        let rows = merge_partials(vec![p1, p2], &steps);
        assert_eq!(rows.len(), 1);
        let avg = rows[0].iter().find(|(k, _)| k == "avg_salary").unwrap();
        assert_eq!(avg.1, "100"); // (300+200)/(3+2) = 100
    }

    #[test]
    fn test_sortby_and_limit() {
        let steps = vec![
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "count".into(),
                }],
            },
            AggregateStep::SortBy {
                fields: vec![("count".into(), SortDir::Desc)],
            },
            AggregateStep::Limit {
                offset: 0,
                count: 2,
            },
        ];

        let p = PartialAggregate {
            groups: vec![
                (
                    vec![("city".into(), "NYC".into())],
                    vec![PartialReducerState::Count(10)],
                ),
                (
                    vec![("city".into(), "LA".into())],
                    vec![PartialReducerState::Count(5)],
                ),
                (
                    vec![("city".into(), "CHI".into())],
                    vec![PartialReducerState::Count(8)],
                ),
            ],
        };

        let rows = merge_partials(vec![p], &steps);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].iter().find(|(k, _)| k == "city").unwrap().1, "NYC");
        assert_eq!(rows[1].iter().find(|(k, _)| k == "city").unwrap().1, "CHI");
    }
}

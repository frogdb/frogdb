//! FT.AGGREGATE pipeline: parser, AST, and executor.
//!
//! Supports: GROUPBY + reducers (COUNT, SUM, MIN, MAX, AVG, COUNT_DISTINCT,
//! COUNT_DISTINCTISH, TOLIST, FIRST_VALUE, STDDEV, QUANTILE, RANDOM_SAMPLE),
//! APPLY, FILTER, LOAD, SORTBY, LIMIT, and multiple GROUPBY stages.

use std::collections::{HashMap, HashSet};

use rand::Rng;

use crate::expression::{self, Expr};

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
    Apply {
        expr: Expr,
        alias: String,
    },
    Filter {
        expr: Expr,
    },
    Load {
        fields: Vec<String>,
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
    Sum {
        field: String,
    },
    Min {
        field: String,
    },
    Max {
        field: String,
    },
    Avg {
        field: String,
    },
    CountDistinct {
        field: String,
    },
    CountDistinctish {
        field: String,
    },
    Tolist {
        field: String,
    },
    FirstValue {
        field: String,
        sort_by: Option<String>,
        sort_dir: SortDir,
    },
    Stddev {
        field: String,
    },
    Quantile {
        field: String,
        quantile: f64,
    },
    RandomSample {
        field: String,
        count: usize,
    },
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
    CountDistinct(HashSet<String>),
    /// 256-register HyperLogLog sketch for approximate distinct count.
    CountDistinctish(Vec<u8>),
    Tolist(Vec<String>),
    FirstValue {
        value: Option<String>,
        sort_key: Option<String>,
        sort_asc: bool,
    },
    Stddev {
        sum: f64,
        sum_sq: f64,
        count: i64,
    },
    Quantile {
        values: Vec<f64>,
        quantile: f64,
    },
    RandomSample {
        reservoir: Vec<String>,
        count: usize,
        seen: usize,
    },
}

// =============================================================================
// Pipeline parser
// =============================================================================

/// Parse FT.AGGREGATE arguments (after the query string) into pipeline steps.
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

                    let function = parse_reducer(&func_name, reducer_nargs, args, &mut i)?;

                    // Parse optional AS alias
                    let alias = if i + 1 < args.len() && args[i].eq_ignore_ascii_case("AS") {
                        i += 1; // skip AS
                        let a = args[i].to_string();
                        i += 1;
                        a
                    } else {
                        default_alias(&function)
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

                    let dir = if consumed < nargs && i < args.len() {
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
                let count: usize = args[i].parse().map_err(|_| "LIMIT count must be integer")?;
                i += 1;
                steps.push(AggregateStep::Limit { offset, count });
            }
            "APPLY" => {
                i += 1;
                if i >= args.len() {
                    return Err("APPLY requires expression".into());
                }
                let expr_str = args[i];
                i += 1;
                if i >= args.len() || !args[i].eq_ignore_ascii_case("AS") {
                    return Err("APPLY requires AS alias".into());
                }
                i += 1; // skip AS
                if i >= args.len() {
                    return Err("APPLY AS requires alias name".into());
                }
                let alias = strip_at(args[i]);
                i += 1;
                let expr = expression::parse_expression(expr_str)
                    .map_err(|e| format!("APPLY expression error: {e}"))?;
                steps.push(AggregateStep::Apply { expr, alias });
            }
            "FILTER" => {
                i += 1;
                if i >= args.len() {
                    return Err("FILTER requires expression".into());
                }
                let expr_str = args[i];
                i += 1;
                let expr = expression::parse_expression(expr_str)
                    .map_err(|e| format!("FILTER expression error: {e}"))?;
                steps.push(AggregateStep::Filter { expr });
            }
            "LOAD" => {
                i += 1;
                if i >= args.len() {
                    return Err("LOAD requires nargs".into());
                }
                let nargs: usize = args[i].parse().map_err(|_| "LOAD nargs must be integer")?;
                i += 1;
                let mut fields = Vec::with_capacity(nargs);
                for _ in 0..nargs {
                    if i >= args.len() {
                        return Err("not enough fields for LOAD".into());
                    }
                    fields.push(strip_at(args[i]));
                    i += 1;
                }
                steps.push(AggregateStep::Load { fields });
            }
            _ => {
                return Err(format!("unknown pipeline step '{}'", args[i]));
            }
        }
    }

    Ok(steps)
}

fn parse_reducer(
    func_name: &str,
    reducer_nargs: usize,
    args: &[&str],
    i: &mut usize,
) -> Result<ReducerFn, String> {
    match func_name {
        "COUNT" => Ok(ReducerFn::Count),
        "SUM" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("SUM requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Sum { field })
        }
        "MIN" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("MIN requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Min { field })
        }
        "MAX" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("MAX requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Max { field })
        }
        "AVG" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("AVG requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Avg { field })
        }
        "COUNT_DISTINCT" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("COUNT_DISTINCT requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::CountDistinct { field })
        }
        "COUNT_DISTINCTISH" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("COUNT_DISTINCTISH requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::CountDistinctish { field })
        }
        "TOLIST" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("TOLIST requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Tolist { field })
        }
        "FIRST_VALUE" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("FIRST_VALUE requires at least 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            // Optional BY @sort_field [ASC|DESC]
            let mut sort_by = None;
            let mut sort_dir = SortDir::Asc;
            if *i < args.len() && args[*i].eq_ignore_ascii_case("BY") {
                *i += 1;
                if *i < args.len() {
                    sort_by = Some(strip_at(args[*i]));
                    *i += 1;
                }
                if *i < args.len() {
                    let d = args[*i].to_ascii_uppercase();
                    if d == "DESC" {
                        sort_dir = SortDir::Desc;
                        *i += 1;
                    } else if d == "ASC" {
                        *i += 1;
                    }
                }
            }
            Ok(ReducerFn::FirstValue {
                field,
                sort_by,
                sort_dir,
            })
        }
        "STDDEV" => {
            if reducer_nargs < 1 || *i >= args.len() {
                return Err("STDDEV requires 1 argument".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            Ok(ReducerFn::Stddev { field })
        }
        "QUANTILE" => {
            if reducer_nargs < 2 || *i + 1 >= args.len() {
                return Err("QUANTILE requires 2 arguments".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            let quantile: f64 = args[*i]
                .parse()
                .map_err(|_| "QUANTILE value must be a number")?;
            *i += 1;
            Ok(ReducerFn::Quantile { field, quantile })
        }
        "RANDOM_SAMPLE" => {
            if reducer_nargs < 2 || *i + 1 >= args.len() {
                return Err("RANDOM_SAMPLE requires 2 arguments".into());
            }
            let field = strip_at(args[*i]);
            *i += 1;
            let count: usize = args[*i]
                .parse()
                .map_err(|_| "RANDOM_SAMPLE count must be integer")?;
            *i += 1;
            Ok(ReducerFn::RandomSample { field, count })
        }
        other => Err(format!("unknown reducer '{other}'")),
    }
}

fn default_alias(function: &ReducerFn) -> String {
    match function {
        ReducerFn::Count => "count".into(),
        ReducerFn::Sum { field } => format!("sum_{field}"),
        ReducerFn::Min { field } => format!("min_{field}"),
        ReducerFn::Max { field } => format!("max_{field}"),
        ReducerFn::Avg { field } => format!("avg_{field}"),
        ReducerFn::CountDistinct { field } => format!("count_distinct_{field}"),
        ReducerFn::CountDistinctish { field } => format!("count_distinctish_{field}"),
        ReducerFn::Tolist { field } => format!("tolist_{field}"),
        ReducerFn::FirstValue { field, .. } => format!("first_{field}"),
        ReducerFn::Stddev { field } => format!("stddev_{field}"),
        ReducerFn::Quantile { field, .. } => format!("quantile_{field}"),
        ReducerFn::RandomSample { field, .. } => format!("sample_{field}"),
    }
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
/// Processes APPLY/FILTER steps before the first GROUPBY at shard level.
/// Returns partial reducer states that can be merged across shards.
pub fn execute_shard_local(rows: &[Row], steps: &[AggregateStep]) -> PartialAggregate {
    let mut current_rows: Vec<Row> = rows.to_vec();

    // Process steps up to (and including) the first GROUPBY
    for (idx, step) in steps.iter().enumerate() {
        match step {
            AggregateStep::Apply { expr, alias } => {
                for row in &mut current_rows {
                    let val = expression::evaluate(expr, row);
                    row.push((alias.clone(), val.as_string()));
                }
            }
            AggregateStep::Filter { expr } => {
                current_rows.retain(|row| expression::evaluate(expr, row).is_truthy());
            }
            AggregateStep::Load { .. } => {
                // LOAD is handled by the shard execution layer (needs store access),
                // skip here.
            }
            AggregateStep::GroupBy { fields, reducers } => {
                // Execute GROUPBY on current rows and return partial aggregate
                let partial = group_rows_partial(&current_rows, fields, reducers);
                // Record which step index we stopped at so the coordinator
                // knows where to resume (encoded as __resume_idx in the first group).
                // We use the PartialAggregate as-is; resume_idx is computed
                // deterministically by the coordinator from the parsed steps.
                let _ = idx; // used implicitly via step ordering
                return partial;
            }
            AggregateStep::SortBy { .. } | AggregateStep::Limit { .. } => {
                // Deferred to coordinator
            }
        }
    }

    // No GROUPBY found — return all rows as pass-through
    PartialAggregate {
        groups: current_rows
            .into_iter()
            .map(|row| (row, Vec::new()))
            .collect(),
    }
}

/// Group rows and produce partial reducer states (for cross-shard merging).
fn group_rows_partial(
    rows: &[Row],
    group_fields: &[String],
    reducers: &[ReducerDef],
) -> PartialAggregate {
    let mut groups: HashMap<Vec<(String, String)>, Vec<PartialReducerState>> = HashMap::new();

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

/// Group rows locally (group + finalize in one step). Used by the coordinator
/// for subsequent GROUPBYs after the first cross-shard merge.
pub fn group_rows_local(rows: &[Row], fields: &[String], reducers: &[ReducerDef]) -> Vec<Row> {
    let mut groups: HashMap<Vec<(String, String)>, Vec<PartialReducerState>> = HashMap::new();

    for row in rows {
        let key: Vec<(String, String)> = fields
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

    let mut result = Vec::with_capacity(groups.len());
    for (key, states) in groups {
        let mut row = key;
        for (state, def) in states.iter().zip(reducers.iter()) {
            row.push((def.alias.clone(), finalize_state(state)));
        }
        result.push(row);
    }
    result
}

// =============================================================================
// Reducer state management
// =============================================================================

/// Initialize reducer states to identity values.
pub fn init_reducer_states(reducers: &[ReducerDef]) -> Vec<PartialReducerState> {
    reducers
        .iter()
        .map(|r| match &r.function {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum { .. } => PartialReducerState::Sum(0.0),
            ReducerFn::Min { .. } => PartialReducerState::Min(f64::INFINITY),
            ReducerFn::Max { .. } => PartialReducerState::Max(f64::NEG_INFINITY),
            ReducerFn::Avg { .. } => PartialReducerState::Avg(0.0, 0),
            ReducerFn::CountDistinct { .. } => PartialReducerState::CountDistinct(HashSet::new()),
            ReducerFn::CountDistinctish { .. } => {
                PartialReducerState::CountDistinctish(vec![0u8; 256])
            }
            ReducerFn::Tolist { .. } => PartialReducerState::Tolist(Vec::new()),
            ReducerFn::FirstValue { sort_dir, .. } => PartialReducerState::FirstValue {
                value: None,
                sort_key: None,
                sort_asc: *sort_dir == SortDir::Asc,
            },
            ReducerFn::Stddev { .. } => PartialReducerState::Stddev {
                sum: 0.0,
                sum_sq: 0.0,
                count: 0,
            },
            ReducerFn::Quantile { quantile, .. } => PartialReducerState::Quantile {
                values: Vec::new(),
                quantile: *quantile,
            },
            ReducerFn::RandomSample { count, .. } => PartialReducerState::RandomSample {
                reservoir: Vec::new(),
                count: *count,
                seen: 0,
            },
        })
        .collect()
}

/// Get a field's string value from a row.
pub fn get_field_str(row: &Row, field: &str) -> String {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

fn get_field_f64(row: &Row, field: &str) -> Option<f64> {
    row.iter()
        .find(|(k, _)| k == field)
        .and_then(|(_, v)| v.parse().ok())
}

/// Update reducer states with one row's values.
fn update_reducer_states(states: &mut [PartialReducerState], reducers: &[ReducerDef], row: &Row) {
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
            (PartialReducerState::CountDistinct(set), ReducerFn::CountDistinct { field }) => {
                let val = get_field_str(row, field);
                if !val.is_empty() {
                    set.insert(val);
                }
            }
            (
                PartialReducerState::CountDistinctish(regs),
                ReducerFn::CountDistinctish { field },
            ) => {
                let val = get_field_str(row, field);
                if !val.is_empty() {
                    hll_add(regs, &val);
                }
            }
            (PartialReducerState::Tolist(list), ReducerFn::Tolist { field }) => {
                let val = get_field_str(row, field);
                if !val.is_empty() {
                    list.push(val);
                }
            }
            (
                PartialReducerState::FirstValue {
                    value,
                    sort_key,
                    sort_asc,
                },
                ReducerFn::FirstValue { field, sort_by, .. },
            ) => {
                let row_val = get_field_str(row, field);
                let row_sort = sort_by
                    .as_ref()
                    .map(|sb| get_field_str(row, sb))
                    .unwrap_or_default();
                let replace = match sort_key {
                    None => true,
                    Some(existing) => {
                        if *sort_asc {
                            row_sort < *existing
                        } else {
                            row_sort > *existing
                        }
                    }
                };
                if replace {
                    *value = Some(row_val);
                    *sort_key = Some(row_sort);
                }
            }
            (PartialReducerState::Stddev { sum, sum_sq, count }, ReducerFn::Stddev { field }) => {
                if let Some(val) = get_field_f64(row, field) {
                    *sum += val;
                    *sum_sq += val * val;
                    *count += 1;
                }
            }
            (PartialReducerState::Quantile { values, .. }, ReducerFn::Quantile { field, .. }) => {
                if let Some(val) = get_field_f64(row, field) {
                    values.push(val);
                }
            }
            (
                PartialReducerState::RandomSample {
                    reservoir,
                    count,
                    seen,
                },
                ReducerFn::RandomSample { field, .. },
            ) => {
                let val = get_field_str(row, field);
                if !val.is_empty() {
                    *seen += 1;
                    if reservoir.len() < *count {
                        reservoir.push(val);
                    } else {
                        let j = rand::thread_rng().gen_range(0..*seen);
                        if j < *count {
                            reservoir[j] = val;
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

// =============================================================================
// HyperLogLog helpers (256-register, inline)
// =============================================================================

fn hll_hash(value: &str) -> u64 {
    // FNV-1a hash with avalanche mixing for better distribution
    let mut h: u64 = 0xcbf29ce484222325;
    for b in value.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Avalanche mix (MurmurHash3 finalizer)
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

fn hll_add(registers: &mut [u8], value: &str) {
    let hash = hll_hash(value);
    let idx = (hash & 0xFF) as usize;
    let remaining = hash >> 8;
    // Count leading zeros in the remaining 56 bits, capped
    let rho = if remaining == 0 {
        57 // all zeros in 56 bits + 1
    } else {
        // leading_zeros of a u64 counts from bit 63; we only have 56 meaningful bits
        let lz = remaining.leading_zeros() as u8;
        // Subtract 8 to account for the 8 bits already used as index
        (lz - 8) + 1
    };
    if rho > registers[idx] {
        registers[idx] = rho;
    }
}

fn hll_estimate(registers: &[u8]) -> f64 {
    let m = registers.len() as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let sum: f64 = registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();
    let estimate = alpha * m * m / sum;

    // Small range correction
    if estimate <= 2.5 * m {
        let zeros = registers.iter().filter(|&&r| r == 0).count() as f64;
        if zeros > 0.0 {
            m * (m / zeros).ln()
        } else {
            estimate
        }
    } else {
        estimate
    }
}

// =============================================================================
// Cross-shard merge
// =============================================================================

/// Merge partial aggregates from multiple shards into final rows.
///
/// After merging the first GROUPBY results, executes remaining pipeline steps
/// locally (APPLY, FILTER, subsequent GROUPBYs, SORTBY, LIMIT).
pub fn merge_partials(partials: Vec<PartialAggregate>, steps: &[AggregateStep]) -> Vec<Row> {
    // Find the first GROUPBY index
    let first_groupby_idx = steps
        .iter()
        .position(|s| matches!(s, AggregateStep::GroupBy { .. }));

    let mut rows = match first_groupby_idx {
        Some(gb_idx) => {
            let (group_fields, reducers) = match &steps[gb_idx] {
                AggregateStep::GroupBy { fields, reducers } => (fields, reducers),
                _ => unreachable!(),
            };

            // Merge grouped results across shards
            let mut merged: HashMap<Vec<(String, String)>, Vec<PartialReducerState>> =
                HashMap::new();

            for partial in partials {
                for (key, states) in partial.groups {
                    let entry = merged
                        .entry(key)
                        .or_insert_with(|| init_reducer_states(reducers));
                    merge_states(entry, &states);
                }
            }

            // Finalize: convert group key + reducer states into rows
            let mut result: Vec<Row> = Vec::with_capacity(merged.len());
            for (key, states) in merged {
                let mut row = key;
                for (state, def) in states.iter().zip(reducers.iter()) {
                    let val = finalize_state(state);
                    row.push((def.alias.clone(), val));
                }
                result.push(row);
            }

            // Execute remaining steps after the first GROUPBY
            let resume_idx = gb_idx + 1;
            apply_post_merge_steps(&mut result, &steps[resume_idx..], group_fields);

            result
        }
        None => {
            // No GROUPBY — concatenate all rows and apply steps
            let mut result: Vec<Row> = partials
                .into_iter()
                .flat_map(|p| p.groups.into_iter().map(|(row, _)| row))
                .collect();

            apply_post_merge_steps(&mut result, steps, &[]);

            result
        }
    };

    // Ensure final output is in `rows`
    let _ = &mut rows;
    rows
}

/// Apply pipeline steps locally at the coordinator after merging.
fn apply_post_merge_steps(
    rows: &mut Vec<Row>,
    steps: &[AggregateStep],
    _prior_group_fields: &[String],
) {
    for step in steps {
        match step {
            AggregateStep::Apply { expr, alias } => {
                for row in rows.iter_mut() {
                    let val = expression::evaluate(expr, row);
                    // Remove existing field with same alias if present
                    row.retain(|(k, _)| k != alias);
                    row.push((alias.clone(), val.as_string()));
                }
            }
            AggregateStep::Filter { expr } => {
                rows.retain(|row| expression::evaluate(expr, row).is_truthy());
            }
            AggregateStep::GroupBy { fields, reducers } => {
                // Subsequent GROUPBY runs locally
                *rows = group_rows_local(rows, fields, reducers);
            }
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
                *rows = rows[start..end].to_vec();
            }
            AggregateStep::Load { .. } => {
                // LOAD is handled at shard level — no-op at coordinator
            }
        }
    }
}

pub fn init_reducer_states_from_refs(reducers: &[&ReducerDef]) -> Vec<PartialReducerState> {
    // Convert &[&ReducerDef] to init_reducer_states-compatible slice
    reducers
        .iter()
        .map(|r| match &r.function {
            ReducerFn::Count => PartialReducerState::Count(0),
            ReducerFn::Sum { .. } => PartialReducerState::Sum(0.0),
            ReducerFn::Min { .. } => PartialReducerState::Min(f64::INFINITY),
            ReducerFn::Max { .. } => PartialReducerState::Max(f64::NEG_INFINITY),
            ReducerFn::Avg { .. } => PartialReducerState::Avg(0.0, 0),
            ReducerFn::CountDistinct { .. } => PartialReducerState::CountDistinct(HashSet::new()),
            ReducerFn::CountDistinctish { .. } => {
                PartialReducerState::CountDistinctish(vec![0u8; 256])
            }
            ReducerFn::Tolist { .. } => PartialReducerState::Tolist(Vec::new()),
            ReducerFn::FirstValue { sort_dir, .. } => PartialReducerState::FirstValue {
                value: None,
                sort_key: None,
                sort_asc: *sort_dir == SortDir::Asc,
            },
            ReducerFn::Stddev { .. } => PartialReducerState::Stddev {
                sum: 0.0,
                sum_sq: 0.0,
                count: 0,
            },
            ReducerFn::Quantile { quantile, .. } => PartialReducerState::Quantile {
                values: Vec::new(),
                quantile: *quantile,
            },
            ReducerFn::RandomSample { count, .. } => PartialReducerState::RandomSample {
                reservoir: Vec::new(),
                count: *count,
                seen: 0,
            },
        })
        .collect()
}

/// Merge source states into destination states.
pub fn merge_states(dst: &mut [PartialReducerState], src: &[PartialReducerState]) {
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
            (PartialReducerState::Avg(dsum, dcount), PartialReducerState::Avg(ssum, scount)) => {
                *dsum += ssum;
                *dcount += scount;
            }
            (
                PartialReducerState::CountDistinct(dset),
                PartialReducerState::CountDistinct(sset),
            ) => {
                for val in sset {
                    dset.insert(val.clone());
                }
            }
            (
                PartialReducerState::CountDistinctish(dregs),
                PartialReducerState::CountDistinctish(sregs),
            ) => {
                // Element-wise max merge
                for (d, s) in dregs.iter_mut().zip(sregs.iter()) {
                    if *s > *d {
                        *d = *s;
                    }
                }
            }
            (PartialReducerState::Tolist(dlist), PartialReducerState::Tolist(slist)) => {
                dlist.extend(slist.iter().cloned());
            }
            (
                PartialReducerState::FirstValue {
                    value: dval,
                    sort_key: dkey,
                    sort_asc,
                },
                PartialReducerState::FirstValue {
                    value: sval,
                    sort_key: Some(sk),
                    ..
                },
            ) => {
                let replace = match dkey {
                    None => true,
                    Some(dk) => {
                        if *sort_asc {
                            sk < dk
                        } else {
                            sk > dk
                        }
                    }
                };
                if replace {
                    *dval = sval.clone();
                    *dkey = Some(sk.clone());
                }
            }
            (
                PartialReducerState::FirstValue { .. },
                PartialReducerState::FirstValue { sort_key: None, .. },
            ) => {}
            (
                PartialReducerState::Stddev {
                    sum: ds,
                    sum_sq: dss,
                    count: dc,
                },
                PartialReducerState::Stddev {
                    sum: ss,
                    sum_sq: sss,
                    count: sc,
                },
            ) => {
                *ds += ss;
                *dss += sss;
                *dc += sc;
            }
            (
                PartialReducerState::Quantile { values: dvals, .. },
                PartialReducerState::Quantile { values: svals, .. },
            ) => {
                dvals.extend(svals);
            }
            (
                PartialReducerState::RandomSample {
                    reservoir: dres,
                    count: dc,
                    seen: dseen,
                },
                PartialReducerState::RandomSample {
                    reservoir: sres,
                    seen: sseen,
                    ..
                },
            ) => {
                // Combine reservoirs with proper weighting
                let mut rng = rand::thread_rng();
                for item in sres {
                    *dseen += 1;
                    if dres.len() < *dc {
                        dres.push(item.clone());
                    } else {
                        let j = rng.gen_range(0..*dseen);
                        if j < *dc {
                            dres[j] = item.clone();
                        }
                    }
                }
                // Adjust seen for items from source that weren't in reservoir
                *dseen = *dseen + sseen - sres.len();
            }
            _ => {}
        }
    }
}

/// Finalize a partial reducer state into a string value.
pub fn finalize_state(state: &PartialReducerState) -> String {
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
        PartialReducerState::CountDistinct(set) => set.len().to_string(),
        PartialReducerState::CountDistinctish(regs) => format_f64(hll_estimate(regs)),
        PartialReducerState::Tolist(list) => {
            // Return as comma-separated for wire format
            list.join(",")
        }
        PartialReducerState::FirstValue { value, .. } => value.clone().unwrap_or_default(),
        PartialReducerState::Stddev { sum, sum_sq, count } => {
            if *count < 2 {
                "0".to_string()
            } else {
                let n = *count as f64;
                let variance = (*sum_sq / n) - (*sum / n).powi(2);
                format_f64(variance.max(0.0).sqrt())
            }
        }
        PartialReducerState::Quantile { values, quantile } => {
            if values.is_empty() {
                "0".to_string()
            } else {
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = ((*quantile * (sorted.len() - 1) as f64).round()) as usize;
                let idx = idx.min(sorted.len() - 1);
                format_f64(sorted[idx])
            }
        }
        PartialReducerState::RandomSample { reservoir, .. } => reservoir.join(","),
    }
}

/// Format f64, omitting trailing ".0" for whole numbers.
pub fn format_f64(v: f64) -> String {
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
            "GROUPBY",
            "1",
            "@category",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "total",
            "SORTBY",
            "2",
            "@total",
            "DESC",
            "LIMIT",
            "0",
            "5",
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
    fn test_parse_apply() {
        let args = ["APPLY", "@price*@qty", "AS", "total"];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], AggregateStep::Apply { .. }));
        if let AggregateStep::Apply { alias, .. } = &steps[0] {
            assert_eq!(alias, "total");
        }
    }

    #[test]
    fn test_parse_filter() {
        let args = ["FILTER", "@age>=18"];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        assert_eq!(steps.len(), 1);
        assert!(matches!(&steps[0], AggregateStep::Filter { .. }));
    }

    #[test]
    fn test_parse_load() {
        let args = ["LOAD", "2", "@field1", "@field2"];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        assert_eq!(steps.len(), 1);
        if let AggregateStep::Load { fields } = &steps[0] {
            assert_eq!(fields, &["field1", "field2"]);
        } else {
            panic!("expected Load");
        }
    }

    #[test]
    fn test_parse_new_reducers() {
        // COUNT_DISTINCT
        let args = [
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "COUNT_DISTINCT",
            "1",
            "@name",
            "AS",
            "uniq",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            assert!(matches!(
                reducers[0].function,
                ReducerFn::CountDistinct { .. }
            ));
        }

        // TOLIST
        let args = [
            "GROUPBY", "1", "@city", "REDUCE", "TOLIST", "1", "@name", "AS", "names",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            assert!(matches!(reducers[0].function, ReducerFn::Tolist { .. }));
        }

        // QUANTILE
        let args = [
            "GROUPBY", "1", "@city", "REDUCE", "QUANTILE", "2", "@price", "0.95", "AS", "p95",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            if let ReducerFn::Quantile { quantile, .. } = &reducers[0].function {
                assert!((quantile - 0.95).abs() < f64::EPSILON);
            } else {
                panic!("expected Quantile");
            }
        }

        // RANDOM_SAMPLE
        let args = [
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "RANDOM_SAMPLE",
            "2",
            "@name",
            "5",
            "AS",
            "sample",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            if let ReducerFn::RandomSample { count, .. } = &reducers[0].function {
                assert_eq!(*count, 5);
            } else {
                panic!("expected RandomSample");
            }
        }

        // FIRST_VALUE with BY
        let args = [
            "GROUPBY",
            "1",
            "@city",
            "REDUCE",
            "FIRST_VALUE",
            "1",
            "@name",
            "BY",
            "@score",
            "DESC",
            "AS",
            "top",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            if let ReducerFn::FirstValue {
                sort_by, sort_dir, ..
            } = &reducers[0].function
            {
                assert_eq!(sort_by.as_deref(), Some("score"));
                assert_eq!(*sort_dir, SortDir::Desc);
            } else {
                panic!("expected FirstValue");
            }
        }

        // STDDEV
        let args = [
            "GROUPBY", "1", "@city", "REDUCE", "STDDEV", "1", "@price", "AS", "sd",
        ];
        let steps = parse_aggregate_pipeline(&args).unwrap();
        if let AggregateStep::GroupBy { reducers, .. } = &steps[0] {
            assert!(matches!(reducers[0].function, ReducerFn::Stddev { .. }));
        }
    }

    #[test]
    fn test_shard_local_groupby_count() {
        let rows = vec![
            vec![
                ("city".into(), "NYC".into()),
                ("name".into(), "Alice".into()),
            ],
            vec![("city".into(), "NYC".into()), ("name".into(), "Bob".into())],
            vec![
                ("city".into(), "LA".into()),
                ("name".into(), "Carol".into()),
            ],
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
        let nyc = partial
            .groups
            .iter()
            .find(|(k, _)| k[0].1 == "NYC")
            .unwrap();
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

    // =========================================================================
    // New reducer tests
    // =========================================================================

    #[test]
    fn test_count_distinct_reducer() {
        let rows = vec![
            vec![
                ("city".into(), "NYC".into()),
                ("color".into(), "red".into()),
            ],
            vec![
                ("city".into(), "NYC".into()),
                ("color".into(), "blue".into()),
            ],
            vec![
                ("city".into(), "NYC".into()),
                ("color".into(), "red".into()),
            ],
            vec![
                ("city".into(), "LA".into()),
                ("color".into(), "green".into()),
            ],
        ];
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["city".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::CountDistinct {
                    field: "color".into(),
                },
                alias: "uniq".into(),
            }],
        }];

        let partial = execute_shard_local(&rows, &steps);
        let nyc = partial
            .groups
            .iter()
            .find(|(k, _)| k[0].1 == "NYC")
            .unwrap();
        if let PartialReducerState::CountDistinct(set) = &nyc.1[0] {
            assert_eq!(set.len(), 2); // red, blue
        } else {
            panic!("expected CountDistinct");
        }

        // Test merge + finalize
        let rows = merge_partials(vec![partial], &steps);
        let nyc_row = rows.iter().find(|r| r[0].1 == "NYC").unwrap();
        assert_eq!(nyc_row.iter().find(|(k, _)| k == "uniq").unwrap().1, "2");
    }

    #[test]
    fn test_tolist_reducer() {
        let rows = vec![
            vec![
                ("city".into(), "NYC".into()),
                ("name".into(), "Alice".into()),
            ],
            vec![("city".into(), "NYC".into()), ("name".into(), "Bob".into())],
        ];
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["city".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Tolist {
                    field: "name".into(),
                },
                alias: "names".into(),
            }],
        }];

        let rows = merge_partials(vec![execute_shard_local(&rows, &steps)], &steps);
        let names = rows[0].iter().find(|(k, _)| k == "names").unwrap();
        assert!(names.1.contains("Alice"));
        assert!(names.1.contains("Bob"));
    }

    #[test]
    fn test_stddev_reducer() {
        // Values: 2, 4, 4, 4, 5, 5, 7, 9 → stddev = 2.0
        let rows: Vec<Row> = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
            .iter()
            .map(|v| vec![("g".into(), "a".into()), ("x".into(), v.to_string())])
            .collect();

        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["g".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Stddev { field: "x".into() },
                alias: "sd".into(),
            }],
        }];

        let result = merge_partials(vec![execute_shard_local(&rows, &steps)], &steps);
        let sd: f64 = result[0]
            .iter()
            .find(|(k, _)| k == "sd")
            .unwrap()
            .1
            .parse()
            .unwrap();
        assert!((sd - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_quantile_reducer() {
        let rows: Vec<Row> = (1..=100)
            .map(|v| vec![("g".into(), "a".into()), ("x".into(), v.to_string())])
            .collect();

        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["g".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::Quantile {
                    field: "x".into(),
                    quantile: 0.5,
                },
                alias: "median".into(),
            }],
        }];

        let result = merge_partials(vec![execute_shard_local(&rows, &steps)], &steps);
        let median: f64 = result[0]
            .iter()
            .find(|(k, _)| k == "median")
            .unwrap()
            .1
            .parse()
            .unwrap();
        assert!((median - 50.0).abs() <= 1.0);
    }

    #[test]
    fn test_first_value_reducer() {
        let rows = vec![
            vec![
                ("city".into(), "NYC".into()),
                ("name".into(), "Alice".into()),
                ("score".into(), "90".into()),
            ],
            vec![
                ("city".into(), "NYC".into()),
                ("name".into(), "Bob".into()),
                ("score".into(), "95".into()),
            ],
            vec![
                ("city".into(), "NYC".into()),
                ("name".into(), "Carol".into()),
                ("score".into(), "85".into()),
            ],
        ];
        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["city".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::FirstValue {
                    field: "name".into(),
                    sort_by: Some("score".into()),
                    sort_dir: SortDir::Desc,
                },
                alias: "top".into(),
            }],
        }];

        let result = merge_partials(vec![execute_shard_local(&rows, &steps)], &steps);
        let top = result[0].iter().find(|(k, _)| k == "top").unwrap();
        assert_eq!(top.1, "Bob"); // highest score
    }

    #[test]
    fn test_random_sample_reducer() {
        let rows: Vec<Row> = (0..100)
            .map(|v| vec![("g".into(), "a".into()), ("x".into(), v.to_string())])
            .collect();

        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["g".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::RandomSample {
                    field: "x".into(),
                    count: 5,
                },
                alias: "sample".into(),
            }],
        }];

        let result = merge_partials(vec![execute_shard_local(&rows, &steps)], &steps);
        let sample = &result[0].iter().find(|(k, _)| k == "sample").unwrap().1;
        let items: Vec<&str> = sample.split(',').collect();
        assert_eq!(items.len(), 5);
    }

    // =========================================================================
    // APPLY / FILTER tests
    // =========================================================================

    #[test]
    fn test_apply_before_groupby() {
        let rows = vec![
            vec![
                ("price".into(), "10".into()),
                ("qty".into(), "3".into()),
                ("cat".into(), "A".into()),
            ],
            vec![
                ("price".into(), "20".into()),
                ("qty".into(), "2".into()),
                ("cat".into(), "A".into()),
            ],
            vec![
                ("price".into(), "5".into()),
                ("qty".into(), "4".into()),
                ("cat".into(), "B".into()),
            ],
        ];
        let steps = vec![
            AggregateStep::Apply {
                expr: expression::parse_expression("@price*@qty").unwrap(),
                alias: "total".into(),
            },
            AggregateStep::GroupBy {
                fields: vec!["cat".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Sum {
                        field: "total".into(),
                    },
                    alias: "sum_total".into(),
                }],
            },
            AggregateStep::SortBy {
                fields: vec![("cat".into(), SortDir::Asc)],
            },
        ];

        let partial = execute_shard_local(&rows, &steps);
        let result = merge_partials(vec![partial], &steps);

        let a = result.iter().find(|r| r[0].1 == "A").unwrap();
        assert_eq!(a.iter().find(|(k, _)| k == "sum_total").unwrap().1, "70"); // 10*3 + 20*2

        let b = result.iter().find(|r| r[0].1 == "B").unwrap();
        assert_eq!(b.iter().find(|(k, _)| k == "sum_total").unwrap().1, "20"); // 5*4
    }

    #[test]
    fn test_filter_before_groupby() {
        let rows = vec![
            vec![("age".into(), "25".into()), ("city".into(), "NYC".into())],
            vec![("age".into(), "15".into()), ("city".into(), "NYC".into())],
            vec![("age".into(), "30".into()), ("city".into(), "LA".into())],
        ];
        let steps = vec![
            AggregateStep::Filter {
                expr: expression::parse_expression("@age>=18").unwrap(),
            },
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "count".into(),
                }],
            },
        ];

        let partial = execute_shard_local(&rows, &steps);
        let result = merge_partials(vec![partial], &steps);

        // Only 2 rows pass filter (age 25, age 30)
        let nyc = result.iter().find(|r| r[0].1 == "NYC").unwrap();
        assert_eq!(nyc.iter().find(|(k, _)| k == "count").unwrap().1, "1");

        let la = result.iter().find(|r| r[0].1 == "LA").unwrap();
        assert_eq!(la.iter().find(|(k, _)| k == "count").unwrap().1, "1");
    }

    #[test]
    fn test_apply_after_groupby() {
        let steps = vec![
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![
                    ReducerDef {
                        function: ReducerFn::Sum {
                            field: "sales".into(),
                        },
                        alias: "total".into(),
                    },
                    ReducerDef {
                        function: ReducerFn::Count,
                        alias: "count".into(),
                    },
                ],
            },
            AggregateStep::Apply {
                expr: expression::parse_expression("@total/@count").unwrap(),
                alias: "avg_sale".into(),
            },
        ];

        let p = PartialAggregate {
            groups: vec![(
                vec![("city".into(), "NYC".into())],
                vec![
                    PartialReducerState::Sum(300.0),
                    PartialReducerState::Count(6),
                ],
            )],
        };

        let result = merge_partials(vec![p], &steps);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].iter().find(|(k, _)| k == "avg_sale").unwrap().1,
            "50"
        );
    }

    #[test]
    fn test_filter_after_groupby() {
        let steps = vec![
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "count".into(),
                }],
            },
            AggregateStep::Filter {
                expr: expression::parse_expression("@count>5").unwrap(),
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
                    vec![PartialReducerState::Count(3)],
                ),
            ],
        };

        let result = merge_partials(vec![p], &steps);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].1, "NYC");
    }

    // =========================================================================
    // Multiple GROUPBY tests
    // =========================================================================

    #[test]
    fn test_multi_groupby() {
        // First GROUPBY: group by city, count
        // Second GROUPBY: group by count ranges
        let steps = vec![
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "count".into(),
                }],
            },
            AggregateStep::GroupBy {
                fields: vec!["count".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "num_cities".into(),
                }],
            },
            AggregateStep::SortBy {
                fields: vec![("count".into(), SortDir::Asc)],
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
                    vec![PartialReducerState::Count(10)],
                ),
            ],
        };

        let result = merge_partials(vec![p], &steps);
        // count=5 → 1 city, count=10 → 2 cities
        assert_eq!(result.len(), 2);
        let five = result.iter().find(|r| r[0].1 == "5").unwrap();
        assert_eq!(five.iter().find(|(k, _)| k == "num_cities").unwrap().1, "1");
        let ten = result.iter().find(|r| r[0].1 == "10").unwrap();
        assert_eq!(ten.iter().find(|(k, _)| k == "num_cities").unwrap().1, "2");
    }

    #[test]
    fn test_groupby_apply_groupby() {
        let steps = vec![
            AggregateStep::GroupBy {
                fields: vec!["city".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Sum {
                        field: "sales".into(),
                    },
                    alias: "total".into(),
                }],
            },
            AggregateStep::Apply {
                expr: expression::parse_expression("@total>100").unwrap(),
                alias: "is_big".into(),
            },
            AggregateStep::GroupBy {
                fields: vec!["is_big".into()],
                reducers: vec![ReducerDef {
                    function: ReducerFn::Count,
                    alias: "num".into(),
                }],
            },
        ];

        let p = PartialAggregate {
            groups: vec![
                (
                    vec![("city".into(), "NYC".into())],
                    vec![PartialReducerState::Sum(200.0)],
                ),
                (
                    vec![("city".into(), "LA".into())],
                    vec![PartialReducerState::Sum(50.0)],
                ),
                (
                    vec![("city".into(), "CHI".into())],
                    vec![PartialReducerState::Sum(150.0)],
                ),
            ],
        };

        let result = merge_partials(vec![p], &steps);
        assert_eq!(result.len(), 2);
        let big = result.iter().find(|r| r[0].1 == "1").unwrap();
        assert_eq!(big.iter().find(|(k, _)| k == "num").unwrap().1, "2"); // NYC + CHI
        let small = result.iter().find(|r| r[0].1 == "0").unwrap();
        assert_eq!(small.iter().find(|(k, _)| k == "num").unwrap().1, "1"); // LA
    }

    #[test]
    fn test_hll_basic() {
        let mut regs = vec![0u8; 256];
        for i in 0..1000 {
            hll_add(&mut regs, &format!("item_{i}"));
        }
        let estimate = hll_estimate(&regs);
        // HLL with 256 registers has ~6.5% standard error; 1000 items should be within 30%
        assert!(
            estimate > 700.0 && estimate < 1300.0,
            "HLL estimate {estimate} out of range"
        );
    }

    #[test]
    fn test_countdistinctish_merge() {
        let rows1: Vec<Row> = (0..500)
            .map(|i| vec![("g".into(), "a".into()), ("x".into(), format!("item_{i}"))])
            .collect();
        let rows2: Vec<Row> = (250..750)
            .map(|i| vec![("g".into(), "a".into()), ("x".into(), format!("item_{i}"))])
            .collect();

        let steps = vec![AggregateStep::GroupBy {
            fields: vec!["g".into()],
            reducers: vec![ReducerDef {
                function: ReducerFn::CountDistinctish { field: "x".into() },
                alias: "approx".into(),
            }],
        }];

        let p1 = execute_shard_local(&rows1, &steps);
        let p2 = execute_shard_local(&rows2, &steps);
        let result = merge_partials(vec![p1, p2], &steps);

        let approx: f64 = result[0]
            .iter()
            .find(|(k, _)| k == "approx")
            .unwrap()
            .1
            .parse()
            .unwrap();
        // 750 unique items; HLL should be within ~30%
        assert!(
            approx > 500.0 && approx < 1000.0,
            "HLL merged estimate {approx} out of range"
        );
    }
}

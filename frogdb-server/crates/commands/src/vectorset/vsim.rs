//! VSIM command — similarity search in a vector set.
//!
//! VSIM key (ELE element | FP32 blob | VALUES count v1..vN)
//!   [COUNT count] [EF ef] [FILTER expr] [FILTER-EF ef]
//!   [WITHSCORES] [WITHATTRIBS] [TRUTH] [NOTHREAD] [EPSILON eps]

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, FilterExpr,
};
use frogdb_protocol::Response;

pub struct VsimCommand;

impl Command for VsimCommand {
    fn name(&self) -> &'static str {
        "VSIM"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(3)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let rest = &args[1..];

        let value = match ctx.store.get(key) {
            Some(v) => v,
            None => return Ok(Response::Array(vec![])),
        };
        let vs = value.as_vectorset().ok_or(CommandError::WrongType)?;

        // Parse query vector source.
        let mut i = 0;
        let query_vector;

        if i >= rest.len() {
            return Err(CommandError::InvalidArgument {
                message: "Missing query vector".to_string(),
            });
        }

        if rest[i].eq_ignore_ascii_case(b"ELE") {
            i += 1;
            if i >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "ELE requires an element name".to_string(),
                });
            }
            let elem_name = &rest[i];
            i += 1;
            match vs.get_vector(elem_name) {
                Some(v) => query_vector = v.to_vec(),
                None => {
                    return Err(CommandError::InvalidArgument {
                        message: "Element not found in vector set".to_string(),
                    });
                }
            }
        } else if rest[i].eq_ignore_ascii_case(b"FP32") {
            i += 1;
            if i >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "FP32 requires a binary blob".to_string(),
                });
            }
            let blob = &rest[i];
            i += 1;
            if !blob.len().is_multiple_of(4) {
                return Err(CommandError::InvalidArgument {
                    message: "FP32 blob length must be a multiple of 4".to_string(),
                });
            }
            query_vector = blob
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
        } else if rest[i].eq_ignore_ascii_case(b"VALUES") {
            i += 1;
            if i >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "VALUES requires a count".to_string(),
                });
            }
            let count = parse_usize(&rest[i])?;
            i += 1;
            if i + count > rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "Not enough values for vector".to_string(),
                });
            }
            let mut vec = Vec::with_capacity(count);
            for _ in 0..count {
                vec.push(parse_f32(&rest[i])?);
                i += 1;
            }
            query_vector = vec;
        } else {
            return Err(CommandError::InvalidArgument {
                message: "Expected ELE, FP32, or VALUES".to_string(),
            });
        }

        // Parse options.
        let mut count = 10usize;
        let mut with_scores = false;
        let mut with_attribs = false;
        let mut truth = false;
        let mut filter_expr: Option<FilterExpr> = None;

        while i < rest.len() {
            let opt = &rest[i];
            if opt.eq_ignore_ascii_case(b"COUNT") {
                i += 1;
                if i >= rest.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "COUNT requires a value".to_string(),
                    });
                }
                count = parse_usize(&rest[i])?;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"WITHSCORES") {
                with_scores = true;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"WITHATTRIBS") {
                with_attribs = true;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"TRUTH") {
                truth = true;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"FILTER") {
                i += 1;
                if i >= rest.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "FILTER requires an expression".to_string(),
                    });
                }
                let expr_str = std::str::from_utf8(&rest[i]).map_err(|_| {
                    CommandError::InvalidArgument {
                        message: "Invalid UTF-8 in FILTER expression".to_string(),
                    }
                })?;
                filter_expr = Some(FilterExpr::parse(expr_str).map_err(|e| {
                    CommandError::InvalidArgument {
                        message: format!("Invalid filter expression: {e}"),
                    }
                })?);
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"EF")
                || opt.eq_ignore_ascii_case(b"FILTER-EF")
                || opt.eq_ignore_ascii_case(b"EPSILON")
            {
                // Accept but skip value argument (these tune search behavior).
                i += 1;
                if i < rest.len() {
                    i += 1; // skip the value
                }
            } else if opt.eq_ignore_ascii_case(b"NOTHREAD") {
                // No-op in FrogDB.
                i += 1;
            } else {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", String::from_utf8_lossy(opt)),
                });
            }
        }

        // Handle REDUCE: if the vector set uses REDUCE, project the query vector.
        let search_vec = if vs.original_dim() > 0 && query_vector.len() == vs.original_dim() {
            vs.project_vector(&query_vector)
        } else {
            query_vector
        };

        // Validate search vector dimension.
        if search_vec.len() != vs.dim() {
            return Err(CommandError::InvalidArgument {
                message: format!(
                    "Query vector dimension mismatch: expected {}, got {}",
                    vs.dim(),
                    search_vec.len()
                ),
            });
        }

        // Perform search.
        // When filtering, over-fetch to account for filtered-out results.
        let fetch_count = if filter_expr.is_some() {
            count * 4 + 10
        } else {
            count
        };

        let results = if truth {
            vs.brute_force_search(&search_vec, if filter_expr.is_some() { vs.card() } else { fetch_count })
        } else {
            vs.search(&search_vec, fetch_count)
        };

        // Apply filter.
        let filtered: Vec<_> = if let Some(ref expr) = filter_expr {
            results
                .into_iter()
                .filter(|r| {
                    let attrs = vs.get_attr(&r.name);
                    expr.evaluate(attrs)
                })
                .take(count)
                .collect()
        } else {
            results.into_iter().take(count).collect()
        };

        // Build response.
        let mut response = Vec::new();
        for hit in &filtered {
            response.push(Response::bulk(hit.name.clone()));
            if with_scores {
                response.push(Response::bulk(Bytes::from(format!("{}", hit.score))));
            }
            if with_attribs {
                match vs.get_attr(&hit.name) {
                    Some(attr) => {
                        response.push(Response::bulk(Bytes::from(attr.to_string())));
                    }
                    None => {
                        response.push(Response::null());
                    }
                }
            }
        }

        Ok(Response::Array(response))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

fn parse_usize(b: &[u8]) -> Result<usize, CommandError> {
    std::str::from_utf8(b)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid integer".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid integer".to_string(),
        })
}

fn parse_f32(b: &[u8]) -> Result<f32, CommandError> {
    std::str::from_utf8(b)
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid float".to_string(),
        })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument {
            message: "Invalid float".to_string(),
        })
}

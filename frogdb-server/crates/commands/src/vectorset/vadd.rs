//! VADD command — add or update elements in a vector set.
//!
//! VADD key [REDUCE dim] (FP32 | VALUES) vector element
//!   [NOQUANT | Q8 | BIN] [EF ef] [SETATTR attr] [M m] [CAS] [NOTHREAD]

use bytes::Bytes;
use frogdb_core::{
    Arity, Command, CommandContext, CommandError, CommandFlags, Value, VectorDistanceMetric,
    VectorQuantization, VectorSetValue, WalStrategy,
};
use frogdb_protocol::Response;

pub struct VaddCommand;

impl Command for VaddCommand {
    fn name(&self) -> &'static str {
        "VADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4)
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn wal_strategy(&self) -> WalStrategy {
        WalStrategy::PersistFirstKey
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let rest = &args[1..];

        // Parse optional flags and vector data.
        let mut i = 0;
        let mut reduce_dim: Option<usize> = None;
        let mut quant = VectorQuantization::NoQuant;
        let mut ef: Option<usize> = None;
        let mut set_attr: Option<serde_json::Value> = None;
        let mut m_param: Option<usize> = None;

        // Check for REDUCE
        if i < rest.len() && rest[i].eq_ignore_ascii_case(b"REDUCE") {
            i += 1;
            if i >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "REDUCE requires a dimension argument".to_string(),
                });
            }
            reduce_dim = Some(parse_usize(&rest[i])?);
            i += 1;
        }

        // Parse FP32/VALUES and vector data.
        if i >= rest.len() {
            return Err(CommandError::InvalidArgument {
                message: "Missing vector format (FP32 or VALUES)".to_string(),
            });
        }

        let (vector, element_name);

        if rest[i].eq_ignore_ascii_case(b"FP32") {
            i += 1;
            if i + 1 >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "FP32 requires a binary blob and element name".to_string(),
                });
            }
            let blob = &rest[i];
            i += 1;
            element_name = rest[i].clone();
            i += 1;

            if !blob.len().is_multiple_of(4) {
                return Err(CommandError::InvalidArgument {
                    message: "FP32 blob length must be a multiple of 4".to_string(),
                });
            }
            vector = blob
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect::<Vec<f32>>();
        } else if rest[i].eq_ignore_ascii_case(b"VALUES") {
            i += 1;
            if i >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "VALUES requires a count".to_string(),
                });
            }
            let count = parse_usize(&rest[i])?;
            i += 1;

            if i + count >= rest.len() {
                return Err(CommandError::InvalidArgument {
                    message: "Not enough values for vector".to_string(),
                });
            }

            let mut vec = Vec::with_capacity(count);
            for _ in 0..count {
                vec.push(parse_f32(&rest[i])?);
                i += 1;
            }
            vector = vec;
            element_name = rest[i].clone();
            i += 1;
        } else {
            return Err(CommandError::InvalidArgument {
                message: "Expected FP32 or VALUES".to_string(),
            });
        }

        // Parse trailing options.
        while i < rest.len() {
            let opt = &rest[i];
            if opt.eq_ignore_ascii_case(b"NOQUANT") {
                quant = VectorQuantization::NoQuant;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"Q8") {
                quant = VectorQuantization::Q8;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"BIN") {
                quant = VectorQuantization::Bin;
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"EF") {
                i += 1;
                if i >= rest.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "EF requires a value".to_string(),
                    });
                }
                ef = Some(parse_usize(&rest[i])?);
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"SETATTR") {
                i += 1;
                if i >= rest.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "SETATTR requires a JSON value".to_string(),
                    });
                }
                let json_str =
                    std::str::from_utf8(&rest[i]).map_err(|_| CommandError::InvalidArgument {
                        message: "Invalid UTF-8 in SETATTR value".to_string(),
                    })?;
                set_attr = Some(serde_json::from_str(json_str).map_err(|_| {
                    CommandError::InvalidArgument {
                        message: "Invalid JSON in SETATTR value".to_string(),
                    }
                })?);
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"M") {
                i += 1;
                if i >= rest.len() {
                    return Err(CommandError::InvalidArgument {
                        message: "M requires a value".to_string(),
                    });
                }
                m_param = Some(parse_usize(&rest[i])?);
                i += 1;
            } else if opt.eq_ignore_ascii_case(b"CAS") || opt.eq_ignore_ascii_case(b"NOTHREAD") {
                // No-ops in FrogDB's single-threaded shard model.
                i += 1;
            } else {
                return Err(CommandError::InvalidArgument {
                    message: format!("Unknown option: {}", String::from_utf8_lossy(opt)),
                });
            }
        }

        // Get or create the vector set.
        let dim = vector.len();

        if let Some(value) = ctx.store.get_mut(key) {
            let vs = value.as_vectorset_mut().ok_or(CommandError::WrongType)?;

            // Validate dimension.
            let expected_dim = if vs.original_dim() > 0 {
                vs.original_dim()
            } else {
                vs.dim()
            };
            if dim != expected_dim {
                return Err(CommandError::InvalidArgument {
                    message: format!(
                        "Vector dimension mismatch: expected {}, got {}",
                        expected_dim, dim
                    ),
                });
            }

            // Project if REDUCE is active.
            let insert_vec = if vs.original_dim() > 0 {
                vs.project_vector(&vector)
            } else {
                vector
            };

            let is_new = vs
                .add(element_name.clone(), insert_vec)
                .map_err(|e| CommandError::InvalidArgument { message: e })?;

            if let Some(attr) = set_attr {
                vs.set_attr(&element_name, attr);
            }

            Ok(Response::Integer(if is_new { 1 } else { 0 }))
        } else {
            // Create new vector set.
            let m = m_param.unwrap_or(16);
            let ef_c = ef.unwrap_or(200);
            let metric = VectorDistanceMetric::Cosine; // default

            let stored_dim = reduce_dim.unwrap_or(dim);

            let mut vs = VectorSetValue::new(metric, quant, stored_dim, m, ef_c)
                .map_err(|e| CommandError::InvalidArgument { message: e })?;

            if let Some(rd) = reduce_dim {
                if rd >= dim {
                    return Err(CommandError::InvalidArgument {
                        message: "REDUCE dimension must be less than vector dimension".to_string(),
                    });
                }
                vs.setup_reduce(dim);
            }

            let insert_vec = if vs.original_dim() > 0 {
                vs.project_vector(&vector)
            } else {
                vector
            };

            vs.add(element_name.clone(), insert_vec)
                .map_err(|e| CommandError::InvalidArgument { message: e })?;

            if let Some(attr) = set_attr {
                vs.set_attr(&element_name, attr);
            }

            ctx.store.set(key.clone(), Value::VectorSet(Box::new(vs)));
            Ok(Response::Integer(1))
        }
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

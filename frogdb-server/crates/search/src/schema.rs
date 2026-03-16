//! Search index schema definitions and FT.CREATE argument parser.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::SearchError;

/// A search index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchIndexDef {
    /// Index name.
    pub name: String,
    /// Key prefixes to match (empty = match all hash keys).
    pub prefix: Vec<String>,
    /// Field definitions.
    pub fields: Vec<FieldDef>,
    /// Schema version (for future migrations).
    pub version: u32,
    /// Synonym groups: group_id -> list of synonym terms.
    #[serde(default)]
    pub synonym_groups: HashMap<String, Vec<String>>,
}

/// A field definition within an index schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    /// Field name (matches hash field name).
    pub name: String,
    /// Field type.
    pub field_type: FieldType,
    /// Whether this field is sortable.
    pub sortable: bool,
    /// Whether this field should not be indexed (stored only).
    pub noindex: bool,
    /// Whether stemming is disabled (TEXT fields only).
    #[serde(default)]
    pub nostem: bool,
}

/// Sort order for SORTBY queries.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VectorDistanceMetric {
    /// Cosine similarity (1 - cosine_distance).
    Cosine,
    /// Euclidean (L2) distance.
    L2,
    /// Inner product.
    IP,
}

/// Field types supported by the search index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    /// Full-text searchable field.
    Text { weight: f64 },
    /// Tag field (exact match, pipe-separated values).
    Tag { separator: char },
    /// Numeric field (range queries).
    Numeric,
    /// Geo field (radius queries). Values stored as "lon,lat" strings.
    Geo,
    /// Vector field for similarity search (KNN).
    Vector {
        /// Number of dimensions.
        dim: usize,
        /// Distance metric.
        distance_metric: VectorDistanceMetric,
    },
}

/// Parse FT.CREATE arguments into a SearchIndexDef.
///
/// Expected format:
/// `FT.CREATE <index> ON HASH [PREFIX <count> <prefix>...] SCHEMA <field> <type> [options]...`
///
/// `args` starts after the index name (i.e., args[0] should be "ON").
pub fn parse_ft_create_args(
    index_name: &str,
    args: &[&[u8]],
) -> Result<SearchIndexDef, SearchError> {
    let mut i = 0;
    let mut prefix = Vec::new();

    // Expect ON HASH
    if i >= args.len() || !args[i].eq_ignore_ascii_case(b"ON") {
        return Err(SearchError::SchemaError("Expected ON keyword".to_string()));
    }
    i += 1;

    if i >= args.len() || !args[i].eq_ignore_ascii_case(b"HASH") {
        return Err(SearchError::SchemaError(
            "Expected HASH after ON".to_string(),
        ));
    }
    i += 1;

    // Parse optional PREFIX
    if i < args.len() && args[i].eq_ignore_ascii_case(b"PREFIX") {
        i += 1;
        if i >= args.len() {
            return Err(SearchError::SchemaError(
                "Expected prefix count after PREFIX".to_string(),
            ));
        }
        let count: usize = std::str::from_utf8(args[i])
            .map_err(|_| SearchError::SchemaError("Invalid prefix count".to_string()))?
            .parse()
            .map_err(|_| SearchError::SchemaError("Invalid prefix count".to_string()))?;
        i += 1;
        for _ in 0..count {
            if i >= args.len() {
                return Err(SearchError::SchemaError(
                    "Not enough prefix values".to_string(),
                ));
            }
            let p = std::str::from_utf8(args[i])
                .map_err(|_| SearchError::SchemaError("Invalid prefix".to_string()))?
                .to_string();
            prefix.push(p);
            i += 1;
        }
    }

    // Skip optional options before SCHEMA (FILTER, LANGUAGE, etc.)
    while i < args.len() && !args[i].eq_ignore_ascii_case(b"SCHEMA") {
        i += 1;
    }

    // Expect SCHEMA
    if i >= args.len() || !args[i].eq_ignore_ascii_case(b"SCHEMA") {
        return Err(SearchError::SchemaError(
            "Expected SCHEMA keyword".to_string(),
        ));
    }
    i += 1;

    // Parse field definitions
    let mut fields = Vec::new();
    while i < args.len() {
        // Field name
        let field_name = std::str::from_utf8(args[i])
            .map_err(|_| SearchError::SchemaError("Invalid field name".to_string()))?
            .to_string();
        i += 1;

        // Field type
        if i >= args.len() {
            return Err(SearchError::SchemaError(format!(
                "Expected type for field '{}'",
                field_name
            )));
        }

        let type_str = args[i].to_ascii_uppercase();
        let mut field_type = match type_str.as_slice() {
            b"TEXT" => FieldType::Text { weight: 1.0 },
            b"TAG" => FieldType::Tag { separator: ',' },
            b"NUMERIC" => FieldType::Numeric,
            b"GEO" => FieldType::Geo,
            b"VECTOR" => {
                i += 1;
                parse_vector_attrs(args, &mut i)?
            }
            _ => {
                return Err(SearchError::SchemaError(format!(
                    "Unknown field type: {}",
                    String::from_utf8_lossy(&type_str)
                )));
            }
        };
        if !matches!(field_type, FieldType::Vector { .. }) {
            i += 1;
        }

        // Parse optional field modifiers
        let mut sortable = false;
        let mut noindex = false;
        let mut nostem = false;
        while i < args.len() {
            let upper = args[i].to_ascii_uppercase();
            match upper.as_slice() {
                b"WEIGHT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SearchError::SchemaError(
                            "Expected value after WEIGHT".to_string(),
                        ));
                    }
                    if let FieldType::Text { ref mut weight } = field_type {
                        *weight = std::str::from_utf8(args[i])
                            .map_err(|_| SearchError::SchemaError("Invalid weight".to_string()))?
                            .parse()
                            .map_err(|_| SearchError::SchemaError("Invalid weight".to_string()))?;
                    }
                    i += 1;
                }
                b"SEPARATOR" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SearchError::SchemaError(
                            "Expected value after SEPARATOR".to_string(),
                        ));
                    }
                    if let FieldType::Tag { ref mut separator } = field_type {
                        let sep_str = std::str::from_utf8(args[i]).map_err(|_| {
                            SearchError::SchemaError("Invalid separator".to_string())
                        })?;
                        *separator = sep_str.chars().next().unwrap_or(',');
                    }
                    i += 1;
                }
                b"SORTABLE" => {
                    sortable = true;
                    i += 1;
                }
                b"NOINDEX" => {
                    noindex = true;
                    i += 1;
                }
                b"NOSTEM" => {
                    nostem = true;
                    i += 1;
                }
                _ => break, // Next field name
            }
        }

        // Check for duplicate fields
        if fields.iter().any(|f: &FieldDef| f.name == field_name) {
            return Err(SearchError::SchemaError(format!(
                "Duplicate field name: {}",
                field_name
            )));
        }

        fields.push(FieldDef {
            name: field_name,
            field_type,
            sortable,
            noindex,
            nostem,
        });
    }

    if fields.is_empty() {
        return Err(SearchError::SchemaError(
            "At least one field is required".to_string(),
        ));
    }

    Ok(SearchIndexDef {
        name: index_name.to_string(),
        prefix,
        fields,
        version: 1,
        synonym_groups: HashMap::new(),
    })
}

/// Parse FT.ALTER arguments to extract new field definitions.
///
/// Expected format after index name:
/// `SCHEMA ADD <field> <type> [options]...`
///
/// `args` starts after the index name (i.e., args[0] should be "SCHEMA").
pub fn parse_ft_alter_args(args: &[&[u8]]) -> Result<Vec<FieldDef>, SearchError> {
    let mut i = 0;

    // Expect SCHEMA ADD
    if i >= args.len() || !args[i].eq_ignore_ascii_case(b"SCHEMA") {
        return Err(SearchError::SchemaError(
            "Expected SCHEMA keyword".to_string(),
        ));
    }
    i += 1;

    if i >= args.len() || !args[i].eq_ignore_ascii_case(b"ADD") {
        return Err(SearchError::SchemaError(
            "Expected ADD after SCHEMA".to_string(),
        ));
    }
    i += 1;

    // Parse field definitions (reuses same logic as parse_ft_create_args)
    let mut fields = Vec::new();
    while i < args.len() {
        let field_name = std::str::from_utf8(args[i])
            .map_err(|_| SearchError::SchemaError("Invalid field name".to_string()))?
            .to_string();
        i += 1;

        if i >= args.len() {
            return Err(SearchError::SchemaError(format!(
                "Expected type for field '{}'",
                field_name
            )));
        }

        let type_str = args[i].to_ascii_uppercase();
        let mut field_type = match type_str.as_slice() {
            b"TEXT" => FieldType::Text { weight: 1.0 },
            b"TAG" => FieldType::Tag { separator: ',' },
            b"NUMERIC" => FieldType::Numeric,
            b"GEO" => FieldType::Geo,
            b"VECTOR" => {
                i += 1;
                parse_vector_attrs(args, &mut i)?
            }
            _ => {
                return Err(SearchError::SchemaError(format!(
                    "Unknown field type: {}",
                    String::from_utf8_lossy(&type_str)
                )));
            }
        };
        if !matches!(field_type, FieldType::Vector { .. }) {
            i += 1;
        }

        let mut sortable = false;
        let mut noindex = false;
        let mut nostem = false;
        while i < args.len() {
            let upper = args[i].to_ascii_uppercase();
            match upper.as_slice() {
                b"WEIGHT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SearchError::SchemaError(
                            "Expected value after WEIGHT".to_string(),
                        ));
                    }
                    if let FieldType::Text { ref mut weight } = field_type {
                        *weight = std::str::from_utf8(args[i])
                            .map_err(|_| SearchError::SchemaError("Invalid weight".to_string()))?
                            .parse()
                            .map_err(|_| SearchError::SchemaError("Invalid weight".to_string()))?;
                    }
                    i += 1;
                }
                b"SEPARATOR" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SearchError::SchemaError(
                            "Expected value after SEPARATOR".to_string(),
                        ));
                    }
                    if let FieldType::Tag { ref mut separator } = field_type {
                        let sep_str = std::str::from_utf8(args[i]).map_err(|_| {
                            SearchError::SchemaError("Invalid separator".to_string())
                        })?;
                        *separator = sep_str.chars().next().unwrap_or(',');
                    }
                    i += 1;
                }
                b"SORTABLE" => {
                    sortable = true;
                    i += 1;
                }
                b"NOINDEX" => {
                    noindex = true;
                    i += 1;
                }
                b"NOSTEM" => {
                    nostem = true;
                    i += 1;
                }
                _ => break,
            }
        }

        fields.push(FieldDef {
            name: field_name,
            field_type,
            sortable,
            noindex,
            nostem,
        });
    }

    if fields.is_empty() {
        return Err(SearchError::SchemaError(
            "At least one field is required".to_string(),
        ));
    }

    Ok(fields)
}

/// Parse VECTOR field attributes from RediSearch syntax.
///
/// Expected format after VECTOR keyword:
/// `FLAT|HNSW <num_attrs> [DIM <dim>] [DISTANCE_METRIC <COSINE|L2|IP>] [TYPE FLOAT32] ...`
fn parse_vector_attrs(args: &[&[u8]], i: &mut usize) -> Result<FieldType, SearchError> {
    // Skip algorithm (FLAT or HNSW) — we always use HNSW via usearch
    if *i >= args.len() {
        return Err(SearchError::SchemaError(
            "Expected algorithm (FLAT or HNSW) after VECTOR".to_string(),
        ));
    }
    *i += 1; // skip FLAT/HNSW

    // Parse num_attrs (count of key-value attribute tokens that follow)
    if *i >= args.len() {
        return Err(SearchError::SchemaError(
            "Expected attribute count after VECTOR algorithm".to_string(),
        ));
    }
    let num_attrs: usize = std::str::from_utf8(args[*i])
        .map_err(|_| SearchError::SchemaError("Invalid attribute count".to_string()))?
        .parse()
        .map_err(|_| SearchError::SchemaError("Invalid attribute count".to_string()))?;
    *i += 1;

    let mut dim: Option<usize> = None;
    let mut distance_metric = VectorDistanceMetric::Cosine;

    // Parse key-value pairs
    let end = (*i + num_attrs).min(args.len());
    while *i < end {
        let attr = args[*i].to_ascii_uppercase();
        match attr.as_slice() {
            b"DIM" => {
                *i += 1;
                if *i >= args.len() {
                    return Err(SearchError::SchemaError(
                        "Expected value after DIM".to_string(),
                    ));
                }
                dim = Some(
                    std::str::from_utf8(args[*i])
                        .map_err(|_| SearchError::SchemaError("Invalid DIM value".to_string()))?
                        .parse()
                        .map_err(|_| SearchError::SchemaError("Invalid DIM value".to_string()))?,
                );
                *i += 1;
            }
            b"DISTANCE_METRIC" => {
                *i += 1;
                if *i >= args.len() {
                    return Err(SearchError::SchemaError(
                        "Expected value after DISTANCE_METRIC".to_string(),
                    ));
                }
                let metric = args[*i].to_ascii_uppercase();
                distance_metric = match metric.as_slice() {
                    b"COSINE" => VectorDistanceMetric::Cosine,
                    b"L2" => VectorDistanceMetric::L2,
                    b"IP" => VectorDistanceMetric::IP,
                    _ => {
                        return Err(SearchError::SchemaError(format!(
                            "Unknown distance metric: {}",
                            String::from_utf8_lossy(&metric)
                        )));
                    }
                };
                *i += 1;
            }
            b"TYPE" | b"INITIAL_CAP" | b"M" | b"EF_CONSTRUCTION" | b"EF_RUNTIME"
            | b"BLOCK_SIZE" => {
                // Skip known attributes we don't use (TYPE FLOAT32, etc.)
                *i += 1;
                if *i < args.len() {
                    *i += 1; // skip value
                }
            }
            _ => {
                *i += 1; // skip unknown attribute key
            }
        }
    }

    let dim = dim.ok_or_else(|| SearchError::SchemaError("DIM is required for VECTOR".to_string()))?;
    if dim == 0 {
        return Err(SearchError::SchemaError(
            "DIM must be greater than 0".to_string(),
        ));
    }

    Ok(FieldType::Vector {
        dim,
        distance_metric,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_schema() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"PREFIX", b"1", b"user:", b"SCHEMA", b"name", b"TEXT", b"age",
            b"NUMERIC", b"tags", b"TAG",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert_eq!(def.name, "idx");
        assert_eq!(def.prefix, vec!["user:"]);
        assert_eq!(def.fields.len(), 3);
        assert!(matches!(def.fields[0].field_type, FieldType::Text { weight } if weight == 1.0));
        assert!(matches!(def.fields[1].field_type, FieldType::Numeric));
        assert!(
            matches!(def.fields[2].field_type, FieldType::Tag { separator } if separator == ',')
        );
    }

    #[test]
    fn test_multiple_prefixes() {
        let args: Vec<&[u8]> = vec![
            b"ON",
            b"HASH",
            b"PREFIX",
            b"2",
            b"user:",
            b"product:",
            b"SCHEMA",
            b"name",
            b"TEXT",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert_eq!(def.prefix, vec!["user:", "product:"]);
    }

    #[test]
    fn test_no_prefix() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"name", b"TEXT"];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert!(def.prefix.is_empty());
    }

    #[test]
    fn test_text_with_weight() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"SCHEMA", b"name", b"TEXT", b"WEIGHT", b"2.0",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert!(matches!(def.fields[0].field_type, FieldType::Text { weight } if weight == 2.0));
    }

    #[test]
    fn test_tag_with_separator() {
        let args: Vec<&[u8]> = vec![
            b"ON",
            b"HASH",
            b"SCHEMA",
            b"tags",
            b"TAG",
            b"SEPARATOR",
            b";",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert!(
            matches!(def.fields[0].field_type, FieldType::Tag { separator } if separator == ';')
        );
    }

    #[test]
    fn test_sortable() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"price", b"NUMERIC", b"SORTABLE"];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert!(def.fields[0].sortable);
    }

    #[test]
    fn test_missing_schema() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"name", b"TEXT"];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_field_type() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"name", b"FOOBAR"];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_vector_field_basic() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"SCHEMA", b"vec", b"VECTOR", b"FLAT", b"6", b"DIM", b"128",
            b"DISTANCE_METRIC", b"COSINE", b"TYPE", b"FLOAT32",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert_eq!(def.fields.len(), 1);
        match &def.fields[0].field_type {
            FieldType::Vector {
                dim,
                distance_metric,
            } => {
                assert_eq!(*dim, 128);
                assert_eq!(*distance_metric, VectorDistanceMetric::Cosine);
            }
            _ => panic!("Expected Vector field"),
        }
    }

    #[test]
    fn test_vector_field_l2() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"SCHEMA", b"emb", b"VECTOR", b"HNSW", b"4", b"DIM", b"3",
            b"DISTANCE_METRIC", b"L2",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        match &def.fields[0].field_type {
            FieldType::Vector {
                dim,
                distance_metric,
            } => {
                assert_eq!(*dim, 3);
                assert_eq!(*distance_metric, VectorDistanceMetric::L2);
            }
            _ => panic!("Expected Vector field"),
        }
    }

    #[test]
    fn test_vector_field_missing_dim() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"SCHEMA", b"vec", b"VECTOR", b"FLAT", b"2", b"DISTANCE_METRIC",
            b"COSINE",
        ];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_vector_field_with_text() {
        let args: Vec<&[u8]> = vec![
            b"ON", b"HASH", b"SCHEMA", b"title", b"TEXT", b"emb", b"VECTOR", b"FLAT", b"4",
            b"DIM", b"64", b"DISTANCE_METRIC", b"IP",
        ];
        let def = parse_ft_create_args("idx", &args).unwrap();
        assert_eq!(def.fields.len(), 2);
        assert!(matches!(def.fields[0].field_type, FieldType::Text { .. }));
        match &def.fields[1].field_type {
            FieldType::Vector {
                dim,
                distance_metric,
            } => {
                assert_eq!(*dim, 64);
                assert_eq!(*distance_metric, VectorDistanceMetric::IP);
            }
            _ => panic!("Expected Vector field"),
        }
    }

    #[test]
    fn test_duplicate_field_names() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"name", b"TEXT", b"name", b"TAG"];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_alter_basic() {
        let args: Vec<&[u8]> = vec![b"SCHEMA", b"ADD", b"category", b"TAG", b"score", b"NUMERIC"];
        let fields = parse_ft_alter_args(&args).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "category");
        assert!(matches!(fields[0].field_type, FieldType::Tag { separator } if separator == ','));
        assert_eq!(fields[1].name, "score");
        assert!(matches!(fields[1].field_type, FieldType::Numeric));
    }

    #[test]
    fn test_alter_with_options() {
        let args: Vec<&[u8]> = vec![
            b"SCHEMA", b"ADD", b"desc", b"TEXT", b"WEIGHT", b"2.0", b"SORTABLE",
        ];
        let fields = parse_ft_alter_args(&args).unwrap();
        assert_eq!(fields.len(), 1);
        assert!(matches!(fields[0].field_type, FieldType::Text { weight } if weight == 2.0));
        assert!(fields[0].sortable);
    }

    #[test]
    fn test_alter_missing_schema_add() {
        let args: Vec<&[u8]> = vec![b"ADD", b"name", b"TEXT"];
        assert!(parse_ft_alter_args(&args).is_err());
    }

    #[test]
    fn test_alter_no_fields() {
        let args: Vec<&[u8]> = vec![b"SCHEMA", b"ADD"];
        assert!(parse_ft_alter_args(&args).is_err());
    }
}

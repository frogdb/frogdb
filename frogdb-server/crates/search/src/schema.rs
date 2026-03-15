//! Search index schema definitions and FT.CREATE argument parser.

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
            _ => {
                return Err(SearchError::SchemaError(format!(
                    "Unknown field type: {}",
                    String::from_utf8_lossy(&type_str)
                )));
            }
        };
        i += 1;

        // Parse optional field modifiers
        let mut sortable = false;
        let mut noindex = false;
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
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"name", b"VECTOR"];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_field_names() {
        let args: Vec<&[u8]> = vec![b"ON", b"HASH", b"SCHEMA", b"name", b"TEXT", b"name", b"TAG"];
        let result = parse_ft_create_args("idx", &args);
        assert!(result.is_err());
    }
}

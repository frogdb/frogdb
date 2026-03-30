//! Documentation data generator for FrogDB.
//!
//! Extracts configuration defaults and JSON Schema metadata from the
//! `Config` struct to produce a `config-reference.json` file consumed
//! by the Astro/Starlight documentation site.

use anyhow::{Context, Result};
use clap::Parser;
use frogdb_config::Config;
use schemars::schema_for;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "docs-gen",
    about = "Generate documentation data files from FrogDB config"
)]
struct Args {
    /// Output directory for generated files
    #[arg(short, long, default_value = "website/src/data")]
    output: PathBuf,

    /// Check mode — verify generated files match existing ones
    #[arg(long)]
    check: bool,
}

/// A single config field with its metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FieldInfo {
    /// The TOML field name.
    name: String,
    /// JSON Schema type (string, integer, boolean, number).
    #[serde(rename = "type")]
    field_type: String,
    /// Default value as a display string.
    default: Value,
    /// Description from the doc comment.
    description: String,
    /// Allowed enum values, if any.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    enum_values: Vec<String>,
}

/// A config section (e.g., `[server]`, `[persistence]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SectionInfo {
    /// Description from the struct-level doc comment.
    description: String,
    /// Fields in this section.
    fields: Vec<FieldInfo>,
}

/// Top-level output structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigReference {
    /// Machine-readable notice that this file is generated.
    #[serde(rename = "_generated")]
    generated: GeneratedNotice,
    sections: BTreeMap<String, SectionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeneratedNotice {
    warning: String,
    source: String,
    regenerate: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let workspace_root = std::env::current_dir()?;
    let output_dir = workspace_root.join(&args.output);

    let reference = generate_config_reference()?;
    let json = serde_json::to_string_pretty(&reference)?;

    let output_path = output_dir.join("config-reference.json");

    if args.check {
        check_file(&output_path, &json)?;
    } else {
        fs::create_dir_all(&output_dir)?;
        fs::write(&output_path, &json)?;
        println!("Generated: {}", output_path.display());
    }

    Ok(())
}

fn check_file(path: &PathBuf, expected: &str) -> Result<()> {
    if !path.exists() {
        anyhow::bail!(
            "Missing: {}. Run 'just docs-gen' to generate.",
            path.display()
        );
    }

    let actual = fs::read_to_string(path)?;
    if actual != expected {
        anyhow::bail!(
            "Differs: {}. Run 'just docs-gen' to regenerate.",
            path.display()
        );
    }

    println!("config-reference.json is up to date.");
    Ok(())
}

fn generate_config_reference() -> Result<ConfigReference> {
    // Get defaults by serializing Config::default()
    let config = Config::default();
    let defaults: Value =
        serde_json::to_value(&config).context("Failed to serialize Config::default()")?;

    // Get schema
    let schema = schema_for!(Config);
    let schema_value: Value =
        serde_json::to_value(&schema).context("Failed to serialize JSON Schema")?;

    let mut sections = BTreeMap::new();

    // The root schema has "properties" corresponding to each config section
    let root_props = schema_value
        .get("properties")
        .and_then(|p| p.as_object())
        .context("Expected root schema to have 'properties'")?;

    let defs = schema_value.get("$defs");

    for (section_name, section_schema) in root_props {
        // Resolve $ref if present
        let resolved = resolve_ref(section_schema, defs);

        let section_desc = resolved
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("")
            .to_string();

        let section_defaults = defaults.get(section_name);

        let fields = extract_fields(resolved, section_defaults, defs)?;

        sections.insert(
            section_name.clone(),
            SectionInfo {
                description: clean_description(&section_desc),
                fields,
            },
        );
    }

    Ok(ConfigReference {
        generated: GeneratedNotice {
            warning: "DO NOT EDIT — this file is auto-generated from Rust source code".into(),
            source: "frogdb-server/crates/config/src/".into(),
            regenerate: "just docs-gen".into(),
        },
        sections,
    })
}

fn extract_fields(
    section_schema: &Value,
    section_defaults: Option<&Value>,
    defs: Option<&Value>,
) -> Result<Vec<FieldInfo>> {
    let mut fields = Vec::new();

    let props = match section_schema.get("properties").and_then(|p| p.as_object()) {
        Some(p) => p,
        None => return Ok(fields),
    };

    for (field_name, field_schema) in props {
        let resolved = resolve_ref(field_schema, defs);

        let description = resolved
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or("")
            .to_string();

        let field_type = infer_type(resolved);

        let default = section_defaults
            .and_then(|d| d.get(field_name))
            .cloned()
            .unwrap_or(Value::Null);

        let enum_values = extract_enum_values(resolved, defs);

        fields.push(FieldInfo {
            name: field_name.clone(),
            field_type,
            default,
            description: clean_description(&description),
            enum_values,
        });
    }

    // Sort fields by their order in the schema (BTreeMap is already sorted alphabetically,
    // which matches what schemars produces)
    Ok(fields)
}

/// Resolve a `$ref` to the actual schema definition.
fn resolve_ref<'a>(schema: &'a Value, defs: Option<&'a Value>) -> &'a Value {
    if let Some(ref_path) = schema.get("$ref").and_then(|r| r.as_str()) {
        // Format: "#/$defs/TypeName"
        let type_name = ref_path.rsplit('/').next().unwrap_or("");
        if let Some(resolved) = defs.and_then(|d| d.get(type_name)) {
            return resolved;
        }
    }
    schema
}

/// Infer a user-friendly type name from the JSON Schema.
fn infer_type(schema: &Value) -> String {
    // Check for oneOf (enum types like SortedSetIndexConfig)
    if schema.get("oneOf").is_some() || schema.get("enum").is_some() {
        return "string".to_string();
    }

    if let Some(ty) = schema.get("type").and_then(|t| t.as_str()) {
        return match ty {
            "integer" => "integer".to_string(),
            "number" => "number".to_string(),
            "boolean" => "boolean".to_string(),
            "string" => "string".to_string(),
            "array" => "array".to_string(),
            "object" => "object".to_string(),
            other => other.to_string(),
        };
    }

    // If there's a format hint
    if let Some(fmt) = schema.get("format").and_then(|f| f.as_str()) {
        return fmt.to_string();
    }

    "string".to_string()
}

/// Extract enum variant names from a schema (handles oneOf with const values).
fn extract_enum_values(schema: &Value, defs: Option<&Value>) -> Vec<String> {
    // Direct enum array
    if let Some(values) = schema.get("enum").and_then(|e| e.as_array()) {
        return values
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }

    // oneOf with const values (schemars pattern for Rust enums)
    if let Some(one_of) = schema.get("oneOf").and_then(|o| o.as_array()) {
        let mut values = Vec::new();
        for variant in one_of {
            let resolved = resolve_ref(variant, defs);
            if let Some(c) = resolved.get("const").and_then(|c| c.as_str()) {
                values.push(c.to_string());
            } else if let Some(c) = resolved.get("enum").and_then(|e| e.as_array()) {
                for v in c {
                    if let Some(s) = v.as_str() {
                        values.push(s.to_string());
                    }
                }
            }
        }
        if !values.is_empty() {
            return values;
        }
    }

    Vec::new()
}

/// Clean up a description: trim whitespace, remove trailing periods for consistency.
fn clean_description(desc: &str) -> String {
    desc.trim().to_string()
}

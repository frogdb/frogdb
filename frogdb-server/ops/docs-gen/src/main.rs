//! Documentation data generator for FrogDB.
//!
//! Extracts configuration defaults and JSON Schema metadata from the
//! `Config` struct to produce a `config-reference.json` file, version
//! identifiers to produce a `versions.json` file, the full command
//! registry to produce a `commands.json` file, and the typed metrics
//! registry to produce a `metrics.json` file, all consumed by the
//! Astro/Starlight documentation site.

use anyhow::{Context, Result};
use clap::Parser;
use frogdb_config::{Config, config_param_registry};
use frogdb_core::{Arity, CommandRegistry, ExecutionStrategy};
use frogdb_telemetry::ALL_METRICS;
use schemars::schema_for;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
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
    /// The TOML field name (kebab-case).
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
    /// CONFIG GET/SET parameter name, if exposed.
    #[serde(skip_serializing_if = "Option::is_none")]
    config_param: Option<String>,
    /// Whether this field can be changed at runtime via CONFIG SET.
    #[serde(skip_serializing_if = "Option::is_none")]
    mutable: Option<bool>,
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

/// Top-level output structure for `versions.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionsInfo {
    /// Machine-readable notice that this file is generated.
    #[serde(rename = "_generated")]
    generated: GeneratedNotice,
    /// The FrogDB workspace version.
    workspace_version: String,
    /// The pinned Rust toolchain channel.
    rust_toolchain: String,
    /// The Redis version FrogDB advertises to clients.
    advertised_redis_version: String,
    /// The upstream Redis version FrogDB's compatibility is measured against.
    redis_compat_target: String,
}

#[derive(Debug, Deserialize)]
struct RustToolchainFile {
    toolchain: RustToolchainSection,
}

#[derive(Debug, Deserialize)]
struct RustToolchainSection {
    channel: String,
}

/// Top-level output structure for `commands.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommandsOutput {
    /// Machine-readable notice that this file is generated.
    #[serde(rename = "_generated")]
    generated: GeneratedNotice,
    /// Number of entries in `commands` (both full and metadata-only).
    count: usize,
    commands: Vec<CommandInfo>,
}

/// A single command registry entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommandInfo {
    /// Upper-case command name (e.g. `GET`, `JSON.SET`).
    name: String,
    arity: ArityInfo,
    /// `CommandFlags` bit names, in declaration order (e.g. `WRITE`, `FAST`).
    flags: Vec<String>,
    /// Namespace derived from a dotted command name (e.g. `JSON.GET` -> `JSON`).
    /// `None` for core Redis-compatible commands, which have no such prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    family: Option<String>,
    /// How the command is executed (`standard`, `blocking`, `scatter_gather`, ...).
    execution_strategy: String,
    /// Whether this is a full command (has `execute()`) versus a metadata-only
    /// entry handled at the connection level (e.g. SUBSCRIBE, MULTI).
    full: bool,
}

/// JSON-friendly view of `frogdb_core::Arity`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArityInfo {
    /// `fixed`, `at_least`, or `range`.
    kind: String,
    /// Minimum number of arguments after the command name.
    min: usize,
    /// Maximum number of arguments, if bounded (`fixed` and `range`).
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<usize>,
}

impl From<Arity> for ArityInfo {
    fn from(arity: Arity) -> Self {
        match arity {
            Arity::Fixed(n) => ArityInfo {
                kind: "fixed".to_string(),
                min: n,
                max: Some(n),
            },
            Arity::AtLeast(n) => ArityInfo {
                kind: "at_least".to_string(),
                min: n,
                max: None,
            },
            Arity::Range { min, max } => ArityInfo {
                kind: "range".to_string(),
                min,
                max: Some(max),
            },
        }
    }
}

/// Top-level output structure for `metrics.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricsOutput {
    /// Machine-readable notice that this file is generated.
    #[serde(rename = "_generated")]
    generated: GeneratedNotice,
    /// Number of entries in `metrics`.
    count: usize,
    metrics: Vec<MetricInfo>,
}

/// A single metric registry entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricInfo {
    /// The Prometheus metric name (e.g. `frogdb_commands_total`).
    name: String,
    /// `counter`, `gauge`, or `histogram`.
    #[serde(rename = "type")]
    metric_type: String,
    /// Help/doc text describing the metric.
    help: String,
    /// Grouping derived from the metric name's prefix (e.g. `frogdb_wal_writes_total`
    /// -> `wal`), matching the categorization the Grafana dashboard generator uses.
    group: String,
    /// Labels attached to this metric, empty if none.
    labels: Vec<LabelInfo>,
}

/// A single metric label.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LabelInfo {
    /// The label name (e.g. `reason`).
    name: String,
    /// The label's value type: `string` for a free-form `&str` label, or the
    /// label enum's type name for a fixed set of values (e.g. `RejectionReason`).
    #[serde(rename = "type")]
    value_type: String,
    /// Every possible value this label can take, in declaration order.
    /// Omitted for free-form `&str` labels, which have no fixed enumeration.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    values: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let workspace_root = std::env::current_dir()?;
    let output_dir = workspace_root.join(&args.output);

    let reference = generate_config_reference()?;
    let reference_json = serde_json::to_string_pretty(&reference)?;
    write_or_check(
        &output_dir,
        "config-reference.json",
        &reference_json,
        args.check,
    )?;

    let versions = generate_versions(&workspace_root)?;
    let versions_json = serde_json::to_string_pretty(&versions)?;
    write_or_check(&output_dir, "versions.json", &versions_json, args.check)?;

    let commands = generate_commands();
    let commands_json = serde_json::to_string_pretty(&commands)?;
    write_or_check(&output_dir, "commands.json", &commands_json, args.check)?;

    let metrics = generate_metrics();
    let metrics_json = serde_json::to_string_pretty(&metrics)?;
    write_or_check(&output_dir, "metrics.json", &metrics_json, args.check)?;

    Ok(())
}

fn write_or_check(output_dir: &PathBuf, file_name: &str, json: &str, check: bool) -> Result<()> {
    let output_path = output_dir.join(file_name);

    if check {
        check_file(&output_path, json)?;
    } else {
        fs::create_dir_all(output_dir)?;
        fs::write(&output_path, json)?;
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

    println!(
        "{} is up to date.",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    Ok(())
}

/// Build a lookup from (section, field) → ConfigParamInfo for enrichment.
fn build_param_lookup()
-> HashMap<(&'static str, &'static str), &'static frogdb_config::ConfigParamInfo> {
    let mut map = HashMap::new();
    for param in config_param_registry() {
        if let (Some(section), Some(field)) = (param.section, param.field) {
            map.insert((section, field), param);
        }
    }
    map
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

    // Build param lookup for CONFIG GET/SET metadata
    let param_lookup = build_param_lookup();

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

        let fields = extract_fields(
            resolved,
            section_defaults,
            defs,
            section_name,
            &param_lookup,
        )?;

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

fn generate_versions(workspace_root: &std::path::Path) -> Result<VersionsInfo> {
    let toolchain_path = workspace_root.join("rust-toolchain.toml");
    let toolchain_toml = fs::read_to_string(&toolchain_path)
        .with_context(|| format!("Failed to read {}", toolchain_path.display()))?;
    let toolchain: RustToolchainFile = toml::from_str(&toolchain_toml)
        .with_context(|| format!("Failed to parse {}", toolchain_path.display()))?;

    Ok(VersionsInfo {
        generated: GeneratedNotice {
            warning: "DO NOT EDIT — this file is auto-generated from Rust source code".into(),
            source: "frogdb-server/crates/types/src/redis_version.rs, rust-toolchain.toml".into(),
            regenerate: "just docs-gen".into(),
        },
        workspace_version: env!("CARGO_PKG_VERSION").to_string(),
        rust_toolchain: toolchain.toolchain.channel,
        advertised_redis_version: frogdb_types::ADVERTISED_REDIS_VERSION.to_string(),
        redis_compat_target: frogdb_types::REDIS_COMPAT_TARGET.to_string(),
    })
}

/// Build the full command registry the same way the server does, then dump
/// every entry (data-structure commands plus server-specific ones) to a
/// diff-stable, name-sorted list.
fn generate_commands() -> CommandsOutput {
    let mut registry = CommandRegistry::new();
    frogdb_server::register_commands(&mut registry);

    let mut commands: Vec<CommandInfo> = registry
        .iter()
        .map(|(name, entry)| CommandInfo {
            name: name.to_string(),
            arity: ArityInfo::from(entry.arity()),
            flags: entry
                .flags()
                .iter_names()
                .map(|(flag_name, _)| flag_name.to_string())
                .collect(),
            family: derive_family(name),
            execution_strategy: execution_strategy_name(&entry.execution_strategy()).to_string(),
            full: entry.is_full(),
        })
        .collect();

    commands.sort_by(|a, b| a.name.cmp(&b.name));

    CommandsOutput {
        generated: GeneratedNotice {
            warning: "DO NOT EDIT — this file is auto-generated from Rust source code".into(),
            source: "frogdb-server/crates/commands/src/lib.rs, \
                     frogdb-server/crates/server/src/server/register.rs"
                .into(),
            regenerate: "just docs-gen".into(),
        },
        count: commands.len(),
        commands,
    }
}

/// Dump the full typed metrics registry (`ALL_METRICS`, generated by the
/// `define_metrics!` macro from `frogdb-telemetry::definitions`) to a
/// diff-stable, name-sorted list.
fn generate_metrics() -> MetricsOutput {
    let mut metrics: Vec<MetricInfo> = ALL_METRICS
        .iter()
        .map(|m| MetricInfo {
            name: m.name.to_string(),
            metric_type: m.metric_type.as_str().to_string(),
            help: m.help.to_string(),
            group: m.category().to_string(),
            labels: m
                .labels
                .iter()
                .zip(m.label_types.iter())
                .zip(m.label_values.iter())
                .map(|((&name, &value_type), &values)| LabelInfo {
                    name: name.to_string(),
                    value_type: value_type.to_string(),
                    values: values.iter().map(|v| v.to_string()).collect(),
                })
                .collect(),
        })
        .collect();

    metrics.sort_by(|a, b| a.name.cmp(&b.name));

    MetricsOutput {
        generated: GeneratedNotice {
            warning: "DO NOT EDIT — this file is auto-generated from Rust source code".into(),
            source: "frogdb-server/crates/telemetry/src/definitions.rs".into(),
            regenerate: "just docs-gen".into(),
        },
        count: metrics.len(),
        metrics,
    }
}

/// Derive a command's family from its dot-namespaced prefix, e.g.
/// `JSON.GET` -> `JSON`, matching the convention FrogDB's Redis-Stack-style
/// extension commands already use (JSON, FT, BF, CF, CMS, TS, TDIGEST, TOPK,
/// ES, FROGDB). Core Redis-compatible commands (GET, HSET, ZADD, ...) have no
/// such prefix and are left unclassified: the registry has no other
/// family/category field to derive one from, and hand-maintaining a
/// per-command table would duplicate the module layout instead of reading it.
fn derive_family(name: &str) -> Option<String> {
    name.split_once('.').map(|(prefix, _)| prefix.to_string())
}

/// Map an `ExecutionStrategy` to a stable, JSON-friendly name.
fn execution_strategy_name(strategy: &ExecutionStrategy) -> &'static str {
    match strategy {
        ExecutionStrategy::Standard => "standard",
        ExecutionStrategy::ConnectionLevel(_) => "connection_level",
        ExecutionStrategy::Blocking { .. } => "blocking",
        ExecutionStrategy::ScatterGather(_) => "scatter_gather",
        ExecutionStrategy::RaftConsensus => "raft_consensus",
        ExecutionStrategy::AsyncExternal => "async_external",
        ExecutionStrategy::ServerWide(_) => "server_wide",
    }
}

fn extract_fields(
    section_schema: &Value,
    section_defaults: Option<&Value>,
    defs: Option<&Value>,
    section_name: &str,
    param_lookup: &HashMap<(&str, &str), &frogdb_config::ConfigParamInfo>,
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

        // Look up CONFIG GET/SET metadata for this field
        let param_info = param_lookup.get(&(section_name, field_name.as_ref()));

        fields.push(FieldInfo {
            name: field_name.clone(),
            field_type,
            default,
            description: clean_description(&description),
            enum_values,
            config_param: param_info.map(|p| p.name.to_string()),
            mutable: param_info.map(|p| p.mutable),
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

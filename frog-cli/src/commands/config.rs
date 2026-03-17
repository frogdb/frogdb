use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output};

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Emit an annotated default TOML configuration to stdout
    Generate {
        /// Generate cluster-mode defaults
        #[arg(long)]
        cluster: bool,
    },

    /// Parse a TOML config file, report errors and warnings
    Validate {
        /// Path to configuration file
        file: PathBuf,
    },

    /// Side-by-side diff of two config files
    Diff {
        /// First config file
        file_a: PathBuf,
        /// Second config file
        file_b: PathBuf,
    },

    /// Fetch running configuration via CONFIG GET
    Show {
        /// Filter to a specific section
        #[arg(long)]
        section: Option<String>,

        /// Compare running config against a file
        #[arg(long)]
        diff: Option<PathBuf>,
    },
}

#[derive(Debug, Serialize)]
struct ConfigEntry {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct ConfigResult {
    entries: Vec<ConfigEntry>,
}

impl Renderable for ConfigResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.entries.is_empty() {
            return "No configuration parameters found.\n".to_string();
        }
        let max_key = self
            .entries
            .iter()
            .map(|e| e.key.len())
            .max()
            .unwrap_or(20)
            .max(20);
        let mut out = format!("{:<width$}  VALUE\n", "PARAMETER", width = max_key);
        for entry in &self.entries {
            out.push_str(&format!(
                "{:<width$}  {}\n",
                entry.key,
                entry.value,
                width = max_key
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        let map: serde_json::Map<String, serde_json::Value> = self
            .entries
            .iter()
            .map(|e| (e.key.clone(), serde_json::Value::String(e.value.clone())))
            .collect();
        serde_json::Value::Object(map)
    }

    fn render_raw(&self) -> String {
        self.entries
            .iter()
            .map(|e| format!("{}:{}", e.key, e.value))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    }
}

pub async fn run(cmd: &ConfigCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        ConfigCommand::Generate { .. } => {
            anyhow::bail!("frog config generate: not yet implemented")
        }
        ConfigCommand::Validate { .. } => {
            anyhow::bail!("frog config validate: not yet implemented")
        }
        ConfigCommand::Diff { .. } => {
            anyhow::bail!("frog config diff: not yet implemented")
        }
        ConfigCommand::Show { section, .. } => run_show(section.as_deref(), ctx).await,
    }
}

async fn run_show(section: Option<&str>, ctx: &mut ConnectionContext) -> Result<i32> {
    let pattern = match section {
        Some(s) => format!("{s}.*"),
        None => "*".to_string(),
    };

    let conn = ctx.resp().await?;
    let pairs: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg(&pattern)
        .query_async(conn)
        .await
        .context("CONFIG GET failed")?;

    let mut entries = Vec::new();
    let mut iter = pairs.into_iter();
    while let Some(key) = iter.next() {
        let value = iter.next().unwrap_or_default();
        entries.push(ConfigEntry { key, value });
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let result = ConfigResult { entries };
    print_output(&result, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

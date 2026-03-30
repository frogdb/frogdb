//! Debian package artifact generator for FrogDB.
//!
//! Generates packaging files from the FrogDB configuration struct:
//! - nfpm.yaml (package manifest for nfpm)
//! - frogdb.toml (production config with FHS paths)
//! - frogdb-server.service (systemd unit)
//! - postinstall.sh (post-installation script)
//! - postremove.sh (post-removal script)
//! - frogdb.logrotate (log rotation config)

use anyhow::{Context, Result};
use askama::Template;
use clap::Parser;
use frogdb_config::{Config, LogOutput, RotationConfig};
use std::fs;
use std::path::PathBuf;

const GENERATED_HEADER_TOML: &str = r#"# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: crates/config/
# Regenerate with: just deb-gen
# ============================================================================="#;

const GENERATED_HEADER_YAML: &str = r#"# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: crates/config/
# Regenerate with: just deb-gen
# ============================================================================="#;

const GENERATED_HEADER_SHELL: &str = r#"# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: ops/deb/deb-gen/
# Regenerate with: just deb-gen
# ============================================================================="#;

// FHS paths for systemd deployment.
const DATA_DIR: &str = "/var/lib/frogdb/data";
const SNAPSHOT_DIR: &str = "/var/lib/frogdb/snapshots";
const CLUSTER_DATA_DIR: &str = "/var/lib/frogdb/cluster";
const LOG_FILE: &str = "/var/log/frogdb/frogdb.log";
const CONFIG_PATH: &str = "/etc/frogdb/frogdb.toml";

#[derive(Parser, Debug)]
#[command(
    name = "deb-gen",
    about = "Generate Debian package artifacts from FrogDB config"
)]
struct Args {
    /// Output directory for the generated files
    #[arg(short, long, default_value = "deploy/deb")]
    output: PathBuf,

    /// Check mode - verify generated files match existing ones
    #[arg(long)]
    check: bool,
}

#[derive(Template)]
#[template(path = "nfpm.yaml.askama", syntax = "deb")]
struct NfpmTemplate<'a> {
    header: &'a str,
    version: &'a str,
}

#[derive(Template)]
#[template(path = "frogdb-server.service.askama", syntax = "deb")]
struct ServiceTemplate<'a> {
    header: &'a str,
    port: u16,
    config_path: &'a str,
}

#[derive(Template)]
#[template(path = "postinstall.sh.askama", syntax = "deb")]
struct PostInstallTemplate<'a> {
    header: &'a str,
}

#[derive(Template)]
#[template(path = "postremove.sh.askama", syntax = "deb")]
struct PostRemoveTemplate<'a> {
    header: &'a str,
}

#[derive(Template)]
#[template(path = "frogdb.logrotate.askama", syntax = "deb")]
struct LogrotateTemplate<'a> {
    header: &'a str,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let workspace_root = std::env::current_dir()?;

    let cargo_toml_path = workspace_root.join("Cargo.toml");
    let cargo_toml = fs::read_to_string(&cargo_toml_path)
        .with_context(|| format!("Failed to read {}", cargo_toml_path.display()))?;
    let cargo: toml::Value = toml::from_str(&cargo_toml)?;
    let version = cargo
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .context("Failed to get workspace.package.version from Cargo.toml")?;

    let output_dir = workspace_root.join(&args.output);

    if args.check {
        check_files(&output_dir, version)?;
    } else {
        generate_files(&output_dir, version)?;
    }

    Ok(())
}

fn generate_files(output_dir: &PathBuf, version: &str) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    let config = production_config();

    // Generate frogdb.toml
    let frogdb_toml = generate_frogdb_toml(&config)?;
    let toml_path = output_dir.join("frogdb.toml");
    fs::write(&toml_path, &frogdb_toml)?;
    println!("Generated: {}", toml_path.display());

    // Generate nfpm.yaml
    let nfpm_yaml = generate_nfpm_yaml(version)?;
    let nfpm_path = output_dir.join("nfpm.yaml");
    fs::write(&nfpm_path, &nfpm_yaml)?;
    println!("Generated: {}", nfpm_path.display());

    // Generate frogdb-server.service
    let service = generate_service(&config)?;
    let service_path = output_dir.join("frogdb-server.service");
    fs::write(&service_path, &service)?;
    println!("Generated: {}", service_path.display());

    // Generate postinstall.sh
    let postinstall = generate_postinstall()?;
    let postinstall_path = output_dir.join("postinstall.sh");
    fs::write(&postinstall_path, &postinstall)?;
    println!("Generated: {}", postinstall_path.display());

    // Generate postremove.sh
    let postremove = generate_postremove()?;
    let postremove_path = output_dir.join("postremove.sh");
    fs::write(&postremove_path, &postremove)?;
    println!("Generated: {}", postremove_path.display());

    // Generate frogdb.logrotate
    let logrotate = generate_logrotate()?;
    let logrotate_path = output_dir.join("frogdb.logrotate");
    fs::write(&logrotate_path, &logrotate)?;
    println!("Generated: {}", logrotate_path.display());

    println!("\nDebian package artifacts generated successfully!");
    Ok(())
}

fn check_files(output_dir: &std::path::Path, version: &str) -> Result<()> {
    let config = production_config();
    let mut has_diff = false;

    let files: Vec<(&str, String)> = vec![
        ("frogdb.toml", generate_frogdb_toml(&config)?),
        ("nfpm.yaml", generate_nfpm_yaml(version)?),
        ("frogdb-server.service", generate_service(&config)?),
        ("postinstall.sh", generate_postinstall()?),
        ("postremove.sh", generate_postremove()?),
        ("frogdb.logrotate", generate_logrotate()?),
    ];

    for (name, expected) in &files {
        let path = output_dir.join(name);
        if check_file(&path, expected)? {
            has_diff = true;
        }
    }

    if has_diff {
        anyhow::bail!(
            "Generated files differ from checked-in files. Run 'just deb-gen' to regenerate."
        );
    }

    println!("All generated files are up to date.");
    Ok(())
}

fn check_file(path: &PathBuf, expected: &str) -> Result<bool> {
    if !path.exists() {
        eprintln!("Missing: {}", path.display());
        return Ok(true);
    }

    let actual = fs::read_to_string(path)?;
    if actual != *expected {
        eprintln!("Differs: {}", path.display());
        return Ok(true);
    }

    Ok(false)
}

/// Create a production-ready config with FHS paths for systemd deployment.
fn production_config() -> Config {
    let mut config = Config::default();

    // Override paths for FHS layout
    config.persistence.data_dir = DATA_DIR.into();
    config.snapshot.snapshot_dir = SNAPSHOT_DIR.into();
    config.cluster.data_dir = CLUSTER_DATA_DIR.into();

    // Production logging: JSON to file with rotation, no console output
    config.logging.format = "json".to_string();
    config.logging.output = LogOutput::None;
    config.logging.file_path = Some(LOG_FILE.into());
    config.logging.rotation = Some(RotationConfig::default());

    config
}

fn generate_frogdb_toml(config: &Config) -> Result<String> {
    let toml_str = toml::to_string_pretty(config)?;
    Ok(format!("{GENERATED_HEADER_TOML}\n\n{toml_str}"))
}

fn generate_nfpm_yaml(version: &str) -> Result<String> {
    let template = NfpmTemplate {
        header: GENERATED_HEADER_YAML,
        version,
    };
    template.render().context("Failed to render nfpm.yaml")
}

fn generate_service(config: &Config) -> Result<String> {
    let template = ServiceTemplate {
        header: GENERATED_HEADER_SHELL,
        port: config.server.port,
        config_path: CONFIG_PATH,
    };
    template
        .render()
        .context("Failed to render frogdb-server.service")
}

fn generate_postinstall() -> Result<String> {
    let template = PostInstallTemplate {
        header: GENERATED_HEADER_SHELL,
    };
    template.render().context("Failed to render postinstall.sh")
}

fn generate_postremove() -> Result<String> {
    let template = PostRemoveTemplate {
        header: GENERATED_HEADER_SHELL,
    };
    template.render().context("Failed to render postremove.sh")
}

fn generate_logrotate() -> Result<String> {
    let template = LogrotateTemplate {
        header: GENERATED_HEADER_SHELL,
    };
    template
        .render()
        .context("Failed to render frogdb.logrotate")
}

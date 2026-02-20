//! GitHub Actions workflow generator for FrogDB.
//!
//! Generates GitHub Actions workflow files from Rust type definitions:
//! - test.yml (lint, unit tests, shuttle tests, turmoil tests, gen checks)
//! - build.yml (multi-arch binary builds, Docker images)
//! - release.yml (release binaries, Docker, Helm chart, GitHub release)
//! - deploy.yml (Terraform + Helm deployment to cloud providers)

use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::path::PathBuf;

mod helpers;
mod types;
mod workflows;

const GENERATED_HEADER: &str = r#"# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: tools/workflow-gen/src/workflows/
# Regenerate with: just workflow-gen
# ============================================================================="#;

#[derive(Parser, Debug)]
#[command(
    name = "workflow-gen",
    about = "Generate GitHub Actions workflow files"
)]
struct Args {
    /// Output directory for workflow files
    #[arg(short, long, default_value = ".github/workflows")]
    output: PathBuf,

    /// Check mode - verify generated files match existing ones
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Get workspace root
    let workspace_root = std::env::current_dir()?;
    let output_dir = workspace_root.join(&args.output);

    if args.check {
        check_files(&output_dir)?;
    } else {
        generate_files(&output_dir)?;
    }

    Ok(())
}

fn generate_files(output_dir: &PathBuf) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    // Generate test.yml
    let test_yaml = generate_workflow_yaml(workflows::test::test_workflow())?;
    let test_path = output_dir.join("test.yml");
    fs::write(&test_path, &test_yaml)?;
    println!("Generated: {}", test_path.display());

    // Generate build.yml
    let build_yaml = generate_workflow_yaml(workflows::build::build_workflow())?;
    let build_path = output_dir.join("build.yml");
    fs::write(&build_path, &build_yaml)?;
    println!("Generated: {}", build_path.display());

    // Generate release.yml
    let release_yaml = generate_workflow_yaml(workflows::release::release_workflow())?;
    let release_path = output_dir.join("release.yml");
    fs::write(&release_path, &release_yaml)?;
    println!("Generated: {}", release_path.display());

    // Generate deploy.yml (using custom types since gh-workflow doesn't support options)
    let deploy_yaml = generate_deploy_workflow_yaml()?;
    let deploy_path = output_dir.join("deploy.yml");
    fs::write(&deploy_path, &deploy_yaml)?;
    println!("Generated: {}", deploy_path.display());

    println!("\nGitHub Actions workflow files generated successfully!");
    Ok(())
}

fn check_files(output_dir: &PathBuf) -> Result<()> {
    let mut has_diff = false;

    // Check test.yml
    let test_yaml = generate_workflow_yaml(workflows::test::test_workflow())?;
    let test_path = output_dir.join("test.yml");
    if check_file(&test_path, &test_yaml)? {
        has_diff = true;
    }

    // Check build.yml
    let build_yaml = generate_workflow_yaml(workflows::build::build_workflow())?;
    let build_path = output_dir.join("build.yml");
    if check_file(&build_path, &build_yaml)? {
        has_diff = true;
    }

    // Check release.yml
    let release_yaml = generate_workflow_yaml(workflows::release::release_workflow())?;
    let release_path = output_dir.join("release.yml");
    if check_file(&release_path, &release_yaml)? {
        has_diff = true;
    }

    // Check deploy.yml (using custom types since gh-workflow doesn't support options)
    let deploy_yaml = generate_deploy_workflow_yaml()?;
    let deploy_path = output_dir.join("deploy.yml");
    if check_file(&deploy_path, &deploy_yaml)? {
        has_diff = true;
    }

    if has_diff {
        anyhow::bail!(
            "Generated workflow files differ from checked-in files. Run 'just workflow-gen' to regenerate."
        );
    }

    println!("All workflow files are up to date.");
    Ok(())
}

fn check_file(path: &PathBuf, expected: &str) -> Result<bool> {
    if !path.exists() {
        eprintln!("Missing: {}", path.display());
        return Ok(true);
    }

    let actual = fs::read_to_string(path)?;
    if actual != expected {
        eprintln!("Differs: {}", path.display());
        return Ok(true);
    }

    Ok(false)
}

fn generate_workflow_yaml(workflow: gh_workflow::Workflow) -> Result<String> {
    let yaml = workflow
        .to_string()
        .map_err(|e| anyhow::anyhow!("Failed to serialize workflow to YAML: {:?}", e))?;

    Ok(format!("{GENERATED_HEADER}\n\n{yaml}"))
}

fn generate_deploy_workflow_yaml() -> Result<String> {
    let workflow = workflows::deploy::deploy_workflow();
    let yaml = workflow
        .to_string()
        .context("Failed to serialize deploy workflow to YAML")?;

    Ok(format!("{GENERATED_HEADER}\n\n{yaml}"))
}

//! FrogDB Kubernetes Operator.
//!
//! Manages FrogDB instances in Kubernetes with continuous reconciliation,
//! supporting standalone and cluster deployment modes.

mod config_gen;
mod controller;
mod crd;
mod health;
mod resources;
mod telemetry;

use anyhow::Result;
use clap::{Parser, Subcommand};
use kube::CustomResourceExt;

#[derive(Parser)]
#[command(name = "frogdb-operator", about = "FrogDB Kubernetes Operator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the operator controller.
    Run,
    /// Generate CRD YAML to stdout.
    GenerateCrd,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run => {
            telemetry::init();
            tracing::info!("Starting frogdb-operator");

            let client = kube::Client::try_default().await?;
            controller::run(client).await?;
        }
        Commands::GenerateCrd => {
            let crd = crd::FrogDB::crd();
            println!("{}", serde_json::to_string_pretty(&crd)?);
        }
    }

    Ok(())
}

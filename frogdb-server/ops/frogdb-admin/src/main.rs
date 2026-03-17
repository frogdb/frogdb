mod client;
mod output;

use std::path::PathBuf;
use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::client::BundleClient;
use crate::output::{format_bytes, print_bundle_table};

#[derive(Parser)]
#[command(
    name = "frogdb-admin",
    about = "CLI tool for FrogDB server administration"
)]
struct Cli {
    /// Server hostname.
    #[arg(long, default_value = "localhost", global = true)]
    host: String,

    /// Metrics port (where the debug HTTP API is served).
    #[arg(long, default_value_t = 9090, global = true)]
    port: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Diagnostic bundle operations.
    DiagnosticBundle {
        #[command(subcommand)]
        action: BundleAction,
    },
}

#[derive(Subcommand)]
enum BundleAction {
    /// Generate and download a new diagnostic bundle.
    Generate {
        /// Collection duration in seconds (0 = instant snapshot).
        #[arg(long, default_value_t = 0)]
        duration: u64,

        /// Directory to write the bundle ZIP to.
        #[arg(long, default_value = ".")]
        output_dir: PathBuf,
    },

    /// List stored bundles on the server.
    List,

    /// Download a previously stored bundle by ID.
    Download {
        /// Bundle ID.
        id: String,

        /// Directory to write the bundle ZIP to.
        #[arg(long, default_value = ".")]
        output_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        eprintln!("error: {:#}", e);
        process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let client = BundleClient::new(&cli.host, cli.port)?;

    match cli.command {
        Commands::DiagnosticBundle { action } => match action {
            BundleAction::Generate {
                duration,
                output_dir,
            } => {
                if duration > 0 {
                    eprintln!(
                        "Generating diagnostic bundle with {}s collection window...",
                        duration
                    );
                } else {
                    eprintln!("Generating diagnostic bundle (instant snapshot)...");
                }

                let (id, zip_bytes) = client.generate_bundle(duration).await?;
                let filename = format!("frogdb-bundle-{}.zip", id);
                let path = output_dir.join(&filename);
                std::fs::write(&path, &zip_bytes)?;

                eprintln!(
                    "Bundle saved: {} ({})",
                    path.display(),
                    format_bytes(zip_bytes.len() as u64)
                );
            }

            BundleAction::List => {
                let bundles = client.list_bundles().await?;
                print_bundle_table(&bundles);
            }

            BundleAction::Download { id, output_dir } => {
                eprintln!("Downloading bundle {}...", id);

                let zip_bytes = client.download_bundle(&id).await?;
                let filename = format!("frogdb-bundle-{}.zip", id);
                let path = output_dir.join(&filename);
                std::fs::write(&path, &zip_bytes)?;

                eprintln!(
                    "Bundle saved: {} ({})",
                    path.display(),
                    format_bytes(zip_bytes.len() as u64)
                );
            }
        },
    }

    Ok(())
}

use std::path::PathBuf;

use clap::Subcommand;

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

pub async fn run(cmd: &ConfigCommand) -> anyhow::Result<i32> {
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
        ConfigCommand::Show { .. } => {
            anyhow::bail!("frog config show: not yet implemented")
        }
    }
}

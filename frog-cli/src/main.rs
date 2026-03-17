mod cli;
mod commands;
mod connection;
mod info_parser;
mod output;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands};
use connection::ConnectionContext;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let exit_code = rt.block_on(run(cli))?;
    std::process::exit(exit_code);
}

async fn run(cli: Cli) -> Result<i32> {
    let mut ctx = ConnectionContext::new(cli.global.clone());

    match &cli.command {
        Commands::Health(args) => commands::health::run(args, &mut ctx).await,
        Commands::Stat(args) => commands::stat::run(args, &mut ctx).await,
        Commands::Config(cmd) => commands::config::run(cmd).await,
        Commands::Cluster(cmd) => commands::cluster::run(cmd).await,
        Commands::Replication(cmd) => commands::replication::run(cmd).await,
        Commands::Debug(cmd) => commands::debug::run(cmd).await,
        Commands::Backup(cmd) => commands::backup::run(cmd).await,
        Commands::Data(cmd) => commands::data::run(cmd).await,
    }
}

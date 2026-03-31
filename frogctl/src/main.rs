use anyhow::Result;
use clap::Parser;

use frogctl::cli::{Cli, Commands};
use frogctl::commands;
use frogctl::connection::ConnectionContext;

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
        Commands::Config(cmd) => commands::config::run(cmd, &mut ctx).await,
        Commands::Cluster(cmd) => commands::cluster::run(cmd, &mut ctx).await,
        Commands::Replication(cmd) => commands::replication::run(cmd, &mut ctx).await,
        Commands::Debug(cmd) => commands::debug::run(cmd, &mut ctx).await,
        Commands::Backup(cmd) => commands::backup::run(cmd, &mut ctx).await,
        Commands::Data(cmd) => commands::data::run(cmd, &mut ctx).await,
        Commands::Exec(args) => commands::exec::run(args, &mut ctx).await,
        Commands::Acl(cmd) => commands::acl::run(cmd, &mut ctx).await,
        Commands::Client(cmd) => commands::client::run(cmd, &mut ctx).await,
        Commands::Scan(args) => commands::scan::run(args, &mut ctx).await,
        Commands::Watch(args) => commands::watch::run(args, &mut ctx).await,
        Commands::Subscribe(cmd) => commands::subscribe::run(cmd, &mut ctx).await,
        Commands::Search(cmd) => commands::search::run(cmd, &mut ctx).await,
        Commands::Benchmark(args) => commands::benchmark::run(args, &mut ctx).await,
        Commands::Upgrade(cmd) => commands::upgrade::run(cmd, &mut ctx).await,
    }
}

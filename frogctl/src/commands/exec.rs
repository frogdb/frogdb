use anyhow::{Context, Result};
use clap::Args;

use crate::connection::ConnectionContext;
use crate::output::render_value;

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// Redis command name (e.g. SET, GET, PING)
    pub command: String,

    /// Command arguments
    #[arg(trailing_var_arg = true)]
    pub args: Vec<String>,

    /// Repeat the command N times
    #[arg(long, default_value_t = 1)]
    pub repeat: u64,

    /// Interval between repeats in milliseconds
    #[arg(long, default_value_t = 0)]
    pub interval: u64,
}

pub async fn run(args: &ExecArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let arg_refs: Vec<&str> = args.args.iter().map(|s| s.as_str()).collect();

    for i in 0..args.repeat {
        let value = ctx
            .cmd_value(&args.command, &arg_refs)
            .await
            .with_context(|| format!("command failed: {}", args.command))?;

        render_value(&value, ctx.global().output, ctx.global().no_color);

        if args.repeat > 1 && i < args.repeat - 1 && args.interval > 0 {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(args.interval)) => {}
                _ = tokio::signal::ctrl_c() => {
                    return Ok(0);
                }
            }
        }
    }

    Ok(0)
}

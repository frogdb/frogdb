use crate::connection::ConnectionContext;
use anyhow::{Context, Result};
use clap::Subcommand;
use futures::StreamExt;

#[derive(Subcommand, Debug)]
pub enum SubscribeCommand {
    /// Subscribe to one or more channels
    Channel {
        /// Channel names
        #[arg(required = true, num_args = 1..)]
        channels: Vec<String>,
    },

    /// Subscribe to one or more patterns
    Pattern {
        /// Pattern strings (glob-style)
        #[arg(required = true, num_args = 1..)]
        patterns: Vec<String>,
    },
}

pub async fn run(cmd: &SubscribeCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    let client = ctx.build_client()?;
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .context("failed to connect for PubSub")?;

    match cmd {
        SubscribeCommand::Channel { channels } => {
            for ch in channels {
                pubsub
                    .subscribe(ch)
                    .await
                    .with_context(|| format!("failed to subscribe to {ch}"))?;
            }
        }
        SubscribeCommand::Pattern { patterns } => {
            for pat in patterns {
                pubsub
                    .psubscribe(pat)
                    .await
                    .with_context(|| format!("failed to psubscribe to {pat}"))?;
            }
        }
    }

    let mut stream = pubsub.into_on_message();

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(item) => {
                        let channel: String = item.get_channel_name().to_string();
                        let payload: String = item.get_payload().unwrap_or_default();
                        println!("[{channel}] {payload}");
                    }
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    Ok(0)
}

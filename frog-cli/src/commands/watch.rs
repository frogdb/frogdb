use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;

use crate::connection::ConnectionContext;

#[derive(Args, Debug)]
pub struct WatchArgs {
    /// Client-side glob filter on the full MONITOR line
    #[arg(long, name = "match")]
    pub match_pattern: Option<String>,

    /// Filter by command name (e.g. SET, GET)
    #[arg(long, name = "type")]
    pub cmd_type: Option<String>,
}

pub async fn run(args: &WatchArgs, ctx: &mut ConnectionContext) -> Result<i32> {
    let client = ctx.build_client()?;

    #[allow(deprecated)]
    let conn = client
        .get_async_connection()
        .await
        .context("failed to connect for MONITOR")?;

    let mut monitor = conn.into_monitor();
    monitor.monitor().await.context("MONITOR command failed")?;

    let mut stream = monitor.into_on_message::<String>();

    let match_pattern = args.match_pattern.clone();
    let cmd_type = args.cmd_type.as_ref().map(|s| s.to_uppercase());

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(line) => {

                        // Apply client-side filters
                        if let Some(ref pat) = match_pattern
                            && !glob_match(pat, &line)
                        {
                            continue;
                        }
                        if let Some(ref ct) = cmd_type
                            && !line_matches_command(&line, ct)
                        {
                            continue;
                        }

                        println!("{line}");
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

/// Simple glob matching supporting * and ? patterns.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat = pattern.as_bytes();
    let txt = text.as_bytes();
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < txt.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi].eq_ignore_ascii_case(&txt[ti])) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Check if a MONITOR line contains the specified command.
/// MONITOR lines look like: `1234567890.123456 [0 127.0.0.1:12345] "SET" "key" "value"`
fn line_matches_command(line: &str, command: &str) -> bool {
    // Find first quoted word after the ] bracket
    if let Some(bracket_end) = line.find(']') {
        let rest = &line[bracket_end + 1..];
        // Extract the command part (first quoted string)
        if let Some(start) = rest.find('"')
            && let Some(end) = rest[start + 1..].find('"')
        {
            let cmd = &rest[start + 1..start + 1 + end];
            return cmd.eq_ignore_ascii_case(command);
        }
    }
    false
}

use anyhow::{Context, Result};
use clap::Subcommand;

use crate::commands::debug::{ClientInfo, parse_client_line};
use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output, render_value};

use serde::Serialize;

#[derive(Subcommand, Debug)]
pub enum ClientCommand {
    /// List connected clients
    List {
        /// Filter by client type (normal, master, replica, pubsub)
        #[arg(long, name = "type")]
        client_type: Option<String>,
    },

    /// Kill client connections by filter
    Kill {
        /// Kill by client ID
        #[arg(long)]
        id: Option<String>,

        /// Kill by address (ip:port)
        #[arg(long)]
        addr: Option<String>,

        /// Kill by client type
        #[arg(long, name = "type")]
        client_type: Option<String>,

        /// Kill by username
        #[arg(long)]
        user: Option<String>,
    },

    /// Pause client processing (for failover)
    Pause {
        /// Pause duration in milliseconds
        ms: u64,

        /// Pause mode
        #[arg(long, default_value = "all")]
        mode: String,
    },

    /// Resume client processing after a pause
    Unpause,

    /// Show info about the current connection
    Info,
}

#[derive(Debug, Serialize)]
struct ClientListResult {
    clients: Vec<ClientInfo>,
}

impl Renderable for ClientListResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.clients.is_empty() {
            return "No clients connected.\n".to_string();
        }
        let mut out = format!(
            "{:<6} {:<22} {:<12} {:<6} {:<6} {:<6} {:<4} {:<10} {}\n",
            "ID", "ADDR", "NAME", "AGE", "IDLE", "FLAGS", "DB", "CMD", "OMEM"
        );
        for c in &self.clients {
            out.push_str(&format!(
                "{:<6} {:<22} {:<12} {:<6} {:<6} {:<6} {:<4} {:<10} {}\n",
                c.id, c.addr, c.name, c.age, c.idle, c.flags, c.db, c.cmd, c.omem
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.clients).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

pub async fn run(cmd: &ClientCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        ClientCommand::List { client_type } => run_list(client_type.as_deref(), ctx).await,
        ClientCommand::Kill {
            id,
            addr,
            client_type,
            user,
        } => {
            run_kill(
                id.as_deref(),
                addr.as_deref(),
                client_type.as_deref(),
                user.as_deref(),
                ctx,
            )
            .await
        }
        ClientCommand::Pause { ms, mode } => run_pause(*ms, mode, ctx).await,
        ClientCommand::Unpause => run_unpause(ctx).await,
        ClientCommand::Info => run_info(ctx).await,
    }
}

async fn run_list(client_type: Option<&str>, ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = if let Some(ct) = client_type {
        ctx.cmd("CLIENT", &["LIST", "TYPE", ct]).await
    } else {
        ctx.cmd("CLIENT", &["LIST"]).await
    }
    .context("CLIENT LIST failed")?;

    let clients: Vec<ClientInfo> = raw
        .lines()
        .filter(|l| !l.is_empty())
        .map(parse_client_line)
        .collect();

    let result = ClientListResult { clients };
    print_output(&result, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_kill(
    id: Option<&str>,
    addr: Option<&str>,
    client_type: Option<&str>,
    user: Option<&str>,
    ctx: &mut ConnectionContext,
) -> Result<i32> {
    let mut args: Vec<&str> = vec!["KILL"];

    if let Some(id) = id {
        args.extend(["ID", id]);
    }
    if let Some(addr) = addr {
        args.extend(["ADDR", addr]);
    }
    if let Some(ct) = client_type {
        args.extend(["TYPE", ct]);
    }
    if let Some(user) = user {
        args.extend(["USER", user]);
    }

    let value = ctx
        .cmd_value("CLIENT", &args)
        .await
        .context("CLIENT KILL failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_pause(ms: u64, mode: &str, ctx: &mut ConnectionContext) -> Result<i32> {
    let ms_str = ms.to_string();
    let value = ctx
        .cmd_value("CLIENT", &["PAUSE", &ms_str, mode])
        .await
        .context("CLIENT PAUSE failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_unpause(ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("CLIENT", &["UNPAUSE"])
        .await
        .context("CLIENT UNPAUSE failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_info(ctx: &mut ConnectionContext) -> Result<i32> {
    let raw = ctx
        .cmd("CLIENT", &["INFO"])
        .await
        .context("CLIENT INFO failed")?;

    if matches!(ctx.global().output, crate::cli::OutputMode::Json) {
        let client = parse_client_line(&raw);
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::to_value(&client).unwrap()).unwrap()
        );
    } else {
        let client = parse_client_line(&raw);
        println!("ID: {}", client.id);
        println!("Addr: {}", client.addr);
        println!("Name: {}", client.name);
        println!("Age: {}", client.age);
        println!("Idle: {}", client.idle);
        println!("Flags: {}", client.flags);
        println!("DB: {}", client.db);
        println!("Cmd: {}", client.cmd);
        println!("Omem: {}", client.omem);
    }
    Ok(0)
}

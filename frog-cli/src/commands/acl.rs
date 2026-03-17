use anyhow::{Context, Result};
use clap::Subcommand;
use serde::Serialize;

use crate::connection::ConnectionContext;
use crate::output::{Renderable, print_output, render_value, value_to_json};

#[derive(Subcommand, Debug)]
pub enum AclCommand {
    /// List all ACL users and their rule strings
    List,

    /// List all ACL usernames
    Users,

    /// Show detailed info for a specific ACL user
    Getuser {
        /// Username to inspect
        name: String,
    },

    /// Create or modify an ACL user
    Setuser {
        /// Username
        name: String,

        /// ACL rules (e.g. on, >password, ~key*, +get, ratelimit:cps=100)
        #[arg(trailing_var_arg = true)]
        rules: Vec<String>,
    },

    /// Delete one or more ACL users
    Deluser {
        /// Usernames to delete
        #[arg(required = true, num_args = 1..)]
        names: Vec<String>,
    },

    /// Show the current connection's username
    Whoami,

    /// Generate a secure random password
    Genpass {
        /// Number of bits (default 256)
        #[arg(long)]
        bits: Option<u64>,
    },

    /// Show or reset the ACL security log
    Log {
        /// Number of entries to show
        #[arg(long)]
        count: Option<u64>,

        /// Reset the log
        #[arg(long)]
        reset: bool,
    },

    /// Save ACL rules to the configured ACL file
    Save,

    /// Load ACL rules from the configured ACL file
    Load,

    /// List ACL categories, or commands in a category
    Cat {
        /// Show commands in this category
        #[arg(long)]
        category: Option<String>,
    },
}

// --- Renderable structs ---

#[derive(Debug, Serialize)]
struct AclUserDetail {
    flags: Vec<String>,
    passwords: Vec<String>,
    commands: String,
    keys: String,
    channels: String,
}

impl Renderable for AclUserDetail {
    fn render_table(&self, _no_color: bool) -> String {
        let mut out = String::new();
        out.push_str(&format!("Flags: {}\n", self.flags.join(", ")));
        out.push_str(&format!(
            "Passwords: {}\n",
            if self.passwords.is_empty() {
                "(none)".to_string()
            } else {
                self.passwords.join(", ")
            }
        ));
        out.push_str(&format!("Commands: {}\n", self.commands));
        out.push_str(&format!("Keys: {}\n", self.keys));
        out.push_str(&format!("Channels: {}\n", self.channels));
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

#[derive(Debug, Serialize)]
struct AclLogEntry {
    count: i64,
    reason: String,
    context: String,
    object: String,
    username: String,
    age_seconds: String,
    client_info: String,
}

#[derive(Debug, Serialize)]
struct AclLogResult {
    entries: Vec<AclLogEntry>,
}

impl Renderable for AclLogResult {
    fn render_table(&self, _no_color: bool) -> String {
        if self.entries.is_empty() {
            return "ACL log is empty.\n".to_string();
        }
        let mut out = format!(
            "{:<6} {:<10} {:<10} {:<16} {:<12} {}\n",
            "COUNT", "REASON", "CONTEXT", "OBJECT", "USER", "AGE"
        );
        for e in &self.entries {
            out.push_str(&format!(
                "{:<6} {:<10} {:<10} {:<16} {:<12} {}\n",
                e.count, e.reason, e.context, e.object, e.username, e.age_seconds
            ));
        }
        out
    }

    fn render_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.entries).unwrap()
    }

    fn render_raw(&self) -> String {
        self.render_table(true)
    }
}

// --- Dispatch ---

pub async fn run(cmd: &AclCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        AclCommand::List => run_list(ctx).await,
        AclCommand::Users => run_users(ctx).await,
        AclCommand::Getuser { name } => run_getuser(name, ctx).await,
        AclCommand::Setuser { name, rules } => run_setuser(name, rules, ctx).await,
        AclCommand::Deluser { names } => run_deluser(names, ctx).await,
        AclCommand::Whoami => run_whoami(ctx).await,
        AclCommand::Genpass { bits } => run_genpass(*bits, ctx).await,
        AclCommand::Log { count, reset } => run_log(*count, *reset, ctx).await,
        AclCommand::Save => run_simple(ctx, "ACL", &["SAVE"]).await,
        AclCommand::Load => run_simple(ctx, "ACL", &["LOAD"]).await,
        AclCommand::Cat { category } => run_cat(category.as_deref(), ctx).await,
    }
}

async fn run_simple(ctx: &mut ConnectionContext, cmd: &str, args: &[&str]) -> Result<i32> {
    let value = ctx
        .cmd_value(cmd, args)
        .await
        .with_context(|| format!("{cmd} {} failed", args.join(" ")))?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_list(ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("ACL", &["LIST"])
        .await
        .context("ACL LIST failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_users(ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("ACL", &["USERS"])
        .await
        .context("ACL USERS failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_getuser(name: &str, ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("ACL", &["GETUSER", name])
        .await
        .with_context(|| format!("ACL GETUSER {name} failed"))?;

    if matches!(ctx.global().output, crate::cli::OutputMode::Json) {
        // For JSON, parse the alternating key-value array into structured output
        let detail = parse_getuser_value(&value);
        print_output(&detail, ctx.global().output, ctx.global().no_color);
    } else {
        let detail = parse_getuser_value(&value);
        print_output(&detail, ctx.global().output, ctx.global().no_color);
    }
    Ok(0)
}

fn parse_getuser_value(value: &redis::Value) -> AclUserDetail {
    let mut flags = Vec::new();
    let mut passwords = Vec::new();
    let mut commands = String::new();
    let mut keys = String::new();
    let mut channels = String::new();

    if let redis::Value::Array(items) = value {
        let mut iter = items.iter();
        while let Some(key) = iter.next() {
            let key_str = extract_string(key);
            if let Some(val) = iter.next() {
                match key_str.as_str() {
                    "flags" => {
                        if let redis::Value::Array(arr) = val {
                            flags = arr.iter().map(extract_string).collect();
                        }
                    }
                    "passwords" => {
                        if let redis::Value::Array(arr) = val {
                            passwords = arr.iter().map(extract_string).collect();
                        }
                    }
                    "commands" => commands = extract_string(val),
                    "keys" => keys = extract_string(val),
                    "channels" => channels = extract_string(val),
                    _ => {}
                }
            }
        }
    }

    AclUserDetail {
        flags,
        passwords,
        commands,
        keys,
        channels,
    }
}

async fn run_setuser(name: &str, rules: &[String], ctx: &mut ConnectionContext) -> Result<i32> {
    let mut args = vec!["SETUSER", name];
    let rule_refs: Vec<&str> = rules.iter().map(|s| s.as_str()).collect();
    args.extend(rule_refs);
    let value = ctx
        .cmd_value("ACL", &args)
        .await
        .with_context(|| format!("ACL SETUSER {name} failed"))?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_deluser(names: &[String], ctx: &mut ConnectionContext) -> Result<i32> {
    let mut args: Vec<&str> = vec!["DELUSER"];
    for name in names {
        args.push(name.as_str());
    }
    let value = ctx
        .cmd_value("ACL", &args)
        .await
        .context("ACL DELUSER failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_whoami(ctx: &mut ConnectionContext) -> Result<i32> {
    let value = ctx
        .cmd_value("ACL", &["WHOAMI"])
        .await
        .context("ACL WHOAMI failed")?;
    render_value(&value, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_genpass(bits: Option<u64>, ctx: &mut ConnectionContext) -> Result<i32> {
    let args = if let Some(b) = bits {
        let b_str = b.to_string();
        ctx.cmd_value("ACL", &["GENPASS", &b_str]).await
    } else {
        ctx.cmd_value("ACL", &["GENPASS"]).await
    }
    .context("ACL GENPASS failed")?;
    render_value(&args, ctx.global().output, ctx.global().no_color);
    Ok(0)
}

async fn run_log(count: Option<u64>, reset: bool, ctx: &mut ConnectionContext) -> Result<i32> {
    if reset {
        let value = ctx
            .cmd_value("ACL", &["LOG", "RESET"])
            .await
            .context("ACL LOG RESET failed")?;
        render_value(&value, ctx.global().output, ctx.global().no_color);
        return Ok(0);
    }

    let count_str = count.unwrap_or(10).to_string();
    let value = ctx
        .cmd_value("ACL", &["LOG", &count_str])
        .await
        .context("ACL LOG failed")?;

    if let redis::Value::Array(entries) = &value {
        let log_entries: Vec<AclLogEntry> = entries.iter().map(parse_log_entry).collect();
        let result = AclLogResult {
            entries: log_entries,
        };
        print_output(&result, ctx.global().output, ctx.global().no_color);
    } else {
        render_value(&value, ctx.global().output, ctx.global().no_color);
    }
    Ok(0)
}

fn parse_log_entry(value: &redis::Value) -> AclLogEntry {
    let mut count = 0i64;
    let mut reason = String::new();
    let mut context = String::new();
    let mut object = String::new();
    let mut username = String::new();
    let mut age_seconds = String::new();
    let mut client_info = String::new();

    if let redis::Value::Array(items) = value {
        let mut iter = items.iter();
        while let Some(key) = iter.next() {
            let key_str = extract_string(key);
            if let Some(val) = iter.next() {
                match key_str.as_str() {
                    "count" => {
                        count = match val {
                            redis::Value::Int(n) => *n,
                            _ => extract_string(val).parse().unwrap_or(0),
                        }
                    }
                    "reason" => reason = extract_string(val),
                    "context" => context = extract_string(val),
                    "object" => object = extract_string(val),
                    "username" => username = extract_string(val),
                    "age-seconds" => age_seconds = extract_string(val),
                    "client-info" => client_info = extract_string(val),
                    _ => {}
                }
            }
        }
    }

    AclLogEntry {
        count,
        reason,
        context,
        object,
        username,
        age_seconds,
        client_info,
    }
}

async fn run_cat(category: Option<&str>, ctx: &mut ConnectionContext) -> Result<i32> {
    let value = if let Some(cat) = category {
        ctx.cmd_value("ACL", &["CAT", cat]).await
    } else {
        ctx.cmd_value("ACL", &["CAT"]).await
    }
    .context("ACL CAT failed")?;

    if let redis::Value::Array(items) = &value {
        if matches!(ctx.global().output, crate::cli::OutputMode::Json) {
            let json: Vec<serde_json::Value> = items.iter().map(value_to_json).collect();
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
        } else {
            for item in items {
                println!("{}", extract_string(item));
            }
        }
    } else {
        render_value(&value, ctx.global().output, ctx.global().no_color);
    }
    Ok(0)
}

fn extract_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        _ => String::new(),
    }
}

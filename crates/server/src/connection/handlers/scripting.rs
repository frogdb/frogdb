//! Scripting command utilities.
//!
//! This module provides utilities for scripting commands. Note that script
//! commands (EVAL, EVALSHA, SCRIPT) are executed through shard messages,
//! as the script cache is managed per-shard. This module provides:
//!
//! - Help text generation
//! - Argument parsing helpers
//! - Response formatting

use bytes::Bytes;
use frogdb_protocol::Response;

/// Parse SCRIPT subcommand and return the subcommand name.
pub fn parse_script_subcommand(args: &[Bytes]) -> Result<(&str, &[Bytes]), Response> {
    if args.is_empty() {
        return Err(Response::error(
            "ERR wrong number of arguments for 'script' command",
        ));
    }

    let subcommand = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Err(Response::error("ERR invalid subcommand")),
    };

    Ok((subcommand, &args[1..]))
}

/// Generate SCRIPT command help text.
pub fn script_help() -> Response {
    let help = vec![
        "SCRIPT LOAD <script>",
        "    Load a script into the script cache.",
        "SCRIPT EXISTS <sha1> [<sha1> ...]",
        "    Check if scripts exist in the cache.",
        "SCRIPT FLUSH [ASYNC|SYNC]",
        "    Flush the script cache.",
        "SCRIPT KILL",
        "    Kill a running script.",
        "SCRIPT HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// Generate FUNCTION command help text.
pub fn function_help() -> Response {
    let help = vec![
        "FUNCTION LOAD [REPLACE] <function-code>",
        "    Load a function library.",
        "FUNCTION DELETE <library-name>",
        "    Delete a function library.",
        "FUNCTION FLUSH [ASYNC|SYNC]",
        "    Delete all function libraries.",
        "FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]",
        "    List loaded function libraries.",
        "FUNCTION STATS",
        "    Show function statistics.",
        "FUNCTION DUMP",
        "    Dump all function libraries as serialized data.",
        "FUNCTION RESTORE <data> [FLUSH|APPEND|REPLACE]",
        "    Restore function libraries from serialized data.",
        "FUNCTION KILL",
        "    Kill a running function.",
        "FUNCTION HELP",
        "    Show this help.",
    ];
    Response::Array(help.into_iter().map(Response::bulk).collect())
}

/// Parse SCRIPT FLUSH arguments.
///
/// Returns true for SYNC mode, false for ASYNC mode.
pub fn parse_script_flush_mode(args: &[Bytes]) -> Result<bool, Response> {
    if args.is_empty() {
        return Ok(true); // Default to SYNC
    }

    let mode = String::from_utf8_lossy(&args[0]).to_uppercase();
    match mode.as_str() {
        "ASYNC" => Ok(false),
        "SYNC" => Ok(true),
        _ => Err(Response::error(
            "ERR SCRIPT FLUSH only supports ASYNC and SYNC",
        )),
    }
}

/// Parse FUNCTION LOAD arguments.
///
/// Returns (replace: bool, code_index: usize).
pub fn parse_function_load_args(args: &[Bytes]) -> Result<(bool, usize), Response> {
    if args.is_empty() {
        return Err(Response::error(
            "ERR wrong number of arguments for 'function load' command",
        ));
    }

    let mut replace = false;
    let mut code_idx = 0;

    if args.len() > 1 {
        let flag = String::from_utf8_lossy(&args[0]).to_uppercase();
        if flag == "REPLACE" {
            replace = true;
            code_idx = 1;
        }
    }

    if code_idx >= args.len() {
        return Err(Response::error("ERR FUNCTION LOAD requires function code"));
    }

    Ok((replace, code_idx))
}

/// Parsed FUNCTION LIST options.
#[derive(Debug, Default)]
pub struct FunctionListOptions<'a> {
    /// Library name pattern filter.
    pub pattern: Option<&'a str>,
    /// Whether to include function code.
    pub with_code: bool,
}

/// Parse FUNCTION LIST arguments.
pub fn parse_function_list_args(args: &[Bytes]) -> Result<FunctionListOptions<'_>, Response> {
    let mut opts = FunctionListOptions::default();
    let mut i = 0;

    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "LIBRARYNAME" => {
                if i + 1 >= args.len() {
                    return Err(Response::error("ERR LIBRARYNAME requires a pattern"));
                }
                opts.pattern = Some(
                    std::str::from_utf8(&args[i + 1]).map_err(|_| Response::error("ERR invalid pattern"))?,
                );
                i += 2;
            }
            "WITHCODE" => {
                opts.with_code = true;
                i += 1;
            }
            _ => {
                return Err(Response::error(format!(
                    "ERR Unknown FUNCTION LIST option '{}'",
                    arg
                )));
            }
        }
    }

    Ok(opts)
}

/// Parse FUNCTION RESTORE policy.
pub fn parse_function_restore_policy(args: &[Bytes]) -> Result<&str, Response> {
    if args.len() <= 1 {
        return Ok("APPEND"); // Default policy
    }

    let policy = String::from_utf8_lossy(&args[1]).to_uppercase();
    match policy.as_str() {
        "FLUSH" | "APPEND" | "REPLACE" => Ok(match policy.as_str() {
            "FLUSH" => "FLUSH",
            "APPEND" => "APPEND",
            "REPLACE" => "REPLACE",
            _ => unreachable!(),
        }),
        _ => Err(Response::error(
            "ERR FUNCTION RESTORE policy must be FLUSH, APPEND, or REPLACE",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_script_flush_mode() {
        assert!(parse_script_flush_mode(&[]).unwrap());
        assert!(parse_script_flush_mode(&[Bytes::from("SYNC")]).unwrap());
        assert!(!parse_script_flush_mode(&[Bytes::from("ASYNC")]).unwrap());
        assert!(parse_script_flush_mode(&[Bytes::from("invalid")]).is_err());
    }

    #[test]
    fn test_parse_function_load_args() {
        // Without REPLACE
        let (replace, idx) = parse_function_load_args(&[Bytes::from("code")]).unwrap();
        assert!(!replace);
        assert_eq!(idx, 0);

        // With REPLACE
        let (replace, idx) =
            parse_function_load_args(&[Bytes::from("REPLACE"), Bytes::from("code")]).unwrap();
        assert!(replace);
        assert_eq!(idx, 1);
    }
}

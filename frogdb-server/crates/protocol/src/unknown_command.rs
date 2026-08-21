//! The "unknown command" error body, byte-for-byte matching Redis.
//!
//! FrogDB used to build this error inline at four call sites, each uppercasing
//! the command name and never listing the offending arguments. Redis echoes
//! the client's original-case spelling and appends up to ~128 bytes of quoted
//! arguments. One helper here is the only place that formats this error, so
//! all four sites (and any added later) stay byte-for-byte identical.

use bytes::Bytes;

/// Redis truncates both the command name and the accumulated args text to
/// this many bytes (`commandCheckExistence` in `server.c`, `%.128s` / the
/// `sdslen(args) < 128` loop guard).
const BUDGET: usize = 128;

/// Format Redis's `unknown command` error body — everything after the
/// `ERR ` prefix that `Response::error` does not itself add.
///
/// Mirrors `commandCheckExistence` in Redis's `server.c` (verified against a
/// locally built Redis 8.6.1):
///
/// - the command name is echoed in the client's original case, truncated to
///   128 bytes;
/// - when at least one argument was supplied, a `, with args beginning with: `
///   clause follows, listing each argument single-quoted and
///   space-separated (`'arg' `, note the trailing space and the *absence* of
///   a comma — Redis does not comma-separate these), truncated so the
///   accumulated args text never exceeds 128 bytes (mid-argument, if
///   necessary — matching Redis's `%.*s` width truncation);
/// - when zero arguments were supplied, the whole `, with args beginning
///   with: ...` clause is omitted, not printed as an empty tail.
///
/// CRLF-safety and non-UTF-8 handling are deliberately *not* this function's
/// job: the caller hands the result to [`crate::Response::error`], which
/// routes every error body through `sanitize_error_message` before it can
/// reach the wire. This function only needs to reproduce Redis's byte
/// layout.
pub fn format_unknown_command_error(name: &[u8], args: &[Bytes]) -> String {
    let truncated_name = &name[..name.len().min(BUDGET)];
    let mut err = format!(
        "unknown command '{}'",
        String::from_utf8_lossy(truncated_name)
    );

    if !args.is_empty() {
        let mut args_str = String::new();
        for arg in args {
            if args_str.len() >= BUDGET {
                break;
            }
            let width = BUDGET - args_str.len();
            let truncated_arg = &arg[..arg.len().min(width)];
            args_str.push('\'');
            args_str.push_str(&String::from_utf8_lossy(truncated_arg));
            args_str.push_str("' ");
        }
        err.push_str(", with args beginning with: ");
        err.push_str(&args_str);
    }

    err
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_args(args: &[&str]) -> Vec<Bytes> {
        args.iter().map(|a| Bytes::from(a.to_string())).collect()
    }

    #[test]
    fn no_args_omits_the_args_clause_entirely() {
        assert_eq!(
            format_unknown_command_error(b"NOTACOMMAND", &[]),
            "unknown command 'NOTACOMMAND'"
        );
    }

    #[test]
    fn original_case_is_preserved_verbatim() {
        assert_eq!(
            format_unknown_command_error(b"notacommand", &bytes_args(&["arg1"])),
            "unknown command 'notacommand', with args beginning with: 'arg1' "
        );
    }

    #[test]
    fn args_are_space_separated_not_comma_separated() {
        // Matches live Redis 8.6.1 exactly: `'a' 'b' 'c' ` (trailing space,
        // no commas between entries).
        assert_eq!(
            format_unknown_command_error(b"FOO", &bytes_args(&["a", "b", "c"])),
            "unknown command 'FOO', with args beginning with: 'a' 'b' 'c' "
        );
    }

    #[test]
    fn command_name_is_truncated_to_128_bytes() {
        let name = "X".repeat(200);
        let got = format_unknown_command_error(name.as_bytes(), &[]);
        assert_eq!(got, format!("unknown command '{}'", "X".repeat(128)));
    }

    #[test]
    fn args_text_is_truncated_to_a_128_byte_budget() {
        // Verified against a locally built Redis 8.6.1 with an equivalent
        // two-argument, 100-byte-each payload: the first argument fits
        // whole, the second is cut down to fill the remaining budget.
        let args = bytes_args(&["A".repeat(100).as_str(), "B".repeat(100).as_str()]);
        let got = format_unknown_command_error(b"X", &args);
        assert_eq!(
            got,
            format!(
                "unknown command 'X', with args beginning with: '{}' '{}' ",
                "A".repeat(100),
                "B".repeat(25),
            )
        );
    }

    #[test]
    fn non_utf8_arg_bytes_render_lossily_rather_than_panicking() {
        let args = vec![Bytes::from_static(b"\xff\xfe")];
        let got = format_unknown_command_error(b"X", &args);
        assert!(got.starts_with("unknown command 'X', with args beginning with: '"));
    }
}

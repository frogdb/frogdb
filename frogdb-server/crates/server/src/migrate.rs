//! MIGRATE command implementation.
//!
//! Provides TCP client for migrating keys to another Redis-compatible server.

use bytes::Bytes;
use frogdb_protocol::Response;
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Error type for MIGRATE operations.
#[derive(Debug)]
pub enum MigrateError {
    /// Failed to connect to target server.
    ConnectionFailed(String),
    /// Authentication failed.
    AuthFailed(String),
    /// Operation timed out.
    Timeout,
    /// I/O error during communication.
    IoError(std::io::Error),
    /// Protocol-level error.
    ProtocolError(String),
    /// Target server returned an error.
    TargetError(String),
}

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrateError::ConnectionFailed(msg) => write!(f, "connection failed: {}", msg),
            MigrateError::AuthFailed(msg) => write!(f, "auth failed: {}", msg),
            MigrateError::Timeout => write!(f, "timeout"),
            MigrateError::IoError(e) => write!(f, "I/O error: {}", e),
            MigrateError::ProtocolError(msg) => write!(f, "protocol error: {}", msg),
            MigrateError::TargetError(msg) => write!(f, "target error: {}", msg),
        }
    }
}

impl std::error::Error for MigrateError {}

impl From<std::io::Error> for MigrateError {
    fn from(e: std::io::Error) -> Self {
        MigrateError::IoError(e)
    }
}

/// Authentication info for MIGRATE.
#[derive(Debug, Clone)]
pub struct AuthInfo {
    /// Optional username (for AUTH2).
    pub username: Option<String>,
    /// Password.
    pub password: String,
}

/// Parsed arguments for MIGRATE command.
#[derive(Debug)]
pub struct MigrateArgs {
    /// Target host.
    pub host: String,
    /// Target port.
    pub port: u16,
    /// Keys to migrate.
    pub keys: Vec<Bytes>,
    /// Destination database.
    pub dest_db: u32,
    /// Timeout in milliseconds.
    pub timeout_ms: u64,
    /// Whether to copy (keep source key).
    pub copy: bool,
    /// Whether to replace existing key on target.
    pub replace: bool,
    /// Authentication info.
    pub auth: Option<AuthInfo>,
}

/// Single grammar walker for MIGRATE key selection.
///
/// Returns the indices into `args` that name keys to migrate, following the
/// MIGRATE grammar:
///
/// `MIGRATE host port <key|""> destination-db timeout [COPY] [REPLACE]
///  [AUTH password] [AUTH2 username password] [KEYS key...]`
///
/// The rule is: `args[2]` is a key iff non-empty, plus every argument after a
/// `KEYS` token. Options are skipped by width — COPY/REPLACE (+1), AUTH (+2),
/// AUTH2 (+3) — and anything else advances by one.
///
/// This is the single source of truth shared by both grammar consumers:
/// [`MigrateArgs::parse`] (the executor's owned-`Bytes` parse) and
/// `MigrateCommand::dynamic_keys` (the dispatcher's borrowed key extraction for
/// slot validation, ACL checks, and locking). Keeping key selection in one
/// place ensures the dispatcher can never guard a different key set than the
/// executor migrates.
///
/// Note on semantics: this walker is intentionally *permissive* — it mirrors
/// the dispatcher's behavior of silently skipping unknown options (+1) so that
/// key extraction still works on inputs the executor would reject.
/// [`MigrateArgs::parse`] layers its own strict validation (unknown-option
/// errors, AUTH arg-count errors) on top, calling this only for the
/// key-selection rule; on any grammar it accepts, the two agree by construction.
pub(crate) fn key_positions(args: &[Bytes]) -> Vec<usize> {
    let mut positions = Vec::new();

    // Positional single key: args[2] iff present and non-empty.
    if args.len() >= 3 && !args[2].is_empty() {
        positions.push(2);
    }

    // Skip loop over the trailing options, collecting any KEYS tail.
    let mut i = 5;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"KEYS" => {
                // All remaining arguments are keys.
                positions.extend((i + 1)..args.len());
                break;
            }
            b"AUTH" => i += 2,
            b"AUTH2" => i += 3,
            // COPY, REPLACE, and (permissively) any unknown option.
            _ => i += 1,
        }
    }

    positions
}

impl MigrateArgs {
    /// Parse MIGRATE arguments.
    ///
    /// Format: MIGRATE host port <key|""> destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key...]
    pub fn parse(args: &[Bytes]) -> Result<Self, String> {
        if args.len() < 5 {
            return Err("ERR wrong number of arguments for 'migrate' command".to_string());
        }

        let host = String::from_utf8_lossy(&args[0]).to_string();

        let port = String::from_utf8_lossy(&args[1])
            .parse::<u16>()
            .map_err(|_| "ERR port is not a valid integer")?;

        let dest_db = String::from_utf8_lossy(&args[3])
            .parse::<u32>()
            .map_err(|_| "ERR destination-db is not a valid integer")?;

        let timeout_ms = String::from_utf8_lossy(&args[4])
            .parse::<u64>()
            .map_err(|_| "ERR timeout is not a valid integer")?;

        let mut copy = false;
        let mut replace = false;
        let mut auth: Option<AuthInfo> = None;

        // Strict validation walk: sets copy/replace/auth and rejects malformed
        // grammar. Key selection is *not* done here — it comes from the shared
        // `key_positions` walker below so the executor and dispatcher can never
        // disagree on which keys are migrated.
        let mut i = 5;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"COPY" => {
                    copy = true;
                    i += 1;
                }
                b"REPLACE" => {
                    replace = true;
                    i += 1;
                }
                b"AUTH" => {
                    if i + 1 >= args.len() {
                        return Err("ERR AUTH requires a password".to_string());
                    }
                    auth = Some(AuthInfo {
                        username: None,
                        password: String::from_utf8_lossy(&args[i + 1]).to_string(),
                    });
                    i += 2;
                }
                b"AUTH2" => {
                    if i + 2 >= args.len() {
                        return Err("ERR AUTH2 requires username and password".to_string());
                    }
                    auth = Some(AuthInfo {
                        username: Some(String::from_utf8_lossy(&args[i + 1]).to_string()),
                        password: String::from_utf8_lossy(&args[i + 2]).to_string(),
                    });
                    i += 3;
                }
                b"KEYS" => {
                    // Remaining arguments are keys; no further options to validate.
                    break;
                }
                _ => {
                    return Err(format!(
                        "ERR Unknown option: {}",
                        String::from_utf8_lossy(&arg)
                    ));
                }
            }
        }

        // Single source of truth for key selection, shared with the dispatcher's
        // `MigrateCommand::dynamic_keys`.
        let keys: Vec<Bytes> = key_positions(args)
            .into_iter()
            .map(|i| args[i].clone())
            .collect();

        Ok(MigrateArgs {
            host,
            port,
            keys,
            dest_db,
            timeout_ms,
            copy,
            replace,
            auth,
        })
    }
}

/// TCP client for MIGRATE operations.
pub struct MigrateClient {
    framed: Framed<TcpStream, Resp2>,
    timeout: Duration,
}

impl MigrateClient {
    /// Connect to a target server.
    pub async fn connect(
        host: &str,
        port: u16,
        timeout_dur: Duration,
    ) -> Result<Self, MigrateError> {
        let addr = format!("{}:{}", host, port);

        let stream = timeout(timeout_dur, TcpStream::connect(&addr))
            .await
            .map_err(|_| MigrateError::Timeout)?
            .map_err(|e| MigrateError::ConnectionFailed(e.to_string()))?;

        Ok(MigrateClient {
            framed: Framed::new(stream, Resp2::default()),
            timeout: timeout_dur,
        })
    }

    /// Send a command and receive a response.
    async fn command(&mut self, args: &[&[u8]]) -> Result<Response, MigrateError> {
        // Build command frame
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::copy_from_slice(s)))
                .collect(),
        );

        // Send with timeout
        timeout(self.timeout, self.framed.send(frame))
            .await
            .map_err(|_| MigrateError::Timeout)?
            .map_err(|e| MigrateError::ProtocolError(e.to_string()))?;

        // Receive with timeout
        let response_frame = timeout(self.timeout, self.framed.next())
            .await
            .map_err(|_| MigrateError::Timeout)?
            .ok_or_else(|| MigrateError::ProtocolError("connection closed".to_string()))?
            .map_err(|e| MigrateError::ProtocolError(e.to_string()))?;

        Ok(frame_to_response(response_frame))
    }

    /// Authenticate with the target server.
    pub async fn auth(
        &mut self,
        password: &str,
        username: Option<&str>,
    ) -> Result<(), MigrateError> {
        let response = if let Some(user) = username {
            self.command(&[b"AUTH", user.as_bytes(), password.as_bytes()])
                .await?
        } else {
            self.command(&[b"AUTH", password.as_bytes()]).await?
        };

        match response {
            Response::Simple(s) if s == "OK" => Ok(()),
            Response::Error(e) => Err(MigrateError::AuthFailed(
                String::from_utf8_lossy(&e).to_string(),
            )),
            _ => Err(MigrateError::ProtocolError(
                "unexpected auth response".to_string(),
            )),
        }
    }

    /// Select a database on the target server.
    pub async fn select_db(&mut self, db: u32) -> Result<(), MigrateError> {
        let db_str = db.to_string();
        let response = self.command(&[b"SELECT", db_str.as_bytes()]).await?;

        match response {
            Response::Simple(s) if s == "OK" => Ok(()),
            Response::Error(e) => Err(MigrateError::TargetError(
                String::from_utf8_lossy(&e).to_string(),
            )),
            _ => Err(MigrateError::ProtocolError(
                "unexpected SELECT response".to_string(),
            )),
        }
    }

    /// RESTORE a key on the target server.
    ///
    /// The `serialized` data is in Redis DUMP format (our internal serialization).
    /// The `ttl` is in milliseconds (0 = no expiry).
    pub async fn restore(
        &mut self,
        key: &[u8],
        ttl: i64,
        serialized: &[u8],
        replace: bool,
    ) -> Result<(), MigrateError> {
        let ttl_str = ttl.to_string();
        let args: Vec<&[u8]> = if replace {
            vec![b"RESTORE", key, ttl_str.as_bytes(), serialized, b"REPLACE"]
        } else {
            vec![b"RESTORE", key, ttl_str.as_bytes(), serialized]
        };

        let response = self.command(&args).await?;

        match response {
            Response::Simple(s) if s == "OK" => Ok(()),
            Response::Error(e) => {
                let err_msg = String::from_utf8_lossy(&e).to_string();
                // If BUSYKEY and no REPLACE, that's expected (key exists)
                Err(MigrateError::TargetError(err_msg))
            }
            _ => Err(MigrateError::ProtocolError(
                "unexpected RESTORE response".to_string(),
            )),
        }
    }
}

/// Convert a BytesFrame to our Response type.
fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(e.into_inner()),
        BytesFrame::Integer(n) => Response::Integer(n),
        BytesFrame::BulkString(b) => Response::Bulk(Some(b)),
        BytesFrame::Null => Response::Bulk(None),
        BytesFrame::Array(items) => {
            Response::Array(items.into_iter().map(frame_to_response).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("mykey"),
            Bytes::from("0"),
            Bytes::from("5000"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 6380);
        assert_eq!(parsed.keys, vec![Bytes::from("mykey")]);
        assert_eq!(parsed.dest_db, 0);
        assert_eq!(parsed.timeout_ms, 5000);
        assert!(!parsed.copy);
        assert!(!parsed.replace);
        assert!(parsed.auth.is_none());
    }

    #[test]
    fn test_parse_with_options() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("mykey"),
            Bytes::from("0"),
            Bytes::from("5000"),
            Bytes::from("COPY"),
            Bytes::from("REPLACE"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        assert!(parsed.copy);
        assert!(parsed.replace);
    }

    #[test]
    fn test_parse_with_auth() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("mykey"),
            Bytes::from("0"),
            Bytes::from("5000"),
            Bytes::from("AUTH"),
            Bytes::from("secret"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        let auth = parsed.auth.unwrap();
        assert!(auth.username.is_none());
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn test_parse_with_auth2() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("mykey"),
            Bytes::from("0"),
            Bytes::from("5000"),
            Bytes::from("AUTH2"),
            Bytes::from("admin"),
            Bytes::from("secret"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        let auth = parsed.auth.unwrap();
        assert_eq!(auth.username, Some("admin".to_string()));
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn test_parse_with_keys() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from(""),
            Bytes::from("0"),
            Bytes::from("5000"),
            Bytes::from("KEYS"),
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        assert_eq!(
            parsed.keys,
            vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("key3")
            ]
        );
    }

    #[test]
    fn test_parse_single_key_and_keys() {
        // When both single key and KEYS are provided, single key is first
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("firstkey"),
            Bytes::from("0"),
            Bytes::from("5000"),
            Bytes::from("KEYS"),
            Bytes::from("key1"),
            Bytes::from("key2"),
        ];

        let parsed = MigrateArgs::parse(&args).unwrap();
        assert_eq!(
            parsed.keys,
            vec![
                Bytes::from("firstkey"),
                Bytes::from("key1"),
                Bytes::from("key2")
            ]
        );
    }

    #[test]
    fn test_parse_too_few_args() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("6380"),
            Bytes::from("mykey"),
        ];

        let result = MigrateArgs::parse(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_port() {
        let args = vec![
            Bytes::from("127.0.0.1"),
            Bytes::from("notaport"),
            Bytes::from("mykey"),
            Bytes::from("0"),
            Bytes::from("5000"),
        ];

        let result = MigrateArgs::parse(&args);
        assert!(result.is_err());
    }
}

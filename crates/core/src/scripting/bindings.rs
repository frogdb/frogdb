//! Redis command bindings for Lua scripts.

use bytes::Bytes;
use frogdb_protocol::Response;
use mlua::{MultiValue, Result as LuaResult, Value};

use super::error::ScriptError;

/// Check if a command is forbidden in scripts.
pub fn is_forbidden_in_script(cmd: &str) -> Option<&'static str> {
    match cmd.to_uppercase().as_str() {
        "MULTI" => Some("ERR MULTI calls can not be nested"),
        "EXEC" => Some("ERR EXEC without MULTI"),
        "DISCARD" => Some("ERR DISCARD without MULTI"),
        "WATCH" => Some("ERR WATCH inside MULTI is not allowed"),
        // Blocking commands
        "BLPOP" | "BRPOP" | "BLMOVE" | "BRPOPLPUSH" | "BLMPOP" | "BZPOPMIN" | "BZPOPMAX"
        | "BZMPOP" => Some("ERR blocking commands not allowed inside scripts"),
        // Nested script calls
        "EVAL" | "EVALSHA" | "SCRIPT" => Some("ERR nested script calls not allowed"),
        // Pub/Sub commands (not meaningful in script context)
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "SSUBSCRIBE"
        | "SUNSUBSCRIBE" | "PUBLISH" | "SPUBLISH" => {
            Some("ERR pub/sub commands not allowed inside scripts")
        }
        _ => None,
    }
}

/// Check if a command is a write command.
#[allow(dead_code)]
pub fn is_write_command(cmd: &str) -> bool {
    match cmd.to_uppercase().as_str() {
        // String writes
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "APPEND" | "SETRANGE"
        | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "GETSET" | "GETDEL"
        | "GETEX" => true,
        // Key operations
        "DEL" | "UNLINK" | "RENAME" | "RENAMENX" | "EXPIRE" | "EXPIREAT" | "PEXPIRE"
        | "PEXPIREAT" | "PERSIST" | "COPY" | "MOVE" | "RESTORE" => true,
        // List writes
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" | "LSET" | "LINSERT"
        | "LREM" | "LTRIM" | "LMOVE" | "LMPOP" | "RPOPLPUSH" => true,
        // Set writes
        "SADD" | "SREM" | "SPOP" | "SMOVE" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" => true,
        // Hash writes
        "HSET" | "HSETNX" | "HMSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" => true,
        // Sorted set writes
        "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" | "ZMPOP" | "ZUNIONSTORE"
        | "ZINTERSTORE" | "ZDIFFSTORE" | "ZRANGESTORE" | "ZREMRANGEBYRANK"
        | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" => true,
        _ => false,
    }
}

/// Validate that a key is declared in the KEYS array.
pub fn validate_key_access(key: &[u8], declared_keys: &[Bytes]) -> Result<(), ScriptError> {
    if !declared_keys.iter().any(|k| k.as_ref() == key) {
        return Err(ScriptError::UndeclaredKey {
            key: String::from_utf8_lossy(key).to_string(),
        });
    }
    Ok(())
}

/// Convert a RESP Response to a Lua Value.
#[allow(dead_code)]
pub fn response_to_lua(lua: &mlua::Lua, response: Response) -> LuaResult<Value> {
    match response {
        Response::Simple(s) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(s.as_ref())?)?;
            Ok(Value::Table(table))
        }
        Response::Error(e) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(e.as_ref())?)?;
            Ok(Value::Table(table))
        }
        Response::Integer(n) => Ok(Value::Integer(n)),
        Response::Bulk(Some(data)) => Ok(Value::String(lua.create_string(data.as_ref())?)),
        Response::Bulk(None) | Response::Null => Ok(Value::Boolean(false)), // Redis Lua convention: nil -> false
        Response::Array(arr) => {
            let table = lua.create_table()?;
            for (i, item) in arr.into_iter().enumerate() {
                let value = response_to_lua(lua, item)?;
                table.set(i + 1, value)?;
            }
            Ok(Value::Table(table))
        }
        Response::Double(n) => {
            // Convert double to number
            Ok(Value::Number(n))
        }
        Response::Boolean(b) => {
            // Redis Lua convention: false -> nil, true -> 1
            if b {
                Ok(Value::Integer(1))
            } else {
                Ok(Value::Boolean(false))
            }
        }
        Response::BlobError(e) => {
            let table = lua.create_table()?;
            table.set("err", lua.create_string(e.as_ref())?)?;
            Ok(Value::Table(table))
        }
        Response::VerbatimString { data, .. } => {
            // Treat verbatim string like bulk string
            Ok(Value::String(lua.create_string(data.as_ref())?))
        }
        Response::Map(pairs) => {
            let table = lua.create_table()?;
            for (key, value) in pairs {
                // For simplicity, convert both key and value
                let lua_key = response_to_lua(lua, key)?;
                let lua_value = response_to_lua(lua, value)?;
                table.set(lua_key, lua_value)?;
            }
            Ok(Value::Table(table))
        }
        Response::Set(items) => {
            // Treat set like array
            let table = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                let value = response_to_lua(lua, item)?;
                table.set(i + 1, value)?;
            }
            Ok(Value::Table(table))
        }
        Response::Push(items) => {
            // Treat push like array
            let table = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                let value = response_to_lua(lua, item)?;
                table.set(i + 1, value)?;
            }
            Ok(Value::Table(table))
        }
        Response::Attribute(inner) => {
            // Just return the inner value, ignoring attributes
            response_to_lua(lua, *inner)
        }
        Response::BigNumber(n) => {
            // Return big number as string
            Ok(Value::String(lua.create_string(n.as_ref())?))
        }
    }
}

/// Convert Lua arguments to command parts.
pub fn lua_args_to_command(args: MultiValue) -> Result<Vec<Bytes>, ScriptError> {
    let mut parts = Vec::new();

    for arg in args {
        match arg {
            Value::String(s) => {
                parts.push(Bytes::copy_from_slice(s.as_bytes().as_ref()));
            }
            Value::Integer(n) => {
                parts.push(Bytes::from(n.to_string()));
            }
            Value::Number(n) => {
                // Format number without unnecessary decimal places
                if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
                    parts.push(Bytes::from((n as i64).to_string()));
                } else {
                    parts.push(Bytes::from(n.to_string()));
                }
            }
            Value::Boolean(b) => {
                parts.push(Bytes::from(if b { "1" } else { "0" }));
            }
            Value::Nil => {
                // Skip nil values (common in Lua when passing optional args)
                continue;
            }
            _ => {
                return Err(ScriptError::Runtime(format!(
                    "ERR wrong type of argument for redis command (got {:?})",
                    arg.type_name()
                )));
            }
        }
    }

    Ok(parts)
}

/// Extract the command name from arguments.
#[allow(dead_code)]
pub fn extract_command_name(args: &[Bytes]) -> Option<String> {
    args.first()
        .map(|b| String::from_utf8_lossy(b).to_uppercase())
}

/// Extract keys from command arguments based on command type.
/// This is a simplified version - in practice, each command has different key positions.
pub fn extract_keys_from_command(cmd: &str, args: &[Bytes]) -> Vec<Bytes> {
    if args.len() < 2 {
        return vec![];
    }

    match cmd.to_uppercase().as_str() {
        // Single key at position 1
        "GET" | "SET" | "SETNX" | "SETEX" | "PSETEX" | "APPEND" | "STRLEN" | "GETRANGE"
        | "SETRANGE" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "GETSET"
        | "GETDEL" | "GETEX" | "DEL" | "UNLINK" | "EXISTS" | "EXPIRE" | "EXPIREAT" | "PEXPIRE"
        | "PEXPIREAT" | "TTL" | "PTTL" | "PERSIST" | "TYPE" | "LPUSH" | "RPUSH" | "LPUSHX"
        | "RPUSHX" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX" | "LSET" | "LINSERT"
        | "LREM" | "LTRIM" | "LPOS" | "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SMISMEMBER"
        | "SCARD" | "SRANDMEMBER" | "SPOP" | "HSET" | "HSETNX" | "HGET" | "HDEL" | "HMSET"
        | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HEXISTS" | "HLEN" | "HINCRBY"
        | "HINCRBYFLOAT" | "HSTRLEN" | "HSCAN" | "HRANDFIELD" | "ZADD" | "ZREM" | "ZSCORE"
        | "ZMSCORE" | "ZCARD" | "ZINCRBY" | "ZRANK" | "ZREVRANK" | "ZRANGE" | "ZRANGEBYSCORE"
        | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGEBYLEX" | "ZCOUNT" | "ZLEXCOUNT"
        | "ZPOPMIN" | "ZPOPMAX" | "ZRANDMEMBER" | "ZSCAN" | "ZREMRANGEBYRANK"
        | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" | "DUMP" | "RESTORE" | "OBJECT" | "TOUCH" => {
            vec![args[1].clone()]
        }

        // MGET/DEL/EXISTS/UNLINK - all args after command are keys
        "MGET" => args[1..].to_vec(),

        // MSET - every other arg starting at position 1 is a key
        "MSET" | "MSETNX" => {
            args[1..].chunks(2).map(|chunk| chunk[0].clone()).collect()
        }

        // RENAME - two keys
        "RENAME" | "RENAMENX" | "COPY" => {
            if args.len() >= 3 {
                vec![args[1].clone(), args[2].clone()]
            } else {
                vec![]
            }
        }

        // LMOVE/SMOVE - source and destination
        "LMOVE" | "SMOVE" => {
            if args.len() >= 3 {
                vec![args[1].clone(), args[2].clone()]
            } else {
                vec![]
            }
        }

        // Set operations with multiple keys
        "SUNION" | "SINTER" | "SDIFF" => args[1..].to_vec(),

        // Store operations - first is destination, rest are sources
        "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "ZUNIONSTORE" | "ZINTERSTORE"
        | "ZDIFFSTORE" => args[1..].to_vec(),

        // Default: assume first arg after command is the key
        _ => {
            if args.len() > 1 {
                vec![args[1].clone()]
            } else {
                vec![]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_forbidden_in_script() {
        assert!(is_forbidden_in_script("MULTI").is_some());
        assert!(is_forbidden_in_script("multi").is_some());
        assert!(is_forbidden_in_script("EVAL").is_some());
        assert!(is_forbidden_in_script("BLPOP").is_some());
        assert!(is_forbidden_in_script("GET").is_none());
        assert!(is_forbidden_in_script("SET").is_none());
    }

    #[test]
    fn test_is_write_command() {
        assert!(is_write_command("SET"));
        assert!(is_write_command("DEL"));
        assert!(is_write_command("LPUSH"));
        assert!(!is_write_command("GET"));
        assert!(!is_write_command("LRANGE"));
    }

    #[test]
    fn test_validate_key_access() {
        let declared = vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
        ];

        assert!(validate_key_access(b"key1", &declared).is_ok());
        assert!(validate_key_access(b"key2", &declared).is_ok());
        assert!(validate_key_access(b"key3", &declared).is_err());
    }

    #[test]
    fn test_extract_keys_get() {
        let args = vec![
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"mykey"),
        ];
        let keys = extract_keys_from_command("GET", &args);
        assert_eq!(keys, vec![Bytes::from_static(b"mykey")]);
    }

    #[test]
    fn test_extract_keys_mset() {
        let args = vec![
            Bytes::from_static(b"MSET"),
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"value1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"value2"),
        ];
        let keys = extract_keys_from_command("MSET", &args);
        assert_eq!(keys, vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
        ]);
    }

    #[test]
    fn test_extract_keys_rename() {
        let args = vec![
            Bytes::from_static(b"RENAME"),
            Bytes::from_static(b"old"),
            Bytes::from_static(b"new"),
        ];
        let keys = extract_keys_from_command("RENAME", &args);
        assert_eq!(keys, vec![
            Bytes::from_static(b"old"),
            Bytes::from_static(b"new"),
        ]);
    }
}

use crate::cli::OutputMode;

pub trait Renderable {
    fn render_table(&self, no_color: bool) -> String;
    fn render_json(&self) -> serde_json::Value;
    fn render_raw(&self) -> String;
}

pub fn print_output(value: &dyn Renderable, mode: OutputMode, no_color: bool) {
    let text = match mode {
        OutputMode::Table => value.render_table(no_color),
        OutputMode::Json => serde_json::to_string_pretty(&value.render_json()).unwrap(),
        OutputMode::Raw => value.render_raw(),
    };
    print!("{text}");
}

/// Render a raw `redis::Value` to stdout in the given output mode.
pub fn render_value(value: &redis::Value, mode: OutputMode, _no_color: bool) {
    match mode {
        OutputMode::Json => {
            let json = value_to_json(value);
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
        }
        OutputMode::Table | OutputMode::Raw => {
            println!("{}", format_value(value, 0));
        }
    }
}

fn format_value(value: &redis::Value, indent: usize) -> String {
    let pad = "  ".repeat(indent);
    match value {
        redis::Value::Nil => format!("{pad}(nil)"),
        redis::Value::Int(n) => format!("{pad}(integer) {n}"),
        redis::Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            format!("{pad}\"{s}\"")
        }
        redis::Value::Okay => format!("{pad}OK"),
        redis::Value::SimpleString(s) => format!("{pad}\"{s}\""),
        redis::Value::Array(items) => {
            if items.is_empty() {
                return format!("{pad}(empty array)");
            }
            let mut out = String::new();
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                }
                out.push_str(&format!(
                    "{pad}{}) {}",
                    i + 1,
                    format_value(item, indent + 1).trim_start()
                ));
            }
            out
        }
        redis::Value::ServerError(err) => format!(
            "{pad}(error) {} {}",
            err.code(),
            err.details().unwrap_or("")
        ),
        _ => format!("{pad}{value:?}"),
    }
}

pub fn value_to_json(value: &redis::Value) -> serde_json::Value {
    match value {
        redis::Value::Nil => serde_json::Value::Null,
        redis::Value::Int(n) => serde_json::json!(n),
        redis::Value::BulkString(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(s) => serde_json::Value::String(s),
            Err(_) => serde_json::json!(bytes),
        },
        redis::Value::SimpleString(s) => serde_json::Value::String(s.clone()),
        redis::Value::Okay => serde_json::Value::String("OK".to_string()),
        redis::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
        redis::Value::ServerError(err) => {
            serde_json::json!({"error": format!("{} {}", err.code(), err.details().unwrap_or(""))})
        }
        _ => serde_json::json!(format!("{value:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_nil() {
        assert_eq!(format_value(&redis::Value::Nil, 0), "(nil)");
    }

    #[test]
    fn test_format_value_int() {
        assert_eq!(format_value(&redis::Value::Int(42), 0), "(integer) 42");
    }

    #[test]
    fn test_format_value_bulk_string() {
        let val = redis::Value::BulkString(b"hello".to_vec());
        assert_eq!(format_value(&val, 0), "\"hello\"");
    }

    #[test]
    fn test_format_value_okay() {
        assert_eq!(format_value(&redis::Value::Okay, 0), "OK");
    }

    #[test]
    fn test_format_value_simple_string() {
        let val = redis::Value::SimpleString("PONG".to_string());
        assert_eq!(format_value(&val, 0), "\"PONG\"");
    }

    #[test]
    fn test_format_value_empty_array() {
        let val = redis::Value::Array(vec![]);
        assert_eq!(format_value(&val, 0), "(empty array)");
    }

    #[test]
    fn test_format_value_array() {
        let val = redis::Value::Array(vec![
            redis::Value::BulkString(b"a".to_vec()),
            redis::Value::BulkString(b"b".to_vec()),
        ]);
        let output = format_value(&val, 0);
        assert!(output.contains("1)"));
        assert!(output.contains("2)"));
        assert!(output.contains("\"a\""));
        assert!(output.contains("\"b\""));
    }

    #[test]
    fn test_format_value_indented() {
        let val = redis::Value::Int(5);
        assert_eq!(format_value(&val, 2), "    (integer) 5");
    }

    #[test]
    fn test_value_to_json_nil() {
        assert_eq!(value_to_json(&redis::Value::Nil), serde_json::Value::Null);
    }

    #[test]
    fn test_value_to_json_int() {
        assert_eq!(value_to_json(&redis::Value::Int(99)), serde_json::json!(99));
    }

    #[test]
    fn test_value_to_json_bulk_string() {
        let val = redis::Value::BulkString(b"test".to_vec());
        assert_eq!(value_to_json(&val), serde_json::json!("test"));
    }

    #[test]
    fn test_value_to_json_okay() {
        assert_eq!(value_to_json(&redis::Value::Okay), serde_json::json!("OK"));
    }

    #[test]
    fn test_value_to_json_array() {
        let val = redis::Value::Array(vec![
            redis::Value::Int(1),
            redis::Value::BulkString(b"two".to_vec()),
        ]);
        assert_eq!(value_to_json(&val), serde_json::json!([1, "two"]));
    }
}

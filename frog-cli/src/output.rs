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

//! Library parsing - shebang extraction and function registration capture.

use super::error::FunctionError;
use super::function::{FunctionFlags, RegisteredFunction};
use super::library::FunctionLibrary;

/// Parsed shebang information from library code.
#[derive(Debug, Clone)]
pub struct ShebangInfo {
    /// Engine name (e.g., "lua").
    pub engine: String,
    /// Library name.
    pub name: String,
}

/// Parse the shebang from library code.
///
/// Expected format: `#!lua name=<library_name>`
/// The shebang must be on the first line.
pub fn parse_shebang(code: &str) -> Result<ShebangInfo, FunctionError> {
    let first_line = code.lines().next().unwrap_or("");

    if !first_line.starts_with("#!") {
        return Err(FunctionError::InvalidShebang {
            message: "Missing shebang (#!)".to_string(),
        });
    }

    let after_shebang = &first_line[2..];

    // Engine is the text immediately after #! up to the first whitespace.
    // For "#!lua name=foo", engine="lua". For "#! name=foo", engine="".
    let (engine_str, metadata_str) = match after_shebang.find(char::is_whitespace) {
        Some(idx) => (&after_shebang[..idx], after_shebang[idx..].trim()),
        None => (after_shebang.trim(), ""),
    };

    let engine = engine_str.to_string();

    // Currently only lua is supported (case-insensitive)
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(FunctionError::UnsupportedEngine { engine });
    }

    // Parse key=value metadata pairs
    let mut name: Option<String> = None;

    for part in metadata_str.split_whitespace() {
        if let Some(("name", value)) = part.split_once('=') {
            if name.is_some() {
                return Err(FunctionError::InvalidShebang {
                    message: "Invalid metadata value, name argument was given multiple times"
                        .to_string(),
                });
            }
            // Strip surrounding quotes if present
            let value = if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
                &value[1..value.len() - 1]
            } else {
                value
            };
            name = Some(value.to_string());
        } else {
            return Err(FunctionError::InvalidShebang {
                message: format!("Invalid metadata value given: {}", part),
            });
        }
    }

    let name = name.ok_or_else(|| FunctionError::InvalidShebang {
        message: "Library name was not given".to_string(),
    })?;

    if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(FunctionError::InvalidShebang {
            message: "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long".to_string(),
        });
    }

    Ok(ShebangInfo { engine, name })
}

/// A function registration captured during sandbox execution.
#[derive(Debug, Clone)]
pub struct CapturedRegistration {
    /// Function name.
    pub name: String,
    /// Function flags.
    pub flags: FunctionFlags,
    /// Optional description.
    pub description: Option<String>,
}

/// Result of parsing a library.
#[derive(Debug)]
pub struct ParsedLibrary {
    /// Library name from shebang.
    pub name: String,
    /// Engine from shebang.
    pub engine: String,
    /// Original source code.
    pub code: String,
    /// Captured function registrations.
    pub registrations: Vec<CapturedRegistration>,
}

impl ParsedLibrary {
    /// Convert to a FunctionLibrary.
    pub fn into_library(self) -> Result<FunctionLibrary, FunctionError> {
        if self.registrations.is_empty() {
            return Err(FunctionError::NoFunctionsRegistered);
        }

        let mut library = FunctionLibrary::with_metadata(
            self.name,
            self.code,
            self.engine,
            None, // Description can be added later if needed
        );

        for reg in self.registrations {
            library.add_function(RegisteredFunction::new(
                reg.name,
                reg.flags,
                reg.description,
            ));
        }

        Ok(library)
    }
}

/// Parse function flags from a table-like structure.
///
/// Flags can be:
/// - A table with flag names as keys: `{["no-writes"] = true, ["allow-oom"] = true}`
/// - An array of flag strings: `{"no-writes", "allow-oom"}`
#[allow(dead_code)]
pub fn parse_flags_from_lua_table(
    flags: &[(String, bool)],
) -> Result<FunctionFlags, FunctionError> {
    let flag_names: Vec<String> = flags
        .iter()
        .filter(|(_, v)| *v)
        .map(|(k, _)| k.clone())
        .collect();

    FunctionFlags::from_strings(&flag_names)
        .map_err(|msg| FunctionError::InvalidFlags { message: msg })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shebang_basic() {
        let code = "#!lua name=mylib\nlocal x = 1";
        let info = parse_shebang(code).unwrap();
        assert_eq!(info.engine, "lua");
        assert_eq!(info.name, "mylib");
    }

    #[test]
    fn test_parse_shebang_with_extra_spaces() {
        let code = "#!lua   name=mylib\ncode";
        let info = parse_shebang(code).unwrap();
        assert_eq!(info.name, "mylib");
    }

    #[test]
    fn test_parse_shebang_with_dashes() {
        let code = "#!lua name=my-lib\ncode";
        let result = parse_shebang(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_parse_shebang_missing() {
        let code = "local x = 1";
        let result = parse_shebang(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_parse_shebang_missing_name() {
        let code = "#!lua\nlocal x = 1";
        let result = parse_shebang(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_parse_shebang_empty_name() {
        let code = "#!lua name=\nlocal x = 1";
        let result = parse_shebang(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_parse_shebang_invalid_engine() {
        let code = "#!python name=mylib\ncode";
        let result = parse_shebang(code);
        assert!(matches!(
            result,
            Err(FunctionError::UnsupportedEngine { .. })
        ));
    }

    #[test]
    fn test_parse_shebang_invalid_chars() {
        let code = "#!lua name=my.lib\ncode";
        let result = parse_shebang(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_parsed_library_to_library() {
        let parsed = ParsedLibrary {
            name: "mylib".to_string(),
            engine: "lua".to_string(),
            code: "#!lua name=mylib\n-- code".to_string(),
            registrations: vec![CapturedRegistration {
                name: "myfunc".to_string(),
                flags: FunctionFlags::NO_WRITES,
                description: Some("A test function".to_string()),
            }],
        };

        let library = parsed.into_library().unwrap();
        assert_eq!(library.name, "mylib");
        assert_eq!(library.function_count(), 1);
        assert!(library.get_function("myfunc").is_some());
    }

    #[test]
    fn test_parsed_library_no_functions() {
        let parsed = ParsedLibrary {
            name: "mylib".to_string(),
            engine: "lua".to_string(),
            code: "#!lua name=mylib".to_string(),
            registrations: vec![],
        };

        let result = parsed.into_library();
        assert!(matches!(result, Err(FunctionError::NoFunctionsRegistered)));
    }

    #[test]
    fn test_parse_flags() {
        let flags = vec![
            ("no-writes".to_string(), true),
            ("allow-oom".to_string(), true),
            ("allow-stale".to_string(), false),
        ];
        let result = parse_flags_from_lua_table(&flags).unwrap();
        assert!(result.contains(FunctionFlags::NO_WRITES));
        assert!(result.contains(FunctionFlags::ALLOW_OOM));
        assert!(!result.contains(FunctionFlags::ALLOW_STALE));
    }
}

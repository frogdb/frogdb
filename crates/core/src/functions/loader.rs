//! Library loader - parses and loads function libraries using Lua sandbox.

use std::sync::{Arc, Mutex};

use mlua::{Lua, MultiValue, Result as LuaResult, StdLib, Value};

use super::error::FunctionError;
use super::function::FunctionFlags;
use super::library::FunctionLibrary;
use super::parser::{parse_shebang, CapturedRegistration, ParsedLibrary};

/// Load a function library from source code.
///
/// This function:
/// 1. Parses the shebang to extract library name
/// 2. Executes the code in a sandbox with `redis.register_function`
/// 3. Captures all function registrations
/// 4. Returns a FunctionLibrary
pub fn load_library(code: &str) -> Result<FunctionLibrary, FunctionError> {
    // Parse shebang to get library name
    let shebang = parse_shebang(code)?;

    // Execute in sandbox to capture registrations
    let registrations = execute_in_sandbox(code)?;

    // Build parsed library
    let parsed = ParsedLibrary {
        name: shebang.name,
        engine: shebang.engine,
        code: code.to_string(),
        registrations,
    };

    // Convert to FunctionLibrary
    parsed.into_library()
}

/// Execute library code in a sandbox to capture function registrations.
fn execute_in_sandbox(code: &str) -> Result<Vec<CapturedRegistration>, FunctionError> {
    // Create minimal Lua environment for parsing
    let libs = StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::MATH;

    let lua = unsafe { Lua::unsafe_new_with(libs, mlua::LuaOptions::default()) };

    // Storage for captured registrations
    let registrations: Arc<Mutex<Vec<CapturedRegistration>>> = Arc::new(Mutex::new(Vec::new()));

    // Set up redis.register_function binding
    setup_register_function_binding(&lua, registrations.clone())
        .map_err(|e| FunctionError::LoadError { message: e })?;

    // Skip the shebang line when executing (Lua doesn't understand #!)
    let code_without_shebang = if code.starts_with("#!") {
        // Find the first newline and skip past it
        code.find('\n').map(|pos| &code[pos + 1..]).unwrap_or("")
    } else {
        code
    };

    // Execute the library code
    lua.load(code_without_shebang)
        .exec()
        .map_err(|e| FunctionError::LoadError {
            message: format!("Lua error: {}", e),
        })?;

    // Extract captured registrations
    let regs = registrations.lock().unwrap().clone();

    Ok(regs)
}

/// Set up the redis.register_function binding that captures registrations.
fn setup_register_function_binding(
    lua: &Lua,
    registrations: Arc<Mutex<Vec<CapturedRegistration>>>,
) -> Result<(), String> {
    let globals = lua.globals();

    // Create redis table
    let redis_table = lua
        .create_table()
        .map_err(|e| format!("Failed to create redis table: {}", e))?;

    // Create register_function that supports both forms:
    // redis.register_function('name', callback)
    // redis.register_function{function_name='name', callback=fn, flags={...}, description='...'}
    let regs = registrations.clone();
    let register_fn = lua
        .create_function(move |_lua_ctx, args: MultiValue| -> LuaResult<()> {
            let regs = regs.clone();

            // Determine which form was used
            let args_vec: Vec<Value> = args.into_iter().collect();

            if args_vec.is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "redis.register_function requires at least one argument".to_string(),
                ));
            }

            let registration = match &args_vec[0] {
                // Table form: redis.register_function{...}
                Value::Table(t) => parse_table_registration(t)?,

                // Simple form: redis.register_function('name', callback)
                Value::String(name) => {
                    if args_vec.len() < 2 {
                        return Err(mlua::Error::RuntimeError(
                            "redis.register_function requires a callback function".to_string(),
                        ));
                    }

                    // Verify second arg is a function
                    if !matches!(args_vec[1], Value::Function(_)) {
                        return Err(mlua::Error::RuntimeError(
                            "Second argument must be a function".to_string(),
                        ));
                    }

                    CapturedRegistration {
                        name: name.to_str()?.to_string(),
                        flags: FunctionFlags::empty(),
                        description: None,
                    }
                }

                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "First argument must be a string or table".to_string(),
                    ))
                }
            };

            // Validate function name
            if registration.name.is_empty() {
                return Err(mlua::Error::RuntimeError(
                    "Function name cannot be empty".to_string(),
                ));
            }

            // Check for duplicate registrations
            {
                let existing = regs.lock().unwrap();
                if existing.iter().any(|r| r.name == registration.name) {
                    return Err(mlua::Error::RuntimeError(format!(
                        "Function '{}' already registered in this library",
                        registration.name
                    )));
                }
            }

            // Store the registration
            regs.lock().unwrap().push(registration);

            Ok(())
        })
        .map_err(|e| format!("Failed to create register_function: {}", e))?;

    redis_table
        .set("register_function", register_fn)
        .map_err(|e| format!("Failed to set register_function: {}", e))?;

    // Add dummy redis.call and redis.pcall that error during library loading
    let call_fn = lua
        .create_function(|_, _: MultiValue| -> LuaResult<Value> {
            Err(mlua::Error::RuntimeError(
                "redis.call is not available during library loading".to_string(),
            ))
        })
        .map_err(|e| format!("Failed to create call fn: {}", e))?;

    let pcall_fn = lua
        .create_function(|_, _: MultiValue| -> LuaResult<Value> {
            Err(mlua::Error::RuntimeError(
                "redis.pcall is not available during library loading".to_string(),
            ))
        })
        .map_err(|e| format!("Failed to create pcall fn: {}", e))?;

    redis_table
        .set("call", call_fn)
        .map_err(|e| format!("Failed to set call: {}", e))?;

    redis_table
        .set("pcall", pcall_fn)
        .map_err(|e| format!("Failed to set pcall: {}", e))?;

    // Add redis.log as no-op during loading
    let log_fn = lua
        .create_function(|_, (_level, _msg): (i32, String)| -> LuaResult<()> { Ok(()) })
        .map_err(|e| format!("Failed to create log fn: {}", e))?;

    redis_table
        .set("log", log_fn)
        .map_err(|e| format!("Failed to set log: {}", e))?;

    // Add log level constants
    redis_table
        .set("LOG_DEBUG", 0i32)
        .map_err(|e| format!("Failed to set LOG_DEBUG: {}", e))?;
    redis_table
        .set("LOG_VERBOSE", 1i32)
        .map_err(|e| format!("Failed to set LOG_VERBOSE: {}", e))?;
    redis_table
        .set("LOG_NOTICE", 2i32)
        .map_err(|e| format!("Failed to set LOG_NOTICE: {}", e))?;
    redis_table
        .set("LOG_WARNING", 3i32)
        .map_err(|e| format!("Failed to set LOG_WARNING: {}", e))?;

    globals
        .set("redis", redis_table)
        .map_err(|e| format!("Failed to set redis: {}", e))?;

    Ok(())
}

/// Parse a table-form registration.
fn parse_table_registration(t: &mlua::Table) -> LuaResult<CapturedRegistration> {
    // Get function_name (required)
    let name: String = t.get("function_name").map_err(|_| {
        mlua::Error::RuntimeError("Missing required field 'function_name'".to_string())
    })?;

    // Get callback (required) - just verify it exists
    let _callback: mlua::Function = t.get("callback").map_err(|_| {
        mlua::Error::RuntimeError("Missing required field 'callback'".to_string())
    })?;

    // Get optional description
    let description: Option<String> = t.get("description").ok();

    // Get optional flags
    let flags = if let Ok(flags_val) = t.get::<Value>("flags") {
        parse_flags_value(&flags_val)?
    } else {
        FunctionFlags::empty()
    };

    Ok(CapturedRegistration {
        name,
        flags,
        description,
    })
}

/// Parse flags from a Lua value.
fn parse_flags_value(value: &Value) -> LuaResult<FunctionFlags> {
    match value {
        Value::Nil => Ok(FunctionFlags::empty()),
        Value::Table(t) => {
            let mut flags = FunctionFlags::empty();

            // Check for array-style flags: {"no-writes", "allow-oom"}
            let mut idx = 1;
            while let Ok(flag_name) = t.get::<String>(idx) {
                match flag_name.as_str() {
                    "no-writes" => flags |= FunctionFlags::NO_WRITES,
                    "allow-oom" => flags |= FunctionFlags::ALLOW_OOM,
                    "allow-stale" => flags |= FunctionFlags::ALLOW_STALE,
                    "no-cluster" => flags |= FunctionFlags::NO_CLUSTER,
                    other => {
                        return Err(mlua::Error::RuntimeError(format!(
                            "Unknown flag: {}",
                            other
                        )))
                    }
                }
                idx += 1;
            }

            // Also check for table-style flags: {["no-writes"] = true}
            for pair in t.pairs::<String, bool>() {
                if let Ok((key, value)) = pair {
                    if value {
                        match key.as_str() {
                            "no-writes" => flags |= FunctionFlags::NO_WRITES,
                            "allow-oom" => flags |= FunctionFlags::ALLOW_OOM,
                            "allow-stale" => flags |= FunctionFlags::ALLOW_STALE,
                            "no-cluster" => flags |= FunctionFlags::NO_CLUSTER,
                            _ => {} // Ignore non-flag keys or already processed array items
                        }
                    }
                }
            }

            Ok(flags)
        }
        _ => Err(mlua::Error::RuntimeError(
            "flags must be a table".to_string(),
        )),
    }
}

/// Validate a loaded library structure.
pub fn validate_library(library: &FunctionLibrary) -> Result<(), FunctionError> {
    // Check that the library has at least one function
    if library.functions.is_empty() {
        return Err(FunctionError::NoFunctionsRegistered);
    }

    // Validate function names
    for name in library.functions.keys() {
        if name.is_empty() {
            return Err(FunctionError::ParseError {
                message: "Function name cannot be empty".to_string(),
            });
        }

        // Function names should be alphanumeric with underscores
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(FunctionError::ParseError {
                message: format!(
                    "Function name '{}' contains invalid characters",
                    name
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::function::RegisteredFunction;

    #[test]
    fn test_load_simple_library() {
        let code = r#"#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
    return 'hello'
end)
"#;

        let library = load_library(code).unwrap();
        assert_eq!(library.name, "mylib");
        assert_eq!(library.function_count(), 1);
        assert!(library.get_function("myfunc").is_some());
    }

    #[test]
    fn test_load_library_with_table_form() {
        let code = r#"#!lua name=mylib
redis.register_function{
    function_name = 'myfunc',
    callback = function(keys, args)
        return 'hello'
    end,
    flags = {'no-writes'},
    description = 'A test function'
}
"#;

        let library = load_library(code).unwrap();
        let func = library.get_function("myfunc").unwrap();
        assert!(func.flags.contains(FunctionFlags::NO_WRITES));
        assert_eq!(func.description, Some("A test function".to_string()));
    }

    #[test]
    fn test_load_library_multiple_functions() {
        let code = r#"#!lua name=mylib
redis.register_function('func1', function(keys, args) return 1 end)
redis.register_function('func2', function(keys, args) return 2 end)
"#;

        let library = load_library(code).unwrap();
        assert_eq!(library.function_count(), 2);
        assert!(library.get_function("func1").is_some());
        assert!(library.get_function("func2").is_some());
    }

    #[test]
    fn test_load_library_missing_shebang() {
        let code = r#"
redis.register_function('myfunc', function() return 1 end)
"#;

        let result = load_library(code);
        assert!(matches!(result, Err(FunctionError::InvalidShebang { .. })));
    }

    #[test]
    fn test_load_library_no_functions() {
        let code = r#"#!lua name=mylib
-- No functions registered
local x = 1
"#;

        let result = load_library(code);
        assert!(matches!(result, Err(FunctionError::NoFunctionsRegistered)));
    }

    #[test]
    fn test_load_library_duplicate_function() {
        let code = r#"#!lua name=mylib
redis.register_function('myfunc', function() return 1 end)
redis.register_function('myfunc', function() return 2 end)
"#;

        let result = load_library(code);
        assert!(matches!(result, Err(FunctionError::LoadError { .. })));
    }

    #[test]
    fn test_load_library_with_flags_table_style() {
        let code = r#"#!lua name=mylib
redis.register_function{
    function_name = 'myfunc',
    callback = function() return 1 end,
    flags = {['no-writes'] = true, ['allow-oom'] = true}
}
"#;

        let library = load_library(code).unwrap();
        let func = library.get_function("myfunc").unwrap();
        assert!(func.flags.contains(FunctionFlags::NO_WRITES));
        assert!(func.flags.contains(FunctionFlags::ALLOW_OOM));
    }

    #[test]
    fn test_load_library_lua_error() {
        let code = r#"#!lua name=mylib
this is not valid lua
"#;

        let result = load_library(code);
        assert!(matches!(result, Err(FunctionError::LoadError { .. })));
    }

    #[test]
    fn test_validate_library_empty() {
        let library = FunctionLibrary::new("test".to_string(), "code".to_string());
        let result = validate_library(&library);
        assert!(matches!(result, Err(FunctionError::NoFunctionsRegistered)));
    }

    #[test]
    fn test_validate_library_valid() {
        let mut library = FunctionLibrary::new("test".to_string(), "code".to_string());
        library.add_function(RegisteredFunction::new(
            "valid_func".to_string(),
            FunctionFlags::empty(),
            None,
        ));
        assert!(validate_library(&library).is_ok());
    }
}

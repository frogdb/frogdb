//! Function library representation.

use std::collections::HashMap;

use super::function::RegisteredFunction;

/// A function library containing one or more functions.
#[derive(Debug, Clone)]
pub struct FunctionLibrary {
    /// The library name (from shebang: #!lua name=<name>).
    pub name: String,

    /// The full library source code.
    pub code: String,

    /// Functions registered by this library.
    pub functions: HashMap<String, RegisteredFunction>,

    /// The engine used (always "lua" for now).
    pub engine: String,

    /// Optional library description.
    pub description: Option<String>,
}

impl FunctionLibrary {
    /// Create a new function library.
    pub fn new(name: String, code: String) -> Self {
        Self {
            name,
            code,
            functions: HashMap::new(),
            engine: "lua".to_string(),
            description: None,
        }
    }

    /// Create a new function library with engine and description.
    pub fn with_metadata(
        name: String,
        code: String,
        engine: String,
        description: Option<String>,
    ) -> Self {
        Self {
            name,
            code,
            functions: HashMap::new(),
            engine,
            description,
        }
    }

    /// Add a function to this library.
    pub fn add_function(&mut self, function: RegisteredFunction) {
        self.functions.insert(function.name.clone(), function);
    }

    /// Get a function by name.
    pub fn get_function(&self, name: &str) -> Option<&RegisteredFunction> {
        self.functions.get(name)
    }

    /// Get all function names in this library.
    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    /// Get the number of functions in this library.
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::FunctionFlags;

    #[test]
    fn test_library_creation() {
        let lib = FunctionLibrary::new("mylib".to_string(), "-- code".to_string());
        assert_eq!(lib.name, "mylib");
        assert_eq!(lib.code, "-- code");
        assert_eq!(lib.engine, "lua");
        assert!(lib.functions.is_empty());
    }

    #[test]
    fn test_library_add_function() {
        let mut lib = FunctionLibrary::new("mylib".to_string(), "-- code".to_string());
        let func = RegisteredFunction::new("myfunc".to_string(), FunctionFlags::empty(), None);
        lib.add_function(func);

        assert_eq!(lib.function_count(), 1);
        assert!(lib.get_function("myfunc").is_some());
        assert!(lib.get_function("nonexistent").is_none());
    }

    #[test]
    fn test_library_function_names() {
        let mut lib = FunctionLibrary::new("mylib".to_string(), "-- code".to_string());
        lib.add_function(RegisteredFunction::new(
            "func1".to_string(),
            FunctionFlags::empty(),
            None,
        ));
        lib.add_function(RegisteredFunction::new(
            "func2".to_string(),
            FunctionFlags::empty(),
            None,
        ));

        let names = lib.function_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"func1"));
        assert!(names.contains(&"func2"));
    }
}

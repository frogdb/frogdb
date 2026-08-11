//! Global function registry for storing and managing function libraries.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::error::FunctionError;
use super::function::RegisteredFunction;
use super::library::FunctionLibrary;

/// Statistics about registered functions.
#[derive(Debug, Clone, Default)]
pub struct FunctionStats {
    /// Total number of libraries.
    pub library_count: usize,
    /// Total number of functions across all libraries.
    pub function_count: usize,
    /// Currently running function (if any).
    pub running_function: Option<RunningFunctionInfo>,
}

/// Information about a currently running function.
#[derive(Debug, Clone)]
pub struct RunningFunctionInfo {
    /// Name of the running function.
    pub name: String,
    /// Library containing the function.
    pub library: String,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Global registry for function libraries.
///
/// This registry stores all loaded function libraries and provides
/// lookup from function name to library. It is designed to be
/// shared across all shards via `Arc<RwLock<FunctionRegistry>>`.
#[derive(Debug)]
pub struct FunctionRegistry {
    /// Libraries indexed by library name.
    libraries: HashMap<String, FunctionLibrary>,

    /// Index from function name to library name for quick lookup.
    function_index: HashMap<String, String>,

    /// Running function info (for FUNCTION STATS and KILL).
    running_function: Option<RunningFunctionInfo>,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    /// Create a new empty function registry.
    pub fn new() -> Self {
        Self {
            libraries: HashMap::new(),
            function_index: HashMap::new(),
            running_function: None,
        }
    }

    /// Load a library into the registry.
    ///
    /// If `replace` is true and the library already exists, it will be replaced.
    /// Returns the names of functions that were registered.
    pub fn load_library(
        &mut self,
        library: FunctionLibrary,
        replace: bool,
    ) -> Result<Vec<String>, FunctionError> {
        let lib_name = library.name.clone();

        // Check if library already exists
        if self.libraries.contains_key(&lib_name) && !replace {
            return Err(FunctionError::LibraryAlreadyExists { name: lib_name });
        }

        // Check for function name conflicts with other libraries
        // Function names are stored lowercase for case-insensitive lookup
        for func_name in library.functions.keys() {
            let func_name_lower = func_name.to_ascii_lowercase();
            if let Some(existing_lib) = self.function_index.get(&func_name_lower) {
                // If replacing the same library, allow overwriting its functions
                if existing_lib != &lib_name {
                    return Err(FunctionError::FunctionNameConflict {
                        function_name: func_name.clone(),
                        existing_library: existing_lib.clone(),
                    });
                }
            }
        }

        // If replacing, first remove old library's function index entries
        if replace && let Some(old_lib) = self.libraries.get(&lib_name) {
            for func_name in old_lib.functions.keys() {
                self.function_index.remove(&func_name.to_ascii_lowercase());
            }
        }

        // Build index for new functions (stored lowercase for case-insensitive lookup)
        let function_names: Vec<String> = library.functions.keys().cloned().collect();
        for func_name in &function_names {
            self.function_index
                .insert(func_name.to_ascii_lowercase(), lib_name.clone());
        }

        // Store the library
        self.libraries.insert(lib_name, library);

        Ok(function_names)
    }

    /// Delete a library by name.
    pub fn delete_library(&mut self, name: &str) -> Result<(), FunctionError> {
        let library =
            self.libraries
                .remove(name)
                .ok_or_else(|| FunctionError::LibraryNotFound {
                    name: name.to_string(),
                })?;

        // Remove function index entries (keys stored lowercase)
        for func_name in library.functions.keys() {
            self.function_index.remove(&func_name.to_ascii_lowercase());
        }

        Ok(())
    }

    /// Flush all libraries.
    pub fn flush(&mut self) {
        self.libraries.clear();
        self.function_index.clear();
    }

    /// Get a library by name.
    pub fn get_library(&self, name: &str) -> Option<&FunctionLibrary> {
        self.libraries.get(name)
    }

    /// Get a function by name (case-insensitive), returning both the function and its library name.
    pub fn get_function(&self, name: &str) -> Option<(&RegisteredFunction, &str)> {
        let lib_name = self.function_index.get(&name.to_ascii_lowercase())?;
        let library = self.libraries.get(lib_name)?;
        let function = library.get_function(name)?;
        Some((function, lib_name))
    }

    /// List all libraries, optionally filtered by a `LIBRARYNAME` pattern.
    ///
    /// The pattern is a full Redis glob evaluated by the canonical
    /// [`frogdb_types::glob_match`] — the same matcher SCAN/KEYS use — so it
    /// supports `[...]` classes and `\` escapes, and its iterative O(nm)
    /// algorithm cannot be driven into exponential backtracking by a hostile
    /// pattern.
    pub fn list_libraries(&self, pattern: Option<&str>) -> Vec<&FunctionLibrary> {
        if let Some(pattern) = pattern {
            self.libraries
                .values()
                .filter(|lib| frogdb_types::glob_match(pattern.as_bytes(), lib.name.as_bytes()))
                .collect()
        } else {
            self.libraries.values().collect()
        }
    }

    /// Get statistics about registered functions.
    pub fn stats(&self) -> FunctionStats {
        FunctionStats {
            library_count: self.libraries.len(),
            function_count: self.function_index.len(),
            running_function: self.running_function.clone(),
        }
    }

    /// Set the currently running function info.
    pub fn set_running_function(&mut self, info: Option<RunningFunctionInfo>) {
        self.running_function = info;
    }

    /// Get the currently running function info.
    pub fn get_running_function(&self) -> Option<&RunningFunctionInfo> {
        self.running_function.as_ref()
    }

    /// Get all library names.
    pub fn library_names(&self) -> Vec<&str> {
        self.libraries.keys().map(|s| s.as_str()).collect()
    }

    /// Get the total number of functions.
    pub fn function_count(&self) -> usize {
        self.function_index.len()
    }

    /// Get the total number of libraries.
    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }
}

/// Thread-safe wrapper around FunctionRegistry.
pub type SharedFunctionRegistry = Arc<RwLock<FunctionRegistry>>;

/// Create a new shared function registry.
pub fn new_shared_registry() -> SharedFunctionRegistry {
    Arc::new(RwLock::new(FunctionRegistry::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::FunctionFlags;

    fn create_test_library(name: &str, functions: &[&str]) -> FunctionLibrary {
        let mut lib = FunctionLibrary::new(name.to_string(), "-- code".to_string());
        for func_name in functions {
            lib.add_function(RegisteredFunction::new(
                func_name.to_string(),
                FunctionFlags::empty(),
                None,
            ));
        }
        lib
    }

    #[test]
    fn test_load_library() {
        let mut registry = FunctionRegistry::new();
        let lib = create_test_library("mylib", &["func1", "func2"]);

        let result = registry.load_library(lib, false);
        assert!(result.is_ok());

        let names = result.unwrap();
        assert_eq!(names.len(), 2);

        assert!(registry.get_library("mylib").is_some());
        assert!(registry.get_function("func1").is_some());
        assert!(registry.get_function("func2").is_some());
    }

    #[test]
    fn test_load_library_already_exists() {
        let mut registry = FunctionRegistry::new();
        let lib1 = create_test_library("mylib", &["func1"]);
        let lib2 = create_test_library("mylib", &["func2"]);

        registry.load_library(lib1, false).unwrap();

        let result = registry.load_library(lib2, false);
        assert!(matches!(
            result,
            Err(FunctionError::LibraryAlreadyExists { .. })
        ));
    }

    #[test]
    fn test_load_library_replace() {
        let mut registry = FunctionRegistry::new();
        let lib1 = create_test_library("mylib", &["func1"]);
        let lib2 = create_test_library("mylib", &["func2"]);

        registry.load_library(lib1, false).unwrap();
        let result = registry.load_library(lib2, true);
        assert!(result.is_ok());

        // Old function should be gone, new function should exist
        assert!(registry.get_function("func1").is_none());
        assert!(registry.get_function("func2").is_some());
    }

    #[test]
    fn test_function_name_conflict() {
        let mut registry = FunctionRegistry::new();
        let lib1 = create_test_library("lib1", &["shared_func"]);
        let lib2 = create_test_library("lib2", &["shared_func"]);

        registry.load_library(lib1, false).unwrap();

        let result = registry.load_library(lib2, false);
        assert!(matches!(
            result,
            Err(FunctionError::FunctionNameConflict { .. })
        ));
    }

    #[test]
    fn test_delete_library() {
        let mut registry = FunctionRegistry::new();
        let lib = create_test_library("mylib", &["func1"]);

        registry.load_library(lib, false).unwrap();
        assert!(registry.get_library("mylib").is_some());

        registry.delete_library("mylib").unwrap();
        assert!(registry.get_library("mylib").is_none());
        assert!(registry.get_function("func1").is_none());
    }

    #[test]
    fn test_delete_nonexistent_library() {
        let mut registry = FunctionRegistry::new();
        let result = registry.delete_library("nonexistent");
        assert!(matches!(result, Err(FunctionError::LibraryNotFound { .. })));
    }

    #[test]
    fn test_flush() {
        let mut registry = FunctionRegistry::new();
        registry
            .load_library(create_test_library("lib1", &["func1"]), false)
            .unwrap();
        registry
            .load_library(create_test_library("lib2", &["func2"]), false)
            .unwrap();

        registry.flush();
        assert_eq!(registry.library_count(), 0);
        assert_eq!(registry.function_count(), 0);
    }

    #[test]
    fn test_list_libraries_with_pattern() {
        let mut registry = FunctionRegistry::new();
        registry
            .load_library(create_test_library("mylib", &["func1"]), false)
            .unwrap();
        registry
            .load_library(create_test_library("yourlib", &["func2"]), false)
            .unwrap();
        registry
            .load_library(create_test_library("myother", &["func3"]), false)
            .unwrap();

        // Match all
        let all = registry.list_libraries(None);
        assert_eq!(all.len(), 3);

        // Match pattern
        let my_libs = registry.list_libraries(Some("my*"));
        assert_eq!(my_libs.len(), 2);

        let lib_suffix = registry.list_libraries(Some("*lib"));
        assert_eq!(lib_suffix.len(), 2);
    }

    /// `LIBRARYNAME <pattern>` matches iff the single registered library
    /// survives the filter.
    fn library_matches(name: &str, pattern: &str) -> bool {
        let mut registry = FunctionRegistry::new();
        registry
            .load_library(create_test_library(name, &["func1"]), false)
            .unwrap();
        !registry.list_libraries(Some(pattern)).is_empty()
    }

    #[test]
    fn test_pattern_matching() {
        assert!(library_matches("mylib", "mylib"));
        assert!(library_matches("mylib", "my*"));
        assert!(library_matches("mylib", "*lib"));
        assert!(library_matches("mylib", "*"));
        assert!(library_matches("mylib", "my???"));
        assert!(library_matches("mylib", "m?l?b"));
        assert!(!library_matches("mylib", "yourlib"));
        assert!(!library_matches("mylib", "my"));
        assert!(!library_matches("mylib", "mylibs"));
    }

    /// Regression: `LIBRARYNAME` used to run a bespoke matcher that only knew
    /// `*` and `?`. It now goes through the canonical Redis glob, so bracket
    /// classes work here exactly as they do for SCAN/KEYS.
    #[test]
    fn test_pattern_matching_bracket_classes() {
        assert!(library_matches("mylib1", "mylib[12]"));
        assert!(library_matches("mylib2", "mylib[12]"));
        assert!(!library_matches("mylib3", "mylib[12]"));

        assert!(library_matches("mylib3", "mylib[^12]"));
        assert!(!library_matches("mylib1", "mylib[^12]"));

        assert!(library_matches("mylibc", "mylib[a-z]"));
        assert!(!library_matches("mylib0", "mylib[a-z]"));
    }

    /// Regression: `\` is an escape in the canonical glob, so a pattern can
    /// name a library whose name contains a wildcard character literally.
    #[test]
    fn test_pattern_matching_escapes() {
        assert!(library_matches("my*lib", r"my\*lib"));
        assert!(!library_matches("mysomethinglib", r"my\*lib"));
        assert!(library_matches("my?lib", r"my\?lib"));
        assert!(library_matches("my[lib", r"my\[lib"));
    }

    /// Regression: the old recursive matcher backtracked exponentially, so a
    /// hostile `LIBRARYNAME` such as `a*a*a*...b` was a post-auth CPU bomb.
    /// The canonical glob is iterative and caps `*` groups, so this returns
    /// (with no match) instead of hanging.
    #[test]
    fn test_pattern_matching_pathological_is_bounded() {
        // Long enough that the `*` cap (100 groups) is actually reached.
        let name = "a".repeat(200);

        // Classic exponential-backtracking trigger: many `*`-separated `a`s
        // followed by a `b` that can never match.
        let evil = format!("{}b", "a*".repeat(50));
        assert!(!library_matches(&name, &evil));

        // Beyond MAX_STAR_COUNT the matcher gives up rather than grinding.
        let many_stars = "*?".repeat(500);
        assert!(!library_matches(&name, &many_stars));
    }

    #[test]
    fn test_stats() {
        let mut registry = FunctionRegistry::new();
        registry
            .load_library(create_test_library("lib1", &["func1", "func2"]), false)
            .unwrap();
        registry
            .load_library(create_test_library("lib2", &["func3"]), false)
            .unwrap();

        let stats = registry.stats();
        assert_eq!(stats.library_count, 2);
        assert_eq!(stats.function_count, 3);
        assert!(stats.running_function.is_none());
    }
}

//! Persistence support for functions - DUMP/RESTORE binary format.

use super::error::FunctionError;
use super::library::FunctionLibrary;
use super::registry::FunctionRegistry;
use std::io::{Read, Write};
use std::path::Path;

/// Current dump format version.
const DUMP_VERSION: u8 = 1;

/// Magic bytes for identifying FrogDB function dumps.
const MAGIC: &[u8; 4] = b"FDBF"; // FrogDB Functions

/// Restore policy for FUNCTION RESTORE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestorePolicy {
    /// Fail if any library already exists (default).
    Append,
    /// Replace existing libraries.
    Replace,
    /// Flush all existing functions before restore.
    Flush,
}

impl RestorePolicy {
    /// Parse a policy from a string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, FunctionError> {
        match s.to_uppercase().as_str() {
            "APPEND" => Ok(RestorePolicy::Append),
            "REPLACE" => Ok(RestorePolicy::Replace),
            "FLUSH" => Ok(RestorePolicy::Flush),
            _ => Err(FunctionError::InvalidRestorePolicy {
                policy: s.to_string(),
            }),
        }
    }
}

/// Serialize all libraries to a binary dump format.
///
/// Format:
/// ```text
/// [magic: 4 bytes "FDBF"]
/// [version: u8]
/// [num_libraries: u32 LE]
/// For each library:
///   [lib_name_len: u32 LE][lib_name: bytes]
///   [lib_code_len: u32 LE][lib_code: bytes]
/// [checksum: u64 LE] (xxhash of all preceding bytes)
/// ```
pub fn dump_libraries(registry: &FunctionRegistry) -> Vec<u8> {
    let mut buf = Vec::new();

    // Magic
    buf.extend_from_slice(MAGIC);

    // Version
    buf.push(DUMP_VERSION);

    // Number of libraries
    let libraries: Vec<&FunctionLibrary> = registry.list_libraries(None);
    buf.extend_from_slice(&(libraries.len() as u32).to_le_bytes());

    // Each library
    for lib in libraries {
        // Library name
        let name_bytes = lib.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        // Library code
        let code_bytes = lib.code.as_bytes();
        buf.extend_from_slice(&(code_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(code_bytes);
    }

    // Checksum (simple sum for now, can use xxhash in production)
    let checksum = compute_checksum(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    buf
}

/// Deserialize libraries from a binary dump.
///
/// Returns a list of (name, code) pairs that need to be loaded.
pub fn restore_libraries(data: &[u8]) -> Result<Vec<(String, String)>, FunctionError> {
    if data.len() < 13 {
        // 4 (magic) + 1 (version) + 4 (num_libs) + 8 (checksum) minimum when empty
        return Err(FunctionError::SerializationError {
            message: "DUMP payload version or checksum are wrong".to_string(),
        });
    }

    // Verify magic
    if &data[0..4] != MAGIC {
        return Err(FunctionError::SerializationError {
            message: "DUMP payload version or checksum are wrong".to_string(),
        });
    }

    // Check version
    let version = data[4];
    if version != DUMP_VERSION {
        return Err(FunctionError::SerializationError {
            message: "DUMP payload version or checksum are wrong".to_string(),
        });
    }

    // Verify checksum
    let checksum_offset = data.len() - 8;
    let stored_checksum = u64::from_le_bytes(data[checksum_offset..].try_into().map_err(|_| {
        FunctionError::SerializationError {
            message: "DUMP payload version or checksum are wrong".to_string(),
        }
    })?);
    let computed_checksum = compute_checksum(&data[..checksum_offset]);
    if stored_checksum != computed_checksum {
        return Err(FunctionError::SerializationError {
            message: "DUMP payload version or checksum are wrong".to_string(),
        });
    }

    // Parse libraries
    let mut offset = 5; // After magic and version
    let num_libraries = read_u32_le(data, &mut offset)?;

    let mut libraries = Vec::with_capacity(num_libraries as usize);

    for _ in 0..num_libraries {
        // Library name
        let name_len = read_u32_le(data, &mut offset)? as usize;
        if offset + name_len > checksum_offset {
            return Err(FunctionError::SerializationError {
                message: "Truncated dump (name)".to_string(),
            });
        }
        let name = String::from_utf8(data[offset..offset + name_len].to_vec()).map_err(|_| {
            FunctionError::SerializationError {
                message: "Invalid UTF-8 in library name".to_string(),
            }
        })?;
        offset += name_len;

        // Library code
        let code_len = read_u32_le(data, &mut offset)? as usize;
        if offset + code_len > checksum_offset {
            return Err(FunctionError::SerializationError {
                message: "Truncated dump (code)".to_string(),
            });
        }
        let code = String::from_utf8(data[offset..offset + code_len].to_vec()).map_err(|_| {
            FunctionError::SerializationError {
                message: "Invalid UTF-8 in library code".to_string(),
            }
        })?;
        offset += code_len;

        libraries.push((name, code));
    }

    Ok(libraries)
}

/// Save all libraries to a file.
///
/// Uses the same binary format as DUMP. The file is written atomically
/// by first writing to a temporary file and then renaming.
pub fn save_to_file(registry: &FunctionRegistry, path: &Path) -> Result<(), FunctionError> {
    let dump = dump_libraries(registry);

    // Write to a temp file first for atomic operation
    let temp_path = path.with_extension("fdb.tmp");

    let mut file = std::fs::File::create(&temp_path).map_err(|e| FunctionError::Internal {
        message: format!("Failed to create temp file: {}", e),
    })?;

    file.write_all(&dump).map_err(|e| FunctionError::Internal {
        message: format!("Failed to write functions: {}", e),
    })?;

    file.sync_all().map_err(|e| FunctionError::Internal {
        message: format!("Failed to sync functions file: {}", e),
    })?;

    drop(file);

    // Atomic rename
    std::fs::rename(&temp_path, path).map_err(|e| FunctionError::Internal {
        message: format!("Failed to rename functions file: {}", e),
    })?;

    Ok(())
}

/// Load libraries from a file.
///
/// Returns a list of (name, code) pairs that need to be loaded into the registry.
/// Returns Ok(vec![]) if the file doesn't exist (fresh start).
pub fn load_from_file(path: &Path) -> Result<Vec<(String, String)>, FunctionError> {
    if !path.exists() {
        return Ok(vec![]);
    }

    let mut file = std::fs::File::open(path).map_err(|e| FunctionError::Internal {
        message: format!("Failed to open functions file: {}", e),
    })?;

    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| FunctionError::Internal {
            message: format!("Failed to read functions file: {}", e),
        })?;

    if data.is_empty() {
        return Ok(vec![]);
    }

    restore_libraries(&data)
}

/// Read a u32 in little-endian from buffer, advancing offset.
fn read_u32_le(data: &[u8], offset: &mut usize) -> Result<u32, FunctionError> {
    if *offset + 4 > data.len() {
        return Err(FunctionError::SerializationError {
            message: "Truncated dump".to_string(),
        });
    }
    let bytes: [u8; 4] =
        data[*offset..*offset + 4]
            .try_into()
            .map_err(|_| FunctionError::SerializationError {
                message: "Failed to read u32".to_string(),
            })?;
    *offset += 4;
    Ok(u32::from_le_bytes(bytes))
}

/// Compute a simple checksum (can be replaced with xxhash).
fn compute_checksum(data: &[u8]) -> u64 {
    // Simple FNV-1a hash for now
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::{FunctionFlags, RegisteredFunction};

    fn create_test_registry() -> FunctionRegistry {
        let mut registry = FunctionRegistry::new();

        let mut lib1 =
            FunctionLibrary::new("lib1".to_string(), "#!lua name=lib1\n-- code1".to_string());
        lib1.add_function(RegisteredFunction::new(
            "func1".to_string(),
            FunctionFlags::empty(),
            None,
        ));

        let mut lib2 =
            FunctionLibrary::new("lib2".to_string(), "#!lua name=lib2\n-- code2".to_string());
        lib2.add_function(RegisteredFunction::new(
            "func2".to_string(),
            FunctionFlags::NO_WRITES,
            None,
        ));

        registry.load_library(lib1, false).unwrap();
        registry.load_library(lib2, false).unwrap();

        registry
    }

    #[test]
    fn test_dump_restore_roundtrip() {
        let registry = create_test_registry();

        let dump = dump_libraries(&registry);
        let restored = restore_libraries(&dump).unwrap();

        assert_eq!(restored.len(), 2);

        // Find lib1 and lib2 in the restored libraries
        let lib1 = restored.iter().find(|(name, _)| name == "lib1");
        let lib2 = restored.iter().find(|(name, _)| name == "lib2");

        assert!(lib1.is_some());
        assert!(lib2.is_some());

        assert!(lib1.unwrap().1.contains("-- code1"));
        assert!(lib2.unwrap().1.contains("-- code2"));
    }

    #[test]
    fn test_dump_empty_registry() {
        let registry = FunctionRegistry::new();
        let dump = dump_libraries(&registry);
        let restored = restore_libraries(&dump).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_restore_invalid_magic() {
        let data = b"BADMxxxxxxxx";
        let result = restore_libraries(data);
        assert!(matches!(
            result,
            Err(FunctionError::SerializationError { .. })
        ));
    }

    #[test]
    fn test_restore_truncated() {
        let data = b"FDBF";
        let result = restore_libraries(data);
        assert!(matches!(
            result,
            Err(FunctionError::SerializationError { .. })
        ));
    }

    #[test]
    fn test_restore_bad_checksum() {
        let registry = create_test_registry();
        let mut dump = dump_libraries(&registry);

        // Corrupt the checksum
        let len = dump.len();
        dump[len - 1] ^= 0xFF;

        let result = restore_libraries(&dump);
        assert!(matches!(
            result,
            Err(FunctionError::SerializationError { .. })
        ));
    }

    #[test]
    fn test_restore_policy_parsing() {
        assert_eq!(
            RestorePolicy::from_str("APPEND").unwrap(),
            RestorePolicy::Append
        );
        assert_eq!(
            RestorePolicy::from_str("append").unwrap(),
            RestorePolicy::Append
        );
        assert_eq!(
            RestorePolicy::from_str("REPLACE").unwrap(),
            RestorePolicy::Replace
        );
        assert_eq!(
            RestorePolicy::from_str("FLUSH").unwrap(),
            RestorePolicy::Flush
        );
        assert!(RestorePolicy::from_str("invalid").is_err());
    }

    #[test]
    fn test_save_and_load_file() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_functions.fdb");

        // Clean up any existing file
        let _ = std::fs::remove_file(&path);

        let registry = create_test_registry();

        // Save to file
        save_to_file(&registry, &path).unwrap();

        // Verify file exists
        assert!(path.exists());

        // Load from file
        let loaded = load_from_file(&path).unwrap();

        assert_eq!(loaded.len(), 2);

        // Find lib1 and lib2 in loaded libraries
        let lib1 = loaded.iter().find(|(name, _)| name == "lib1");
        let lib2 = loaded.iter().find(|(name, _)| name == "lib2");

        assert!(lib1.is_some());
        assert!(lib2.is_some());

        assert!(lib1.unwrap().1.contains("-- code1"));
        assert!(lib2.unwrap().1.contains("-- code2"));

        // Clean up
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_missing_file() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("nonexistent_functions.fdb");

        // Ensure file doesn't exist
        let _ = std::fs::remove_file(&path);

        // Should return empty vec, not error
        let loaded = load_from_file(&path).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_save_empty_registry() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_empty_functions.fdb");

        // Clean up any existing file
        let _ = std::fs::remove_file(&path);

        let registry = FunctionRegistry::new();

        // Save empty registry to file
        save_to_file(&registry, &path).unwrap();

        // Load from file
        let loaded = load_from_file(&path).unwrap();
        assert!(loaded.is_empty());

        // Clean up
        let _ = std::fs::remove_file(&path);
    }
}

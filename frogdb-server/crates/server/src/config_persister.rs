//! `ConfigPersister`: pure TOML doc-merge + atomic file write for CONFIG REWRITE.
//!
//! `ConfigManager::rewrite_config` (in `runtime_config.rs`) used to mix five
//! responsibilities in one function: read the file, parse it, walk the param
//! registry converting runtime strings to TOML types (re-guessing each type
//! from its formatted string), and atomically write the result. This module
//! owns exactly two of those responsibilities, both as small pure functions:
//!
//! - [`ConfigPersister::merge`] — parse existing TOML text, overlay already-typed
//!   values, and render the merged document back to text. String in, string
//!   out, so it is unit-testable without a filesystem.
//! - [`ConfigPersister::atomic_write`] — write text to a path safely (temp file +
//!   fsync + rename).
//!
//! Deciding *which* values to write and *what TOML type* each one has stays
//! with `ConfigManager`: it owns the parameter registry and, via each
//! parameter's own type, knows how to render a genuinely-typed
//! [`toml_edit::Value`] (see `ToTomlValue`/`TomlRenderable` in
//! `runtime_config.rs`) instead of guessing from a string.

use std::io::Write;
use std::path::Path;

use toml_edit::{DocumentMut, Item, Table, Value};

/// One already-typed update to merge into a TOML document: write `value`
/// into `document[section][field]`, creating `section` if it doesn't exist.
///
/// The value must already be the correct TOML type for the field (bool, int,
/// string, ...); `ConfigPersister` does no type inference of its own.
pub struct ConfigUpdate {
    /// TOML table name, e.g. `"memory"`.
    pub section: &'static str,
    /// Field name within the section, e.g. `"maxmemory"`.
    pub field: &'static str,
    /// The correctly-typed value to write.
    pub value: Value,
}

/// Stateless TOML config-file persister.
///
/// Every method is a pure function over its arguments (or does only the IO
/// its name promises), so the merge logic is testable without a tempdir and
/// the IO is isolated to a single, small, auditable function.
pub struct ConfigPersister;

impl ConfigPersister {
    /// Parse `doc_text`, apply `updates` in order (creating missing sections
    /// as needed), and return the re-rendered document text.
    ///
    /// Preserves comments, formatting, and key ordering for everything not
    /// touched by `updates` (this is exactly what `toml_edit::DocumentMut`
    /// buys us over the plain `toml` crate).
    pub fn merge(
        doc_text: &str,
        updates: impl IntoIterator<Item = ConfigUpdate>,
    ) -> Result<String, String> {
        let mut doc: DocumentMut = doc_text
            .parse()
            .map_err(|e: toml_edit::TomlError| e.to_string())?;

        for update in updates {
            if !doc.contains_table(update.section) {
                doc[update.section] = Item::Table(Table::new());
            }
            doc[update.section][update.field] = Item::Value(update.value);
        }

        Ok(doc.to_string())
    }

    /// Atomically write `contents` to `path`.
    ///
    /// Writes to a pid-suffixed temp file next to `path`, fsyncs it, then
    /// renames it over `path` -- so a crash or concurrent reader never
    /// observes a partially-written config file.
    pub fn atomic_write(path: &Path, contents: &str) -> Result<(), String> {
        let pid = std::process::id();
        let tmp_path = path.with_extension(format!("tmp.{}", pid));

        let mut file = std::fs::File::create(&tmp_path).map_err(|e| {
            format!(
                "ERR failed to create temp file '{}': {}",
                tmp_path.display(),
                e
            )
        })?;

        file.write_all(contents.as_bytes()).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to write temp file: {}", e)
        })?;

        file.sync_all().map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to fsync temp file: {}", e)
        })?;

        drop(file);

        std::fs::rename(&tmp_path, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            format!("ERR failed to rename temp file to config: {}", e)
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(section: &'static str, field: &'static str, value: impl Into<Value>) -> ConfigUpdate {
        ConfigUpdate {
            section,
            field,
            value: value.into(),
        }
    }

    #[test]
    fn merge_updates_existing_scalar_value() {
        let doc = "[memory]\nmaxmemory = 0\n";
        let merged =
            ConfigPersister::merge(doc, vec![update("memory", "maxmemory", 1048576i64)]).unwrap();
        assert!(merged.contains("maxmemory = 1048576"), "got: {merged}");
    }

    #[test]
    fn merge_creates_missing_section() {
        let doc = "[server]\nbind = \"127.0.0.1\"\n";
        let merged =
            ConfigPersister::merge(doc, vec![update("memory", "maxmemory", 0i64)]).unwrap();
        assert!(merged.contains("[memory]"), "got: {merged}");
        assert!(merged.contains("maxmemory = 0"), "got: {merged}");
    }

    #[test]
    fn merge_preserves_comments_and_formatting() {
        let doc = r#"# top comment
[logging]
# inline section comment
level = "info"  # trailing comment
"#;
        let merged =
            ConfigPersister::merge(doc, vec![update("logging", "level", "debug")]).unwrap();
        assert!(merged.contains("# top comment"), "got: {merged}");
        assert!(merged.contains("# inline section comment"), "got: {merged}");
        assert!(merged.contains("\"debug\""), "got: {merged}");
    }

    #[test]
    fn merge_writes_each_value_with_its_own_toml_type() {
        // The whole point of pushing typed `Value`s into `merge` (rather than
        // strings) is that it never re-guesses: a bool stays a bool, an int
        // stays an int, and a numeric-looking string stays a string.
        let doc = "[section]\n";
        let merged = ConfigPersister::merge(
            doc,
            vec![
                update("section", "a_bool", true),
                update("section", "an_int", 42i64),
                update("section", "a_numeric_string", "42"),
            ],
        )
        .unwrap();

        let reparsed: DocumentMut = merged.parse().unwrap();
        assert_eq!(reparsed["section"]["a_bool"].as_bool(), Some(true));
        assert_eq!(reparsed["section"]["an_int"].as_integer(), Some(42));
        assert_eq!(reparsed["section"]["a_numeric_string"].as_str(), Some("42"));
    }

    #[test]
    fn merge_rejects_invalid_toml() {
        let result = ConfigPersister::merge("not [ valid toml", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn merge_with_no_updates_is_a_no_op_besides_reformatting() {
        let doc = "[memory]\nmaxmemory = 0\n";
        let merged = ConfigPersister::merge(doc, vec![]).unwrap();
        assert!(merged.contains("maxmemory = 0"));
    }

    #[test]
    fn atomic_write_writes_contents_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frogdb.toml");
        std::fs::write(&path, "[server]\nbind = \"127.0.0.1\"\n").unwrap();

        ConfigPersister::atomic_write(&path, "[server]\nbind = \"0.0.0.0\"\n").unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "[server]\nbind = \"0.0.0.0\"\n");

        // No leftover `*.tmp.<pid>` file next to it.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[test]
    fn atomic_write_creates_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.toml");
        assert!(!path.exists());

        ConfigPersister::atomic_write(&path, "[a]\nb = 1\n").unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "[a]\nb = 1\n");
    }
}

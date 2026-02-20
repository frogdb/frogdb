use shuttle::sync::Mutex;
use std::collections::HashMap;

/// Simulates a store with version tracking for WATCH functionality.
pub struct WatchableStore {
    data: Mutex<HashMap<String, (String, u64)>>, // (value, version)
}

impl WatchableStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_version(&self, key: &str) -> Option<u64> {
        self.data.lock().unwrap().get(key).map(|(_, v)| *v)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.lock().unwrap().get(key).map(|(v, _)| v.clone())
    }

    pub fn set(&self, key: &str, value: &str) {
        let mut data = self.data.lock().unwrap();
        let version = data.get(key).map(|(_, v)| v + 1).unwrap_or(1);
        data.insert(key.to_string(), (value.to_string(), version));
    }

    /// Check if version is unchanged (EXEC check)
    #[allow(dead_code)]
    pub fn exec_if_unchanged(&self, key: &str, watched_version: Option<u64>) -> bool {
        let data = self.data.lock().unwrap();
        let current_version = data.get(key).map(|(_, v)| *v);
        watched_version == current_version
    }

    /// Execute transaction only if watched keys unchanged
    pub fn exec_with_watch<F, R>(&self, watched_keys: &[(&str, Option<u64>)], f: F) -> Option<R>
    where
        F: FnOnce(&mut HashMap<String, (String, u64)>) -> R,
    {
        let mut data = self.data.lock().unwrap();

        // Check all watched keys
        for (key, watched_version) in watched_keys {
            let current_version = data.get(*key).map(|(_, v)| *v);
            if *watched_version != current_version {
                return None; // Abort - watched key changed
            }
        }

        // Execute transaction
        Some(f(&mut data))
    }
}

/// Typed value enum for testing type conflicts.
#[derive(Clone, Debug)]
pub enum TypedValue {
    String(String),
    List(Vec<String>),
}

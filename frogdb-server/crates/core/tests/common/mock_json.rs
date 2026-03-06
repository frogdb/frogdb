use shuttle::sync::Mutex;

/// Simulated JSON value with path-based access.
/// Used to test concurrent JSON operations.
pub struct MockJsonValue {
    data: Mutex<serde_json::Value>,
}

impl MockJsonValue {
    pub fn new(data: serde_json::Value) -> Self {
        Self {
            data: Mutex::new(data),
        }
    }

    pub fn get(&self, path: &str) -> Option<serde_json::Value> {
        let data = self.data.lock().unwrap();
        // Simplified path resolution for testing (only supports $.field)
        if path == "$" {
            return Some(data.clone());
        }
        if let Some(field) = path.strip_prefix("$.") {
            return data.get(field).cloned();
        }
        None
    }

    pub fn set(&self, path: &str, value: serde_json::Value) -> bool {
        let mut data = self.data.lock().unwrap();
        if path == "$" {
            *data = value;
            return true;
        }
        if let Some(field) = path.strip_prefix("$.")
            && let serde_json::Value::Object(ref mut obj) = *data
        {
            obj.insert(field.to_string(), value);
            return true;
        }
        false
    }

    pub fn incr_by(&self, path: &str, delta: i64) -> Option<i64> {
        let mut data = self.data.lock().unwrap();
        if let Some(field) = path.strip_prefix("$.")
            && let serde_json::Value::Object(ref mut obj) = *data
            && let Some(serde_json::Value::Number(n)) = obj.get(field)
            && let Some(current) = n.as_i64()
        {
            let new_val = current + delta;
            obj.insert(field.to_string(), serde_json::json!(new_val));
            return Some(new_val);
        }
        None
    }

    pub fn arr_append(&self, path: &str, value: serde_json::Value) -> Option<usize> {
        let mut data = self.data.lock().unwrap();
        if let Some(field) = path.strip_prefix("$.")
            && let serde_json::Value::Object(ref mut obj) = *data
            && let Some(serde_json::Value::Array(arr)) = obj.get_mut(field)
        {
            arr.push(value);
            return Some(arr.len());
        }
        None
    }

    pub fn arr_pop(&self, path: &str) -> Option<serde_json::Value> {
        let mut data = self.data.lock().unwrap();
        if let Some(field) = path.strip_prefix("$.")
            && let serde_json::Value::Object(ref mut obj) = *data
            && let Some(serde_json::Value::Array(arr)) = obj.get_mut(field)
        {
            return arr.pop();
        }
        None
    }
}

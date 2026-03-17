//! Cursor store for FT.AGGREGATE WITHCURSOR / FT.CURSOR READ/DEL.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Row type matching the aggregate pipeline output.
pub type Row = Vec<(String, String)>;

struct CursorState {
    rows: Vec<Row>,
    count: usize,
    last_access: Instant,
    timeout: Duration,
    index_name: String,
}

/// Shared cursor store for aggregate result pagination.
///
/// Uses DashMap (sharded locks) for concurrent access across connections.
/// This is coordinator-level shared state, matching existing patterns like
/// ClientRegistry, ConfigManager, and MonitorBroadcaster.
pub struct AggregateCursorStore {
    cursors: DashMap<u64, CursorState>,
    next_id: AtomicU64,
}

impl Default for AggregateCursorStore {
    fn default() -> Self {
        Self {
            cursors: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }
}

impl AggregateCursorStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a cursor holding the remaining rows. Returns the cursor ID.
    pub fn create_cursor(
        &self,
        rows: Vec<Row>,
        count: usize,
        index_name: String,
        timeout: Duration,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.cursors.insert(
            id,
            CursorState {
                rows,
                count,
                last_access: Instant::now(),
                timeout,
                index_name,
            },
        );
        id
    }

    /// Read the next batch from a cursor.
    ///
    /// Returns `Some((batch, new_cursor_id))` where `new_cursor_id` is 0 if done.
    /// Returns `None` if the cursor doesn't exist.
    pub fn read_cursor(
        &self,
        id: u64,
        count: Option<usize>,
        expected_index: &str,
    ) -> Option<(Vec<Row>, u64)> {
        let mut entry = self.cursors.get_mut(&id)?;
        let state = entry.value_mut();

        // Validate index name matches
        if state.index_name != expected_index {
            return None;
        }

        state.last_access = Instant::now();
        let batch_size = count.unwrap_or(state.count);

        if batch_size == 0 || batch_size >= state.rows.len() {
            // Return all remaining rows and close cursor
            let rows = std::mem::take(&mut state.rows);
            drop(entry);
            self.cursors.remove(&id);
            Some((rows, 0))
        } else {
            // Return a batch and keep the rest
            let remaining = state.rows.split_off(batch_size);
            let batch = std::mem::replace(&mut state.rows, remaining);
            Some((batch, id))
        }
    }

    /// Delete a cursor. Returns true if it existed.
    pub fn delete_cursor(&self, id: u64) -> bool {
        self.cursors.remove(&id).is_some()
    }

    /// Evict expired cursors. Called periodically from a background task.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        self.cursors
            .retain(|_, state| now.duration_since(state.last_access) < state.timeout);
    }
}

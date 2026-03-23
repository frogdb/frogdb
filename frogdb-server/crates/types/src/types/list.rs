use bytes::Bytes;
use std::collections::VecDeque;

// ============================================================================
// List Type
// ============================================================================

/// List value - a doubly-linked list of values.
#[derive(Debug, Clone)]
pub struct ListValue {
    data: VecDeque<Bytes>,
}

impl Default for ListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl ListValue {
    /// Create a new empty list.
    pub fn new() -> Self {
        Self {
            data: VecDeque::new(),
        }
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Push an element to the front (left).
    pub fn push_front(&mut self, value: Bytes) {
        self.data.push_front(value);
    }

    /// Push an element to the back (right).
    pub fn push_back(&mut self, value: Bytes) {
        self.data.push_back(value);
    }

    /// Pop an element from the front (left).
    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.data.pop_front()
    }

    /// Pop an element from the back (right).
    pub fn pop_back(&mut self) -> Option<Bytes> {
        self.data.pop_back()
    }

    /// Normalize a Redis index (supports negative indices).
    fn normalize_index(&self, index: i64) -> Option<usize> {
        let len = self.len() as i64;
        if len == 0 {
            return None;
        }
        let normalized = if index < 0 { len + index } else { index };
        if normalized < 0 || normalized >= len {
            None
        } else {
            Some(normalized as usize)
        }
    }

    /// Get an element by index (supports negative indices).
    pub fn get(&self, index: i64) -> Option<&Bytes> {
        self.normalize_index(index).and_then(|i| self.data.get(i))
    }

    /// Set an element by index (supports negative indices).
    ///
    /// Returns true if the index was valid and the element was set.
    pub fn set(&mut self, index: i64, value: Bytes) -> bool {
        if let Some(i) = self.normalize_index(index)
            && let Some(elem) = self.data.get_mut(i)
        {
            *elem = value;
            return true;
        }
        false
    }

    /// Resolve start/end into (skip, take) counts. Returns (0, 0) for empty ranges.
    fn resolve_range(&self, start: i64, end: i64) -> (usize, usize) {
        let len = self.len() as i64;
        if len == 0 {
            return (0, 0);
        }

        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return (0, 0);
        }

        (start, end as usize - start + 1)
    }

    /// Get a range of elements (inclusive, supports negative indices).
    pub fn range(&self, start: i64, end: i64) -> Vec<Bytes> {
        let (skip, take) = self.resolve_range(start, end);
        self.data.iter().skip(skip).take(take).cloned().collect()
    }

    /// Iterate over a range of elements without intermediate allocation.
    pub fn range_iter(&self, start: i64, end: i64) -> impl Iterator<Item = &Bytes> {
        let (skip, take) = self.resolve_range(start, end);
        self.data.iter().skip(skip).take(take)
    }

    /// Trim the list to only contain elements in the specified range.
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.len() as i64;
        if len == 0 {
            return;
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            // Empty range - clear the list
            self.data.clear();
            return;
        }

        let end = end as usize;

        // Keep only elements in range [start, end]
        let new_data: VecDeque<_> = self
            .data
            .iter()
            .skip(start)
            .take(end - start + 1)
            .cloned()
            .collect();
        self.data = new_data;
    }

    /// Find the position of an element.
    ///
    /// Returns the first position where element is found, or None.
    /// `rank`: how many matches to skip (0 = first, 1 = second, etc.)
    /// `count`: maximum number of positions to return
    /// `maxlen`: maximum number of elements to scan
    pub fn position(
        &self,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: Option<usize>,
    ) -> Vec<usize> {
        let maxlen = maxlen.unwrap_or(self.len());

        if rank >= 0 {
            // Forward scan
            let rank = rank as usize;
            let mut matches = 0;
            let mut positions = Vec::new();

            for (i, item) in self.data.iter().enumerate().take(maxlen) {
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        } else {
            // Backward scan
            let rank = (-rank - 1) as usize;
            let mut matches = 0;
            let mut positions = Vec::new();
            let scan_start = if maxlen < self.len() {
                self.len() - maxlen
            } else {
                0
            };

            for (i, item) in self.data.iter().enumerate().rev() {
                if i < scan_start {
                    break;
                }
                if item.as_ref() == element {
                    if matches >= rank {
                        positions.push(i);
                        if positions.len() >= count {
                            break;
                        }
                    }
                    matches += 1;
                }
            }
            positions
        }
    }

    /// Insert an element before or after a pivot element.
    ///
    /// Returns the new length of the list, -1 if pivot not found, 0 if list is empty.
    pub fn insert(&mut self, before: bool, pivot: &[u8], element: Bytes) -> i64 {
        if self.is_empty() {
            return 0;
        }

        // Find pivot position
        let pos = self.data.iter().position(|e| e.as_ref() == pivot);

        match pos {
            Some(i) => {
                let insert_pos = if before { i } else { i + 1 };
                self.data.insert(insert_pos, element);
                self.len() as i64
            }
            None => -1,
        }
    }

    /// Remove elements equal to value.
    ///
    /// `count` determines direction and number:
    /// - count > 0: Remove first count occurrences (head to tail)
    /// - count < 0: Remove first |count| occurrences (tail to head)
    /// - count = 0: Remove all occurrences
    ///
    /// Returns the number of elements removed.
    pub fn remove(&mut self, count: i64, element: &[u8]) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut removed = 0;

        if count == 0 {
            // Remove all
            let original_len = self.len();
            self.data.retain(|e| e.as_ref() != element);
            removed = original_len - self.len();
        } else if count > 0 {
            // Remove from head
            let max_remove = count as usize;
            let mut new_data = VecDeque::with_capacity(self.len());
            for item in self.data.drain(..) {
                if removed < max_remove && item.as_ref() == element {
                    removed += 1;
                } else {
                    new_data.push_back(item);
                }
            }
            self.data = new_data;
        } else {
            // Remove from tail
            let max_remove = (-count) as usize;
            let mut indices_to_remove = Vec::new();
            for (i, item) in self.data.iter().enumerate().rev() {
                if item.as_ref() == element {
                    indices_to_remove.push(i);
                    if indices_to_remove.len() >= max_remove {
                        break;
                    }
                }
            }
            // Sort indices in descending order to remove from end first
            indices_to_remove.sort_by(|a, b| b.cmp(a));
            for i in indices_to_remove {
                self.data.remove(i);
                removed += 1;
            }
        }

        removed
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .data
            .iter()
            .map(|e| e.len() + 8) // 8 for VecDeque node overhead
            .sum();
        base_size + entries_size
    }

    /// Get all elements as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.data.iter().cloned().collect()
    }

    /// Iterate over all elements.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.data.iter()
    }
}

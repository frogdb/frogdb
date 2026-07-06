//! TimeSeries label index, extracted from [`HashMapStore`].
//!
//! RedisTimeSeries lets clients look up series by their labels
//! (`TS.QUERYINDEX` / `TS.MGET` / `TS.MRANGE`). FrogDB maintains a per-shard
//! [`LabelIndex`] mapping labels to keys for those lookups. This wrapper owns
//! the index and the small amount of reconciliation logic that keeps it in step
//! with the keyspace, so the store body no longer has to pattern-match on
//! [`Value::TimeSeries`] at every write and delete site.

use bytes::Bytes;

use crate::LabelIndex;
use crate::types::Value;

/// Owns the timeseries label index plus its keyspace-reconciliation logic.
#[derive(Debug, Default)]
pub(super) struct TimeSeriesLabels {
    index: LabelIndex,
}

impl TimeSeriesLabels {
    /// Create an empty label index.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Reconcile a key's labels against a value that is being (over)written:
    /// index a `TimeSeries`'s labels, or drop any stale labels for a value that
    /// is no longer a time series (e.g. a key overwritten with a string).
    pub(super) fn reconcile(&mut self, key: &Bytes, value: &Value) {
        if let Value::TimeSeries(ts) = value {
            self.index.add(key.clone(), ts.labels());
        } else {
            self.index.remove(key);
        }
    }

    /// Drop a key's labels (deletion / expiry).
    pub(super) fn remove(&mut self, key: &[u8]) {
        self.index.remove(key);
    }

    /// Forget every label (FLUSHDB / `clear`).
    pub(super) fn clear(&mut self) {
        self.index = LabelIndex::new();
    }

    /// Read-only access to the underlying index (TS.QUERYINDEX / MGET / MRANGE).
    pub(super) fn index(&self) -> &LabelIndex {
        &self.index
    }

    /// Mutable access to the underlying index (TS.ALTER label updates).
    pub(super) fn index_mut(&mut self) -> &mut LabelIndex {
        &mut self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TimeSeriesValue;

    fn timeseries_with_labels(labels: &[(&str, &str)]) -> Value {
        let mut ts = TimeSeriesValue::new();
        ts.set_labels(
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        Value::TimeSeries(ts)
    }

    #[test]
    fn reconcile_indexes_timeseries_labels() {
        let mut labels = TimeSeriesLabels::new();
        let key = Bytes::from("ts:1");
        labels.reconcile(
            &key,
            &timeseries_with_labels(&[("region", "us"), ("env", "prod")]),
        );

        assert_eq!(labels.index().get_labels(b"ts:1").map(Vec::len), Some(2));
        assert_eq!(labels.index().label_values("region"), vec!["us"]);
    }

    #[test]
    fn reconcile_drops_labels_when_value_is_no_longer_a_timeseries() {
        let mut labels = TimeSeriesLabels::new();
        let key = Bytes::from("ts:1");
        labels.reconcile(&key, &timeseries_with_labels(&[("region", "us")]));
        assert!(labels.index().get_labels(b"ts:1").is_some());

        // Overwrite the same key with a non-timeseries value.
        labels.reconcile(&key, &Value::string("plain"));
        assert!(labels.index().get_labels(b"ts:1").is_none());
        assert!(labels.index().is_empty());
    }

    #[test]
    fn remove_and_clear_forget_labels() {
        let mut labels = TimeSeriesLabels::new();
        labels.reconcile(&Bytes::from("a"), &timeseries_with_labels(&[("l", "1")]));
        labels.reconcile(&Bytes::from("b"), &timeseries_with_labels(&[("l", "2")]));

        labels.remove(b"a");
        assert!(labels.index().get_labels(b"a").is_none());
        assert!(labels.index().get_labels(b"b").is_some());

        labels.clear();
        assert!(labels.index().is_empty());
    }
}

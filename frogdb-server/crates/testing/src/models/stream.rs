//! Stream sequential model (XADD/XLEN/XREAD; XREADGROUP deferred).

use super::{Model, expect_int};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;

/// A stream entry id (`ms-seq`). Ordering is (ms, seq) lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    /// Milliseconds component.
    pub ms: u64,
    /// Sequence component.
    pub seq: u64,
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parse an explicit `ms` or `ms-seq` id. `*` and unparsable ids return None.
pub(crate) fn parse_id(bytes: &[u8]) -> Option<StreamId> {
    let s = String::from_utf8_lossy(bytes);
    if s == "*" {
        return None;
    }
    let mut it = s.splitn(2, '-');
    let ms = it.next()?.parse::<u64>().ok()?;
    let seq = match it.next() {
        Some(x) => x.parse::<u64>().ok()?,
        None => 0,
    };
    Some(StreamId { ms, seq })
}

/// Per-key stream contents.
#[derive(Debug, Clone, Default)]
pub struct StreamData {
    /// Entries in id order.
    pub entries: Vec<(StreamId, Vec<Bytes>)>,
    /// Highest id assigned so far.
    pub last_id: StreamId,
}

/// Sequential model for Redis streams.
#[derive(Debug, Clone, Default)]
pub struct StreamModel;

/// State for the stream model.
#[derive(Debug, Clone, Default)]
pub struct StreamState {
    /// key -> stream data.
    pub streams: HashMap<Bytes, StreamData>,
}

impl Model for StreamModel {
    type State = StreamState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "xadd" => {
                // args: key id field value [field value...]
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let id_arg = &args[1];
                let fields: Vec<Bytes> = args[2..].to_vec();
                // XADD always returns the assigned id; it is authoritative.
                let assigned = parse_id(result?.as_ref())?;
                let last = state
                    .streams
                    .get(key)
                    .map(|s| s.last_id)
                    .unwrap_or_default();
                if assigned <= last {
                    return None;
                }
                if id_arg.as_ref() != b"*" {
                    let requested = parse_id(id_arg)?;
                    if requested != assigned {
                        return None;
                    }
                }
                let mut new = state.clone();
                let sd = new.streams.entry(key.clone()).or_default();
                sd.entries.push((assigned, fields));
                sd.last_id = assigned;
                Some(new)
            }
            "xlen" => {
                if args.is_empty() {
                    return None;
                }
                let len = state.streams.get(&args[0]).map_or(0, |s| s.entries.len()) as i64;
                if expect_int(result, len) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xread" => {
                // args: key after_id  (non-group; blocking hit or timeout)
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let after = parse_id(&args[1])?;
                let entries: Vec<String> = state
                    .streams
                    .get(key)
                    .map(|s| {
                        s.entries
                            .iter()
                            .filter(|(id, _)| *id > after)
                            .map(|(id, fields)| {
                                let mut parts = vec![id.to_string()];
                                parts.extend(
                                    fields
                                        .iter()
                                        .map(|f| String::from_utf8_lossy(f).to_string()),
                                );
                                parts.join(",")
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                match result {
                    None if entries.is_empty() => Some(state.clone()),
                    Some(r)
                        if !entries.is_empty()
                            && String::from_utf8_lossy(r) == entries.join("|") =>
                    {
                        Some(state.clone())
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn stream_explicit_id_happy_path() {
        let s = StreamState::default();
        // XADD st 1-1 f v -> "1-1"
        let s = StreamModel::step(
            &s,
            "xadd",
            &[b("st"), b("1-1"), b("f"), b("v")],
            Some(&b("1-1")),
        )
        .unwrap();
        assert!(StreamModel::step(&s, "xlen", &[b("st")], Some(&b("1"))).is_some());
        // XREAD st 0 -> "1-1,f,v"
        assert!(StreamModel::step(&s, "xread", &[b("st"), b("0")], Some(&b("1-1,f,v"))).is_some());
        // XREAD after the only entry -> nil
        assert!(StreamModel::step(&s, "xread", &[b("st"), b("1-1")], None).is_some());
    }

    #[test]
    fn stream_auto_id_must_increase() {
        let s = StreamState::default();
        let s = StreamModel::step(
            &s,
            "xadd",
            &[b("st"), b("*"), b("f"), b("v")],
            Some(&b("5-0")),
        )
        .unwrap();
        // A returned id <= last is illegal.
        assert!(
            StreamModel::step(
                &s,
                "xadd",
                &[b("st"), b("*"), b("f"), b("v")],
                Some(&b("5-0"))
            )
            .is_none()
        );
        // A strictly larger id is fine.
        assert!(
            StreamModel::step(
                &s,
                "xadd",
                &[b("st"), b("*"), b("f"), b("v")],
                Some(&b("6-0"))
            )
            .is_some()
        );
    }

    #[test]
    fn stream_explicit_id_must_match_result() {
        let s = StreamState::default();
        assert!(
            StreamModel::step(
                &s,
                "xadd",
                &[b("st"), b("2-2"), b("f"), b("v")],
                Some(&b("3-3"))
            )
            .is_none()
        );
    }

    #[test]
    fn stream_strict_rejects_unknown() {
        let s = StreamState::default();
        assert!(StreamModel::step(&s, "xrange", &[b("st")], Some(&b("x"))).is_none());
    }

    #[test]
    fn stream_linearizable() {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let r = h.invoke(2, "xread", vec![b("st"), b("0")]);
        h.respond(r, Some(b("1-1,f,v")));
        assert!(check_linearizability::<StreamModel>(&h).is_linearizable);
    }
}

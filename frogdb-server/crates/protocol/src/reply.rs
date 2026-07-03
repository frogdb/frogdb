//! Protocol-shaped reply builders.
//!
//! RESP2 and RESP3 disagree about the *shape* of a key/value reply: RESP3 sends
//! a [`Response::Map`], RESP2 a flat [`Response::Array`] of alternating
//! `key, value, key, value, …`. [`MapReply`] owns that flatten-or-map rule in
//! ONE place so handlers list their fields once and let the seam pick the shape,
//! instead of writing the two protocol arms by hand (which is one careless edit
//! from a silent shape divergence).

use bytes::Bytes;

use crate::{ProtocolVersion, Response};

/// Accumulates a key/value reply and emits the protocol-correct shape.
///
/// - RESP3 → [`Response::Map`]`([(k, v), …])`
/// - RESP2 → flattened [`Response::Array`]`([k, v, k, v, …])`
///
/// The flatten/map rule lives in [`MapReply::finish`] and nowhere else, so a
/// field is added once (not once per protocol arm) and the two protocols cannot
/// drift.
#[derive(Debug, Clone)]
pub struct MapReply {
    proto: ProtocolVersion,
    pairs: Vec<(Response, Response)>,
}

impl MapReply {
    /// Start a new map reply for the given protocol version.
    pub fn new(proto: ProtocolVersion) -> Self {
        Self {
            proto,
            pairs: Vec::new(),
        }
    }

    /// Start a new map reply with capacity for `n` fields.
    pub fn with_capacity(proto: ProtocolVersion, n: usize) -> Self {
        Self {
            proto,
            pairs: Vec::with_capacity(n),
        }
    }

    /// Add a field unconditionally.
    pub fn field(&mut self, key: &'static [u8], value: Response) -> &mut Self {
        self.pairs
            .push((Response::bulk(Bytes::from_static(key)), value));
        self
    }

    /// Add a field only when `cond` holds.
    ///
    /// The predicate lives ONCE here, not once per protocol arm. `value` is a
    /// closure so the (possibly expensive) value is built only when included.
    pub fn field_if(
        &mut self,
        cond: bool,
        key: &'static [u8],
        value: impl FnOnce() -> Response,
    ) -> &mut Self {
        if cond {
            self.pairs
                .push((Response::bulk(Bytes::from_static(key)), value()));
        }
        self
    }

    /// Emit the protocol-correct shape. The flatten/map rule lives here.
    pub fn finish(self) -> Response {
        if self.proto.is_resp3() {
            Response::Map(self.pairs)
        } else {
            let mut flat = Vec::with_capacity(self.pairs.len() * 2);
            for (k, v) in self.pairs {
                flat.push(k);
                flat.push(v);
            }
            Response::Array(flat)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field_val(n: i64) -> Response {
        Response::Integer(n)
    }

    #[test]
    fn resp3_finishes_as_map_in_field_order() {
        let mut m = MapReply::new(ProtocolVersion::Resp3);
        m.field(b"a", field_val(1));
        m.field(b"b", field_val(2));
        let resp = m.finish();

        match resp {
            Response::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0, Response::bulk(Bytes::from_static(b"a")));
                assert_eq!(pairs[0].1, Response::Integer(1));
                assert_eq!(pairs[1].0, Response::bulk(Bytes::from_static(b"b")));
                assert_eq!(pairs[1].1, Response::Integer(2));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn resp2_finishes_as_flat_array_in_field_order() {
        let mut m = MapReply::new(ProtocolVersion::Resp2);
        m.field(b"a", field_val(1));
        m.field(b"b", field_val(2));
        let resp = m.finish();

        match resp {
            Response::Array(items) => {
                assert_eq!(items.len(), 4);
                assert_eq!(items[0], Response::bulk(Bytes::from_static(b"a")));
                assert_eq!(items[1], Response::Integer(1));
                assert_eq!(items[2], Response::bulk(Bytes::from_static(b"b")));
                assert_eq!(items[3], Response::Integer(2));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn field_if_includes_only_when_true() {
        // RESP3: the conditional field is present when the predicate holds.
        let mut m = MapReply::new(ProtocolVersion::Resp3);
        m.field(b"always", field_val(1));
        m.field_if(true, b"maybe", || field_val(2));
        m.field_if(false, b"never", || {
            panic!("closure must not run when cond is false")
        });
        match m.finish() {
            Response::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[1].0, Response::bulk(Bytes::from_static(b"maybe")));
            }
            other => panic!("expected Map, got {other:?}"),
        }

        // RESP2: same inclusion rule, flattened.
        let mut m = MapReply::new(ProtocolVersion::Resp2);
        m.field(b"always", field_val(1));
        m.field_if(false, b"skip", || field_val(9));
        match m.finish() {
            Response::Array(items) => assert_eq!(items.len(), 2),
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn empty_builder_finishes_empty() {
        assert_eq!(
            MapReply::new(ProtocolVersion::Resp3).finish(),
            Response::Map(vec![])
        );
        assert_eq!(
            MapReply::new(ProtocolVersion::Resp2).finish(),
            Response::Array(vec![])
        );
    }
}

use std::time::{Duration, Instant};

use frogdb_types::types::{ClaimClock, IdempotencyState, StreamId, StreamIdSpec, StreamValue};

use super::*;

/// On-disk version of the stream payload.
///
/// Bumped from the original (implicit, version-less) layout to `1` when consumer
/// groups were added (proposal 45). FrogDB is pre-production, so this is a clean
/// breaking change with no migration path: the version byte is the first byte of
/// every stream payload and an unrecognized value is rejected rather than guessed
/// at. (Precedent: the HyperLogLog delta encoding byte `2` added in proposal 42.)
const STREAM_FORMAT_VERSION: u8 = 1;

/// Map a monotonic [`Instant`] to a wall-clock millisecond, anchored to `clock`.
///
/// The inverse of [`unix_ms_from_clock`]. Persisting PEL/consumer times as
/// wall-clock ms (rather than the un-serializable monotonic `Instant`) lets idle
/// time resume across a restart from the real elapsed wall-clock — matching Redis,
/// which persists absolute delivery times. Anchoring every entry to one sampled
/// [`ClaimClock`] keeps the whole snapshot internally consistent.
fn instant_to_unix_ms(instant: Instant, clock: ClaimClock) -> u64 {
    if instant >= clock.now {
        clock
            .unix_ms
            .saturating_add(instant.duration_since(clock.now).as_millis() as u64)
    } else {
        clock
            .unix_ms
            .saturating_sub(clock.now.duration_since(instant).as_millis() as u64)
    }
}

/// Map a persisted wall-clock millisecond back to a monotonic [`Instant`],
/// anchored to `clock`. Inverse of [`instant_to_unix_ms`].
///
/// A time far enough in the past that it would predate the monotonic clock's
/// origin clamps to `clock.now` (via `checked_sub`) rather than panicking, exactly
/// as the [`ClaimClock`] `IDLE`/`TIME` paths do.
fn unix_ms_from_clock(unix_ms: u64, clock: ClaimClock) -> Instant {
    if unix_ms >= clock.unix_ms {
        clock.now + Duration::from_millis(unix_ms - clock.unix_ms)
    } else {
        clock
            .now
            .checked_sub(Duration::from_millis(clock.unix_ms - unix_ms))
            .unwrap_or(clock.now)
    }
}

/// Serialize a stream, including its consumer groups.
///
/// Format (version 1, little-endian):
/// - version (1 byte) = [`STREAM_FORMAT_VERSION`]
/// - last_id_ms (8) / last_id_seq (8)
/// - entries_added (8)  — lifetime counter (XINFO STREAM, lag)
/// - max_deleted_id: present (1) [+ ms (8) + seq (8) if present]
/// - num_entries (4)
///   - per entry: id_ms (8), id_seq (8), num_fields (4),
///     per field: field_len (4) + field + value_len (4) + value
/// - total_appended (8)
/// - num_idem_keys (4), per key: key_len (4) + key bytes
/// - num_groups (4), per group:
///   - name_len (4) + name
///   - last_delivered_ms (8) / last_delivered_seq (8)
///   - entries_read: present (1) [+ value (8) if present]
///   - num_consumers (4), per consumer:
///     - name_len (4) + name
///     - last_seen_unix_ms (8)
///     - active_time: present (1) [+ unix_ms (8) if present]
///   - num_pending (4), per pending entry:
///     - id_ms (8) / id_seq (8)
///     - consumer_len (4) + consumer name
///     - delivery_unix_ms (8)
///     - delivery_count (4)
///
/// PEL and consumer times are wall-clock milliseconds anchored to one sampled
/// [`ClaimClock`] (see [`instant_to_unix_ms`]).
pub(super) fn serialize_stream(stream: &StreamValue) -> (TypeMarker, Vec<u8>) {
    let clock = ClaimClock::sample();
    let entries = stream.to_vec();
    let last_id = stream.last_id();

    let mut payload = Vec::new();

    // Version byte.
    payload.push(STREAM_FORMAT_VERSION);

    // Header: last id, entries-added, max-deleted-id.
    payload.extend_from_slice(&last_id.ms.to_le_bytes());
    payload.extend_from_slice(&last_id.seq.to_le_bytes());
    payload.extend_from_slice(&stream.entries_added().to_le_bytes());
    match stream.max_deleted_id() {
        Some(id) => {
            payload.push(1);
            payload.extend_from_slice(&id.ms.to_le_bytes());
            payload.extend_from_slice(&id.seq.to_le_bytes());
        }
        None => payload.push(0),
    }

    // Entries.
    payload.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in &entries {
        payload.extend_from_slice(&entry.id.ms.to_le_bytes());
        payload.extend_from_slice(&entry.id.seq.to_le_bytes());
        payload.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());
        for (field, value) in &entry.fields {
            payload.extend_from_slice(&(field.len() as u32).to_le_bytes());
            payload.extend_from_slice(field);
            payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
            payload.extend_from_slice(value);
        }
    }

    // Event-sourcing counters + idempotency keys.
    payload.extend_from_slice(&stream.total_appended().to_le_bytes());
    if let Some(idem) = stream.idempotency() {
        payload.extend_from_slice(&(idem.len() as u32).to_le_bytes());
        for key in idem.iter() {
            payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
            payload.extend_from_slice(key);
        }
    } else {
        payload.extend_from_slice(&0u32.to_le_bytes());
    }

    // Consumer groups.
    payload.extend_from_slice(&(stream.group_count() as u32).to_le_bytes());
    for group in stream.groups() {
        payload.extend_from_slice(&(group.name.len() as u32).to_le_bytes());
        payload.extend_from_slice(&group.name);

        let ld = group.last_delivered_id();
        payload.extend_from_slice(&ld.ms.to_le_bytes());
        payload.extend_from_slice(&ld.seq.to_le_bytes());

        match group.entries_read() {
            Some(n) => {
                payload.push(1);
                payload.extend_from_slice(&n.to_le_bytes());
            }
            None => payload.push(0),
        }

        // Consumer table.
        let consumers: Vec<_> = group.consumers().collect();
        payload.extend_from_slice(&(consumers.len() as u32).to_le_bytes());
        for consumer in consumers {
            payload.extend_from_slice(&(consumer.name().len() as u32).to_le_bytes());
            payload.extend_from_slice(consumer.name());
            payload
                .extend_from_slice(&instant_to_unix_ms(consumer.last_seen(), clock).to_le_bytes());
            match consumer.active_time() {
                Some(t) => {
                    payload.push(1);
                    payload.extend_from_slice(&instant_to_unix_ms(t, clock).to_le_bytes());
                }
                None => payload.push(0),
            }
        }

        // PEL.
        let pending: Vec<_> = group.pending_iter().collect();
        payload.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        for (id, pe) in pending {
            payload.extend_from_slice(&id.ms.to_le_bytes());
            payload.extend_from_slice(&id.seq.to_le_bytes());
            payload.extend_from_slice(&(pe.consumer().len() as u32).to_le_bytes());
            payload.extend_from_slice(pe.consumer());
            payload.extend_from_slice(&instant_to_unix_ms(pe.delivery_time(), clock).to_le_bytes());
            payload.extend_from_slice(&pe.delivery_count().to_le_bytes());
        }
    }

    (TypeMarker::Stream, payload)
}

/// Deserialize a stream (version 1) from payload.
pub(super) fn deserialize_stream(payload: &[u8]) -> Result<StreamValue, SerializationError> {
    let clock = ClaimClock::sample();
    let mut reader = FrameReader::new(payload);

    let version = reader.read_u8()?;
    if version != STREAM_FORMAT_VERSION {
        return Err(SerializationError::InvalidPayload(format!(
            "unsupported stream format version {version} (expected {STREAM_FORMAT_VERSION})"
        )));
    }

    // Header.
    let last_id = StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?);
    let entries_added = reader.read_le_u64()?;
    let max_deleted_id = if reader.read_u8()? == 1 {
        Some(StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?))
    } else {
        None
    };

    let num_entries = reader.read_le_u32()? as usize;
    let mut stream = StreamValue::new();
    for _ in 0..num_entries {
        let id = StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?);
        let num_fields = reader.read_le_u32()? as usize;
        let mut fields = Vec::with_capacity(safe_capacity(num_fields, 8, reader.remaining()));
        for _ in 0..num_fields {
            let field = reader.read_bytes_u32()?;
            let value = reader.read_bytes_u32()?;
            fields.push((field, value));
        }
        let _ = stream.add(StreamIdSpec::Explicit(id), fields);
    }

    // Event-sourcing counters + idempotency keys.
    stream.set_total_appended(reader.read_le_u64()?);
    let num_idem_keys = reader.read_le_u32()? as usize;
    if num_idem_keys > 0 {
        let mut idem = IdempotencyState::new();
        for _ in 0..num_idem_keys {
            idem.record(reader.read_bytes_u32()?);
        }
        stream.set_idempotency(idem);
    }

    // `add` above overwrote last_id/entries_added with per-entry values; restore
    // the persisted authoritative values (they can diverge via XSETID and are the
    // basis for XINFO STREAM and consumer-group lag).
    stream.set_last_id(last_id);
    stream.set_entries_added(entries_added);
    if let Some(id) = max_deleted_id {
        stream.set_max_deleted_id(id);
    }

    // Consumer groups.
    let num_groups = reader.read_le_u32()? as usize;
    for _ in 0..num_groups {
        let name = reader.read_bytes_u32()?;
        let last_delivered = StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?);
        let entries_read = if reader.read_u8()? == 1 {
            Some(reader.read_le_u64()?)
        } else {
            None
        };

        // create_group only fails on a duplicate name, which a well-formed
        // snapshot never contains; ignore the result to stay infallible.
        let _ = stream.create_group(name.clone(), last_delivered, entries_read);
        let group = stream
            .get_group_mut(&name)
            .expect("group just created above");

        let num_consumers = reader.read_le_u32()? as usize;
        for _ in 0..num_consumers {
            let cname = reader.read_bytes_u32()?;
            let last_seen = unix_ms_from_clock(reader.read_le_u64()?, clock);
            let active_time = if reader.read_u8()? == 1 {
                Some(unix_ms_from_clock(reader.read_le_u64()?, clock))
            } else {
                None
            };
            group.restore_consumer(cname, last_seen, active_time);
        }

        let num_pending = reader.read_le_u32()? as usize;
        for _ in 0..num_pending {
            let id = StreamId::new(reader.read_le_u64()?, reader.read_le_u64()?);
            let consumer = reader.read_bytes_u32()?;
            let delivery_time = unix_ms_from_clock(reader.read_le_u64()?, clock);
            let delivery_count = reader.read_le_u32()?;
            group.restore_pending(id, consumer, delivery_time, delivery_count);
        }
    }

    Ok(stream)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use frogdb_types::types::{ClaimOpts, StreamRangeBound};

    use super::*;

    fn round_trip(stream: &StreamValue) -> StreamValue {
        let (marker, payload) = serialize_stream(stream);
        assert_eq!(marker, TypeMarker::Stream);
        deserialize_stream(&payload).expect("round-trip deserialization must succeed")
    }

    /// Build a stream with `n` entries `1-1 .. n-1`, each `{f: v}`.
    fn stream_with_entries(n: u64) -> StreamValue {
        let mut s = StreamValue::new();
        for ms in 1..=n {
            let _ = s.add(
                StreamIdSpec::Explicit(StreamId::new(ms, 1)),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            );
        }
        s
    }

    /// Full shape: two groups, consumers, and PEL entries all survive the codec.
    #[test]
    fn round_trips_groups_pel_and_consumers() {
        let mut s = stream_with_entries(3);
        s.create_group(Bytes::from_static(b"g1"), StreamId::new(2, 1), Some(2))
            .unwrap();
        s.create_group(Bytes::from_static(b"g2"), StreamId::new(0, 0), None)
            .unwrap();

        let g1 = s.get_group_mut(b"g1").unwrap();
        g1.create_consumer(Bytes::from_static(b"alice"));
        g1.create_consumer(Bytes::from_static(b"bob"));
        g1.add_pending(StreamId::new(1, 1), Bytes::from_static(b"alice"));
        g1.add_pending(StreamId::new(2, 1), Bytes::from_static(b"bob"));

        let restored = round_trip(&s);

        assert_eq!(restored.group_count(), 2);

        let g1 = restored.get_group(b"g1").unwrap();
        assert_eq!(g1.last_delivered_id(), StreamId::new(2, 1));
        assert_eq!(g1.entries_read(), Some(2));
        assert_eq!(g1.consumer_count(), 2);
        assert_eq!(g1.pending_count(), 2);
        let pel = g1.pending_entries(
            StreamRangeBound::Min,
            StreamRangeBound::Max,
            usize::MAX,
            None,
            None,
        );
        assert_eq!(pel.len(), 2);
        assert_eq!(pel[0].id, StreamId::new(1, 1));
        assert_eq!(pel[0].consumer, Bytes::from_static(b"alice"));
        assert_eq!(pel[1].id, StreamId::new(2, 1));
        assert_eq!(pel[1].consumer, Bytes::from_static(b"bob"));

        // The PEL invariant is rebuilt: each consumer owns exactly one entry.
        let counts: Vec<(Bytes, usize)> = g1
            .consumers()
            .map(|c| (c.name().clone(), c.pending_count()))
            .collect();
        assert_eq!(
            counts,
            vec![
                (Bytes::from_static(b"alice"), 1),
                (Bytes::from_static(b"bob"), 1)
            ]
        );

        let g2 = restored.get_group(b"g2").unwrap();
        assert_eq!(g2.last_delivered_id(), StreamId::new(0, 0));
        assert_eq!(g2.entries_read(), None);
        assert_eq!(g2.consumer_count(), 0);
        assert_eq!(g2.pending_count(), 0);
    }

    /// An empty group (no consumers, no PEL) survives — the XGROUP CREATE case.
    #[test]
    fn round_trips_empty_group() {
        let mut s = stream_with_entries(1);
        s.create_group(Bytes::from_static(b"g"), StreamId::new(1, 1), Some(1))
            .unwrap();

        let restored = round_trip(&s);

        let g = restored.get_group(b"g").unwrap();
        assert_eq!(g.last_delivered_id(), StreamId::new(1, 1));
        assert_eq!(g.entries_read(), Some(1));
        assert_eq!(g.consumer_count(), 0);
        assert_eq!(g.pending_count(), 0);
    }

    /// Consumers with an empty PEL (e.g. all entries acked, or XGROUP
    /// CREATECONSUMER) survive with a zero pending count.
    #[test]
    fn round_trips_group_with_consumers_but_empty_pel() {
        let mut s = stream_with_entries(1);
        s.create_group(Bytes::from_static(b"g"), StreamId::new(1, 1), None)
            .unwrap();
        let g = s.get_group_mut(b"g").unwrap();
        g.create_consumer(Bytes::from_static(b"alice"));
        g.create_consumer(Bytes::from_static(b"bob"));

        let restored = round_trip(&s);

        let g = restored.get_group(b"g").unwrap();
        assert_eq!(g.pending_count(), 0);
        let consumers: Vec<(Bytes, usize)> = g
            .consumers()
            .map(|c| (c.name().clone(), c.pending_count()))
            .collect();
        assert_eq!(
            consumers,
            vec![
                (Bytes::from_static(b"alice"), 0),
                (Bytes::from_static(b"bob"), 0)
            ]
        );
    }

    /// A delivery count driven above 1 by claims survives exactly — losing it
    /// would break XCLAIM retry/dead-letter logic after a restart.
    #[test]
    fn round_trips_delivery_count() {
        let mut s = stream_with_entries(1);
        s.create_group(Bytes::from_static(b"g"), StreamId::new(1, 1), None)
            .unwrap();
        let g = s.get_group_mut(b"g").unwrap();
        g.create_consumer(Bytes::from_static(b"alice"));
        g.add_pending(StreamId::new(1, 1), Bytes::from_static(b"alice"));
        // Force the count to a distinctive value via the RETRYCOUNT claim path.
        g.claim_pending(
            StreamId::new(1, 1),
            &Bytes::from_static(b"alice"),
            ClaimOpts {
                retrycount: Some(41),
                ..Default::default()
            },
            ClaimClock::sample(),
        );

        let restored = round_trip(&s);

        let pel = restored.get_group(b"g").unwrap().pending_entries(
            StreamRangeBound::Min,
            StreamRangeBound::Max,
            usize::MAX,
            None,
            None,
        );
        assert_eq!(pel.len(), 1);
        assert_eq!(pel[0].delivery_count, 41);
    }

    /// PEL idle time resumes across the codec: an entry pushed 5s into the past
    /// via the IDLE claim path reports at least that idle after a round-trip
    /// (the ClaimClock unix-ms mapping), never a reset-to-zero value.
    #[test]
    fn round_trip_preserves_pel_idle_time() {
        let mut s = stream_with_entries(1);
        s.create_group(Bytes::from_static(b"g"), StreamId::new(1, 1), None)
            .unwrap();
        let g = s.get_group_mut(b"g").unwrap();
        g.create_consumer(Bytes::from_static(b"alice"));
        g.add_pending(StreamId::new(1, 1), Bytes::from_static(b"alice"));
        g.claim_pending(
            StreamId::new(1, 1),
            &Bytes::from_static(b"alice"),
            ClaimOpts {
                idle: Some(5_000),
                justid: true,
                ..Default::default()
            },
            ClaimClock::sample(),
        );

        let restored = round_trip(&s);

        let idle = restored
            .get_group(b"g")
            .unwrap()
            .pending_idle(&StreamId::new(1, 1))
            .expect("entry must still be pending");
        assert!(
            (5_000..10_000).contains(&idle),
            "idle must resume from the persisted wall-clock value, got {idle}ms"
        );
    }

    /// Stream-level counters that feed XINFO STREAM / lag survive: last_id
    /// beyond the newest entry (XSETID), entries_added, and max_deleted_id.
    #[test]
    fn round_trips_stream_header_counters() {
        let mut s = stream_with_entries(3);
        let _ = s.delete(&[StreamId::new(2, 1)]);
        s.set_last_id(StreamId::new(100, 0)); // XSETID past the newest entry
        s.set_entries_added(7); // XSETID ENTRIESADDED

        let restored = round_trip(&s);

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.last_id(), StreamId::new(100, 0));
        assert_eq!(restored.entries_added(), 7);
        assert_eq!(restored.max_deleted_id(), Some(StreamId::new(2, 1)));
    }

    /// An unknown version byte is rejected loudly, never mis-parsed as data.
    #[test]
    fn rejects_unknown_format_version() {
        let (_, mut payload) = serialize_stream(&stream_with_entries(1));
        payload[0] = 0xFF;
        assert!(matches!(
            deserialize_stream(&payload),
            Err(SerializationError::InvalidPayload(_))
        ));
    }
}

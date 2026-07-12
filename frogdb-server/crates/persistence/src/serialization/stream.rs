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

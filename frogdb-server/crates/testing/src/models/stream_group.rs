//! Consumer-group stream model (superset of StreamModel): XADD/XLEN/XREAD plus
//! XGROUP CREATE, non-blocking XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM.
//! PEL: entry id -> (owning consumer, delivery count). Bounded option set:
//! min-idle-time is always 0, no NOACK, no blocking, no XDEL edge cases.
//!
//! `delivery_count` bookkeeping (phase 4a): tracked on every PEL record but
//! intentionally inert — no implemented result encoding surfaces it.
//! XPENDING here is summary-form only (no per-entry counts), XAUTOCLAIM is
//! JUSTID-only (no count change), and XCLAIM returns entries without counts.
//! So `dc += 1` never affects accept/reject in phase 4a. Phase 4b's
//! extended-form XPENDING (with per-entry delivery counts in its reply) is
//! expected to make it observable.

use super::Model;
use crate::models::stream::{StreamId, parse_id};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

/// One pending-entry-list record: which consumer owns it and how many times it
/// has been delivered.
#[derive(Debug, Clone)]
pub struct PelEntry {
    pub consumer: Bytes,
    /// Incremented on redelivery (XREADGROUP `>` initial delivery, XCLAIM).
    /// Intentionally inert in phase 4a: no implemented result encoding
    /// surfaces it (XPENDING here is summary-form only, XAUTOCLAIM is
    /// JUSTID-only, XCLAIM returns entries without counts), so this
    /// bookkeeping never affects accept/reject. Phase 4b's extended-form
    /// XPENDING is expected to make it observable.
    pub delivery_count: u64,
}

/// One consumer group: the high-water delivered id and the group PEL.
#[derive(Debug, Clone, Default)]
pub struct Group {
    pub last_delivered: StreamId,
    /// entry id -> PEL record. BTreeMap so id order is free.
    pub pel: BTreeMap<StreamId, PelEntry>,
}

/// Per-key stream contents plus its consumer groups.
#[derive(Debug, Clone, Default)]
pub struct StreamGroupData {
    pub entries: Vec<(StreamId, Vec<Bytes>)>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, Group>,
}

impl StreamGroupData {
    fn entry_fields(&self, id: StreamId) -> Option<&Vec<Bytes>> {
        self.entries.iter().find(|(i, _)| *i == id).map(|(_, f)| f)
    }
}

/// Sequential model for streams with consumer groups (superset of StreamModel).
#[derive(Debug, Clone, Default)]
pub struct StreamGroupModel;

/// State for the consumer-group stream model.
#[derive(Debug, Clone, Default)]
pub struct StreamGroupState {
    pub streams: HashMap<Bytes, StreamGroupData>,
}

/// Encode an ordered set of `(id, fields)` as `"id,f,v,…"` entries `|`-joined.
fn encode_entries(entries: &[(StreamId, Vec<Bytes>)]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for (id, fields) in entries {
        let mut one = vec![id.to_string()];
        one.extend(
            fields
                .iter()
                .map(|f| String::from_utf8_lossy(f).to_string()),
        );
        parts.push(one.join(","));
    }
    Some(parts.join("|"))
}

/// Read the trailing `STREAMS key id` triple from an xreadgroup arg list.
fn streams_key_id(args: &[Bytes]) -> Option<(Bytes, Bytes)> {
    let pos = args
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))?;
    let key = args.get(pos + 1)?.clone();
    let id = args.get(pos + 2)?.clone();
    Some((key, id))
}

impl Model for StreamGroupModel {
    type State = StreamGroupState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "xadd" => {
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let id_arg = &args[1];
                let fields: Vec<Bytes> = args[2..].to_vec();
                let assigned = parse_id(result?.as_ref())?;
                let last = state
                    .streams
                    .get(key)
                    .map(|s| s.last_id)
                    .unwrap_or_default();
                if assigned <= last {
                    return None;
                }
                if id_arg.as_ref() != b"*" && parse_id(id_arg)? != assigned {
                    return None;
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
                if result
                    .is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(len))
                {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xread" => {
                if args.len() < 2 {
                    return None;
                }
                let after = parse_id(&args[1])?;
                let entries: Vec<(StreamId, Vec<Bytes>)> = state
                    .streams
                    .get(&args[0])
                    .map(|s| {
                        s.entries
                            .iter()
                            .filter(|(id, _)| *id > after)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                let expected = encode_entries(&entries);
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xgroup" => {
                // Bounded: only `CREATE key group id [MKSTREAM]`.
                if args.len() < 4 || !args[0].eq_ignore_ascii_case(b"CREATE") {
                    return None;
                }
                let key = &args[1];
                let group = &args[2];
                let id_arg = &args[3];
                let exists = state
                    .streams
                    .get(key)
                    .is_some_and(|s| s.groups.contains_key(group));
                match result {
                    // BUSYGROUP or other error -> None: legal only if the group
                    // already existed (idempotent re-create attempt).
                    None => {
                        if exists {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                    Some(r) if r.as_ref() == b"OK" => {
                        if exists {
                            return None; // OK requires the group did not exist
                        }
                        let mut new = state.clone();
                        let sd = new.streams.entry(key.clone()).or_default();
                        let last_delivered = if id_arg.as_ref() == b"$" {
                            sd.last_id
                        } else {
                            parse_id(id_arg)?
                        };
                        sd.groups.insert(
                            group.clone(),
                            Group {
                                last_delivered,
                                pel: BTreeMap::new(),
                            },
                        );
                        Some(new)
                    }
                    Some(_) => None,
                }
            }
            "xreadgroup" => {
                // GROUP g c [COUNT n] STREAMS key id
                let gi = args.iter().position(|a| a.eq_ignore_ascii_case(b"GROUP"))?;
                let group = args.get(gi + 1)?.clone();
                let consumer = args.get(gi + 2)?.clone();
                let (key, id) = streams_key_id(args)?;
                let count = args
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
                    .and_then(|ci| args.get(ci + 1))
                    .and_then(|c| String::from_utf8_lossy(c).parse::<usize>().ok())
                    .unwrap_or(usize::MAX);

                let mut new = state.clone();
                let sd = new.streams.entry(key.clone()).or_default();
                if !sd.groups.contains_key(&group) {
                    // NOGROUP -> recorded as None; accept only as None.
                    return if result.is_none() {
                        Some(state.clone())
                    } else {
                        None
                    };
                }
                if id.as_ref() == b">" {
                    // New messages after last_delivered, up to count.
                    let last = sd.groups[&group].last_delivered;
                    let to_deliver: Vec<(StreamId, Vec<Bytes>)> = sd
                        .entries
                        .iter()
                        .filter(|(eid, _)| *eid > last)
                        .take(count)
                        .cloned()
                        .collect();
                    let expected = encode_entries(&to_deliver);
                    if result.map(|r| String::from_utf8_lossy(r).to_string()) != expected {
                        return None;
                    }
                    let g = sd.groups.get_mut(&group)?;
                    for (eid, _) in &to_deliver {
                        g.last_delivered = g.last_delivered.max(*eid);
                        g.pel.insert(
                            *eid,
                            PelEntry {
                                consumer: consumer.clone(),
                                delivery_count: 1,
                            },
                        );
                    }
                    Some(new)
                } else {
                    // History re-read of the consumer's own pending with id > `id`.
                    let after = parse_id(&id)?;
                    let g = &sd.groups[&group];
                    let own_ids: Vec<StreamId> = g
                        .pel
                        .iter()
                        .filter(|(eid, p)| **eid > after && p.consumer == consumer)
                        .take(count)
                        .map(|(eid, _)| *eid)
                        .collect();
                    let own: Vec<(StreamId, Vec<Bytes>)> = own_ids
                        .into_iter()
                        .filter_map(|eid| sd.entry_fields(eid).map(|f| (eid, f.clone())))
                        .collect();
                    // Empty own-pending re-read is `[[key, []]]` -> None encoding.
                    let expected = encode_entries(&own);
                    if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                        // Re-read refreshes idle but does NOT change delivery_count.
                        Some(new)
                    } else {
                        None
                    }
                }
            }
            "xack" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let group = &args[1];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(sd) = new.streams.get_mut(key)
                    && let Some(g) = sd.groups.get_mut(group)
                {
                    for id_arg in &args[2..] {
                        if let Some(id) = parse_id(id_arg)
                            && g.pel.remove(&id).is_some()
                        {
                            removed += 1;
                        }
                    }
                }
                if result.is_some_and(|r| {
                    String::from_utf8_lossy(r).parse::<i64>().ok() == Some(removed)
                }) {
                    Some(new)
                } else {
                    None
                }
            }
            "xpending" => {
                // Summary form: key group -> "total|min|max|c:n,c:n" (sorted); "0" if empty.
                if args.len() < 2 {
                    return None;
                }
                let g = state
                    .streams
                    .get(&args[0])
                    .and_then(|s| s.groups.get(&args[1]));
                let expected = match g {
                    None => "0".to_string(),
                    Some(g) if g.pel.is_empty() => "0".to_string(),
                    Some(g) => {
                        let total = g.pel.len();
                        let min = g.pel.keys().next().unwrap().to_string();
                        let max = g.pel.keys().next_back().unwrap().to_string();
                        let mut per: BTreeMap<String, u64> = BTreeMap::new();
                        for p in g.pel.values() {
                            *per.entry(String::from_utf8_lossy(&p.consumer).to_string())
                                .or_default() += 1;
                        }
                        let consumers = per
                            .into_iter()
                            .map(|(c, n)| format!("{c}:{n}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("{total}|{min}|{max}|{consumers}")
                    }
                };
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == Some(expected) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xclaim" => {
                // key group consumer min-idle-time id... (no JUSTID): returns
                // claimed entries, transfers ownership, delivery_count += 1.
                if args.len() < 5 {
                    return None;
                }
                let key = &args[0];
                let group = &args[1];
                let consumer = &args[2];
                let mut new = state.clone();
                let sd = new.streams.get_mut(key)?;
                let entries_snapshot = sd.entries.clone();
                let g = sd.groups.get_mut(group)?;
                let mut claimed: Vec<(StreamId, Vec<Bytes>)> = Vec::new();
                for id_arg in &args[4..] {
                    let Some(id) = parse_id(id_arg) else { continue };
                    if let Some(p) = g.pel.get_mut(&id) {
                        p.consumer = consumer.clone();
                        p.delivery_count += 1;
                        if let Some((_, f)) = entries_snapshot.iter().find(|(i, _)| *i == id) {
                            claimed.push((id, f.clone()));
                        }
                    }
                }
                claimed.sort_by_key(|(i, _)| *i);
                let expected = encode_entries(&claimed);
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                    Some(new)
                } else {
                    None
                }
            }
            "xautoclaim" => {
                // key group consumer min-idle start [COUNT n] JUSTID: returns
                // "cursor;id,id" (ids only), transfers ownership, no count change.
                //
                // Cursor assumption: this model always expects the returned cursor
                // to be `0-0` (a single call claims the full PEL range starting at
                // `start`). The workload generator MUST issue XAUTOCLAIM with a
                // COUNT exceeding any achievable PEL size for this to hold — if the
                // real server ever has more matching entries than COUNT, it legally
                // returns a non-zero cursor to continue the scan on a later call,
                // and this model would spuriously reject that (correct) reply.
                let justid = args.iter().any(|a| a.eq_ignore_ascii_case(b"JUSTID"));
                if args.len() < 5 || !justid {
                    return None; // bounded set uses JUSTID only
                }
                let key = &args[0];
                let group = &args[1];
                let consumer = &args[2];
                let start = parse_id(&args[4])?;
                let count = args
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
                    .and_then(|ci| args.get(ci + 1))
                    .and_then(|c| String::from_utf8_lossy(c).parse::<usize>().ok())
                    .unwrap_or(100);
                let mut new = state.clone();
                let sd = new.streams.get_mut(key)?;
                let g = sd.groups.get_mut(group)?;
                let ids: Vec<StreamId> = g
                    .pel
                    .range(start..)
                    .map(|(id, _)| *id)
                    .take(count)
                    .collect();
                for id in &ids {
                    if let Some(p) = g.pel.get_mut(id) {
                        p.consumer = consumer.clone(); // JUSTID: no delivery_count change
                    }
                }
                // Cursor 0-0 = scan complete (bounded workloads claim all at once).
                let cursor = StreamId::default();
                let expected = format!(
                    "{};{}",
                    cursor,
                    ids.iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == Some(expected) {
                    Some(new)
                } else {
                    None
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

    fn step(
        s: &StreamGroupState,
        f: &str,
        args: &[&str],
        r: Option<&str>,
    ) -> Option<StreamGroupState> {
        let a: Vec<Bytes> = args.iter().map(|x| b(x)).collect();
        StreamGroupModel::step(s, f, &a, r.map(b).as_ref())
    }

    #[test]
    fn group_happy_path() {
        let s = StreamGroupState::default();
        // XADD st 1-1 f v ; XADD st 2-1 f w
        let s = step(&s, "xadd", &["st", "1-1", "f", "v"], Some("1-1")).unwrap();
        let s = step(&s, "xadd", &["st", "2-1", "f", "w"], Some("2-1")).unwrap();
        // XGROUP CREATE st g1 0 MKSTREAM
        let s = step(
            &s,
            "xgroup",
            &["CREATE", "st", "g1", "0", "MKSTREAM"],
            Some("OK"),
        )
        .unwrap();
        // XREADGROUP GROUP g1 c1 COUNT 10 STREAMS st '>' -> both entries, PEL={1-1,2-1}->c1
        let s = step(
            &s,
            "xreadgroup",
            &["GROUP", "g1", "c1", "COUNT", "10", "STREAMS", "st", ">"],
            Some("1-1,f,v|2-1,f,w"),
        )
        .unwrap();
        // XPENDING st g1 (summary) -> total|min|max|c1:2
        assert!(step(&s, "xpending", &["st", "g1"], Some("2|1-1|2-1|c1:2")).is_some());
        // Re-read own pending from 0 -> both again, no count change.
        assert!(
            step(
                &s,
                "xreadgroup",
                &["GROUP", "g1", "c1", "STREAMS", "st", "0"],
                Some("1-1,f,v|2-1,f,w"),
            )
            .is_some()
        );
        // XACK st g1 1-1 -> 1 removed.
        let s = step(&s, "xack", &["st", "g1", "1-1"], Some("1")).unwrap();
        // XACK again -> 0 (already acked).
        assert!(step(&s, "xack", &["st", "g1", "1-1"], Some("0")).is_some());
        // XPENDING now -> only 2-1 pending for c1.
        assert!(step(&s, "xpending", &["st", "g1"], Some("1|2-1|2-1|c1:1")).is_some());
    }

    #[test]
    fn readgroup_gt_delivers_only_new() {
        let s = StreamGroupState::default();
        let s = step(&s, "xadd", &["st", "1-1", "f", "v"], Some("1-1")).unwrap();
        let s = step(&s, "xgroup", &["CREATE", "st", "g", "0"], Some("OK")).unwrap();
        // First '>' delivers 1-1.
        let s = step(
            &s,
            "xreadgroup",
            &["GROUP", "g", "c", "STREAMS", "st", ">"],
            Some("1-1,f,v"),
        )
        .unwrap();
        // Second '>' with nothing new -> nil (None).
        assert!(
            step(
                &s,
                "xreadgroup",
                &["GROUP", "g", "c", "STREAMS", "st", ">"],
                None
            )
            .is_some()
        );
        // Claiming a nonexistent-in-PEL is a no-op; wrong delivery result rejected.
        assert!(
            step(
                &s,
                "xreadgroup",
                &["GROUP", "g", "c", "STREAMS", "st", ">"],
                Some("1-1,f,v")
            )
            .is_none()
        );
    }

    #[test]
    fn xclaim_transfers_and_increments() {
        let s = StreamGroupState::default();
        let s = step(&s, "xadd", &["st", "5-0", "f", "v"], Some("5-0")).unwrap();
        let s = step(&s, "xgroup", &["CREATE", "st", "g", "0"], Some("OK")).unwrap();
        let s = step(
            &s,
            "xreadgroup",
            &["GROUP", "g", "c1", "STREAMS", "st", ">"],
            Some("5-0,f,v"),
        )
        .unwrap();
        // XCLAIM to c2 (min-idle 0) -> returns the entry, delivery_count now 2
        // (bumped internally but not observable here — see PelEntry::delivery_count).
        let s = step(
            &s,
            "xclaim",
            &["st", "g", "c2", "0", "5-0"],
            Some("5-0,f,v"),
        )
        .unwrap();
        // XPENDING (summary-form) observes ownership transfer only, not the count.
        assert!(step(&s, "xpending", &["st", "g"], Some("1|5-0|5-0|c2:1")).is_some());
        // XAUTOCLAIM JUSTID from 0 to c1 -> cursor 0-0, id 5-0, no count change.
        assert!(
            step(
                &s,
                "xautoclaim",
                &["st", "g", "c1", "0", "0", "COUNT", "10", "JUSTID"],
                Some("0-0;5-0"),
            )
            .is_some()
        );
    }

    #[test]
    fn readgroup_reread_honors_count() {
        let s = StreamGroupState::default();
        let s = step(&s, "xadd", &["st", "1-1", "f", "v"], Some("1-1")).unwrap();
        let s = step(&s, "xadd", &["st", "2-1", "f", "w"], Some("2-1")).unwrap();
        let s = step(&s, "xgroup", &["CREATE", "st", "g", "0"], Some("OK")).unwrap();
        // XREADGROUP GROUP g c STREAMS st '>' -> both entries pending for c.
        let s = step(
            &s,
            "xreadgroup",
            &["GROUP", "g", "c", "STREAMS", "st", ">"],
            Some("1-1,f,v|2-1,f,w"),
        )
        .unwrap();
        // Re-read own pending from 0 with COUNT 1 -> only the first entry.
        assert!(
            step(
                &s,
                "xreadgroup",
                &["GROUP", "g", "c", "COUNT", "1", "STREAMS", "st", "0"],
                Some("1-1,f,v"),
            )
            .is_some()
        );
        // The uncapped (both-entries) encoding must be rejected once COUNT 1 caps it.
        assert!(
            step(
                &s,
                "xreadgroup",
                &["GROUP", "g", "c", "COUNT", "1", "STREAMS", "st", "0"],
                Some("1-1,f,v|2-1,f,w"),
            )
            .is_none()
        );
    }

    #[test]
    fn strict_rejects_unknown() {
        let s = StreamGroupState::default();
        assert!(step(&s, "sadd", &["st", "x"], Some("1")).is_none());
    }

    #[test]
    fn group_linearizable_end_to_end() {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(
            2,
            "xreadgroup",
            vec![b("GROUP"), b("g"), b("c"), b("STREAMS"), b("st"), b(">")],
        );
        h.respond(r, Some(b("1-1,f,v")));
        assert!(check_linearizability::<StreamGroupModel>(&h).is_linearizable);
    }
}

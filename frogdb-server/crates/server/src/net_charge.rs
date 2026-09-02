//! [`Charge`] plumbing for transient reply accumulators on the connection
//! thread.
//!
//! The fan-out merges (`scatter::broadcast`, `connection::search::merge`) and
//! the INFO section builder all buffer reply material *before* it reaches the
//! connection's output buffer, where [`crate::connection::output_buffer`]
//! charges it against this core's `NetworkOutput` budget at feed time. Until
//! then those accumulators are network-output bytes the budget cannot see —
//! a cluster-wide FT.SEARCH overfetch is held here in full while shards are
//! still replying. Each accumulator therefore owns a [`Charge`] against the
//! same thread-local budget ([`network_output::current`]), grown as material
//! is absorbed and released (via `Drop`) when the accumulator is consumed.
//!
//! Refusal follows the budget's declared disposition
//! (`Disposition::Shed`): the accumulated material is dropped and the client
//! is told, via [`shed_error`], rather than the server keeping bytes the
//! budget refused.

use frogdb_memory::{Charge, network_output};
use frogdb_protocol::Response;

/// Open a zero-byte charge against this thread's `NetworkOutput` budget.
pub(crate) fn open_charge() -> Charge {
    network_output::current().open_charge()
}

/// Grow `charge` by `bytes`. Returns `false` on refusal — the caller must
/// stop accumulating and surface the shed to the client.
pub(crate) fn try_grow(charge: &mut Charge, bytes: u64) -> bool {
    charge.grow(bytes).is_ok()
}

/// The error a merge replies with when the `NetworkOutput` budget refused its
/// accumulation: the reply is shed (the budget's declared disposition), the
/// client is told, and the partial material is dropped.
pub(crate) fn shed_error(what: &str) -> Response {
    Response::error(format!(
        "ERR {what} reply dropped: network output memory budget exceeded"
    ))
}

/// Approximate heap bytes a [`Response`] retains while parked in an
/// accumulator.
///
/// This feeds [`Charge::grow`] for merge accumulators, so it only has to be
/// an honest estimate of resident size — not the wire encoding. Leaf slack
/// (discriminant, `Bytes` handle, container header) is folded into a flat
/// per-node constant.
pub(crate) fn approx_response_bytes(resp: &Response) -> u64 {
    const NODE: u64 = 32;
    match resp {
        Response::Simple(s) => NODE + s.as_bytes().len() as u64,
        Response::Error(b) | Response::BlobError(b) | Response::BigNumber(b) => {
            NODE + b.len() as u64
        }
        Response::Bulk(Some(b)) => NODE + b.len() as u64,
        Response::VerbatimString { data, .. } => NODE + data.len() as u64,
        Response::Array(items) | Response::Set(items) | Response::Push(items) => {
            NODE + items.iter().map(approx_response_bytes).sum::<u64>()
        }
        Response::Map(pairs) => {
            NODE + pairs
                .iter()
                .map(|(k, v)| approx_response_bytes(k) + approx_response_bytes(v))
                .sum::<u64>()
        }
        Response::Attribute { attrs, data } => {
            NODE + attrs
                .iter()
                .map(|(k, v)| approx_response_bytes(k) + approx_response_bytes(v))
                .sum::<u64>()
                + approx_response_bytes(data)
        }
        // Scalars and the internal control-flow variants: flat node cost.
        _ => NODE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn nested_responses_count_their_payload_bytes() {
        let resp = Response::Array(vec![
            Response::bulk(Bytes::from_static(b"0123456789")),
            Response::Integer(7),
        ]);
        let approx = approx_response_bytes(&resp);
        // 3 nodes of slack + the 10 payload bytes.
        assert_eq!(approx, 32 * 3 + 10);
    }

    #[test]
    fn a_refused_grow_reports_false_and_keeps_the_charge_consistent() {
        use frogdb_memory::{Budget, Disposition, Subsystem};
        let budget = Budget::new(Subsystem::NetworkOutput, Disposition::Shed, 100);
        let mut charge = budget.open_charge();
        assert!(try_grow(&mut charge, 60));
        assert!(!try_grow(&mut charge, 60));
        assert_eq!(charge.bytes(), 60);
        drop(charge);
        assert_eq!(budget.charged(), 0);
    }
}

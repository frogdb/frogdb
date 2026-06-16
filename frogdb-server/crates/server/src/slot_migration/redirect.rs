//! The single owner of cluster redirect wire formats: MOVED / ASK / CLUSTERDOWN
//! / CROSSSLOT.
//!
//! Every slot-redirect reply a client can receive is constructed here; nowhere
//! else formats these strings. Centralizing the format settles the IPv4/IPv6
//! address-rendering question in exactly one place ([`fmt_addr`]).

use frogdb_protocol::Response;
use std::net::SocketAddr;

/// `MOVED <slot> <host>:<port>` — the slot is owned by another node; the client
/// should reconnect to the owner.
pub fn moved(slot: u16, addr: SocketAddr) -> Response {
    Response::error(format!("MOVED {} {}", slot, fmt_addr(addr)))
}

/// `ASK <slot> <host>:<port>` — one-shot redirect to the importing target
/// during a slot migration.
pub fn ask(slot: u16, addr: SocketAddr) -> Response {
    Response::error(format!("ASK {} {}", slot, fmt_addr(addr)))
}

/// `CLUSTERDOWN Hash slot <slot> not served` — the slot is unassigned, or its
/// owner's node info is missing from the local view.
pub fn clusterdown_slot(slot: u16) -> Response {
    Response::error(format!("CLUSTERDOWN Hash slot {} not served", slot))
}

/// `CROSSSLOT Keys in request don't hash to the same slot`.
pub fn crossslot() -> Response {
    Response::error("CROSSSLOT Keys in request don't hash to the same slot")
}

/// The single decision about how an owner address is rendered on the wire:
/// `<host>:<port>`, where IPv6 hosts are bracketed (`[2001:db8::1]:6379`).
///
/// This is what `SocketAddr`'s `Display` produces and what the rest of FrogDB
/// uses for addresses; it is the only unambiguous rendering (the unbracketed
/// `ip():port()` form `2001:db8::1:6379` is unparseable for IPv6). Settling on
/// it here replaces the three divergent renderings the redirect sites used.
fn fmt_addr(addr: SocketAddr) -> String {
    addr.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_protocol::Response;

    fn error_text(resp: &Response) -> String {
        match resp {
            Response::Error(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("expected error response, got {other:?}"),
        }
    }

    #[test]
    fn moved_ipv4() {
        let addr = "127.0.0.1:6379".parse().unwrap();
        assert_eq!(error_text(&moved(42, addr)), "MOVED 42 127.0.0.1:6379");
    }

    #[test]
    fn moved_ipv6_is_bracketed() {
        let addr = "[2001:db8::1]:6379".parse().unwrap();
        // The bracketed form is the only unambiguous rendering for IPv6.
        assert_eq!(
            error_text(&moved(866, addr)),
            "MOVED 866 [2001:db8::1]:6379"
        );
    }

    #[test]
    fn ask_ipv4() {
        let addr = "10.0.0.5:6380".parse().unwrap();
        assert_eq!(error_text(&ask(7, addr)), "ASK 7 10.0.0.5:6380");
    }

    #[test]
    fn ask_ipv6_is_bracketed() {
        let addr = "[fe80::1]:6380".parse().unwrap();
        assert_eq!(error_text(&ask(7, addr)), "ASK 7 [fe80::1]:6380");
    }

    #[test]
    fn clusterdown_slot_format() {
        assert_eq!(
            error_text(&clusterdown_slot(99)),
            "CLUSTERDOWN Hash slot 99 not served"
        );
    }

    #[test]
    fn crossslot_format() {
        assert_eq!(
            error_text(&crossslot()),
            "CROSSSLOT Keys in request don't hash to the same slot"
        );
    }
}

//! Wire-size ceilings, in one place.
//!
//! Three transports carry the same user bytes: the client connection
//! (`frogdb-server`'s RESP codec), the replication link
//! (`frogdb-replication`'s frame codec) and the cluster bus
//! (`frogdb-cluster`'s length-delimited codec). Each used to pick its own
//! constant, and the numbers disagreed: a value the connection layer accepted
//! (512 MB) could not cross a replication frame (64 MB), so the primary
//! committed a write and then emitted a frame its replica's decoder refused —
//! the link dropped, the backlog re-sent the same frame on reconnect, and it
//! never recovered (round-2 issue 69).
//!
//! So the ceilings are related here rather than coincidentally: what a client
//! may send is the ceiling, and every internal transport is sized to carry it.

/// Maximum length of a single RESP bulk string a client may send (512 MB).
///
/// Redis `proto-max-bulk-len` / `PROTO_MAX_BULK_LEN`. This is the ceiling on
/// **user data**: the biggest value a `SET` can carry, and therefore the
/// dominant term in every internal frame that replicates or forwards it.
pub const PROTO_MAX_BULK_LEN: usize = 512 * 1024 * 1024;

/// Maximum number of elements in a multibulk (array) request.
///
/// Redis `PROTO_MAX_MULTIBULK_LEN` (1024 * 1024).
pub const PROTO_MAX_MULTIBULK_LEN: i64 = 1_048_576;

/// The ceiling every FrogDB-internal transport must be able to carry.
///
/// Derived from [`PROTO_MAX_BULK_LEN`], not chosen independently: an internal
/// frame carries one accepted command (a replicated write, a forwarded
/// publish), whose dominant term is a maximal bulk value plus the command name,
/// key and RESP framing around it. The allowance is one further maximal bulk,
/// which puts this at 1 GiB — the same number Redis caps a client's whole
/// accumulated request at (`PROTO_MAX_QUERYBUF_LEN`), and therefore the same
/// bound Redis places on how large a single replicated command can be.
///
/// An internal frame is refused at this ceiling rather than truncated. A
/// command whose *replicated* encoding exceeds it (a multi-value `MSET` of
/// several maximal bulks, or a synthesized effect such as a `SORT ... STORE`
/// result) therefore fails loudly on the wire instead of silently crossing it
/// short — see `frogdb_replication::frame::MAX_FRAME_SIZE`.
pub const MAX_INTERNAL_FRAME_LEN: usize = 2 * PROTO_MAX_BULK_LEN;

//! The memory-accounting seam for the primary→replica feed.
//!
//! A replica feed holds replica-bound bytes in the primary's memory in three
//! places, and only three: the live-dataset blobs staged for a full sync, the
//! backlog handoff tail materialized when streaming starts, and the frames
//! [`crate::FeedSequencer`] holds behind an armed slot-handoff barrier. Those
//! bytes cost the primary exactly as much as a client's queued replies do, and
//! Redis governs them with the same knob — `client-output-buffer-limit slave`.
//!
//! This module is how the feed reports them without knowing any of that. The
//! replication crate owns *which* bytes are feed bytes and what a shed verdict
//! does to the link; the server owns the budget they are charged to, the class
//! limits they are judged against, the clock the soft window runs on and the
//! `omem` an operator reads. The whole of the contract between the two is
//! [`FeedOutputAccount`]: one absolute figure out, one [`FeedVerdict`] back.
//!
//! Reporting an absolute total rather than a delta is deliberate and matches
//! the connection-side seam it is implemented over: a missed report costs one
//! stale figure, where a missed decrement in matched `+`/`-` bookkeeping would
//! drift the charge upward for the life of the process.
//!
//! See `specs/replication.md` FM-REPLICATION-069.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

/// What the account says about a feed that has just reported its size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an unread verdict is a client-output-buffer-limit that does not exist"]
pub enum FeedVerdict {
    /// The feed is within its limits. Carry on.
    Keep,
    /// The feed is over its limits and must be dropped now. `reason` is the
    /// server's own shed-reason label (`"hard_limit"`, `"soft_limit"`,
    /// `"budget_refused"`), carried through for the log line rather than
    /// re-derived here — the replication crate does not model the classes.
    Shed { reason: &'static str },
}

/// The bytes a replica feed is holding, reported to whoever is accounting for
/// them.
///
/// Implemented by the server over the very `OutputBufferAccount` the client
/// connection held before `PSYNC`, so the charge crosses the handoff without a
/// gap and the class policy has one implementation. Implemented in this crate's
/// tests by a recorder, so the seam's own behaviour is forced without a server.
pub trait FeedOutputAccount: fmt::Debug + Send + Sync {
    /// Report the total replica-bound payload bytes this feed currently holds
    /// in memory — an absolute figure, not a delta, and zero when it holds
    /// nothing.
    fn set_buffered(&self, total_bytes: u64) -> FeedVerdict;

    /// The feed is over. Release everything it was charged.
    ///
    /// Called once, from the session's single exit path, on every way a link
    /// can end.
    fn release(&self);

    /// The account's out-of-band way of dropping this link, taken once when the
    /// session starts.
    ///
    /// [`set_buffered`](Self::set_buffered) can only rule on a feed that is
    /// still moving, and the failure mode `client-output-buffer-limit`'s soft
    /// window exists for is a feed that has *stopped* — a replica that stalled
    /// mid-full-sync holds its bytes and reports nothing more, so no later
    /// verdict is ever asked for. The account watches that case on its own
    /// clock and fires this one-shot with the shed reason when the window
    /// expires.
    ///
    /// The session arms it over its socket (see [`ShedGuardedStream`]), so it
    /// reaches a link parked inside a write or a read rather than waiting for
    /// code that is never going to run. An account with no out-of-band shed
    /// returns `None`.
    fn take_shed_signal(&self) -> Option<oneshot::Receiver<&'static str>>;
}

/// The error a shed signal turns into on the socket it was armed over.
fn shed_error(reason: &'static str) -> io::Error {
    io::Error::other(format!(
        "replica feed exceeded its client-output-buffer-limit ({reason}) while its link was idle"
    ))
}

/// A session's socket with its account's shed signal armed over it.
///
/// Every read and write polls the signal first, which is what makes an
/// out-of-band shed reach a session parked in `write_all` on a replica that has
/// stopped reading — the exact position a stalled full sync occupies. Polling
/// the one-shot in place registers the task's waker with it, so the parked
/// future is woken by the shed rather than only noticing it after the socket
/// next moves.
///
/// Wrapping the whole stream, before the session splits it, covers both halves
/// of a streaming link with one signal.
pub struct ShedGuardedStream<S> {
    inner: S,
    /// The signal, until it resolves or its sender is dropped.
    shed: Option<oneshot::Receiver<&'static str>>,
    /// The reason it resolved with, once it has: every subsequent poll fails
    /// the same way rather than re-polling a completed one-shot.
    fired: Option<&'static str>,
}

impl<S> ShedGuardedStream<S> {
    /// Arm `shed` over `inner`.
    pub fn new(inner: S, shed: oneshot::Receiver<&'static str>) -> Self {
        Self {
            inner,
            shed: Some(shed),
            fired: None,
        }
    }

    /// The error this stream is now failing with, if the shed has fired.
    fn poll_shed(&mut self, cx: &mut Context<'_>) -> Option<io::Error> {
        if let Some(reason) = self.fired {
            return Some(shed_error(reason));
        }
        let receiver = self.shed.as_mut()?;
        match Pin::new(receiver).poll(cx) {
            Poll::Ready(Ok(reason)) => {
                self.fired = Some(reason);
                self.shed = None;
                Some(shed_error(reason))
            }
            // The account is gone, so nothing will ever fire: stop watching and
            // let the link live or die on its own I/O.
            Poll::Ready(Err(_)) => {
                self.shed = None;
                None
            }
            Poll::Pending => None,
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for ShedGuardedStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShedGuardedStream")
            .field("inner", &self.inner)
            .field("fired", &self.fired)
            .finish()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ShedGuardedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(error) = this.poll_shed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ShedGuardedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(error) = this.poll_shed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(error) = this.poll_shed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Not guarded: shutting the socket down is how a shed link is closed,
        // so failing it on the shed would leave the socket open.
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(error) = this.poll_shed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A [`FeedOutputAccount`] as the session carries it: one account, shared
/// between the driver and the spawned write task.
pub type SharedFeedAccount = Arc<dyn FeedOutputAccount>;

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    /// A recording account: remembers every figure reported to it, and sheds on
    /// demand.
    ///
    /// `shed_at` is the figure at or above which it starts refusing, standing
    /// in for a class limit; `shed_after` is how many reports it lets past
    /// before it does, which is how the soft window's "not the first crossing"
    /// shape is forced without a clock.
    #[derive(Debug, Default)]
    pub(crate) struct RecordingFeedAccount {
        state: Mutex<RecordingState>,
        shed_at: u64,
        shed_after: usize,
        reason: &'static str,
    }

    #[derive(Debug, Default)]
    struct RecordingState {
        reports: Vec<u64>,
        over_limit_reports: usize,
        released: usize,
        /// The out-of-band shed, until the session takes it or the test fires
        /// it. Both ends are held here so a test can arm one and pull the
        /// trigger later, standing in for the server's re-judge ticker.
        shed_tx: Option<oneshot::Sender<&'static str>>,
        shed_rx: Option<oneshot::Receiver<&'static str>>,
    }

    impl RecordingFeedAccount {
        /// An account that never sheds — for asserting on what gets charged.
        pub(crate) fn unlimited() -> Arc<Self> {
            Arc::new(Self {
                shed_at: u64::MAX,
                ..Self::default()
            })
        }

        /// Sheds `reason` the first time a report reaches `shed_at`.
        pub(crate) fn shedding_at(shed_at: u64, reason: &'static str) -> Arc<Self> {
            Arc::new(Self {
                shed_at,
                shed_after: 0,
                reason,
                ..Self::default()
            })
        }

        /// Sheds `reason` once `shed_after` reports have already been at or
        /// above `shed_at` — the sampled soft window, without a clock.
        pub(crate) fn shedding_after(
            shed_at: u64,
            shed_after: usize,
            reason: &'static str,
        ) -> Arc<Self> {
            Arc::new(Self {
                shed_at,
                shed_after,
                reason,
                ..Self::default()
            })
        }

        /// Every figure reported, in order.
        pub(crate) fn reports(&self) -> Vec<u64> {
            self.state.lock().unwrap().reports.clone()
        }

        /// The largest figure reported, or 0 if nothing was.
        pub(crate) fn peak(&self) -> u64 {
            self.reports().into_iter().max().unwrap_or(0)
        }

        /// How many times the feed released its charge.
        pub(crate) fn releases(&self) -> usize {
            self.state.lock().unwrap().released
        }

        /// An account that never sheds on a report but carries an out-of-band
        /// shed the test fires by hand — the server's re-judge ticker, with the
        /// clock replaced by the test's own timing.
        pub(crate) fn with_shed_signal() -> Arc<Self> {
            let (tx, rx) = oneshot::channel();
            Arc::new(Self {
                shed_at: u64::MAX,
                state: Mutex::new(RecordingState {
                    shed_tx: Some(tx),
                    shed_rx: Some(rx),
                    ..RecordingState::default()
                }),
                ..Self::default()
            })
        }

        /// Fire the out-of-band shed, as the re-judge does when the soft window
        /// expires on a feed that has stopped reporting.
        pub(crate) fn shed_now(&self, reason: &'static str) {
            let sender = self.state.lock().unwrap().shed_tx.take();
            sender
                .expect("armed with with_shed_signal")
                .send(reason)
                .ok();
        }
    }

    impl FeedOutputAccount for RecordingFeedAccount {
        fn set_buffered(&self, total_bytes: u64) -> FeedVerdict {
            let mut state = self.state.lock().unwrap();
            state.reports.push(total_bytes);
            if total_bytes < self.shed_at {
                return FeedVerdict::Keep;
            }
            state.over_limit_reports += 1;
            if state.over_limit_reports > self.shed_after {
                FeedVerdict::Shed {
                    reason: self.reason,
                }
            } else {
                FeedVerdict::Keep
            }
        }

        fn release(&self) {
            self.state.lock().unwrap().released += 1;
        }

        fn take_shed_signal(&self) -> Option<oneshot::Receiver<&'static str>> {
            self.state.lock().unwrap().shed_rx.take()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A sink that reports whatever vectored capability it is built with, so a
    /// test can tell delegation apart from a hardcoded answer.
    struct VectoredSink {
        vectored: bool,
    }

    impl AsyncWrite for VectoredSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }
    }

    // FM-REPLICATION-069
    /// A shed fails a flush as well as a write.
    ///
    /// `write_all` is not the only place a session can be parked: a buffered
    /// writer's flush is the tail of every framed write, and a guard that let
    /// it through would keep a shed link alive for exactly as long as its peer
    /// stayed unreadable.
    #[tokio::test]
    async fn a_shed_fails_the_flush_too() {
        let (tx, rx) = oneshot::channel();
        let (client, _server) = tokio::io::duplex(1024);
        let mut guarded = ShedGuardedStream::new(client, rx);

        tx.send("soft_limit")
            .expect("the stream holds the receiver");
        let error = guarded
            .flush()
            .await
            .expect_err("a shed link must not report a successful flush");
        assert!(
            error.to_string().contains("soft_limit"),
            "the flush must fail naming the limit that shed it; got {error}"
        );
    }

    // FM-REPLICATION-069
    /// Shutdown is deliberately *not* guarded, and must really reach the
    /// socket: closing the link is how a shed is carried out, so a shutdown
    /// that quietly succeeded without shutting anything down would leave the
    /// replica connected to a primary that believes it dropped it.
    #[tokio::test]
    async fn a_guarded_shutdown_still_closes_the_socket() {
        let (tx, rx) = oneshot::channel();
        let (client, mut server) = tokio::io::duplex(1024);
        let mut guarded = ShedGuardedStream::new(client, rx);

        tx.send("hard_limit")
            .expect("the stream holds the receiver");
        guarded
            .shutdown()
            .await
            .expect("a shed must not block the close it exists to cause");

        let mut scratch = [0u8; 8];
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(5), server.read(&mut scratch))
                .await
                .expect("the peer must see the close, not hang waiting for it")
                .expect("a closed duplex reads clean EOF");
        assert_eq!(read, 0, "the peer must be at end of stream");
    }

    // FM-REPLICATION-069
    /// Vectored writes go to the inner stream, and stop when the shed fires.
    #[tokio::test]
    async fn vectored_writes_pass_through_and_are_guarded() {
        let (tx, rx) = oneshot::channel();
        let (client, mut server) = tokio::io::duplex(1024);
        let mut guarded = ShedGuardedStream::new(client, rx);

        let written = guarded
            .write_vectored(&[io::IoSlice::new(b"frame"), io::IoSlice::new(b"tail")])
            .await
            .expect("an unshed stream writes");
        assert!(
            written > 0,
            "a vectored write must report the bytes it actually handed to the socket"
        );
        let mut scratch = vec![0u8; written];
        server
            .read_exact(&mut scratch)
            .await
            .expect("what was reported written must be readable at the peer");
        assert_eq!(
            &scratch,
            &b"frametail"[..written],
            "the payload must arrive in order and unaltered"
        );

        tx.send("soft_limit")
            .expect("the stream holds the receiver");
        let error = guarded
            .write_vectored(&[io::IoSlice::new(b"more")])
            .await
            .expect_err("a shed link must not accept another vectored write");
        assert!(
            error.to_string().contains("soft_limit"),
            "the write must fail naming the limit that shed it; got {error}"
        );
    }

    // FM-REPLICATION-069
    /// The wrapper reports the inner stream's vectored capability rather than
    /// an answer of its own: a `TcpStream` writes vectored and a duplex does
    /// not, and callers pick their write path off this.
    #[test]
    fn the_vectored_capability_is_the_inner_stream_s() {
        for vectored in [true, false] {
            let (_tx, rx) = oneshot::channel();
            let guarded = ShedGuardedStream::new(VectoredSink { vectored }, rx);
            assert_eq!(
                guarded.is_write_vectored(),
                vectored,
                "the wrapper must answer for the stream it wraps, not for itself"
            );
        }
    }
}

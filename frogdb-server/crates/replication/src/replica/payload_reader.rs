//! The buffered reader every full-sync payload path reads through.
//!
//! A full sync ends at the `FullSyncMetadata` trailer, but the socket does not:
//! the primary starts streaming live WAL frames the instant the payload is
//! written, so the trailer and the first frames routinely arrive in the *same*
//! TCP segment. Reading the payload through a plain `BufReader` therefore ends
//! with those frames sitting in the reader's buffer, and dropping the reader
//! throws them away — the socket has no more bytes to re-read them from, so the
//! streaming loop never decodes them, never applies them and never ACKs them.
//! The replica's offset stays permanently short of the primary's while the link
//! reports `up` (hardening issue 01).
//!
//! [`PayloadReader`] makes that unrepresentable: it owns the `BufReader`, and
//! whatever the buffering read past the payload is handed back to the
//! connection's residual buffer on drop — on every exit path, including the `?`
//! returns of a failed sync. The streaming loop seeds its decode buffer from
//! that residual, so "the payload path stops reading" and "the stream starts
//! reading" meet on the same byte.

use crate::BoxedStream;
use bytes::BytesMut;
use std::ops::{Deref, DerefMut};
use tokio::io::BufReader;

/// A [`BufReader`] over the replica's socket that cannot lose the bytes it read
/// past the payload it was asked for.
///
/// Deref-transparent: the payload paths drive it exactly like the `BufReader`
/// it wraps (`AsyncRead` + `AsyncBufRead` via `&mut *reader`). The only added
/// behaviour is in [`Drop`].
pub(crate) struct PayloadReader<'a> {
    reader: BufReader<&'a mut BoxedStream>,
    /// Where the unconsumed tail goes when this reader is dropped — the
    /// connection's `pending_stream_bytes`, which the streaming loop drains.
    residual: &'a mut BytesMut,
}

impl<'a> PayloadReader<'a> {
    /// Wrap `stream` for the duration of one payload, spilling whatever it
    /// over-reads into `residual`.
    ///
    /// Built through [`ReplicaConnection::payload_reader`] so the two halves
    /// always come from the same connection.
    ///
    /// [`ReplicaConnection::payload_reader`]: super::connection::ReplicaConnection::payload_reader
    pub(crate) fn new(stream: &'a mut BoxedStream, residual: &'a mut BytesMut) -> Self {
        Self {
            reader: BufReader::new(stream),
            residual,
        }
    }
}

impl<'a> Deref for PayloadReader<'a> {
    type Target = BufReader<&'a mut BoxedStream>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl DerefMut for PayloadReader<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl Drop for PayloadReader<'_> {
    fn drop(&mut self) {
        // `buffer()` is exactly the bytes read off the socket and not yet
        // consumed by the payload decode: the live tail that arrived alongside
        // the trailer. Append rather than replace — a sync that ran two payload
        // readers must not lose the first one's tail.
        let unconsumed = self.reader.buffer();
        if !unconsumed.is_empty() {
            self.residual.extend_from_slice(unconsumed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // FM-REPLICATION-005
    /// The mechanism in isolation: bytes the reader buffered past what its
    /// caller consumed survive the reader's drop.
    #[tokio::test]
    async fn dropping_the_reader_hands_back_what_it_read_past_the_payload() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        client
            .write_all(b"PAYLOADtrailing-live-frames")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let mut stream: BoxedStream = Box::new(server);
        let mut residual = BytesMut::new();
        {
            let mut reader = PayloadReader::new(&mut stream, &mut residual);
            let mut payload = [0u8; 7];
            reader.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"PAYLOAD");
        }

        assert_eq!(
            &residual[..],
            b"trailing-live-frames",
            "the over-read tail is handed back, not dropped with the reader"
        );
    }

    // FM-REPLICATION-005
    /// A reader that consumed everything it buffered leaves nothing behind, so
    /// the streaming loop is not seeded with phantom bytes.
    #[tokio::test]
    async fn a_fully_consumed_reader_leaves_no_residual() {
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        client.write_all(b"PAYLOAD").await.unwrap();
        client.shutdown().await.unwrap();

        let mut stream: BoxedStream = Box::new(server);
        let mut residual = BytesMut::new();
        {
            let mut reader = PayloadReader::new(&mut stream, &mut residual);
            let mut payload = [0u8; 7];
            reader.read_exact(&mut payload).await.unwrap();
        }

        assert!(residual.is_empty());
    }
}

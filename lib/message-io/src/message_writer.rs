use std::future::Future;

use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{LENGTH_PREFIX_LEN, MessageWriteError, bounded_frame::BoundedFrame};

/// Async framed Postcard serialization on an exclusively borrowed destination.
///
/// Blanket-implemented for `AsyncWrite + Unpin`, including unsized destinations.
/// Futures are concrete; no additional `Send` or `Sync` bound is imposed.
pub trait MessageWriteExt: AsyncWrite + Unpin {
    /// Encodes a bounded complete frame, then writes all of it without flushing.
    ///
    /// Encoding errors (including an oversized body) write nothing to the
    /// destination. Success means `AsyncWrite` accepted the frame, which may
    /// still be buffered. It guarantees no flush, durability or peer acknowledgement.
    ///
    /// # Errors and cancellation
    ///
    /// An I/O error, panic or cancellation of a polled write can leave partial
    /// progress. Discard the connection rather than retrying on the same stream.
    /// Encoding errors occur before I/O and permit reuse of the destination.
    /// Dropping an unpolled future performs no serialization or I/O.
    fn write_message<T: Serialize + ?Sized>(
        &mut self,
        message: &T,
    ) -> impl Future<Output = Result<(), MessageWriteError>> {
        async move {
            let mut frame = vec![0; LENGTH_PREFIX_LEN];
            let mut too_large = false;
            let result = postcard::serialize_with_flavor(
                message,
                BoundedFrame {
                    bytes: &mut frame,
                    too_large: &mut too_large,
                },
            );
            if too_large {
                return Err(MessageWriteError::TooLarge);
            }
            result.map_err(MessageWriteError::Encode)?;
            let body_len = (frame.len() - LENGTH_PREFIX_LEN) as u32;
            frame[..LENGTH_PREFIX_LEN].copy_from_slice(&body_len.to_le_bytes());
            self.write_all(&frame).await?;
            Ok(())
        }
    }
}

impl<W: AsyncWrite + Unpin + ?Sized> MessageWriteExt for W {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serializer;
    use std::{
        cell::Cell,
        io,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    #[derive(Clone, Copy)]
    enum Stop {
        Error,
        Zero,
        Pending,
        Panic,
    }

    struct Destination {
        bytes: Vec<u8>,
        stop_at: usize,
        stop: Stop,
        flushes: usize,
    }

    impl Destination {
        fn new(stop_at: usize, stop: Stop) -> Self {
            Self {
                bytes: Vec::new(),
                stop_at,
                stop,
                flushes: 0,
            }
        }
    }

    impl AsyncWrite for Destination {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.bytes.len() >= self.stop_at {
                return match self.stop {
                    Stop::Error => Poll::Ready(Err(io::Error::from_raw_os_error(4321))),
                    Stop::Zero => Poll::Ready(Ok(0)),
                    Stop::Pending => Poll::Pending,
                    Stop::Panic => panic!("destination panic"),
                };
            }
            // Force one-byte acceptance, exercising partial-write handling.
            self.bytes.push(bytes[0]);
            Poll::Ready(Ok(1))
        }
        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn writes_literal_frames_with_partial_acceptance_and_no_implicit_flush() {
        let mut writer = Destination::new(usize::MAX, Stop::Error);
        writer.write_message("abc").await.unwrap();
        writer.write_message(&true).await.unwrap();
        writer.write_message(&()).await.unwrap();
        assert_eq!(
            writer.bytes,
            [4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 1, 0, 0, 0, 0]
        );
        assert_eq!(writer.flushes, 0);
        writer.flush().await.unwrap();
        assert_eq!(writer.flushes, 1);
    }

    #[tokio::test]
    async fn accepts_exact_limit_and_rejects_oversize_before_io() {
        let mut writer = Vec::new();
        writer.write_message(&"a".repeat(1_048_573)).await.unwrap();
        assert_eq!(&writer[..7], &[0, 0, 16, 0, 253, 255, 63]);
        assert_eq!(writer.len(), 1_048_580);
        assert!(writer[7..].iter().all(|byte| *byte == b'a'));
        let before = writer.len();
        assert!(matches!(
            writer.write_message(&"a".repeat(1_048_574)).await,
            Err(MessageWriteError::TooLarge)
        ));
        assert_eq!(writer.len(), before);
        // Sequence encoding exercises single-byte appends at the limit, rather
        // than only the large slice append used for strings.
        assert!(matches!(
            writer.write_message(&vec![true; 1_048_576]).await,
            Err(MessageWriteError::TooLarge)
        ));
        assert_eq!(writer.len(), before);
    }

    #[tokio::test]
    async fn encoder_failure_leaves_destination_reusable() {
        struct Rejected;
        impl Serialize for Rejected {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("intentional encoding failure"))
            }
        }
        let mut writer = Vec::new();
        assert!(matches!(
            writer.write_message(&Rejected).await,
            Err(MessageWriteError::Encode(_))
        ));
        assert!(writer.is_empty());
        writer.write_message(&true).await.unwrap();
        assert_eq!(writer, [1, 0, 0, 0, 1]);
    }

    #[tokio::test]
    async fn preserves_io_error_and_write_zero_after_partial_acceptance() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        for stop_at in 0..8 {
            for stop in [Stop::Error, Stop::Zero] {
                let mut writer = Destination::new(stop_at, stop);
                let MessageWriteError::Io(error) = writer.write_message("abc").await.unwrap_err()
                else {
                    panic!()
                };
                match stop {
                    Stop::Error => assert_eq!(error.raw_os_error(), Some(4321)),
                    Stop::Zero => assert_eq!(error.kind(), io::ErrorKind::WriteZero),
                    _ => unreachable!(),
                }
                assert_eq!(writer.bytes, fixture[..stop_at]);
            }
        }
    }

    #[test]
    fn cancellation_and_panics_preserve_partial_acceptance() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        let mut context = Context::from_waker(Waker::noop());
        for stop_at in 0..8 {
            let mut writer = Destination::new(stop_at, Stop::Pending);
            {
                let mut future = std::pin::pin!(writer.write_message("abc"));
                assert!(future.as_mut().poll(&mut context).is_pending());
            }
            assert_eq!(writer.bytes, fixture[..stop_at]);
            let mut writer = Destination::new(stop_at, Stop::Panic);
            {
                let mut future = std::pin::pin!(writer.write_message("abc"));
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future
                        .as_mut()
                        .poll(&mut context)))
                    .is_err()
                );
            }
            assert_eq!(writer.bytes, fixture[..stop_at]);
            // These destinations are discarded; no unsafe replay is attempted.
        }
    }

    #[tokio::test]
    async fn unpolled_future_does_no_work_and_non_send_message_is_supported() {
        struct Counted(Rc<Cell<usize>>);
        impl Serialize for Counted {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_bool(true)
            }
        }
        let calls = Rc::new(Cell::new(0));
        let message = Counted(calls.clone());
        let mut writer = Vec::new();
        drop(writer.write_message(&message));
        assert_eq!(calls.get(), 0);
        assert!(writer.is_empty());
        let destination: &mut (dyn AsyncWrite + Unpin) = &mut writer;
        destination.write_message(&message).await.unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(writer, [1, 0, 0, 0, 1]);
    }

    struct YieldingDestination {
        bytes: Vec<u8>,
        chunk: usize,
        yield_next: bool,
        yields: usize,
    }

    impl AsyncWrite for YieldingDestination {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.yield_next {
                self.yield_next = false;
                self.yields += 1;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let count = bytes.len().min(self.chunk);
            self.bytes.extend_from_slice(&bytes[..count]);
            self.yield_next = true;
            Poll::Ready(Ok(count))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn resumes_after_wakes_without_reencoding_or_replaying_bytes() {
        struct Counted<'a>(&'a Cell<usize>);
        impl Serialize for Counted<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_str("abc")
            }
        }
        for chunk in 1..=8 {
            let calls = Cell::new(0);
            let mut writer = YieldingDestination {
                bytes: Vec::new(),
                chunk,
                yield_next: true,
                yields: 0,
            };
            writer.write_message(&Counted(&calls)).await.unwrap();
            writer.write_message(&false).await.unwrap();
            assert_eq!(
                writer.bytes,
                [4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 0]
            );
            assert_eq!(calls.get(), 1);
            assert!(writer.yields >= 2);
        }
    }

    #[tokio::test]
    async fn encoder_error_after_partial_serialization_writes_nothing() {
        use serde::ser::SerializeTuple;
        struct PartialFailure;
        impl Serialize for PartialFailure {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element("already encoded")?;
                Err(serde::ser::Error::custom("late failure"))
            }
        }
        let mut writer = vec![99];
        let error = writer.write_message(&PartialFailure).await.unwrap_err();
        assert!(matches!(error, MessageWriteError::Encode(_)));
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<postcard::Error>()
                .is_some()
        );
        assert_eq!(writer, [99]);
        writer.write_message(&false).await.unwrap();
        assert_eq!(writer, [99, 1, 0, 0, 0, 0]);
    }

    #[test]
    fn encoder_panic_after_partial_serialization_never_touches_destination() {
        use serde::ser::SerializeTuple;
        struct PartialPanic;
        impl Serialize for PartialPanic {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&true)?;
                panic!("encoder panic")
            }
        }
        let mut writer = vec![99];
        let mut context = Context::from_waker(Waker::noop());
        {
            let mut future = std::pin::pin!(writer.write_message(&PartialPanic));
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future
                    .as_mut()
                    .poll(&mut context)))
                .is_err()
            );
        }
        assert_eq!(writer, [99]);
    }

    #[tokio::test]
    async fn distinguishes_custom_buffer_error_from_actual_overflow_even_when_swallowed() {
        use serde::ser::SerializeTuple;
        struct BufferFailure;
        impl Serialize for BufferFailure {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("buffer failure"))
            }
        }
        struct SwallowedOverflow<'a>(&'a str);
        impl Serialize for SwallowedOverflow<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut tuple = serializer.serialize_tuple(1)?;
                let _ = tuple.serialize_element(self.0);
                tuple.end()
            }
        }
        let mut writer = Vec::new();
        assert!(matches!(
            writer.write_message(&BufferFailure).await,
            Err(MessageWriteError::Encode(_))
        ));
        assert!(matches!(
            writer
                .write_message(&SwallowedOverflow(&"a".repeat(1_048_576)))
                .await,
            Err(MessageWriteError::TooLarge)
        ));
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn successful_message_can_remain_buffered_until_explicit_flush() {
        let mut writer = tokio::io::BufWriter::with_capacity(64, Vec::new());
        writer.write_message("abc").await.unwrap();
        writer.write_message(&true).await.unwrap();
        assert!(writer.get_ref().is_empty());
        assert_eq!(
            writer.buffer(),
            &[4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 1]
        );
        writer.flush().await.unwrap();
        assert!(writer.buffer().is_empty());
        assert_eq!(
            writer.get_ref(),
            &[4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 1]
        );
    }

    #[tokio::test]
    async fn flush_failure_is_separate_from_successful_message_acceptance() {
        let destination = Destination::new(3, Stop::Error);
        let mut writer = tokio::io::BufWriter::with_capacity(64, destination);
        writer.write_message("abc").await.unwrap();
        assert!(writer.get_ref().bytes.is_empty());
        let error = writer.flush().await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(4321));
        assert_eq!(writer.get_ref().bytes, [4, 0, 0]);
        // The connection owner now discards the buffered destination.
    }

    #[tokio::test]
    async fn writes_independent_numeric_and_nested_wire_fixtures() {
        for (value, expected) in [
            (0u64, &[1, 0, 0, 0, 0][..]),
            (127, &[1, 0, 0, 0, 127][..]),
            (128, &[2, 0, 0, 0, 128, 1][..]),
            (16_384, &[3, 0, 0, 0, 128, 128, 1][..]),
            (
                u64::MAX,
                &[10, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1][..],
            ),
        ] {
            let mut writer = Vec::new();
            writer.write_message(&value).await.unwrap();
            assert_eq!(writer, expected);
        }
        let mut writer = Vec::new();
        writer
            .write_message(&(128u16, Some("hi"), vec![true, false, true]))
            .await
            .unwrap();
        assert_eq!(writer, [10, 0, 0, 0, 128, 1, 1, 2, b'h', b'i', 3, 1, 0, 1]);
    }

    #[tokio::test]
    async fn duplex_backpressure_preserves_many_consecutive_messages_and_clean_eof() {
        use crate::{MessageReadError, MessageReadExt};
        for capacity in [1, 3, 4, 17] {
            let (mut sender, mut receiver) = tokio::io::duplex(capacity);
            let operation = async {
                tokio::join!(
                    async {
                        for index in 0u32..32 {
                            let length = [0, 1, 127, 128, 255, 1024][index as usize % 6];
                            let text = "x".repeat(length);
                            sender
                                .write_message(&(index, text, Some(index % 2 == 0)))
                                .await
                                .unwrap();
                        }
                        sender.shutdown().await.unwrap();
                    },
                    async {
                        for expected in 0u32..32 {
                            let (index, text, flag) = receiver
                                .read_message::<(u32, String, Option<bool>)>()
                                .await
                                .unwrap();
                            let length = [0, 1, 127, 128, 255, 1024][expected as usize % 6];
                            assert_eq!(index, expected);
                            assert_eq!(text, "x".repeat(length));
                            assert_eq!(flag, Some(expected % 2 == 0));
                        }
                        assert!(matches!(
                            receiver.read_message::<bool>().await,
                            Err(MessageReadError::EndOfStream)
                        ));
                    }
                );
            };
            tokio::time::timeout(std::time::Duration::from_secs(10), operation)
                .await
                .expect("duplex exchange stalled");
        }
    }

    #[tokio::test]
    async fn broken_duplex_peer_preserves_transport_error() {
        let (mut writer, reader) = tokio::io::duplex(1);
        drop(reader);
        let MessageWriteError::Io(error) = writer.write_message(&true).await.unwrap_err() else {
            panic!()
        };
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn pinned_non_unpin_destination_is_supported_without_unsafe() {
        use std::{cell::RefCell, marker::PhantomPinned};
        struct PinnedWriter {
            bytes: RefCell<Vec<u8>>,
            _pin: PhantomPinned,
        }
        impl AsyncWrite for PinnedWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.as_ref()
                    .get_ref()
                    .bytes
                    .borrow_mut()
                    .extend_from_slice(bytes);
                Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let mut writer = std::pin::pin!(PinnedWriter {
            bytes: RefCell::new(Vec::new()),
            _pin: PhantomPinned
        });
        writer.write_message(&true).await.unwrap();
        assert_eq!(*writer.bytes.borrow(), [1, 0, 0, 0, 1]);
    }
}

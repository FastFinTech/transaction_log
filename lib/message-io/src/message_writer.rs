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
        for stop_at in [0, 1, 3, 4, 6] {
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
        for stop_at in [0, 1, 3, 4, 6] {
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
}

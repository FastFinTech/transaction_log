use std::future::Future;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{LENGTH_PREFIX_LEN, MAX_MESSAGE_BODY_LEN, MessageReadError};

/// Async message decoding on an exclusively borrowed, unpinned byte source.
///
/// Blanket-implemented for `AsyncRead + Unpin`, including unsized sources and
/// pinned references implementing those bounds. Futures are concrete and have
/// no additional `Send` bound. The caller chooses the expected Serde type.
pub trait MessageReadExt: AsyncRead + Unpin {
    /// Reads a bounded length prefix and exactly one Postcard-encoded owned value.
    ///
    /// `EndOfStream` means EOF before the next prefix. Partial prefix/body EOF
    /// preserves an I/O `UnexpectedEof`; oversized lengths are rejected before
    /// allocating or reading the body. Trailing body bytes are rejected.
    ///
    /// The temporary body buffer is dropped before returning; `DeserializeOwned`
    /// prevents a returned message from borrowing it. This method does not validate
    /// application semantics beyond the supplied type's deserializer.
    ///
    /// # Errors and cancellation
    ///
    /// After a read error, panic or cancellation of a polled operation, discard
    /// the source/connection. It may be mid-frame and the extension has no retained
    /// progress or poison flag. Dropping an unpolled future performs no work.
    fn read_message<T: DeserializeOwned>(
        &mut self,
    ) -> impl Future<Output = Result<T, MessageReadError>> {
        async move {
            let mut prefix = [0; LENGTH_PREFIX_LEN];
            if self.read(&mut prefix[..1]).await? == 0 {
                return Err(MessageReadError::EndOfStream);
            }
            self.read_exact(&mut prefix[1..]).await?;
            let body_len = u32::from_le_bytes(prefix);
            if body_len > MAX_MESSAGE_BODY_LEN as u32 {
                return Err(MessageReadError::TooLarge { length: body_len });
            }
            let mut body = vec![0; body_len as usize];
            self.read_exact(&mut body).await?;
            let (message, remainder) =
                postcard::take_from_bytes(&body).map_err(MessageReadError::Decode)?;
            if !remainder.is_empty() {
                return Err(MessageReadError::TrailingBytes {
                    count: remainder.len(),
                });
            }
            Ok(message)
        }
    }
}

impl<R: AsyncRead + Unpin + ?Sized> MessageReadExt for R {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use tokio::io::ReadBuf;

    struct Fragmented<'a> {
        bytes: &'a [u8],
        consumed: usize,
        stop_at: usize,
        fail: bool,
    }

    impl AsyncRead for Fragmented<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.consumed >= self.stop_at {
                return if self.fail {
                    Poll::Ready(Err(io::Error::from_raw_os_error(4321)))
                } else {
                    Poll::Pending
                };
            }
            if let Some(byte) = self.bytes.get(self.consumed).copied() {
                output.put_slice(&[byte]);
                self.consumed += 1;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn decodes_literal_fragmented_frames_without_consuming_next_frame() {
        // Postcard string: one-byte length 3 followed by UTF-8. The prefix
        // counts four body bytes. Second message is bool true, followed by unit.
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 1, 0, 0, 0, 0];
        let first = {
            let mut reader = Fragmented {
                bytes: &fixture,
                consumed: 0,
                stop_at: usize::MAX,
                fail: false,
            };
            let first = reader.read_message::<String>().await.unwrap();
            assert_eq!(reader.consumed, 8);
            assert!(reader.read_message::<bool>().await.unwrap());
            reader.read_message::<()>().await.unwrap();
            assert!(matches!(
                reader.read_message::<bool>().await,
                Err(MessageReadError::EndOfStream)
            ));
            first
        };
        assert_eq!(first, "abc");
    }

    #[tokio::test]
    async fn distinguishes_clean_eof_and_every_truncated_prefix_or_body() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        let mut empty = &[][..];
        assert!(matches!(
            empty.read_message::<String>().await,
            Err(MessageReadError::EndOfStream)
        ));
        for length in 1..fixture.len() {
            let mut reader = &fixture[..length];
            let MessageReadError::Io(error) = reader.read_message::<String>().await.unwrap_err()
            else {
                panic!("length {length}")
            };
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    #[tokio::test]
    async fn rejects_oversize_before_consuming_body_and_accepts_exact_limit() {
        // Literal 1 MiB + 1 length, then one body byte that must remain unread.
        let mut reader = &[1, 0, 16, 0, 99][..];
        assert!(matches!(
            reader.read_message::<()>().await,
            Err(MessageReadError::TooLarge { length: 1_048_577 })
        ));
        assert_eq!(reader, &[99]);
        let mut reader = &[255, 255, 255, 255][..];
        assert!(matches!(
            reader.read_message::<()>().await,
            Err(MessageReadError::TooLarge { length: u32::MAX })
        ));
        // Literal Postcard varint length 1,048,573 + that many ASCII bytes.
        let mut fixture = vec![0, 0, 16, 0, 253, 255, 63];
        fixture.resize(1_048_580, b'a');
        let mut reader = fixture.as_slice();
        assert_eq!(
            reader.read_message::<String>().await.unwrap().len(),
            1_048_573
        );
        assert!(reader.is_empty());
    }

    #[tokio::test]
    async fn rejects_malformed_postcard_and_trailing_body_bytes() {
        let mut invalid_bool = &[1, 0, 0, 0, 2][..];
        assert!(matches!(
            invalid_bool.read_message::<bool>().await,
            Err(MessageReadError::Decode(_))
        ));
        let mut missing_bool = &[0, 0, 0, 0][..];
        assert!(matches!(
            missing_bool.read_message::<bool>().await,
            Err(MessageReadError::Decode(_))
        ));
        let mut trailing = &[2, 0, 0, 0, 1, 42][..];
        assert!(matches!(
            trailing.read_message::<bool>().await,
            Err(MessageReadError::TrailingBytes { count: 1 })
        ));
    }

    #[tokio::test]
    async fn preserves_transport_errors_after_partial_progress() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        for stop_at in [0, 1, 3, 4, 6] {
            let mut reader = Fragmented {
                bytes: &fixture,
                consumed: 0,
                stop_at,
                fail: true,
            };
            let MessageReadError::Io(error) = reader.read_message::<String>().await.unwrap_err()
            else {
                panic!()
            };
            assert_eq!(error.raw_os_error(), Some(4321));
            assert_eq!(reader.consumed, stop_at);
        }
    }

    #[test]
    fn dropping_unpolled_or_pending_read_preserves_actual_partial_consumption() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        let mut context = Context::from_waker(Waker::noop());
        for stop_at in 0..8 {
            let mut reader = Fragmented {
                bytes: &fixture,
                consumed: 0,
                stop_at,
                fail: false,
            };
            drop(reader.read_message::<String>());
            assert_eq!(reader.consumed, 0);
            {
                let mut future = std::pin::pin!(reader.read_message::<String>());
                assert!(future.as_mut().poll(&mut context).is_pending());
            }
            assert_eq!(reader.consumed, stop_at);
            // The caller discards this source; no invalid retry is attempted.
        }
    }

    #[tokio::test]
    async fn accepts_unsized_reader_and_preserves_domain_deserializer_errors() {
        use serde::{Deserialize, Deserializer, de::Error};
        #[derive(Debug)]
        struct Validated;
        impl<'de> Deserialize<'de> for Validated {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _ = bool::deserialize(deserializer)?;
                Err(D::Error::custom("domain rejection"))
            }
        }
        let mut input = &[1, 0, 0, 0, 1][..];
        let reader: &mut (dyn AsyncRead + Unpin) = &mut input;
        assert!(matches!(
            reader.read_message::<Validated>().await,
            Err(MessageReadError::Decode(_))
        ));
    }

    struct YieldingReader<'a> {
        bytes: &'a [u8],
        consumed: usize,
        chunk: usize,
        yield_next: bool,
        yields: usize,
    }

    impl AsyncRead for YieldingReader<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.yield_next {
                self.yield_next = false;
                self.yields += 1;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let count = (self.bytes.len() - self.consumed)
                .min(output.remaining())
                .min(self.chunk);
            output.put_slice(&self.bytes[self.consumed..self.consumed + count]);
            self.consumed += count;
            self.yield_next = true;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn resumes_after_wakes_across_prefix_body_and_message_boundaries() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 0];
        for chunk in 1..=fixture.len() {
            let mut reader = YieldingReader {
                bytes: &fixture,
                consumed: 0,
                chunk,
                yield_next: true,
                yields: 0,
            };
            assert_eq!(reader.read_message::<String>().await.unwrap(), "abc");
            assert_eq!(reader.consumed, 8);
            assert!(!reader.read_message::<bool>().await.unwrap());
            assert_eq!(reader.consumed, 13);
            assert!(reader.yields >= 4);
            assert!(matches!(
                reader.read_message::<bool>().await,
                Err(MessageReadError::EndOfStream)
            ));
        }
    }

    #[tokio::test]
    async fn buffered_reader_prefetch_keeps_next_frame_available() {
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 1];
        for capacity in [1, 3, 4, 8, 64] {
            let mut reader = tokio::io::BufReader::with_capacity(capacity, fixture.as_slice());
            assert_eq!(reader.read_message::<String>().await.unwrap(), "abc");
            assert!(reader.read_message::<bool>().await.unwrap());
            assert!(matches!(
                reader.read_message::<bool>().await,
                Err(MessageReadError::EndOfStream)
            ));
        }
    }

    fn frame(body: &[u8]) -> Vec<u8> {
        // Independent framing helper; never calls the production writer/encoder.
        let mut fixture = (body.len() as u32).to_le_bytes().to_vec();
        fixture.extend_from_slice(body);
        fixture
    }

    async fn decode_error<T: DeserializeOwned>(body: &[u8]) {
        let fixture = frame(body);
        let error = fixture
            .as_slice()
            .read_message::<T>()
            .await
            .err()
            .expect("malformed value accepted");
        assert!(matches!(error, MessageReadError::Decode(_)), "{error:?}");
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<postcard::Error>()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rejects_internal_length_mismatches_invalid_utf8_and_bad_varints() {
        decode_error::<String>(&[3, b'a', b'b']).await;
        decode_error::<String>(&[1, 255]).await;
        decode_error::<String>(&[128]).await; // Incomplete length varint.
        decode_error::<String>(&[255; 10]).await; // Overlong length varint.
        decode_error::<Vec<u16>>(&[2, 1]).await; // Missing second element.
        decode_error::<u64>(&[128]).await;
        decode_error::<u64>(&[255; 10]).await;
        decode_error::<u16>(&[255, 255, 4]).await; // Overflow in last varint byte.
        decode_error::<Option<u8>>(&[2]).await;
        decode_error::<char>(&[255]).await;
    }

    #[tokio::test]
    async fn rejects_unknown_enum_variant_and_every_invalid_bool_byte() {
        #[derive(serde::Deserialize)]
        enum Choice {
            First,
            Second,
        }
        decode_error::<Choice>(&[2]).await;
        decode_error::<Choice>(&[128]).await;
        for byte in 0u8..=255 {
            let fixture = [1, 0, 0, 0, byte];
            let result = fixture.as_slice().read_message::<bool>().await;
            if byte <= 1 {
                assert_eq!(result.unwrap(), byte == 1);
            } else {
                assert!(matches!(result, Err(MessageReadError::Decode(_))), "{byte}");
            }
        }
    }

    #[tokio::test]
    async fn decodes_independent_numeric_and_nested_fixtures_at_varint_boundaries() {
        for (body, expected) in [
            (&[0][..], 0u64),
            (&[127][..], 127),
            (&[128, 1][..], 128),
            (&[255, 127][..], 16_383),
            (&[128, 128, 1][..], 16_384),
            (
                &[255, 255, 255, 255, 255, 255, 255, 255, 255, 1][..],
                u64::MAX,
            ),
        ] {
            assert_eq!(
                frame(body).as_slice().read_message::<u64>().await.unwrap(),
                expected
            );
        }
        for (body, expected) in [
            (&[0][..], 0i64),
            (&[1][..], -1),
            (&[2][..], 1),
            (
                &[255, 255, 255, 255, 255, 255, 255, 255, 255, 1][..],
                i64::MIN,
            ),
            (
                &[254, 255, 255, 255, 255, 255, 255, 255, 255, 1][..],
                i64::MAX,
            ),
        ] {
            assert_eq!(
                frame(body).as_slice().read_message::<i64>().await.unwrap(),
                expected
            );
        }
        // Tuple: u16 128, Some("hi"), vector [true, false, true].
        let fixture = [10, 0, 0, 0, 128, 1, 1, 2, b'h', b'i', 3, 1, 0, 1];
        let actual = fixture
            .as_slice()
            .read_message::<(u16, Option<String>, Vec<bool>)>()
            .await
            .unwrap();
        assert_eq!(actual, (128, Some("hi".into()), vec![true, false, true]));
    }

    #[tokio::test]
    async fn malformed_or_trailing_body_never_consumes_following_frame() {
        for bad in [vec![1, 0, 0, 0, 2], vec![3, 0, 0, 0, 1, 99, 100]] {
            let mut fixture = bad;
            fixture.extend_from_slice(&[1, 0, 0, 0, 1]);
            let mut reader = fixture.as_slice();
            let error = reader.read_message::<bool>().await.unwrap_err();
            assert!(matches!(
                error,
                MessageReadError::Decode(_) | MessageReadError::TrailingBytes { .. }
            ));
            assert_eq!(reader, &[1, 0, 0, 0, 1]);
            // The source is discarded per contract; do not reuse after the error.
        }
    }

    #[test]
    fn transport_and_deserializer_panics_leave_actual_consumption_visible() {
        struct Panics<'a>(Fragmented<'a>);
        impl AsyncRead for Panics<'_> {
            fn poll_read(
                mut self: Pin<&mut Self>,
                context: &mut Context<'_>,
                output: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                assert!(self.0.consumed < self.0.stop_at, "reader panic");
                Pin::new(&mut self.0).poll_read(context, output)
            }
        }
        let fixture = [4, 0, 0, 0, 3, b'a', b'b', b'c'];
        let mut context = Context::from_waker(Waker::noop());
        for stop_at in 0..fixture.len() {
            let mut reader = Panics(Fragmented {
                bytes: &fixture,
                consumed: 0,
                stop_at,
                fail: false,
            });
            {
                let mut future = std::pin::pin!(reader.read_message::<String>());
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future
                        .as_mut()
                        .poll(&mut context)))
                    .is_err()
                );
            }
            assert_eq!(reader.0.consumed, stop_at);
        }
        struct PanicValue;
        impl<'de> serde::Deserialize<'de> for PanicValue {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let _ = bool::deserialize(deserializer)?;
                panic!("deserializer panic")
            }
        }
        let mut reader = &[1, 0, 0, 0, 1, 99][..];
        {
            let mut future = std::pin::pin!(reader.read_message::<PanicValue>());
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future
                    .as_mut()
                    .poll(&mut context)))
                .is_err()
            );
        }
        assert_eq!(reader, &[99]);
    }

    #[tokio::test]
    async fn pinned_non_unpin_source_is_supported_without_unsafe() {
        use std::{cell::Cell, marker::PhantomPinned};
        struct PinnedReader {
            bytes: &'static [u8],
            consumed: Cell<usize>,
            _pin: PhantomPinned,
        }
        impl AsyncRead for PinnedReader {
            fn poll_read(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                output: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                let this = self.as_ref().get_ref();
                let start = this.consumed.get();
                let count = output.remaining().min(this.bytes.len() - start);
                output.put_slice(&this.bytes[start..start + count]);
                this.consumed.set(start + count);
                Poll::Ready(Ok(()))
            }
        }
        let mut reader = std::pin::pin!(PinnedReader {
            bytes: &[1, 0, 0, 0, 1],
            consumed: Cell::new(0),
            _pin: PhantomPinned
        });
        assert!(reader.read_message::<bool>().await.unwrap());
        assert_eq!(reader.consumed.get(), 5);
    }
}

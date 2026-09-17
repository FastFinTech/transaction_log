//! Buffered reading and complete wire validation for transaction log records.
//!
//! See the [`record`](crate::record) specification for the shared wire format,
//! immutable record invariants, and the reasons for validating and splitting
//! complete batches before returning records.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::record::{Record, StreamId, StreamIdError, record_protocol as protocol};

const INITIAL_BUFFER_CAPACITY: usize = 64 * 1024;

/// Reads fully validated records from an async socket, file, or other byte source.
///
/// The reader owns a reusable receive buffer. Each wait validates the complete
/// record prefix, stopping at incomplete or invalid input, then splits and freezes
/// that prefix once. Any tail stays in the receive buffer; invalid input is reported
/// on the next wait after the valid batch is drained. Draining the validated batch
/// creates shared byte handles without repeating validation or copying records.
/// Returned records remain valid after the reader is dropped or its buffer is
/// replaced. Refilling may move an unfinished tail.
///
/// Each encoded record is at most [`protocol::MAX_RECORD_LEN`] bytes, including
/// its header and CRC trailer. This protocol limit is fixed. The receive buffer
/// can contain multiple records at once.
/// Validation covers framing, the stream ID range, and CRC, not application
/// rules such as sequence ordering or whether a stream ID is authorized.
///
/// ```
/// use transaction_log_exports::{RecordReadError, RecordReader};
///
/// # async fn example() -> Result<(), RecordReadError> {
/// let source = &b""[..]; // Any Tokio AsyncRead + Unpin, including sockets/files.
/// let mut reader = RecordReader::new(source);
/// while reader.wait_to_read().await? {
///     while let Some(record) = reader.try_read_next()? {
///         let header = record.get_header();
///         let payload = record.body();
///         // Process or retain this fully validated record.
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RecordReader<R> {
    source: R,
    buffer: BytesMut,
    // A pending length also means that frame's stream ID was already validated.
    pending_length: Option<usize>,
    batch: Bytes,
    batch_offset: usize,
    state: ReaderState,
}

#[derive(Debug, Clone, Copy)]
enum ReaderState {
    Active,
    Finished,
    Failed,
}

impl<R> RecordReader<R> {
    /// Creates a reader using the fixed record size limits of the protocol.
    pub fn new(source: R) -> Self {
        Self {
            source,
            buffer: BytesMut::with_capacity(INITIAL_BUFFER_CAPACITY),
            pending_length: None,
            batch: Bytes::new(),
            batch_offset: 0,
            state: ReaderState::Active,
        }
    }

    /// Returns the next record from the validated batch without reading the source.
    ///
    /// `Ok(None)` means the prepared batch is exhausted. Use
    /// [`wait_to_read`](Self::wait_to_read) to prepare another batch or detect EOF.
    /// This method only reads the already validated length to locate the next
    /// record; it performs no CRC, stream ID, or framing validation and does not
    /// touch the receive buffer. Returns `ReaderFailed` after a previous wait failed.
    pub fn try_read_next(&mut self) -> Result<Option<Record>, RecordReadError> {
        if matches!(self.state, ReaderState::Failed) {
            return Err(RecordReadError::ReaderFailed);
        }
        if self.batch.is_empty() {
            return Ok(None);
        }

        let start = self.batch_offset;
        // SAFETY: prepare_batch validated every complete frame before freezing
        // the batch. The cursor always points to an unread record's header.
        let header = unsafe { protocol::read::header_unchecked(&self.batch[start..]) };
        let length = protocol::read::length(header);
        let end = start + usize::from(length);
        let bytes = if end == self.batch.len() {
            // Transfer the batch's handle to its final record, avoiding another
            // reference-count increment and releasing our ownership promptly.
            let mut bytes = std::mem::take(&mut self.batch);
            bytes.advance(start);
            self.batch_offset = 0;
            bytes
        } else {
            self.batch_offset = end;
            self.batch.slice(start..end)
        };
        // SAFETY: prepare_batch checked every frame's complete header/trailer,
        // exact declared extent, stream ID, and CRC before
        // freezing the batch. The cursor advances only by those lengths and
        // each handle retains exactly one immutable, fully validated frame.
        Ok(Some(unsafe { Record::from_validated_bytes(bytes) }))
    }

    fn prepare_batch(&mut self) -> Result<bool, RecordReadError> {
        let mut validated_len = 0;
        let validation = loop {
            let remaining = &self.buffer[validated_len..];
            let length = match self.pending_length {
                Some(length) => length,
                None => {
                    let Some(header) = remaining.first_chunk::<{ protocol::HEADER_LEN }>() else {
                        break Ok(());
                    };
                    let declared = protocol::read::length(header);
                    let length = usize::from(declared);
                    if length < protocol::MIN_RECORD_LEN {
                        break Err(RecordReadError::InvalidLength { declared });
                    }
                    if let Err(error) = StreamId::new(protocol::read::stream_id(header)) {
                        break Err(RecordReadError::InvalidStreamId(error));
                    }
                    // A u16 length is inherently within the protocol maximum.
                    self.pending_length = Some(length);
                    length
                }
            };

            if remaining.len() < length {
                break Ok(());
            }

            let frame = &remaining[..length];
            // SAFETY: length has passed the protocol minimum check, and frame
            // contains exactly that many initialized bytes, including its trailer.
            let stored = unsafe { protocol::read::crc(frame) };
            let computed = protocol::read::compute_crc(frame);
            if stored != computed {
                break Err(RecordReadError::CrcMismatch { stored, computed });
            }

            validated_len += length;
            self.pending_length = None;
        };

        if validated_len == 0 {
            return validation.map(|()| false);
        }
        // Publish every good record before the first invalid or unfinished one.
        // Keep the offending bytes, not the error: after this batch is drained,
        // the next wait rediscovers it at offset zero and returns it. Only corrupt
        // input repeats validation; good records are split off and never rechecked.
        self.batch = self.buffer.split_to(validated_len).freeze();
        self.batch_offset = 0;
        // pending_length, if present, belongs to the unfinished or CRC-invalid
        // frame now at the receive buffer's start. Its header checks still hold.
        Ok(true)
    }
}

impl<R: AsyncRead + Unpin> RecordReader<R> {
    /// Waits until at least one complete, validated record is ready to read.
    ///
    /// Returns `Ok(true)` when [`try_read_next`](Self::try_read_next) can return a
    /// record. It validates the complete prefix up to incomplete or invalid input
    /// and splits that prefix once, leaving the tail in the receive buffer. It
    /// does not wait to fill the buffer once a complete record is available.
    /// Repeated waits while the batch has unread records return immediately
    /// without I/O or validation.
    ///
    /// Valid records preceding a validation failure are exposed first. After that
    /// batch is drained, the next wait rediscovers and reports the error without
    /// reading more source bytes. Invalid input at the start fails immediately;
    /// the reader never skips it to expose later records. No error is stored.
    /// Returns `Ok(false)` only at a clean end of input between records. EOF in a header,
    /// body, or CRC trailer is an error. EOF is terminal; subsequent calls return
    /// `Ok(false)` without reading again. Once a validation or I/O error is returned,
    /// the reader becomes terminal and subsequent calls return `ReaderFailed`.
    ///
    /// Cancelling this future preserves received bytes and any validated header
    /// length and stream ID in the reader. Calling it again continues the same
    /// pending record without repeating those checks.
    pub async fn wait_to_read(&mut self) -> Result<bool, RecordReadError> {
        match self.state {
            ReaderState::Finished => return Ok(false),
            ReaderState::Failed => return Err(RecordReadError::ReaderFailed),
            ReaderState::Active => {}
        }
        if !self.batch.is_empty() {
            return Ok(true);
        }

        loop {
            match self.prepare_batch() {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(error) => {
                    self.state = ReaderState::Failed;
                    return Err(error);
                }
            }

            // prepare_batch returned false, so required is strictly larger than
            // buffer.len(). Reserve is only necessary for an incomplete frame;
            // spare capacity can also receive following records in the batch.
            let required = self.pending_length.unwrap_or(protocol::HEADER_LEN);
            self.buffer.reserve(required - self.buffer.len());
            let spare = self.buffer.capacity() - self.buffer.len();
            let received = self
                .source
                .read_buf(&mut (&mut self.buffer).limit(spare))
                .await;

            match received {
                Ok(0) if self.buffer.is_empty() => {
                    self.state = ReaderState::Finished;
                    return Ok(false);
                }
                Ok(0) => {
                    self.state = ReaderState::Failed;
                    return Err(match self.pending_length {
                        Some(expected) => RecordReadError::TruncatedRecord {
                            expected,
                            actual: self.buffer.len(),
                        },
                        None => RecordReadError::TruncatedHeader {
                            actual: self.buffer.len(),
                        },
                    });
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.state = ReaderState::Failed;
                    return Err(RecordReadError::Io(error));
                }
            }
        }
    }
}

/// Malformed wire data or a failed source read.
#[derive(Debug, Error)]
pub enum RecordReadError {
    #[error(
        "record header requires {} bytes, got {actual} before end of input",
        protocol::HEADER_LEN
    )]
    TruncatedHeader { actual: usize },

    #[error(
        "record length {declared} is smaller than the {} bytes required for its header and CRC trailer",
        protocol::MIN_RECORD_LEN
    )]
    InvalidLength { declared: u16 },

    #[error("record header contains an invalid stream ID: {0}")]
    InvalidStreamId(#[from] StreamIdError),

    #[error("record requires {expected} bytes, got {actual} before end of input")]
    TruncatedRecord { expected: usize, actual: usize },

    #[error("record CRC-32C mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },

    #[error("failed to read a record: {0}")]
    Io(#[from] io::Error),

    #[error("record reader cannot continue after an earlier error")]
    ReaderFailed,
}

#[cfg(test)]
mod tests {
    use super::{RecordReadError, RecordReader, protocol};
    use crate::{Record, RecordId, SequenceNumber, StreamId};
    use bytes::{Buf, Bytes, BytesMut};
    use protocol::test_data::{FRAME, encode_frame, reference_crc};
    use std::{
        future::Future,
        io::{self, Cursor},
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use tokio::io::{AsyncRead, ReadBuf};

    #[tokio::test]
    async fn reads_the_minimum_and_maximum_record_sizes_without_configuration() {
        for body_len in [0, 65_519] {
            let body: Vec<u8> = (0..body_len).map(|index| index as u8).collect();
            let frame = encode_frame(RecordId::new(StreamId::MAX, SequenceNumber::MAX), &body);
            for chunk_size in [4093, usize::MAX] {
                let source = TestSource::new(frame.clone(), chunk_size);
                let mut reader = RecordReader::new(source);
                let record = read_record(&mut reader).await.unwrap().unwrap();
                assert_eq!(record.as_bytes(), &frame);
                assert!(read_record(&mut reader).await.unwrap().is_none());
            }
        }
    }

    #[tokio::test]
    async fn reports_a_truncated_maximum_length_record() {
        let mut header = FRAME[..12].to_vec();
        header[..2].copy_from_slice(&u16::MAX.to_le_bytes());
        let mut reader = RecordReader::new(Cursor::new(header));
        assert!(matches!(
            read_record(&mut reader).await,
            Err(RecordReadError::TruncatedRecord {
                expected: 65_535,
                actual: 12
            })
        ));
    }

    #[tokio::test]
    async fn reads_a_batch_larger_than_the_per_record_limit() {
        let frame = encode_frame(
            RecordId::new(StreamId::MIN, SequenceNumber::MIN),
            &vec![0; 32_752],
        );
        let mut reader = RecordReader::new(Cursor::new(frame.repeat(2)));
        assert!(reader.wait_to_read().await.unwrap());
        assert_eq!(reader.batch.len(), 65_536);
        for _ in 0..2 {
            let record = reader.try_read_next().unwrap().unwrap();
            assert_eq!(record.as_bytes(), &frame);
        }
        assert!(reader.try_read_next().unwrap().is_none());
        assert!(!reader.wait_to_read().await.unwrap());
    }

    #[tokio::test]
    async fn reads_batches_without_copying_or_padding_records() {
        let mut batch = BytesMut::new();
        for _ in 0..8 {
            batch.extend_from_slice(FRAME);
        }
        let source = TestSource::new(batch.freeze(), usize::MAX);
        let mut reader = RecordReader::new(source);
        let mut records = Vec::new();

        assert!(reader.try_read_next().unwrap().is_none());
        assert_eq!(reader.source.read_calls, 0);
        assert!(reader.wait_to_read().await.unwrap());
        assert!(reader.wait_to_read().await.unwrap());
        assert_eq!(reader.source.read_calls, 1);
        assert_eq!(reader.batch.len(), 8 * FRAME.len());
        assert!(reader.buffer.is_empty());
        let receive_pointer = reader.buffer.as_ptr();
        let first = reader.try_read_next().unwrap().unwrap();
        let start = first.as_bytes().as_ptr().addr();
        records.push(first);
        for index in 1..8 {
            assert!(reader.wait_to_read().await.unwrap());
            let record = reader.try_read_next().unwrap().unwrap();
            assert_eq!(
                record.as_bytes().as_ptr().addr(),
                start + index * FRAME.len()
            );
            assert_eq!(reader.buffer.as_ptr(), receive_pointer);
            assert!(reader.buffer.is_empty());
            records.push(record);
        }
        assert!(reader.batch.is_empty());
        assert_eq!(reader.source.read_calls, 1);
        assert!(reader.try_read_next().unwrap().is_none());
        assert_eq!(reader.source.read_calls, 1);
        assert!(!reader.wait_to_read().await.unwrap());
        let read_calls = reader.source.read_calls;
        assert!(!reader.wait_to_read().await.unwrap());
        assert!(reader.try_read_next().unwrap().is_none());
        assert_eq!(reader.source.read_calls, read_calls);
        drop(reader);
        for record in records {
            assert_eq!(record.as_ref(), FRAME);
        }
    }

    #[tokio::test]
    async fn splits_only_complete_records_and_preserves_every_partial_tail() {
        for tail_len in 1..FRAME.len() {
            let input = Bytes::from(FRAME.repeat(3));
            let source = TestSource::new(input, 2 * FRAME.len() + tail_len);
            let mut reader = RecordReader::new(source);

            assert!(reader.wait_to_read().await.unwrap());
            assert_eq!(reader.batch.len(), 2 * FRAME.len());
            assert_eq!(reader.buffer.as_ref(), &FRAME[..tail_len]);
            let tail_pointer = reader.buffer.as_ptr();
            let first = reader.try_read_next().unwrap().unwrap();
            assert!(reader.wait_to_read().await.unwrap());
            let second = reader.try_read_next().unwrap().unwrap();
            assert!(reader.try_read_next().unwrap().is_none());
            assert_eq!(reader.source.read_calls, 1);
            assert_eq!(reader.buffer.as_ptr(), tail_pointer);
            assert_eq!(reader.buffer.as_ref(), &FRAME[..tail_len]);

            // Cancelling a wait must also preserve a tail left by a batch split.
            reader.source.pause_after = Some(reader.source.delivered);
            let mut pending = Box::pin(reader.wait_to_read());
            let mut context = Context::from_waker(Waker::noop());
            assert!(pending.as_mut().poll(&mut context).is_pending());
            drop(pending);
            assert_eq!(reader.buffer.as_ref(), &FRAME[..tail_len]);
            assert!(reader.try_read_next().unwrap().is_none());
            reader.source.pause_after = None;

            assert!(reader.wait_to_read().await.unwrap());
            let third = reader.try_read_next().unwrap().unwrap();
            assert!(reader.try_read_next().unwrap().is_none());
            assert!(!reader.wait_to_read().await.unwrap());
            drop(reader);
            for record in [first, second, third] {
                assert_eq!(record.as_ref(), FRAME);
            }
        }
    }

    #[tokio::test]
    async fn handles_fragments_across_header_body_and_trailer() {
        for chunk_size in 1..=FRAME.len() {
            let source = TestSource::new(Bytes::from_static(FRAME), chunk_size);
            let mut reader = RecordReader::new(source);
            let record = read_record(&mut reader).await.unwrap().unwrap();
            assert_eq!(record.as_ref(), FRAME);
            assert!(read_record(&mut reader).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn preserves_partial_records_when_a_read_is_cancelled() {
        for pause_at in [1, 2, 3, 4, 11, 12, 14, 17, 20] {
            let mut source = TestSource::new(Bytes::from_static(FRAME), usize::MAX);
            source.pause_after = Some(pause_at);
            let mut reader = RecordReader::new(source);
            let mut pending = Box::pin(reader.wait_to_read());
            let mut context = Context::from_waker(Waker::noop());
            assert!(pending.as_mut().poll(&mut context).is_pending());
            drop(pending);
            assert_eq!(reader.buffer.len(), pause_at);
            let read_calls = reader.source.read_calls;
            assert!(reader.try_read_next().unwrap().is_none());
            assert_eq!(reader.source.read_calls, read_calls);

            reader.source.pause_after = None;
            let record = read_record(&mut reader).await.unwrap().unwrap();
            assert_eq!(record.as_ref(), FRAME);
            assert!(read_record(&mut reader).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn distinguishes_clean_eof_from_every_truncated_frame() {
        let mut empty = RecordReader::new(Cursor::new(&[]));
        assert!(read_record(&mut empty).await.unwrap().is_none());
        for actual in 1..FRAME.len() {
            let mut reader = RecordReader::new(Cursor::new(&FRAME[..actual]));
            let error = read_record(&mut reader).await.unwrap_err();
            if actual < 12 {
                assert!(
                    matches!(error, RecordReadError::TruncatedHeader { actual: n } if n == actual)
                );
            } else {
                assert!(
                    matches!(error, RecordReadError::TruncatedRecord { expected: 21, actual: n } if n == actual)
                );
            }
        }
    }

    #[tokio::test]
    async fn rejects_missing_crc_bytes_even_for_an_empty_body() {
        for actual in 12..16 {
            let mut frame = BytesMut::from(&FRAME[..actual]);
            frame[..2].copy_from_slice(&16_u16.to_le_bytes());
            let mut reader = RecordReader::new(Cursor::new(frame));
            assert!(matches!(
                read_record(&mut reader).await,
                Err(RecordReadError::TruncatedRecord { expected: 16, actual: n }) if n == actual
            ));
        }
    }

    #[tokio::test]
    async fn rejects_short_lengths_before_reading_a_body() {
        for declared in 0_u16..16 {
            let mut frame = BytesMut::from(&FRAME[..12]);
            frame[..2].copy_from_slice(&declared.to_le_bytes());
            let source = TestSource::new(frame.freeze(), usize::MAX);
            let mut reader = RecordReader::new(source);
            let capacity = reader.buffer.capacity();
            assert!(matches!(
                read_record(&mut reader).await,
                Err(RecordReadError::InvalidLength { declared: actual }) if actual == declared
            ));
            assert_eq!(reader.source.read_calls, 1);
            assert_eq!(reader.buffer.capacity(), capacity);
        }
    }

    #[tokio::test]
    async fn rejects_corruption_before_returning_a_record() {
        for offset in 2..FRAME.len() {
            let mut frame = BytesMut::from(FRAME);
            frame[offset] ^= 1;
            let mut reader = RecordReader::new(Cursor::new(frame));
            assert!(matches!(
                read_record(&mut reader).await,
                Err(RecordReadError::CrcMismatch { .. })
            ));
            assert!(matches!(
                read_record(&mut reader).await,
                Err(RecordReadError::ReaderFailed)
            ));
        }
    }

    #[tokio::test]
    async fn publishes_the_valid_prefix_before_rediscovering_a_crc_error() {
        let mut batch = BytesMut::from(FRAME.repeat(2).as_slice());
        let mut corrupted = FRAME.to_vec();
        corrupted[12] ^= 1;
        batch.extend_from_slice(&corrupted);
        batch.extend_from_slice(FRAME);
        let source = TestSource::new(batch.freeze(), usize::MAX);
        let mut reader = RecordReader::new(source);
        let receive_pointer = reader.buffer.as_ptr();

        assert!(reader.wait_to_read().await.unwrap());
        assert_eq!(reader.batch.len(), 2 * FRAME.len());
        assert_eq!(reader.batch.as_ptr(), receive_pointer);
        assert_eq!(reader.buffer.len(), 2 * FRAME.len());
        assert_eq!(&reader.buffer[..FRAME.len()], &corrupted);
        let tail_pointer = reader.buffer.as_ptr();
        assert_eq!(
            tail_pointer.addr(),
            receive_pointer.addr() + 2 * FRAME.len()
        );

        // Neither repeated waits nor error rediscovery may read the source.
        // If they did, this unrelated error would mask the buffered CRC failure.
        reader.source.error = Some(io::ErrorKind::ConnectionReset);
        let mut retained = Vec::new();
        for _ in 0..2 {
            assert!(reader.wait_to_read().await.unwrap());
            assert!(reader.wait_to_read().await.unwrap());
            retained.push(reader.try_read_next().unwrap().unwrap());
            assert_eq!(reader.buffer.as_ptr(), tail_pointer);
            assert_eq!(reader.source.read_calls, 1);
        }
        assert!(reader.try_read_next().unwrap().is_none());
        let expected_stored = u32::from_le_bytes(FRAME[17..21].try_into().unwrap());
        let expected_computed = reference_crc(&corrupted[..17]);
        assert!(matches!(
            reader.wait_to_read().await,
            Err(RecordReadError::CrcMismatch { stored, computed })
                if stored == expected_stored && computed == expected_computed
        ));
        assert_eq!(reader.buffer.as_ptr(), tail_pointer);
        assert!(reader.batch.is_empty());
        assert!(matches!(
            reader.try_read_next(),
            Err(RecordReadError::ReaderFailed)
        ));
        assert!(matches!(
            reader.wait_to_read().await,
            Err(RecordReadError::ReaderFailed)
        ));
        assert_eq!(reader.source.read_calls, 1);
        assert_eq!(reader.source.error, Some(io::ErrorKind::ConnectionReset));
        drop(reader);
        for record in retained {
            assert_eq!(record.as_ref(), FRAME);
        }
    }

    #[tokio::test]
    async fn delivers_the_same_valid_prefix_before_corruption_at_every_chunk_size() {
        let first = encode_frame(RecordId::new(StreamId::MIN, SequenceNumber::MIN), b"");
        let mut corrupted = FRAME.to_vec();
        corrupted[12] ^= 1;
        let input = [&first[..], FRAME, &corrupted, FRAME].concat();
        for chunk_size in 1..=input.len() {
            let source = TestSource::new(Bytes::copy_from_slice(&input), chunk_size);
            let mut reader = RecordReader::new(source);
            let a = read_record(&mut reader).await.unwrap().unwrap();
            let b = read_record(&mut reader).await.unwrap().unwrap();
            assert!(matches!(
                read_record(&mut reader).await,
                Err(RecordReadError::CrcMismatch { .. })
            ));
            let calls = reader.source.read_calls;
            assert!(matches!(
                reader.try_read_next(),
                Err(RecordReadError::ReaderFailed)
            ));
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::ReaderFailed)
            ));
            assert_eq!(reader.source.read_calls, calls);
            drop(reader);
            assert_eq!(a.as_ref(), &first);
            assert_eq!(b.as_ref(), FRAME);
        }
    }

    #[tokio::test]
    async fn rejects_invalid_stream_ids_even_with_a_correct_crc() {
        for value in [4096_u16, 0x8000, u16::MAX] {
            let mut invalid = FRAME.to_vec();
            invalid[2..4].copy_from_slice(&value.to_le_bytes());
            let crc_offset = invalid.len() - protocol::CRC_LEN;
            let crc = reference_crc(&invalid[..crc_offset]);
            invalid[crc_offset..].copy_from_slice(&crc.to_le_bytes());

            // A later failure must not withhold the valid prefix, and neither
            // position may allow the valid record after the failure through.
            for prefix_count in [0, 2] {
                let mut input = BytesMut::from(FRAME.repeat(prefix_count).as_slice());
                input.extend_from_slice(&invalid);
                input.extend_from_slice(FRAME);
                let source = TestSource::new(input.freeze(), usize::MAX);
                let mut reader = RecordReader::new(source);
                let pointer = reader.buffer.as_ptr();
                let mut retained = Vec::new();
                if prefix_count > 0 {
                    assert!(reader.wait_to_read().await.unwrap());
                    assert_eq!(reader.batch.len(), prefix_count * FRAME.len());
                    for _ in 0..prefix_count {
                        retained.push(reader.try_read_next().unwrap().unwrap());
                    }
                    assert!(reader.try_read_next().unwrap().is_none());
                }
                assert!(matches!(
                    reader.wait_to_read().await,
                    Err(RecordReadError::InvalidStreamId(error))
                        if error.value() == value
                ));
                assert_eq!(reader.buffer.as_ref(), [&invalid[..], FRAME].concat());
                assert_eq!(
                    reader.buffer.as_ptr().addr(),
                    pointer.addr() + prefix_count * FRAME.len()
                );
                assert!(reader.batch.is_empty());
                assert!(matches!(
                    reader.try_read_next(),
                    Err(RecordReadError::ReaderFailed)
                ));
                assert!(matches!(
                    reader.wait_to_read().await,
                    Err(RecordReadError::ReaderFailed)
                ));
                assert_eq!(reader.source.read_calls, 1);
                drop(reader);
                for record in retained {
                    assert_eq!(record.as_ref(), FRAME);
                }
            }
        }
    }

    #[tokio::test]
    async fn checks_stream_id_before_reading_a_body() {
        let mut header = BytesMut::from(&FRAME[..12]);
        header[..2].copy_from_slice(&u16::MAX.to_le_bytes());
        header[2..4].copy_from_slice(&4096_u16.to_le_bytes());
        let source = TestSource::new(header.freeze(), usize::MAX);
        let mut reader = RecordReader::new(source);
        let capacity = reader.buffer.capacity();
        assert!(matches!(
            reader.wait_to_read().await,
            Err(RecordReadError::InvalidStreamId(error)) if error.value() == 4096
        ));
        assert_eq!(reader.source.read_calls, 1);
        assert_eq!(reader.buffer.capacity(), capacity);
    }

    #[tokio::test]
    async fn accepts_stream_boundaries_and_all_raw_sequence_values() {
        let mut input = BytesMut::new();
        let expected = [
            RecordId::new(StreamId::MIN, SequenceNumber::MIN),
            RecordId::new(StreamId::MAX, SequenceNumber::MAX),
            RecordId::new(StreamId::MAX, SequenceNumber::MIN),
        ];
        for id in expected {
            input.extend_from_slice(&encode_frame(id, b"hello"));
        }
        // The generic reader accepts mixed streams and imposes no sequence
        // policy. Stream-specific ingestion/replay must enforce continuity.
        let mut reader = RecordReader::new(Cursor::new(input));
        assert!(reader.wait_to_read().await.unwrap());
        for id in expected {
            let record = reader.try_read_next().unwrap().unwrap();
            assert_eq!(record.id(), id);
        }
        assert!(!reader.wait_to_read().await.unwrap());
    }

    #[tokio::test]
    async fn publishes_valid_records_before_rejecting_every_short_length() {
        for declared in 0_u16..16 {
            let mut input = BytesMut::from(FRAME.repeat(2).as_slice());
            let mut header = FRAME[..12].to_vec();
            header[..2].copy_from_slice(&declared.to_le_bytes());
            input.extend_from_slice(&header);
            input.extend_from_slice(FRAME);
            let source = TestSource::new(input.freeze(), usize::MAX);
            let mut reader = RecordReader::new(source);
            let receive_pointer = reader.buffer.as_ptr();
            assert!(reader.wait_to_read().await.unwrap());
            assert_eq!(reader.batch.len(), 2 * FRAME.len());
            assert_eq!(reader.batch.as_ptr(), receive_pointer);
            let a = reader.try_read_next().unwrap().unwrap();
            let b = reader.try_read_next().unwrap().unwrap();
            assert!(reader.try_read_next().unwrap().is_none());
            let tail_pointer = reader.buffer.as_ptr();
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::InvalidLength { declared: actual }) if actual == declared
            ));
            assert_eq!(reader.source.read_calls, 1);
            assert_eq!(reader.buffer.as_ptr(), tail_pointer);
            assert_eq!(reader.buffer.as_ref(), [&header[..], FRAME].concat());
            assert!(reader.batch.is_empty());
            assert!(matches!(
                reader.try_read_next(),
                Err(RecordReadError::ReaderFailed)
            ));
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::ReaderFailed)
            ));
            assert_eq!(reader.source.read_calls, 1);
            drop(reader);
            assert_eq!(a.as_ref(), FRAME);
            assert_eq!(b.as_ref(), FRAME);
        }
    }

    #[tokio::test]
    async fn publishes_valid_records_before_every_truncated_tail() {
        for tail_len in 1..FRAME.len() {
            let mut batch = BytesMut::from(FRAME.repeat(2).as_slice());
            batch.extend_from_slice(&FRAME[..tail_len]);
            let source = TestSource::new(batch.freeze(), usize::MAX);
            let mut reader = RecordReader::new(source);
            let a = read_record(&mut reader).await.unwrap().unwrap();
            let b = read_record(&mut reader).await.unwrap().unwrap();
            assert_eq!(reader.source.read_calls, 1);
            let error = reader.wait_to_read().await.unwrap_err();
            if tail_len < 12 {
                assert!(
                    matches!(error, RecordReadError::TruncatedHeader { actual } if actual == tail_len)
                );
            } else {
                assert!(
                    matches!(error, RecordReadError::TruncatedRecord { expected: 21, actual } if actual == tail_len)
                );
            }
            assert!(matches!(
                reader.try_read_next(),
                Err(RecordReadError::ReaderFailed)
            ));
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::ReaderFailed)
            ));
            assert_eq!(reader.source.read_calls, 2);
            drop(reader);
            assert_eq!(a.as_ref(), FRAME);
            assert_eq!(b.as_ref(), FRAME);
        }
    }

    #[tokio::test]
    async fn reads_mixed_lengths_across_batch_refills_while_earlier_records_are_retained() {
        let mut input = BytesMut::new();
        let mut expected = Vec::new();
        for body_len in [0, 1, 7, 8, 65_519, 255, 0] {
            let body: Vec<u8> = (0..body_len).map(|index| index as u8).collect();
            let frame = encode_frame(RecordId::new(StreamId::MIN, SequenceNumber::MIN), &body);
            input.extend_from_slice(&frame);
            expected.push(frame);
        }
        let source = TestSource::new(input.freeze(), usize::MAX);
        let mut reader = RecordReader::new(source);
        let mut records = Vec::new();
        let mut batches = 0;
        while reader.wait_to_read().await.unwrap() {
            batches += 1;
            while let Some(record) = reader.try_read_next().unwrap() {
                records.push(record);
            }
        }
        assert!(batches > 1);
        drop(reader);
        assert_eq!(records.len(), expected.len());
        for (record, frame) in records.iter().zip(expected) {
            assert_eq!(record.as_bytes(), &frame);
        }
    }

    #[tokio::test]
    async fn propagates_source_errors_and_retries_interrupted_reads() {
        let mut source = TestSource::new(Bytes::from_static(FRAME), usize::MAX);
        source.error = Some(io::ErrorKind::Interrupted);
        let mut reader = RecordReader::new(source);
        assert!(read_record(&mut reader).await.unwrap().is_some());

        let mut source = TestSource::new(Bytes::from_static(FRAME), usize::MAX);
        source.error = Some(io::ErrorKind::ConnectionReset);
        let mut reader = RecordReader::new(source);
        assert!(
            matches!(read_record(&mut reader).await, Err(RecordReadError::Io(error)) if error.kind() == io::ErrorKind::ConnectionReset)
        );
        assert!(matches!(
            read_record(&mut reader).await,
            Err(RecordReadError::ReaderFailed)
        ));
    }

    #[tokio::test]
    async fn source_errors_after_a_valid_batch_are_terminal_without_retry() {
        for tail_len in 0..FRAME.len() {
            let mut input = BytesMut::from(FRAME.repeat(2).as_slice());
            input.extend_from_slice(&FRAME[..tail_len]);
            let source = TestSource::new(input.freeze(), usize::MAX);
            let mut reader = RecordReader::new(source);
            assert!(reader.wait_to_read().await.unwrap());
            reader.source.error = Some(io::ErrorKind::ConnectionReset);
            let a = read_record(&mut reader).await.unwrap().unwrap();
            let b = read_record(&mut reader).await.unwrap().unwrap();
            assert!(reader.try_read_next().unwrap().is_none());
            assert_eq!(reader.source.read_calls, 1);
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::Io(error)) if error.kind() == io::ErrorKind::ConnectionReset
            ));
            assert!(matches!(
                reader.wait_to_read().await,
                Err(RecordReadError::ReaderFailed)
            ));
            assert!(matches!(
                reader.try_read_next(),
                Err(RecordReadError::ReaderFailed)
            ));
            assert_eq!(reader.source.read_calls, 2);
            drop(reader);
            assert_eq!(a.as_ref(), FRAME);
            assert_eq!(b.as_ref(), FRAME);
        }
    }

    async fn read_record<R: AsyncRead + Unpin>(
        reader: &mut RecordReader<R>,
    ) -> Result<Option<Record>, RecordReadError> {
        if reader.wait_to_read().await? {
            reader.try_read_next()
        } else {
            Ok(None)
        }
    }
    struct TestSource {
        bytes: Bytes,
        chunk_size: usize,
        read_calls: usize,
        delivered: usize,
        pause_after: Option<usize>,
        error: Option<io::ErrorKind>,
    }

    impl TestSource {
        fn new(bytes: Bytes, chunk_size: usize) -> Self {
            Self {
                bytes,
                chunk_size,
                read_calls: 0,
                delivered: 0,
                pause_after: None,
                error: None,
            }
        }
    }

    impl AsyncRead for TestSource {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            destination: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let source = self.get_mut();
            source.read_calls += 1;
            if let Some(error) = source.error.take() {
                return Poll::Ready(Err(error.into()));
            }
            let mut count = source
                .bytes
                .len()
                .min(source.chunk_size)
                .min(destination.remaining());
            if let Some(pause_after) = source.pause_after {
                if source.delivered >= pause_after {
                    // Tests explicitly resume and repoll after cancelling the
                    // pending future; there is no asynchronous wakeup producer.
                    return Poll::Pending;
                }
                count = count.min(pause_after - source.delivered);
            }
            destination.put_slice(&source.bytes[..count]);
            source.bytes.advance(count);
            source.delivered += count;
            Poll::Ready(Ok(()))
        }
    }
}

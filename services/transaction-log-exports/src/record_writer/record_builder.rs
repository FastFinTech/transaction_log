use std::{
    fmt, io,
    ptr::{self, NonNull},
};

use bytes::BytesMut;

use super::RecordBuildError;
use crate::RecordId;
use crate::record::record_protocol::{
    self as protocol, HEADER_LEN, MAX_PAYLOAD_LEN, MAX_RECORD_LEN, MIN_RECORD_LEN,
};

/// A temporary builder for constructing exactly one record in the writer's buffer.
///
/// Each instance borrows the buffer for one body-writing callback. The
/// callback can make many `std::io::Write` calls, all appending to that record's
/// payload. Construction reserves space for the complete record; body writes
/// begin after the uninitialized header space. Earlier records are untouched.
/// The buffer's readable length advances only when the complete record is ready.
///
/// After the callback succeeds, the enclosing writer calls `finish`, which
/// consumes this builder, fills the header, and appends the checksum. The writer
/// constructs a fresh builder for the next record, borrowing the same buffer
/// again. The writer and buffer are reused across records; a builder has no
/// reset or reuse cycle. With enough buffer capacity, creating the next builder
/// requires no heap allocation.
///
/// The first failed write is retained and prevents further writes or successful
/// finalization of this record. Dropping an unfinished builder rolls it back,
/// including during unwinding, without discarding preceding records or buffer
/// capacity. This concrete type stays internal; callbacks use only `Write`.
/// The callback is synchronous on the producer thread. Only completed buffers
/// will enter the output queue; this builder needs no thread-transfer support.
pub(super) struct RecordBuilder<'a> {
    // Borrowed for this record only; the enclosing writer retains ownership.
    // Its readable length stays at start until finish publishes the complete
    // record. No byte view escapes while the cached pointer is used.
    buffer: &'a mut BytesMut,
    // This record's header offset and the buffer length to restore on rollback.
    start: usize,
    // Derived from spare capacity after reservation, before initializing the
    // header. Covers the header, payload, and trailer. The allocation cannot move
    // during construction, and the pointer is never used after publishing length.
    record_start: NonNull<u8>,
    // Payload bytes only: excludes the header and CRC trailer. While serializing,
    // exactly this many bytes after the header space have been initialized, but
    // remain outside buffer.len(). Header and trailer are initialized by finish.
    // Only successful appends advance this count; it never exceeds MAX_PAYLOAD_LEN.
    payload_len: usize,
    error: Option<RecordBuildError>,
    finished: bool,
}

impl<'a> RecordBuilder<'a> {
    /// Borrows the writer's buffer to construct one new record with an empty payload.
    ///
    /// Existing bytes remain untouched and do not count towards the payload
    /// limit. Reserves `MAX_RECORD_LEN` additional bytes without initializing any
    /// of them. The buffer's length stays unchanged until successful finalization.
    /// Body writes and header/CRC finalization then fit without growth. A writer
    /// that supplies sufficient spare capacity avoids allocation at construction too.
    /// Call this constructor again for each subsequent record after the previous
    /// builder has been consumed by `finish` or dropped.
    // Expose the initial zero payload length and empty error state when the
    // public writer is monomorphized in a downstream crate.
    #[inline]
    pub(super) fn new(buffer: &'a mut BytesMut) -> Self {
        let start = buffer.len();
        buffer.reserve(MAX_RECORD_LEN);
        let record_start = NonNull::from(buffer.spare_capacity_mut()).cast::<u8>();
        Self {
            buffer,
            start,
            record_start,
            payload_len: 0,
            error: None,
            finished: false,
        }
    }

    /// Returns payload bytes written for this record, excluding its header,
    /// CRC trailer, and all preceding records in the buffer.
    #[inline]
    pub(super) fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Returns whether this record's payload contains no bytes, regardless of
    /// preceding records or its reserved header.
    #[cfg(test)]
    #[inline]
    pub(super) fn is_payload_empty(&self) -> bool {
        self.payload_len() == 0
    }

    /// Reports the first rejected write, even if the callback ignored its error.
    ///
    /// This reads retained error state without validating body contents,
    /// framing, or a checksum. The error belongs to this record construction;
    /// a fresh builder on the same buffer starts with no retained error.
    #[inline]
    pub(super) fn check_error(&self) -> Result<(), RecordBuildError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Completes this record in place, consuming the builder.
    ///
    /// The supplied ID is the only caller-provided header data. Total length is
    /// derived from bytes actually written; CRC-32C covers the finalized header
    /// and payload. The complete record remains in the borrowed buffer without a
    /// split, payload move, or conversion to an owning `Record`.
    ///
    /// A retained write error causes failure and rollback, even if the callback
    /// ignored it. The caller must separately check the callback's own result
    /// before calling this method; body-writing callback failures are opaque.
    /// Both success and failure consume `self` and release the buffer borrow.
    /// Construct a fresh builder to write another record into the same buffer.
    pub(super) fn finish(mut self, id: RecordId) -> Result<(), RecordBuildError> {
        self.check_error()?;
        // Accepted writes bound payload_len() to MAX_PAYLOAD_LEN. Adding the fixed
        // overhead therefore fits both usize and u16 without another range check.
        let length = self.payload_len + MIN_RECORD_LEN;
        let record_start = self.record_start.as_ptr();
        // SAFETY: new reserved the entire record region and the exclusive borrow
        // keeps it live and stationary, with no overlapping byte views. Accepted
        // writes initialized exactly payload_len bytes after the header space;
        // length is in the protocol range and fits the reservation. write::header
        // initializes every header byte with that length and the typed ID. With
        // header and payload now initialized, write::crc can read them and initialize
        // the remaining trailer. Only then are all length bytes valid to expose
        // through set_len. Neither protocol writer requires integer alignment,
        // and the cached pointer is never used after publishing length.
        unsafe {
            protocol::write::header(record_start, length as u16, id);
            protocol::write::crc(record_start, length);
            self.buffer.set_len(self.start + length);
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for RecordBuilder<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // The exclusive borrow prevents any following records from being
            // appended while this one is unfinished. Truncation keeps capacity.
            self.buffer.truncate(self.start);
        }
    }
}

/// Appends a chunk to this record's body, accepting the entire input or returning
/// an error without changing the buffer.
///
/// A serializer may call these methods many times while constructing one record.
/// Write-call boundaries do not create record boundaries; `finish` completes the
/// record, and the next record requires a fresh builder.
///
/// Errors use `io::ErrorKind::InvalidInput` and contain a `RecordBuildError`.
/// `write_all` also checks retained errors for empty input. `flush` performs no
/// I/O; it only reports retained errors, since writes already reach the buffer.
impl io::Write for RecordBuilder<'_> {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.check_error().map_err(io::Error::from)?;
        // The cached count is bounded by accepted writes. This check needs no
        // buffer length/capacity lookup and cannot overflow for large inputs.
        if bytes.len() > MAX_PAYLOAD_LEN - self.payload_len {
            self.error = Some(RecordBuildError::PayloadTooLarge);
            return Err(RecordBuildError::PayloadTooLarge.into());
        }
        let payload_len = self.payload_len + bytes.len();
        // SAFETY: new reserved the complete maximum-sized record before deriving
        // record_start from its spare capacity. The exclusive borrow keeps that
        // allocation alive; nothing reallocates, splits, or creates overlapping
        // byte views while this pointer is in use. The check bounds payload_len
        // to MAX_PAYLOAD_LEN, so this append fits after the header space and the
        // initialized payload, leaving CRC space untouched. No destination view
        // escapes, so the source slice cannot overlap it. Both pointers are valid and
        // byte-aligned even for an empty write. Copying initializes exactly
        // bytes.len() bytes; only the local count advances until finish publishes
        // the complete record.
        unsafe {
            ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.record_start
                    .as_ptr()
                    .add(HEADER_LEN + self.payload_len),
                bytes.len(),
            );
        }
        self.payload_len = payload_len;
        Ok(bytes.len())
    }

    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write(bytes).map(|_| ())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.check_error().map_err(io::Error::from)
    }
}

impl fmt::Debug for RecordBuilder<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not expose the earlier records or outer header in the borrowed buffer.
        formatter
            .debug_struct("RecordBuilder")
            .field("payload_len", &self.payload_len())
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{RecordBuildError, RecordBuilder};
    use crate::record::record_protocol::test_data::{FRAME, encode_frame};
    use crate::{RecordId, RecordReader, SequenceNumber, StreamId};
    use bytes::BytesMut;
    use std::io::{self, Write};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn accepts_serialized_transactions_through_write_without_touching_the_prefix() {
        // The serializer knows only Write, with no access to body bookkeeping.
        fn serialize<W: Write + ?Sized>(writer: &mut W) -> io::Result<()> {
            writer.write_all(&[2, 0, 7, 0, 3, 0])?;
            writer.write_all(b"abc")?;
            writer.write_all(&[9, 0, 2, 0, 0xfe, 0xff])
        }

        let mut buffer = BytesMut::from(&b"previous records"[..]);
        let prefix = buffer.to_vec();
        let id = RecordId::new(StreamId::MAX, SequenceNumber::new(42));
        {
            let mut builder = RecordBuilder::new(&mut buffer);
            assert!(builder.is_payload_empty());
            serialize(&mut builder).unwrap();
            builder.flush().unwrap();
            assert_eq!(builder.payload_len(), 15);
            assert_eq!(builder.check_error(), Ok(()));
            builder.finish(id).unwrap();
        }
        assert_eq!(&buffer[..prefix.len()], prefix);
        assert_eq!(
            &buffer[prefix.len()..],
            encode_frame(
                id,
                &[2, 0, 7, 0, 3, 0, b'a', b'b', b'c', 9, 0, 2, 0, 0xfe, 0xff]
            )
        );
    }

    #[test]
    fn fills_the_exact_payload_limit_without_growing_preallocated_storage() {
        // Independent protocol expectation: 65,535 total minus 12 header and 4 CRC.
        const PAYLOAD_LIMIT: usize = 65_519;
        for chunk_size in [1, 2048, PAYLOAD_LIMIT] {
            let mut buffer = BytesMut::with_capacity(5 + 65_535);
            buffer.extend_from_slice(b"outer");
            let pointer = buffer.as_ptr();
            let capacity = buffer.capacity();
            let input = vec![0xa5; PAYLOAD_LIMIT];
            let id = RecordId::new(StreamId::MAX, SequenceNumber::MAX);
            {
                let mut builder = RecordBuilder::new(&mut buffer);
                for chunk in input.chunks(chunk_size) {
                    assert_eq!(builder.write(chunk).unwrap(), chunk.len());
                }
                assert_eq!(builder.payload_len(), PAYLOAD_LIMIT);
                assert_eq!(builder.write(&[]).unwrap(), 0);
                builder.write_all(&[]).unwrap();
                builder.flush().unwrap();
                assert_eq!(builder.check_error(), Ok(()));
                builder.finish(id).unwrap();
            }
            assert_eq!(buffer.as_ptr(), pointer);
            assert_eq!(buffer.capacity(), capacity);
            assert_eq!(&buffer[..5], b"outer");
            assert_eq!(&buffer[5..], encode_frame(id, &input));
        }
    }

    #[test]
    fn oversized_writes_are_atomic_and_remain_failed_even_if_ignored() {
        for use_write_all in [false, true] {
            for existing_len in [0, 4, 65_519] {
                let mut buffer = BytesMut::from(&b"prefix"[..]);
                let existing = vec![0xab; existing_len];
                {
                    let mut builder = RecordBuilder::new(&mut buffer);
                    builder.write_all(&existing).unwrap();
                    let before_rejection = builder.buffer.to_vec();
                    let input = vec![0xcc; 65_520 - existing_len];
                    let error = if use_write_all {
                        builder.write_all(&input).unwrap_err()
                    } else {
                        builder.write(&input).unwrap_err()
                    };
                    assert_io_error(error);
                    assert_eq!(builder.payload_len(), existing_len);
                    assert_eq!(
                        builder.check_error(),
                        Err(RecordBuildError::PayloadTooLarge)
                    );
                    assert_io_error(builder.write(b"x").unwrap_err());
                    assert_io_error(builder.write_all(&[]).unwrap_err());
                    assert_io_error(builder.flush().unwrap_err());
                    assert_eq!(&builder.buffer[..], before_rejection);
                    // SAFETY: the initial accepted write initialized existing_len
                    // payload bytes after the 12-byte header. Rejected writes
                    // must not alter them.
                    // This temporary shared view ends before finish/rollback.
                    let preserved_payload = unsafe {
                        std::slice::from_raw_parts(
                            builder.record_start.as_ptr().add(12),
                            existing_len,
                        )
                    };
                    assert_eq!(preserved_payload, existing);
                    assert_eq!(
                        builder.finish(RecordId::new(StreamId::MIN, SequenceNumber::MIN)),
                        Err(RecordBuildError::PayloadTooLarge)
                    );
                }
                assert_eq!(&buffer[..], b"prefix");
                // A rejected record cannot poison the next independent builder.
                let id = RecordId::new(StreamId::MIN, SequenceNumber::new(1));
                RecordBuilder::new(&mut buffer).finish(id).unwrap();
                assert_eq!(&buffer[6..], encode_frame(id, b""));
            }
        }
    }

    #[test]
    fn reserves_a_full_record_upfront_and_keeps_storage_stable_during_small_writes() {
        let mut buffer = BytesMut::from(&b"prefix"[..]);
        assert!(buffer.capacity() - buffer.len() < 65_535);
        let payload: Vec<u8> = (0..65_519).map(|index| index as u8).collect();
        let id = RecordId::new(StreamId::MIN, SequenceNumber::MAX);
        let pointer;
        let capacity;
        {
            let mut builder = RecordBuilder::new(&mut buffer);
            pointer = builder.record_start.as_ptr().cast_const();
            capacity = builder.buffer.capacity();
            assert!(capacity >= 6 + 65_535);
            assert_eq!(builder.buffer.len(), 6);
            let mut expected_written = 0;
            for chunk in payload.chunks(13) {
                builder.write_all(chunk).unwrap();
                expected_written += chunk.len();
                assert_eq!(builder.payload_len(), expected_written);
                // The record remains outside BytesMut's readable extent until
                // finish. Each chunk advances only the builder's counter.
                assert_eq!(builder.buffer.len(), 6);
                assert_eq!(builder.record_start.as_ptr().cast_const(), pointer);
                assert_eq!(builder.buffer.capacity(), capacity);
            }
            builder.finish(id).unwrap();
        }
        assert_eq!(buffer[6..].as_ptr(), pointer);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(&buffer[..6], b"prefix");
        assert_eq!(&buffer[6..], encode_frame(id, &payload));
    }

    #[test]
    fn each_builder_has_its_own_payload_length_and_limit_within_a_larger_buffer() {
        let mut buffer = BytesMut::new();
        let first_payload = vec![0x31; 65_519];
        let second_payload = vec![0x32; 65_519];
        let first_id = RecordId::new(StreamId::MIN, SequenceNumber::new(1));
        let second_id = RecordId::new(StreamId::MAX, SequenceNumber::MAX);
        {
            let mut first = RecordBuilder::new(&mut buffer);
            first.write_all(&first_payload).unwrap();
            first.finish(first_id).unwrap();
        }
        {
            let mut second = RecordBuilder::new(&mut buffer);
            assert!(second.is_payload_empty());
            second.write_all(&second_payload).unwrap();
            assert_eq!(second.payload_len(), 65_519);
            second.finish(second_id).unwrap();
        }
        assert_eq!(buffer.len(), 2 * 65_535);
        assert_eq!(&buffer[..65_535], encode_frame(first_id, &first_payload));
        assert_eq!(&buffer[65_535..], encode_frame(second_id, &second_payload));
    }

    #[test]
    fn reservation_and_appends_preserve_shared_prefixes_and_earlier_records() {
        let mut buffer = BytesMut::with_capacity(64);
        buffer.extend_from_slice(FRAME);
        let retained = buffer.split_to(FRAME.len()).freeze();
        buffer.extend_from_slice(FRAME);
        let id = RecordId::new(StreamId::MAX, SequenceNumber::new(42));
        let payload = vec![0xa5; 65_519];
        let mut builder = RecordBuilder::new(&mut buffer);
        for chunk in payload.chunks(3) {
            builder.write_all(chunk).unwrap();
        }
        builder.finish(id).unwrap();
        assert_eq!(&retained[..], FRAME);
        assert_eq!(&buffer[..FRAME.len()], FRAME);
        assert_eq!(&buffer[FRAME.len()..], encode_frame(id, &payload));
    }

    #[test]
    fn finishes_the_golden_frame_at_any_byte_offset_without_moving_the_payload() {
        let id = RecordId::new(
            StreamId::new(0x0708).unwrap(),
            SequenceNumber::new(0x1112_1314_1516_1718),
        );
        for offset in 0..8 {
            // Seed a guard after the final trailer to detect an over-wide store.
            // Full reservation prevents construction from relocating this storage.
            let mut buffer = BytesMut::with_capacity(offset + 65_535);
            let guard_start = offset + FRAME.len();
            buffer.resize(guard_start + 8, 0xa5);
            buffer.truncate(offset);
            let allocation = buffer.as_ptr();
            let mut builder = RecordBuilder::new(&mut buffer);
            builder.write_all(b"hello").unwrap();
            let payload_pointer = builder.record_start.as_ptr().wrapping_add(12).cast_const();
            builder.finish(id).unwrap();
            assert_eq!(buffer.as_ptr(), allocation);
            assert_eq!(buffer.len(), guard_start);
            assert_eq!(&buffer[..offset], &[0xa5; 7][..offset]);
            assert_eq!(&buffer[offset..], FRAME);
            assert_eq!(buffer[offset + 12..].as_ptr(), payload_pointer);
            // SAFETY: resize initialized these guard bytes before construction;
            // the allocation is unchanged, and truncate preserved spare storage.
            // Expose them only after the builder and its cached pointer are gone.
            unsafe { buffer.set_len(guard_start + 8) };
            assert_eq!(&buffer[guard_start..], &[0xa5; 8]);
        }
    }

    #[test]
    fn finishes_empty_and_boundary_payloads_with_independent_length_and_crc() {
        for length in [0, 1, 7, 8, 9, 239, 240, 241, 65_518] {
            for id in [
                RecordId::new(StreamId::MIN, SequenceNumber::MIN),
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
            ] {
                let input: Vec<u8> = (0..length).map(|index| index as u8).collect();
                let mut buffer = BytesMut::new();
                let mut builder = RecordBuilder::new(&mut buffer);
                builder.write_all(&input).unwrap();
                builder.finish(id).unwrap();
                assert_eq!(&buffer[..], encode_frame(id, &input));
            }
        }
    }

    #[test]
    fn abandoning_serialization_rolls_back_on_drop_error_and_unwinding() {
        fn serialize_with_semantic_error(buffer: &mut BytesMut) -> io::Result<()> {
            let mut builder = RecordBuilder::new(buffer);
            builder.write_all(b"partial transaction")?;
            // A serializer can fail independently of the builder's bounds check.
            Err(io::Error::other("serialization failed"))
        }

        let mut buffer = BytesMut::with_capacity(FRAME.len() + 65_535);
        buffer.extend_from_slice(FRAME);
        let pointer = buffer.as_ptr();
        let capacity = buffer.capacity();
        {
            let mut builder = RecordBuilder::new(&mut buffer);
            builder.write_all(b"abandoned transaction").unwrap();
            builder.flush().unwrap(); // Flush must not commit a record.
        }
        assert_eq!(&buffer[..], FRAME);
        assert!(serialize_with_semantic_error(&mut buffer).is_err());
        assert_eq!(&buffer[..], FRAME);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut builder = RecordBuilder::new(&mut buffer);
            builder.write_all(b"interrupted transaction").unwrap();
            panic!("serializer panicked");
        }));
        assert!(result.is_err());
        assert_eq!(&buffer[..], FRAME);
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(buffer.capacity(), capacity);

        let id = RecordId::new(StreamId::MIN, SequenceNumber::new(42));
        RecordBuilder::new(&mut buffer).finish(id).unwrap();
        assert_eq!(&buffer[..FRAME.len()], FRAME);
        assert_eq!(&buffer[FRAME.len()..], encode_frame(id, b""));
    }

    #[tokio::test]
    async fn finished_records_pass_the_real_reader_in_a_mixed_stream_batch() {
        let records = [
            (
                RecordId::new(StreamId::MIN, SequenceNumber::MIN),
                Vec::new(),
            ),
            (
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
                vec![0xff; 65_519],
            ),
            (
                RecordId::new(StreamId::new(42).unwrap(), SequenceNumber::new(256)),
                b"last transaction".to_vec(),
            ),
        ];
        let mut buffer = BytesMut::new();
        for (id, payload) in &records {
            let mut builder = RecordBuilder::new(&mut buffer);
            for chunk in payload.chunks(2048) {
                builder.write_all(chunk).unwrap();
            }
            builder.finish(*id).unwrap();
        }

        let mut reader = RecordReader::new(&buffer[..]);
        let mut index = 0;
        while reader.wait_to_read().await.unwrap() {
            while let Some(record) = reader.try_read_next().unwrap() {
                let (id, payload) = &records[index];
                assert_eq!(record.id(), *id);
                assert_eq!(record.body(), payload);
                assert_eq!(usize::from(record.length()), payload.len() + 16);
                index += 1;
            }
        }
        assert_eq!(index, records.len());
    }

    fn assert_io_error(error: io::Error) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.get_ref().unwrap().downcast_ref::<RecordBuildError>(),
            Some(&RecordBuildError::PayloadTooLarge)
        );
    }
}

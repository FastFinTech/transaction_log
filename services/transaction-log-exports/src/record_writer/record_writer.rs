use std::{fmt, io::Write};

use bytes::{Bytes, BytesMut};

use super::{RecordWriteError, record_builder::RecordBuilder};
use crate::RecordId;

/// Writes opaque bodies into complete records in reusable, owned storage.
///
/// Each [`write`](Self::write) invokes one body-writing callback synchronously,
/// then finalizes the record's header and CRC. Completed records are concatenated
/// without padding or per-record buffer splits. Failed records are rolled back.
/// Callers assign IDs and preserve ordering within each stream; this writer does
/// not allocate sequence numbers or check continuity.
///
/// This is the construction API. It performs no socket I/O, queue submission,
/// replication, or durable flush. Consume [`buffered_bytes`](Self::buffered_bytes)
/// before [`clear`](Self::clear) to reuse storage, or transfer the whole completed
/// buffer with [`into_bytes`](Self::into_bytes). Storage grows as needed; callers
/// currently control how much to accumulate. Output coordination and bounded
/// buffer recycling will be added by `RecordOutput` in a subsequent step.
///
/// Use one writer per producer/execution context. Writing requires exclusive
/// access and takes no locks. A writer may move between threads between calls;
/// the temporary internal builder exists only within the synchronous call.
#[derive(Default)]
pub struct RecordWriter {
    buffer: BytesMut,
}

impl RecordWriter {
    /// Creates an empty writer without allocating record storage yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty writer with at least `capacity` bytes of storage.
    ///
    /// This is an initial allocation size, not a record or batch limit. Before
    /// each callback, the builder reserves enough additional space for a
    /// maximum-sized record. With that much spare capacity already available,
    /// construction, body writing, and finalization need no buffer growth.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Appends one complete record using the caller's ID and body-writing callback.
    ///
    /// The callback runs once on this thread with a borrowed [`Write`] destination.
    /// It can capture borrowed or owned data and make many incremental writes,
    /// including calls to a serializer, without staging the payload elsewhere.
    /// The callback is generic and unboxed; its destination is `&mut dyn Write`
    /// so the builder stays private. Inlining can eliminate destination dispatch,
    /// but this is compiler-dependent. Only successful body writing and finalization
    /// publish the new record in [`buffered_bytes`](Self::buffered_bytes).
    ///
    /// # Errors
    ///
    /// Returns [`RecordWriteError::Build`] for a rejected write even if the
    /// callback ignores or translates its error. Otherwise a callback error
    /// is returned unchanged inside [`RecordWriteError::BodyWrite`]. Earlier
    /// completed records and the current allocation's capacity are preserved.
    /// The caller can retry with the same ID; no sequence state is advanced here.
    ///
    /// Callback panics propagate. During unwinding the current record is
    /// rolled back as well; process abort does not run destructors. Allocation
    /// failure follows `BytesMut`/Rust allocation behavior, not this error type.
    #[inline]
    pub fn write<F, E>(&mut self, id: RecordId, write_body: F) -> Result<(), RecordWriteError<E>>
    where
        F: FnOnce(&mut dyn Write) -> Result<(), E>,
    {
        // The builder acquires capacity once and confines all raw-pointer work
        // to its exclusive borrow. No reference to its storage escapes this call.
        let mut builder = RecordBuilder::new(&mut self.buffer);
        if let Err(error) = write_body(&mut builder) {
            // Preserve the construction error if the callback translated it.
            // Either return drops the unfinished builder and rolls back.
            builder.check_error()?;
            return Err(RecordWriteError::BodyWrite(error));
        }
        // finish also checks retained errors, including ignored failed writes.
        // The successful path checks that state only once after the callback.
        builder.finish(id)?;
        Ok(())
    }

    /// Borrows the concatenated, fully finalized records awaiting consumption.
    ///
    /// The slice contains no provisional header, partial payload, or padding.
    /// Taking this view does not copy bytes or change reference counts. The
    /// borrow prevents writing or clearing while the view is still in use.
    pub fn buffered_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Discards all buffered records while retaining storage for reuse.
    ///
    /// Call this after consuming the bytes, or when intentionally abandoning
    /// them. This method sends nothing and does not establish remote receipt.
    /// It does not zero storage or reset any sequence state.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Consumes the writer and transfers all completed records as immutable bytes.
    ///
    /// Freezes the whole buffer without copying its contents. The writer no
    /// longer exists to reuse this allocation; this is an ownership transfer,
    /// not the future output pool's recycling mechanism.
    pub fn into_bytes(self) -> Bytes {
        self.buffer.freeze()
    }
}

impl fmt::Debug for RecordWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordWriter")
            .field("buffered_bytes", &self.buffer.len())
            .field("capacity", &self.buffer.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io::{self, Write},
        panic::{AssertUnwindSafe, catch_unwind},
        rc::Rc,
        thread,
    };

    use super::{RecordWriteError, RecordWriter};
    use crate::record::record_protocol::test_data::encode_frame;
    use crate::{RecordBuildError, RecordId, RecordReader, SequenceNumber, StreamId};

    fn write_payload<W: Write + ?Sized>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
        // Exercise many writes through the public callback contract.
        for chunk in payload.chunks(7) {
            writer.write_all(chunk)?;
        }
        writer.flush()
    }

    fn id(sequence: u64) -> RecordId {
        RecordId::new(StreamId::MIN, SequenceNumber::new(sequence))
    }

    #[test]
    fn invokes_a_consuming_callback_once_on_the_calling_thread() {
        let calls = Rc::new(Cell::new(0));
        let captured = Rc::clone(&calls); // Deliberately not Send or Sync.
        let expected_thread = thread::current().id();
        let mut writer = RecordWriter::new();
        assert_eq!(writer.buffer.capacity(), 0);
        writer
            .write(id(1), move |body| {
                assert_eq!(thread::current().id(), expected_thread);
                captured.set(captured.get() + 1);
                drop(captured); // Consuming this capture requires FnOnce support.
                body.write_all(&2_u16.to_le_bytes())?;
                body.write_all(&42_u64.to_le_bytes())
            })
            .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(
            writer.buffered_bytes(),
            encode_frame(id(1), &[2, 0, 42, 0, 0, 0, 0, 0, 0, 0])
        );
    }

    #[test]
    fn preserves_semantic_errors_and_prior_records_then_accepts_a_retry() {
        // A callback error need not implement Error, Display, or Send.
        #[derive(Debug)]
        struct SemanticError(Rc<()>);

        let mut writer = RecordWriter::with_capacity(2 * 65_535);
        writer
            .write(id(1), |body| write_payload(body, b"earlier"))
            .unwrap();
        let before = writer.buffered_bytes().to_vec();
        let pointer = writer.buffer.as_ptr();
        let capacity = writer.buffer.capacity();
        let token = Rc::new(());
        let error = writer
            .write(id(2), |body| {
                body.write_all(b"partial body").unwrap();
                Err(SemanticError(Rc::clone(&token)))
            })
            .unwrap_err();
        match error {
            RecordWriteError::BodyWrite(SemanticError(returned)) => {
                assert!(Rc::ptr_eq(&returned, &token));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(writer.buffered_bytes(), before);
        assert_eq!(writer.buffer.as_ptr(), pointer);
        assert_eq!(writer.buffer.capacity(), capacity);
        writer
            .write(id(2), |body| write_payload(body, b"retry"))
            .unwrap();
        assert_eq!(
            &writer.buffered_bytes()[before.len()..],
            encode_frame(id(2), b"retry")
        );
    }

    #[test]
    fn reports_build_errors_when_propagated_translated_or_ignored() {
        enum Handling {
            Propagate,
            Translate,
            Ignore,
        }

        let too_large = vec![0xa5; 65_519];
        let mut writer = RecordWriter::with_capacity(2 * 65_535);
        writer
            .write(id(1), |body| write_payload(body, b"keep"))
            .unwrap();
        let before = writer.buffered_bytes().to_vec();
        for handling in [Handling::Propagate, Handling::Translate, Handling::Ignore] {
            let error = writer
                .write(id(2), |body| {
                    body.write_all(b"partial")?;
                    let result = body.write_all(&too_large);
                    assert!(result.is_err());
                    match handling {
                        Handling::Propagate => result,
                        Handling::Translate => Err(io::Error::other("callback failed")),
                        Handling::Ignore => Ok(()),
                    }
                })
                .unwrap_err();
            assert!(matches!(
                error,
                RecordWriteError::Build(RecordBuildError::PayloadTooLarge)
            ));
            assert_eq!(writer.buffered_bytes(), before);
        }
        writer
            .write(id(2), |body| write_payload(body, b"retry"))
            .unwrap();
        assert_eq!(
            &writer.buffered_bytes()[before.len()..],
            encode_frame(id(2), b"retry")
        );
    }

    #[test]
    fn unwinding_rolls_back_only_the_current_record_and_keeps_writer_usable() {
        let mut writer = RecordWriter::with_capacity(2 * 65_535);
        writer
            .write(id(1), |body| write_payload(body, b"keep"))
            .unwrap();
        let before = writer.buffered_bytes().to_vec();
        let pointer = writer.buffer.as_ptr();
        let capacity = writer.buffer.capacity();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = writer.write(id(2), |body| -> io::Result<()> {
                body.write_all(b"unfinished")?;
                panic!("callback panic");
            });
        }));
        assert!(result.is_err());
        assert_eq!(writer.buffered_bytes(), before);
        assert_eq!(writer.buffer.as_ptr(), pointer);
        assert_eq!(writer.buffer.capacity(), capacity);
        writer
            .write(id(2), |body| write_payload(body, b"retry"))
            .unwrap();
        assert_eq!(
            &writer.buffered_bytes()[before.len()..],
            encode_frame(id(2), b"retry")
        );
    }

    #[test]
    fn clearing_reuses_storage_and_whole_buffer_transfer_preserves_its_address() {
        let mut writer = RecordWriter::with_capacity(2 * 65_535);
        let pointer = writer.buffer.as_ptr();
        let capacity = writer.buffer.capacity();
        let large = vec![0x5a; 65_519];
        for sequence in 0..3 {
            writer
                .write(id(sequence), |body| write_payload(body, &large))
                .unwrap();
            assert_eq!(writer.buffered_bytes(), encode_frame(id(sequence), &large));
            assert_eq!(writer.buffer.as_ptr(), pointer);
            assert_eq!(writer.buffer.capacity(), capacity);
            writer.clear();
            assert!(writer.buffered_bytes().is_empty());
            assert_eq!(writer.buffer.capacity(), capacity);
        }
        writer
            .write(id(3), |body| write_payload(body, b"last"))
            .unwrap();
        let bytes = writer.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(&bytes[..], encode_frame(id(3), b"last"));
    }

    #[test]
    fn writer_can_move_between_threads_between_calls() {
        let mut writer = RecordWriter::new();
        writer
            .write(id(1), |body| write_payload(body, b"first"))
            .unwrap();
        let mut writer = thread::spawn(move || {
            writer
                .write(id(2), |body| write_payload(body, b"second"))
                .unwrap();
            writer
        })
        .join()
        .unwrap();
        writer
            .write(id(3), |body| write_payload(body, b"third"))
            .unwrap();
        let expected = [
            encode_frame(id(1), b"first"),
            encode_frame(id(2), b"second"),
            encode_frame(id(3), b"third"),
        ]
        .concat();
        assert_eq!(writer.buffered_bytes(), expected);
    }

    #[tokio::test]
    async fn mixed_stream_records_with_boundary_payloads_pass_the_reader() {
        let records = [
            (id(0), Vec::new()),
            (
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
                vec![0xff; 65_519],
            ),
            (id(0), vec![0x42; 2048]), // Continuity belongs to the service.
        ];
        let mut writer = RecordWriter::new();
        let mut expected = Vec::new();
        for (id, payload) in &records {
            writer
                .write(*id, |body| write_payload(body, payload))
                .unwrap();
            expected.extend_from_slice(&encode_frame(*id, payload));
        }
        assert_eq!(writer.buffered_bytes(), expected);
        let bytes = writer.into_bytes();
        let mut reader = RecordReader::new(&bytes[..]);
        let mut index = 0;
        while reader.wait_to_read().await.unwrap() {
            while let Some(record) = reader.try_read_next().unwrap() {
                let (id, payload) = &records[index];
                assert_eq!(record.id(), *id);
                assert_eq!(record.body(), payload);
                index += 1;
            }
        }
        assert_eq!(index, records.len());
    }
}

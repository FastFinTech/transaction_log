use getset::CopyGetters;
use tokio::{fs::File, io::AsyncWrite};
use transaction_log_exports::{AsyncSyncData, ExistingRecords, Record, RecordId, RecordWriter};

use crate::streams::{
    IndexWriter, InitializedStream, LogFileId, RECORDS_PER_FILE, RecordEndLocation,
    RecordStartLocation,
};

use super::{AppendOutcome, IndexedLogWriteError, indexed_log_writer_state::IndexedLogWriterState};

/// Appends ordered records and their dense index to one owned output pair.
///
/// `L` receives unchanged record encodings; `I` receives little-endian `u64`
/// exclusive record ends. Both destinations are already open and positioned by
/// their owner. This component never opens, reads, seeks, renames or deletes files.
/// Public construction takes an owned Tokio index file; the default `I = File`
/// keeps ordinary use as `IndexedLogWriter<L>`. Other index types support internal
/// scripted tests. Consume an [`InitializedStream`] with [`Self::from`] to resume
/// its recovered pair while preserving its synchronized progress.
///
/// [`write_record`](Self::write_record) buffers synchronously. Owner-driven
/// [`flush`](Self::flush) completes log output and its destination flush before
/// starting any index output. [`sync_data`](Self::sync_data) includes pending
/// output and synchronizes both destinations. The three endpoint getters keep
/// buffering, read visibility and durability distinct.
///
/// One exclusive owner drives all operations. There are no workers, timers,
/// channels, locks or imposed `Send` bounds. Historical readers use independent
/// file handles and an owner-published boundary; this writer serves no requests
/// and retains no record history after its batch has been flushed.
///
/// I/O failure, panic or cancellation makes the entire pair unusable, even if
/// only one destination failed. Dropping or extracting it never implicitly
/// flushes, synchronizes or finalizes. See the module specification for recovery
/// responsibilities and the trusted validation handover.
#[derive(CopyGetters)]
pub struct IndexedLogWriter<L, I = File> {
    /// File whose stream and assigned sequence range govern every append.
    #[getset(get_copy = "pub")]
    file_id: LogFileId,
    /// Required ID for the next append, advanced only after both buffers accept a record.
    ///
    /// Starts at the file's first assigned ID or the recovered end's successor.
    /// After the last slot it points into the next file, but this writer remains
    /// full and rejects further appends. Rejections and output operations leave it unchanged.
    #[getset(get_copy = "pub")]
    expected_record_id: RecordId,
    /// Records in the existing validated prefix plus successful buffered appends.
    #[getset(get_copy = "pub")]
    record_count: u64,
    /// Last accepted record, including records still buffered locally.
    #[getset(get_copy = "pub")]
    buffered_end: Option<RecordEndLocation>,
    /// Last complete record whose log bytes and index entry finished flushing.
    ///
    /// A validated starting prefix is already readable. This boundary advances
    /// only after both outputs finish; it is not a durability guarantee.
    #[getset(get_copy = "pub")]
    flushed_end: Option<RecordEndLocation>,
    /// Last complete record covered by a successful synchronization of both outputs.
    ///
    /// Starts at the recovered endpoint when consuming an initialized stream,
    /// or `None` for an empty pair. After an I/O failure, this remains the last known success.
    #[getset(get_copy = "pub")]
    synced_end: Option<RecordEndLocation>,
    log: RecordWriter<L, ExistingRecords>,
    index: IndexWriter<I>,
    // Arm around paired appends and I/O; restore only on complete success.
    // This covers failures between writers that either one alone cannot see.
    state: IndexedLogWriterState,
}

impl<L> IndexedLogWriter<L> {
    /// Takes ownership of an empty log/index pair for the supplied file identity.
    ///
    /// The caller must supply two distinct, empty destinations positioned at zero
    /// and retain exclusive write ownership. Read-only handles may coexist. This
    /// constructor performs no I/O or file inspection. A new nonzero-numbered
    /// file starts at its assigned first record ID, not sequence zero.
    pub fn new(file_id: LogFileId, log: L, index: File) -> Self {
        Self {
            file_id,
            expected_record_id: file_id.first_record_id(),
            record_count: 0,
            buffered_end: None,
            flushed_end: None,
            synced_end: None,
            log: RecordWriter::for_records(log),
            index: IndexWriter::new(index),
            state: IndexedLogWriterState::Open,
        }
    }
}

impl From<InitializedStream> for IndexedLogWriter<File> {
    /// Consumes an initialized stream's active pair, preserving recovered durability.
    ///
    /// The initializer has repaired and synchronized any recovered records and
    /// positioned both files for appending. All three progress endpoints start
    /// at that file-local end; an empty active pair starts with all three absent,
    /// even if the stream checkpoint covers a preceding completed file.
    ///
    /// Moves the existing handles without reopening, seeking, validation or I/O.
    /// The owner must preserve the initializer's exclusive-write contract through
    /// handover. Fresh empty files require no initial synchronization here.
    fn from(initialized: InitializedStream) -> Self {
        let InitializedStream {
            file_id,
            log,
            index,
            end,
        } = initialized;
        Self {
            file_id,
            expected_record_id: end
                .map_or_else(|| file_id.first_record_id(), |end| end.record_id().next()),
            record_count: end.map_or(0, |end| LogFileId::record_count_through(end.record_id())),
            buffered_end: end,
            flushed_end: end,
            synced_end: end,
            log: RecordWriter::for_records(log),
            index: IndexWriter::new(index),
            state: IndexedLogWriterState::Open,
        }
    }
}

impl<L, I> IndexedLogWriter<L, I> {
    /// Whether all assigned record positions have been accepted, even if buffered.
    pub fn is_full(&self) -> bool {
        self.record_count == RECORDS_PER_FILE
    }

    /// Synchronously buffers a validated record and its exclusive end offset.
    ///
    /// Checks identity and capacity before changing either buffer. Wrong-stream,
    /// duplicate and skipped-sequence records return the expected and actual IDs.
    /// Framing, stream-ID domain and CRC validity are reused from `Record`.
    ///
    /// Copies the encoding once into the reusable record batch and appends eight
    /// index bytes. It does not retain the source handle, serialize a payload,
    /// perform I/O or publish read visibility. The owner controls batch size and
    /// retained capacity by deciding when to flush.
    ///
    /// Success returns the accepted endpoint and whether this record filled the
    /// file. Neither is a flush or durability acknowledgement. The cached expected
    /// ID advances only after both appends succeed; all existing guards remain.
    pub fn write_record(&mut self, record: &Record) -> Result<AppendOutcome, IndexedLogWriteError> {
        self.check_open()?;
        if self.is_full() {
            return Err(IndexedLogWriteError::Full);
        }
        let expected = self.expected_record_id;
        let actual = record.id();
        if actual != expected {
            return Err(IndexedLogWriteError::UnexpectedRecordId { expected, actual });
        }
        let start = match self.buffered_end {
            Some(end) => end.next_record_start(),
            None => RecordStartLocation::new(self.file_id.first_record_id(), 0),
        };
        let end = start.to_end(record.length());
        let next_expected_record_id = expected.next();
        // The two appends are one logical acceptance. If either unexpectedly
        // fails or unwinds, do not allow a partly updated pair to continue.
        self.state = IndexedLogWriterState::Unusable;
        self.log
            .write_record(record)
            .map_err(IndexedLogWriteError::Log)?;
        self.index
            .write(end.position())
            .map_err(IndexedLogWriteError::Index)?;
        self.record_count += 1;
        self.buffered_end = Some(end);
        self.expected_record_id = next_expected_record_id;
        self.state = IndexedLogWriterState::Open;
        Ok(AppendOutcome {
            end,
            file_full: self.is_full(),
        })
    }

    /// Ends appending once the full pair has completed durable synchronization.
    ///
    /// Performs no I/O. The owner explicitly calls `sync_data` before finalizing.
    /// Returns the completed file's exclusive end for the owner's publication
    /// step; it does not publish a stream checkpoint or change any path.
    ///
    /// `NotFull` and `NotSynchronized` leave the writer open. On success all further
    /// append, flush, sync and finalize calls return `Finalized`. The owner can
    /// drop the writer or use `into_inner` to take its destinations.
    pub fn finalize(&mut self) -> Result<RecordEndLocation, IndexedLogWriteError> {
        self.check_open()?;
        if !self.is_full() {
            return Err(IndexedLogWriteError::NotFull {
                record_count: self.record_count,
            });
        }
        if self.synced_end != self.buffered_end {
            return Err(IndexedLogWriteError::NotSynchronized);
        }
        let end = self
            .buffered_end
            .expect("a full indexed log has a last record");
        self.state = IndexedLogWriterState::Finalized;
        Ok(end)
    }

    /// Returns both destinations, discarding unsent buffers without any I/O.
    ///
    /// Available after finalization or failure so the owner can close or recover
    /// the pair. Extraction itself does not repair partial output or authorize
    /// resumption. Call `flush`/`sync_data` beforehand to preserve pending records.
    pub fn into_inner(self) -> (L, I) {
        (self.log.into_inner(), self.index.into_inner())
    }

    fn check_open(&self) -> Result<(), IndexedLogWriteError> {
        match self.state {
            IndexedLogWriterState::Open => Ok(()),
            IndexedLogWriterState::Unusable => Err(IndexedLogWriteError::Unusable),
            IndexedLogWriterState::Finalized => Err(IndexedLogWriteError::Finalized),
        }
    }
}

impl<L: AsyncWrite + Unpin, I: AsyncWrite + Unpin> IndexedLogWriter<L, I> {
    /// Flushes the record batch completely before starting its index output.
    ///
    /// Sends the record buffer, awaits the log destination's flush, then writes
    /// and flushes the index batch. In particular, a completed Tokio write alone
    /// is insufficient: its flush must finish before an index entry can become
    /// visible. Index readers must still handle incomplete trailing entries and
    /// respect the owner's published boundary while a batch is in progress.
    ///
    /// Advances `flushed_end` only after both destinations succeed. Empty batches
    /// perform no I/O. Buffers retain capacity; neither file is synchronized.
    /// Failure, panic or cancellation after I/O starts leaves the pair unusable.
    /// Dropping an unpolled future has no effect.
    pub async fn flush(&mut self) -> Result<(), IndexedLogWriteError> {
        self.check_open()?;
        if self.buffered_end == self.flushed_end {
            return Ok(());
        }
        self.state = IndexedLogWriterState::Unusable;
        self.log
            .flush_buffer()
            .await
            .map_err(IndexedLogWriteError::Log)?;
        self.log.flush().await.map_err(IndexedLogWriteError::Log)?;
        self.index
            .flush_buffer()
            .await
            .map_err(IndexedLogWriteError::Index)?;
        self.index
            .flush()
            .await
            .map_err(IndexedLogWriteError::Index)?;
        self.flushed_end = self.buffered_end;
        self.state = IndexedLogWriterState::Open;
        Ok(())
    }
}

impl<L: AsyncWrite + AsyncSyncData + Unpin, I: AsyncWrite + AsyncSyncData + Unpin>
    IndexedLogWriter<L, I>
{
    /// Flushes pending records, then synchronizes the log followed by its index.
    ///
    /// Both destinations must implement the existing static-dispatch sync
    /// capability. Log synchronization completes before index synchronization
    /// starts. This ordering does not make a pair of files an atomic transaction.
    ///
    /// Advances `synced_end` only after both calls succeed. A failure may leave
    /// `flushed_end` ahead of `synced_end`, but never advances the latter based
    /// on only one file's success. Failure, panic or cancellation makes the pair
    /// unusable; the last successful boundaries remain available for observation.
    ///
    /// With no new records, still synchronizes both outputs, even when recovery
    /// already established the starting prefix's durability.
    pub async fn sync_data(&mut self) -> Result<(), IndexedLogWriteError> {
        self.flush().await?;
        self.state = IndexedLogWriterState::Unusable;
        self.log
            .sync_data()
            .await
            .map_err(IndexedLogWriteError::Log)?;
        self.index
            .sync_data()
            .await
            .map_err(IndexedLogWriteError::Index)?;
        self.synced_end = self.flushed_end;
        self.state = IndexedLogWriterState::Open;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        future::{Future, poll_fn},
        io,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    use transaction_log_exports::{
        RecordId, RecordOutputError, RecordReader, SequenceNumber, StreamId,
    };

    use super::*;
    use crate::{
        storage::test_support::storage_fixture,
        streams::{IndexWriteError, StreamCheckpoint, StreamInitializer},
    };

    // Deterministic destinations distinguish accepted bytes from flushed bytes.
    // Rc also proves that the writer does not impose Send on outputs or futures.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Stage {
        LogWrite,
        LogFlush,
        IndexWrite,
        IndexFlush,
        LogSync,
        IndexSync,
    }

    const STAGES: [Stage; 6] = [
        Stage::LogWrite,
        Stage::LogFlush,
        Stage::IndexWrite,
        Stage::IndexFlush,
        Stage::LogSync,
        Stage::IndexSync,
    ];

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        Error,
        Pending,
        Panic,
        WriteZero,
    }

    #[derive(Default)]
    struct Observed {
        log: Vec<u8>,
        index: Vec<u8>,
        log_visible: usize,
        index_visible: usize,
        calls: Vec<Stage>,
        fault: Option<(Stage, Fault)>,
        // Write faults occur after one short write, including within an entry.
        calls_before_fault: usize,
    }

    struct Output {
        observed: Rc<RefCell<Observed>>,
        is_index: bool,
    }

    impl Output {
        fn poll_stage(&self, stage: Stage) -> Poll<io::Result<()>> {
            let mut observed = self.observed.borrow_mut();
            observed.calls.push(stage);
            if stage == Stage::IndexWrite {
                assert_eq!(
                    observed.log_visible,
                    observed.log.len(),
                    "index output must wait for the entire log batch to finish flushing"
                );
            }
            if let Some((target, fault)) = observed.fault
                && target == stage
            {
                if observed.calls_before_fault > 0 {
                    observed.calls_before_fault -= 1;
                } else {
                    return match fault {
                        Fault::Error => Poll::Ready(Err(io::Error::from_raw_os_error(123))),
                        Fault::Pending => Poll::Pending,
                        Fault::Panic => panic!("scripted destination panic"),
                        Fault::WriteZero => Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                    };
                }
            }
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Output {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let stage = if self.is_index {
                Stage::IndexWrite
            } else {
                Stage::LogWrite
            };
            match self.poll_stage(stage) {
                Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::WriteZero => {
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
            let count = bytes.len().min(3);
            let mut observed = self.observed.borrow_mut();
            let destination = if self.is_index {
                &mut observed.index
            } else {
                &mut observed.log
            };
            destination.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let stage = if self.is_index {
                Stage::IndexFlush
            } else {
                Stage::LogFlush
            };
            match self.poll_stage(stage) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
            let mut observed = self.observed.borrow_mut();
            if self.is_index {
                observed.index_visible = observed.index.len();
            } else {
                observed.log_visible = observed.log.len();
            }
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            panic!("the indexed writer must not shut down its destinations implicitly");
        }
    }

    impl AsyncSyncData for Output {
        async fn sync_data(&mut self) -> io::Result<()> {
            let stage = if self.is_index {
                Stage::IndexSync
            } else {
                Stage::LogSync
            };
            poll_fn(|_| self.poll_stage(stage)).await
        }
    }

    type TestWriter = IndexedLogWriter<Output, Output>;

    fn id(stream: u16, sequence: u64) -> RecordId {
        RecordId::new(
            StreamId::new(stream).unwrap(),
            SequenceNumber::new(sequence),
        )
    }

    fn writer(first: RecordId) -> (TestWriter, Rc<RefCell<Observed>>) {
        let observed = Rc::new(RefCell::new(Observed::default()));
        let log = Output {
            observed: Rc::clone(&observed),
            is_index: false,
        };
        let index = Output {
            observed: Rc::clone(&observed),
            is_index: true,
        };
        (
            IndexedLogWriter {
                file_id: LogFileId::from_record_id(first),
                expected_record_id: LogFileId::from_record_id(first).first_record_id(),
                record_count: 0,
                buffered_end: None,
                flushed_end: None,
                synced_end: None,
                log: RecordWriter::for_records(log),
                index: IndexWriter::with_file(index),
                state: IndexedLogWriterState::Open,
            },
            observed,
        )
    }

    // App tests enter through the real validating boundary, without bypassing
    // Record's crate-private constructor or duplicating its framing/CRC codec.
    async fn records(stream: u16, first: u64, payload_lengths: &[usize]) -> Vec<Record> {
        let mut encoder = RecordWriter::for_serialization(Vec::new());
        for (offset, &length) in payload_lengths.iter().enumerate() {
            encoder
                .write(id(stream, first + offset as u64), |body| {
                    std::io::Write::write_all(body, &vec![0xa5; length])
                })
                .unwrap();
        }
        encoder.flush_buffer().await.unwrap();
        let bytes = encoder.into_inner();
        let mut reader = RecordReader::new(bytes.as_slice());
        let mut records = Vec::new();
        while reader.wait_to_read().await.unwrap() {
            while let Some(record) = reader.try_read_next().unwrap() {
                records.push(record);
            }
        }
        assert_eq!(records.len(), payload_lengths.len());
        records
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test]
    async fn buffers_without_io_then_flushes_exact_bytes_and_dense_offsets() {
        let records = records(42, 100_000, &[0, 3, 1]).await;
        let (mut writer, observed) = writer(records[0].id());
        assert_eq!(writer.file_id(), LogFileId::from_record_id(records[0].id()));
        assert_eq!(writer.record_count(), 0);
        assert_eq!(writer.buffered_end(), None);
        assert_eq!(writer.expected_record_id(), id(42, 100_000));
        let mut outcomes = Vec::new();
        for (offset, (record, position)) in records.iter().zip([16, 35, 52]).enumerate() {
            let outcome = writer.write_record(record).unwrap();
            assert_eq!(outcome.end(), RecordEndLocation::new(record.id(), position));
            assert!(!outcome.file_full());
            assert_eq!(writer.expected_record_id(), id(42, 100_001 + offset as u64));
            outcomes.push(outcome);
        }
        // Earlier outcomes remain snapshots after more records are accepted.
        assert_eq!(
            outcomes[0].end(),
            RecordEndLocation::new(id(42, 100_000), 16)
        );
        let end = Some(RecordEndLocation::new(id(42, 100_002), 52));
        assert_eq!(writer.record_count(), 3);
        assert_eq!(writer.buffered_end(), end);
        assert_eq!(writer.flushed_end(), None);
        assert_eq!(writer.synced_end(), None);
        assert!(observed.borrow().calls.is_empty());
        writer.flush().await.unwrap();
        assert_eq!(writer.flushed_end(), end);
        assert_eq!(writer.synced_end(), None);
        {
            let output = observed.borrow();
            let expected_log: Vec<u8> = records
                .iter()
                .flat_map(|r| r.as_bytes().iter().copied())
                .collect();
            assert_eq!(output.log, expected_log);
            // Independent literal fixture: 16-byte framing plus payloads 0, 3, 1.
            assert_eq!(
                output.index,
                [
                    16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0,
                ]
            );
            assert_eq!(output.log_visible, 52);
            assert_eq!(output.index_visible, 24);
            let mut phases = output.calls.clone();
            phases.dedup();
            assert_eq!(
                phases,
                [
                    Stage::LogWrite,
                    Stage::LogFlush,
                    Stage::IndexWrite,
                    Stage::IndexFlush
                ]
            );
        }
        writer.sync_data().await.unwrap();
        assert_eq!(writer.synced_end(), end);
        assert_eq!(writer.expected_record_id(), id(42, 100_003));
        assert!(
            observed
                .borrow()
                .calls
                .ends_with(&[Stage::LogSync, Stage::IndexSync])
        );

        let next = self::records(42, 100_003, &[0]).await;
        writer.write_record(&next[0]).unwrap();
        writer.flush().await.unwrap();
        assert_eq!(&observed.borrow().index[24..], &[68, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(writer.synced_end(), end);
    }

    #[tokio::test]
    async fn identity_rejections_preserve_both_buffers_and_allow_the_correct_record() {
        let valid = records(7, 100_000, &[1, 2]).await;
        let wrong_stream = records(8, 100_000, &[0]).await;
        let previous_file = records(7, 99_999, &[0]).await;
        let (mut writer, observed) = writer(valid[0].id());
        for invalid in [&wrong_stream[0], &previous_file[0], &valid[1]] {
            assert!(matches!(writer.write_record(invalid),
                Err(IndexedLogWriteError::UnexpectedRecordId { expected, actual })
                    if expected == valid[0].id() && actual == invalid.id()));
            assert_eq!(writer.record_count(), 0);
            assert_eq!(writer.buffered_end(), None);
            assert_eq!(writer.expected_record_id(), valid[0].id());
        }
        writer.write_record(&valid[0]).unwrap();
        assert_eq!(writer.expected_record_id(), valid[1].id());
        let end = writer.buffered_end();
        assert!(matches!(writer.write_record(&valid[0]),
            Err(IndexedLogWriteError::UnexpectedRecordId { expected, actual })
                if expected == valid[1].id() && actual == valid[0].id()));
        assert_eq!(writer.record_count(), 1);
        assert_eq!(writer.buffered_end(), end);
        assert_eq!(writer.expected_record_id(), valid[1].id());
        writer.write_record(&valid[1]).unwrap();
        assert_eq!(writer.expected_record_id(), id(7, 100_002));
        writer.flush().await.unwrap();
        assert_eq!(observed.borrow().log.len(), 35);
        assert_eq!(
            observed.borrow().index,
            [17, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[tokio::test]
    async fn rejected_inner_append_does_not_advance_expected_id_or_report_acceptance() {
        let records = records(0, 0, &[0]).await;
        for stage in [Stage::LogFlush, Stage::IndexFlush] {
            let (mut writer, observed) = writer(records[0].id());
            // Exercise either lower writer refusing its append, without adding
            // test-specific hooks to the pair's synchronous buffering path.
            observed.borrow_mut().fault = Some((stage, Fault::Error));
            match stage {
                Stage::LogFlush => {
                    writer.log.flush().await.unwrap_err();
                }
                Stage::IndexFlush => {
                    writer.index.flush().await.unwrap_err();
                }
                _ => unreachable!(),
            }
            observed.borrow_mut().fault = None;
            let calls = observed.borrow().calls.len();
            match writer.write_record(&records[0]).unwrap_err() {
                IndexedLogWriteError::Log(RecordOutputError::Unusable) => {
                    assert_eq!(stage, Stage::LogFlush)
                }
                IndexedLogWriteError::Index(IndexWriteError::Unusable) => {
                    assert_eq!(stage, Stage::IndexFlush)
                }
                error => panic!("unexpected append failure: {error:?}"),
            }
            assert_eq!(writer.expected_record_id(), id(0, 0));
            assert_eq!(writer.record_count(), 0);
            assert_eq!(writer.buffered_end(), None);
            assert_eq!(writer.flushed_end(), None);
            assert_eq!(writer.synced_end(), None);
            assert!(matches!(
                writer.write_record(&records[0]),
                Err(IndexedLogWriteError::Unusable)
            ));
            assert_eq!(writer.expected_record_id(), id(0, 0));
            assert_eq!(observed.borrow().calls.len(), calls);
        }
    }

    #[tokio::test]
    async fn empty_and_unpolled_operations_do_not_send_pending_records() {
        let records = records(0, 0, &[0]).await;
        let (mut writer, observed) = writer(records[0].id());
        writer.flush().await.unwrap();
        assert!(observed.borrow().calls.is_empty());
        writer.sync_data().await.unwrap();
        assert_eq!(observed.borrow().calls, [Stage::LogSync, Stage::IndexSync]);
        assert_eq!(writer.synced_end(), None);
        observed.borrow_mut().calls.clear();
        writer.write_record(&records[0]).unwrap();
        drop(writer.flush());
        drop(writer.sync_data());
        assert!(observed.borrow().calls.is_empty());
        assert!(writer.check_open().is_ok());
        assert_eq!(writer.expected_record_id(), id(0, 1));
        drop(writer.into_inner());
        assert!(observed.borrow().calls.is_empty());

        let (mut writer, observed) = self::writer(records[0].id());
        writer.write_record(&records[0]).unwrap();
        drop(writer);
        assert!(observed.borrow().calls.is_empty());
    }

    #[tokio::test]
    async fn appending_after_a_large_prefix_preserves_absolute_offsets() {
        // A real prefix can exceed 4 GiB: 99,999 records can contain 6.55 GB.
        // Seed internal progress to test offset arithmetic without allocating
        // that prefix. Separate filesystem tests exercise the real handover.
        let prefix = RecordEndLocation::new(id(3, 99_998), 4_294_967_300);
        let records = records(3, 99_999, &[65_519]).await;
        let (mut writer, observed) = writer(records[0].id());
        writer.record_count = 99_999;
        writer.expected_record_id = id(3, 99_999);
        writer.buffered_end = Some(prefix);
        writer.flushed_end = Some(prefix);
        writer.synced_end = Some(prefix);
        assert_eq!(writer.record_count(), 99_999);
        assert_eq!(writer.flushed_end(), Some(prefix));
        assert_eq!(writer.buffered_end(), Some(prefix));
        assert_eq!(writer.synced_end(), Some(prefix));
        writer.flush().await.unwrap();
        assert!(observed.borrow().calls.is_empty());
        writer.sync_data().await.unwrap();
        assert_eq!(writer.synced_end(), Some(prefix));
        let outcome = writer.write_record(&records[0]).unwrap();
        assert_eq!(
            outcome.end(),
            RecordEndLocation::new(id(3, 99_999), 4_295_032_835)
        );
        assert!(outcome.file_full());
        assert_eq!(writer.expected_record_id(), id(3, 100_000));
        assert!(writer.is_full());
        writer.sync_data().await.unwrap();
        assert_eq!(writer.synced_end().unwrap().position(), 4_295_032_835);
        assert_eq!(observed.borrow().index, [3, 0, 1, 0, 1, 0, 0, 0]);
        assert_eq!(observed.borrow().log, records[0].as_bytes().as_ref());
    }

    #[tokio::test]
    async fn full_file_has_exactly_one_hundred_thousand_entries_and_finalizes_only_after_sync() {
        let (mut writer, observed) = writer(id(12, 0));
        assert!(matches!(
            writer.finalize(),
            Err(IndexedLogWriteError::NotFull { record_count: 0 })
        ));
        let records = records(12, 0, &vec![0; 100_001]).await;
        for (offset, record) in records[..100_000].iter().enumerate() {
            let outcome = writer.write_record(record).unwrap();
            assert_eq!(
                outcome.end(),
                RecordEndLocation::new(record.id(), (offset as u64 + 1) * 16)
            );
            assert_eq!(outcome.file_full(), offset == 99_999);
        }
        assert!(writer.is_full());
        assert_eq!(writer.record_count(), 100_000);
        assert_eq!(writer.expected_record_id(), id(12, 100_000));
        assert!(matches!(
            writer.write_record(&records[100_000]),
            Err(IndexedLogWriteError::Full)
        ));
        assert_eq!(writer.expected_record_id(), id(12, 100_000));
        assert!(matches!(
            writer.finalize(),
            Err(IndexedLogWriteError::NotSynchronized)
        ));
        writer.flush().await.unwrap();
        assert!(matches!(
            writer.finalize(),
            Err(IndexedLogWriteError::NotSynchronized)
        ));
        writer.sync_data().await.unwrap();
        let calls = observed.borrow().calls.len();
        let end = writer.finalize().unwrap();
        assert_eq!(end, RecordEndLocation::new(id(12, 99_999), 1_600_000));
        assert_eq!(observed.borrow().index.len(), 800_000);
        assert_eq!(
            &observed.borrow().index[799_992..],
            &[0, 106, 24, 0, 0, 0, 0, 0]
        );
        for (position, entry) in observed
            .borrow()
            .index
            .as_chunks::<8>()
            .0
            .iter()
            .enumerate()
        {
            assert_eq!(u64::from_le_bytes(*entry), (position as u64 + 1) * 16);
        }
        assert!(matches!(
            writer.write_record(&records[0]),
            Err(IndexedLogWriteError::Finalized)
        ));
        assert_eq!(writer.expected_record_id(), id(12, 100_000));
        assert!(matches!(
            writer.flush().await,
            Err(IndexedLogWriteError::Finalized)
        ));
        assert!(matches!(
            writer.sync_data().await,
            Err(IndexedLogWriteError::Finalized)
        ));
        assert!(matches!(
            writer.finalize(),
            Err(IndexedLogWriteError::Finalized)
        ));
        assert_eq!(observed.borrow().calls.len(), calls);
        drop(writer.into_inner());
        assert_eq!(observed.borrow().calls.len(), calls);
    }

    #[tokio::test]
    async fn every_io_stage_preserves_progress_and_poisoning_on_error_cancellation_or_panic() {
        let records = records(0, 0, &[0, 1]).await;
        for stage in STAGES {
            for fault in [Fault::Error, Fault::Pending, Fault::Panic] {
                let (mut writer, observed) = writer(records[0].id());
                writer.write_record(&records[0]).unwrap();
                writer.sync_data().await.unwrap();
                let previous = writer.synced_end();
                writer.write_record(&records[1]).unwrap();
                {
                    let mut output = observed.borrow_mut();
                    output.calls.clear();
                    output.fault = Some((stage, fault));
                    output.calls_before_fault =
                        usize::from(matches!(stage, Stage::LogWrite | Stage::IndexWrite));
                }
                let mut operation = Box::pin(writer.sync_data());
                let outcome = catch_unwind(AssertUnwindSafe(|| poll_once(operation.as_mut())));
                match fault {
                    Fault::Error => {
                        let Ok(Poll::Ready(Err(error))) = outcome else {
                            panic!("expected error at {stage:?}")
                        };
                        // Preserve both the destination identity and its concrete error.
                        let source = match &error {
                            IndexedLogWriteError::Log(RecordOutputError::Io(source)) => {
                                assert!(matches!(
                                    stage,
                                    Stage::LogWrite | Stage::LogFlush | Stage::LogSync
                                ));
                                source
                            }
                            IndexedLogWriteError::Index(IndexWriteError::Io(source)) => {
                                assert!(matches!(
                                    stage,
                                    Stage::IndexWrite | Stage::IndexFlush | Stage::IndexSync
                                ));
                                source
                            }
                            other => panic!("unexpected error: {other:?}"),
                        };
                        assert_eq!(source.raw_os_error(), Some(123));
                        assert!(std::error::Error::source(&error).is_some());
                    }
                    Fault::Pending => assert!(matches!(outcome, Ok(Poll::Pending)), "{stage:?}"),
                    Fault::Panic => assert!(outcome.is_err(), "{stage:?}"),
                    Fault::WriteZero => unreachable!(),
                }
                // Dropping a partially completed operation must not authorize replay.
                drop(operation);
                assert_eq!(writer.synced_end(), previous, "{stage:?} {fault:?}");
                let expected_flushed = if matches!(stage, Stage::LogSync | Stage::IndexSync) {
                    writer.buffered_end()
                } else {
                    previous
                };
                assert_eq!(
                    writer.flushed_end(),
                    expected_flushed,
                    "{stage:?} {fault:?}"
                );
                assert_eq!(
                    writer.buffered_end(),
                    Some(RecordEndLocation::new(id(0, 1), 33))
                );
                assert_eq!(writer.record_count(), 2);
                assert_eq!(writer.expected_record_id(), id(0, 2));
                if matches!(stage, Stage::LogWrite | Stage::LogFlush) {
                    assert_eq!(observed.borrow().index.len(), 8);
                    assert!(!observed.borrow().calls.contains(&Stage::IndexWrite));
                }
                let calls = observed.borrow().calls.len();
                assert!(matches!(
                    writer.write_record(&records[1]),
                    Err(IndexedLogWriteError::Unusable)
                ));
                assert_eq!(writer.expected_record_id(), id(0, 2));
                assert!(matches!(
                    writer.flush().await,
                    Err(IndexedLogWriteError::Unusable)
                ));
                assert!(matches!(
                    writer.sync_data().await,
                    Err(IndexedLogWriteError::Unusable)
                ));
                assert!(matches!(
                    writer.finalize(),
                    Err(IndexedLogWriteError::Unusable)
                ));
                drop(writer.into_inner());
                assert_eq!(observed.borrow().calls.len(), calls);
            }
        }
    }

    #[tokio::test]
    async fn pending_operations_resume_in_order_without_replaying_bytes() {
        let records = records(0, 0, &[0, 3]).await;
        for stage in STAGES {
            let (mut writer, observed) = writer(records[0].id());
            for record in &records {
                writer.write_record(record).unwrap();
            }
            observed.borrow_mut().fault = Some((stage, Fault::Pending));
            observed.borrow_mut().calls_before_fault =
                usize::from(matches!(stage, Stage::LogWrite | Stage::IndexWrite));
            let mut operation = Box::pin(writer.sync_data());
            assert!(poll_once(operation.as_mut()).is_pending());
            assert!(poll_once(operation.as_mut()).is_pending());
            observed.borrow_mut().fault = None;
            assert!(matches!(poll_once(operation.as_mut()), Poll::Ready(Ok(()))));
            drop(operation);
            assert_eq!(writer.synced_end(), writer.buffered_end());
            let output = observed.borrow();
            assert_eq!(output.log.len(), 35);
            assert_eq!(
                output.index,
                [16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0]
            );
            let mut phases = output.calls.clone();
            phases.dedup();
            assert_eq!(phases, STAGES);
        }
    }

    #[tokio::test]
    async fn zero_progress_on_either_output_is_terminal() {
        let records = records(0, 0, &[0]).await;
        for stage in [Stage::LogWrite, Stage::IndexWrite] {
            let (mut writer, observed) = writer(records[0].id());
            writer.write_record(&records[0]).unwrap();
            observed.borrow_mut().fault = Some((stage, Fault::WriteZero));
            observed.borrow_mut().calls_before_fault = 1;
            match writer.flush().await.unwrap_err() {
                IndexedLogWriteError::Log(RecordOutputError::Io(error)) => {
                    assert_eq!(stage, Stage::LogWrite);
                    assert_eq!(error.kind(), io::ErrorKind::WriteZero);
                }
                IndexedLogWriteError::Index(IndexWriteError::Io(error)) => {
                    assert_eq!(stage, Stage::IndexWrite);
                    assert_eq!(error.kind(), io::ErrorKind::WriteZero);
                }
                error => panic!("unexpected error: {error:?}"),
            }
            assert_eq!(writer.flushed_end(), None);
            assert_eq!(writer.synced_end(), None);
            assert!(matches!(
                writer.flush().await,
                Err(IndexedLogWriteError::Unusable)
            ));
        }
    }

    #[tokio::test]
    async fn independent_file_handles_read_flushed_data_and_resumption_preserves_the_prefix() {
        let fixture = storage_fixture().await;
        let provider = fixture.provider();
        let stream = fixture.stream_id();
        let file_id = LogFileId::first(stream);
        let log_path = provider.log_file_path(file_id);
        let index_path = provider.index_file_path(file_id);
        let (log, index) = provider.create_log_and_index(file_id).await.unwrap();
        let mut log_reader = tokio::fs::File::open(&log_path).await.unwrap();
        let mut index_reader = tokio::fs::File::open(&index_path).await.unwrap();
        let records = records(stream.get(), 0, &[0, 3]).await;
        let mut writer = IndexedLogWriter::new(file_id, log, index);
        assert_eq!(writer.expected_record_id(), id(stream.get(), 0));
        assert_eq!(writer.record_count(), 0);
        assert_eq!(writer.buffered_end(), None);
        assert_eq!(writer.flushed_end(), None);
        assert_eq!(writer.synced_end(), None);
        writer.write_record(&records[0]).unwrap();
        assert_eq!(log_reader.metadata().await.unwrap().len(), 0);
        assert_eq!(index_reader.metadata().await.unwrap().len(), 0);
        writer.flush().await.unwrap();
        assert_eq!(writer.synced_end(), None);
        let mut first = [0; 16];
        log_reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, records[0].as_bytes().as_ref());
        let mut entry = [0; 8];
        index_reader.read_exact(&mut entry).await.unwrap();
        assert_eq!(entry, [16, 0, 0, 0, 0, 0, 0, 0]);
        let end = writer.flushed_end();
        drop(writer);

        // No reader I/O runs during recovery. Their independent cursors stay
        // after the first record while the initializer prepares the writer.
        let initialized = StreamInitializer::initialize(stream, provider)
            .await
            .unwrap();
        let mut writer = IndexedLogWriter::from(initialized);
        assert_eq!(writer.file_id(), file_id);
        assert_eq!(writer.record_count(), 1);
        assert_eq!(writer.expected_record_id(), records[1].id());
        assert_eq!(writer.buffered_end(), end);
        assert_eq!(writer.flushed_end(), end);
        assert_eq!(writer.synced_end(), end);
        writer.write_record(&records[1]).unwrap();
        assert_eq!(writer.flushed_end(), end);
        assert_eq!(writer.synced_end(), end);
        assert_eq!(log_reader.metadata().await.unwrap().len(), 16);
        assert_eq!(index_reader.metadata().await.unwrap().len(), 8);
        writer.flush().await.unwrap();
        let appended_end = Some(RecordEndLocation::new(id(stream.get(), 1), 35));
        assert_eq!(writer.flushed_end(), appended_end);
        assert_eq!(writer.synced_end(), end);
        writer.sync_data().await.unwrap();
        let mut second = [0; 19];
        log_reader.read_exact(&mut second).await.unwrap();
        assert_eq!(&second, records[1].as_bytes().as_ref());
        index_reader.read_exact(&mut entry).await.unwrap();
        assert_eq!(entry, [35, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(log_reader.metadata().await.unwrap().len(), 35);
        assert_eq!(index_reader.metadata().await.unwrap().len(), 16);
        assert_eq!(writer.synced_end(), appended_end);
        let bytes = tokio::fs::read(&log_path).await.unwrap();
        let expected: Vec<u8> = records
            .iter()
            .flat_map(|r| r.as_bytes().iter().copied())
            .collect();
        assert_eq!(bytes, expected);
    }

    #[tokio::test]
    async fn initialized_empty_pairs_start_without_progress_and_accept_sequence_zero() {
        for corrupt in [false, true] {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let stream = fixture.stream_id();
            let file_id = LogFileId::first(stream);
            if corrupt {
                drop(provider.create_log_and_index(file_id).await.unwrap());
                std::fs::write(provider.log_file_path(file_id), [1, 2, 3]).unwrap();
                std::fs::write(provider.index_file_path(file_id), [9; 13]).unwrap();
            }
            let initialized = StreamInitializer::initialize(stream, provider)
                .await
                .unwrap();
            let mut writer: IndexedLogWriter<File> = initialized.into();
            assert_eq!(writer.file_id(), file_id);
            assert_eq!(writer.record_count(), 0);
            assert_eq!(writer.expected_record_id(), id(stream.get(), 0));
            assert_eq!(writer.buffered_end(), None);
            assert_eq!(writer.flushed_end(), None);
            assert_eq!(writer.synced_end(), None);
            assert!(!writer.is_full());
            writer.flush().await.unwrap();
            assert!(
                std::fs::read(provider.log_file_path(file_id))
                    .unwrap()
                    .is_empty()
            );
            assert!(
                std::fs::read(provider.index_file_path(file_id))
                    .unwrap()
                    .is_empty()
            );

            let records = records(stream.get(), 0, &[3]).await;
            let outcome = writer.write_record(&records[0]).unwrap();
            assert_eq!(outcome.end(), RecordEndLocation::new(records[0].id(), 19));
            assert!(!outcome.file_full());
            assert_eq!(writer.expected_record_id(), id(stream.get(), 1));
            writer.sync_data().await.unwrap();
            assert_eq!(writer.record_count(), 1);
            assert_eq!(
                writer.synced_end(),
                Some(RecordEndLocation::new(records[0].id(), 19))
            );
            assert_eq!(
                std::fs::read(provider.log_file_path(file_id)).unwrap(),
                records[0].as_bytes().as_ref()
            );
            assert_eq!(
                std::fs::read(provider.index_file_path(file_id)).unwrap(),
                [19, 0, 0, 0, 0, 0, 0, 0]
            );
            // Live checkpoint publication is separate from writer synchronization.
            assert_eq!(provider.read_checkpoint(stream).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn repaired_prefix_handover_preserves_durability_and_file_local_count() {
        for first_sequence in [0, 100_000] {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let stream = fixture.stream_id();
            let records = records(stream.get(), first_sequence, &[0, 3, 1]).await;
            let file_id = LogFileId::from_record_id(records[0].id());
            let (log, index) = provider.create_log_and_index(file_id).await.unwrap();
            let mut original = IndexedLogWriter::new(file_id, log, index);
            assert_eq!(original.expected_record_id(), records[0].id());
            original.write_record(&records[0]).unwrap();
            original.sync_data().await.unwrap();
            let checkpoint = StreamCheckpoint::new(original.synced_end().unwrap());
            provider.write_checkpoint(&checkpoint).await.unwrap();
            original.write_record(&records[1]).unwrap();
            original.sync_data().await.unwrap();
            drop(original);

            // Leave a valid uncheckpointed record, corrupt log tail and invalid
            // index suffix. The checkpoint's first entry remains trustworthy.
            let mut bytes = std::fs::read(provider.log_file_path(file_id)).unwrap();
            bytes.extend_from_slice(&[1, 2, 3]);
            std::fs::write(provider.log_file_path(file_id), bytes).unwrap();
            std::fs::write(
                provider.index_file_path(file_id),
                [16, 0, 0, 0, 0, 0, 0, 0, 255],
            )
            .unwrap();
            let initialized = StreamInitializer::initialize(stream, provider)
                .await
                .unwrap();
            let end = Some(RecordEndLocation::new(records[1].id(), 35));
            let mut writer = IndexedLogWriter::from(initialized);
            assert_eq!(writer.file_id(), file_id);
            assert_eq!(writer.record_count(), 2);
            assert_eq!(writer.expected_record_id(), records[2].id());
            assert_eq!(writer.buffered_end(), end);
            assert_eq!(writer.flushed_end(), end);
            assert_eq!(writer.synced_end(), end);
            assert!(!writer.is_full());
            // A duplicate stays rejected without disturbing recovered progress.
            assert!(matches!(writer.write_record(&records[1]),
                Err(IndexedLogWriteError::UnexpectedRecordId { expected, actual })
                    if expected == records[2].id() && actual == records[1].id()));
            assert_eq!(writer.synced_end(), end);
            assert_eq!(writer.expected_record_id(), records[2].id());
            let outcome = writer.write_record(&records[2]).unwrap();
            assert_eq!(outcome.end(), RecordEndLocation::new(records[2].id(), 52));
            assert!(!outcome.file_full());
            assert_eq!(
                writer.expected_record_id(),
                id(stream.get(), first_sequence + 3)
            );
            writer.flush().await.unwrap();
            let appended_end = Some(RecordEndLocation::new(records[2].id(), 52));
            assert_eq!(writer.record_count(), 3);
            assert_eq!(writer.buffered_end(), appended_end);
            assert_eq!(writer.flushed_end(), appended_end);
            assert_eq!(writer.synced_end(), end);
            writer.sync_data().await.unwrap();
            assert_eq!(writer.synced_end(), appended_end);
            let (mut log, mut index) = writer.into_inner();
            assert_eq!(log.stream_position().await.unwrap(), 52);
            assert_eq!(index.stream_position().await.unwrap(), 24);
            let expected: Vec<_> = records
                .iter()
                .flat_map(|record| record.as_bytes().iter().copied())
                .collect();
            assert_eq!(
                std::fs::read(provider.log_file_path(file_id)).unwrap(),
                expected
            );
            assert_eq!(
                std::fs::read(provider.index_file_path(file_id)).unwrap(),
                [
                    16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0
                ]
            );
            assert_eq!(
                provider.read_checkpoint(stream).await.unwrap(),
                end.map(StreamCheckpoint::new)
            );
        }
    }

    #[tokio::test]
    async fn initialized_successor_keeps_the_previous_files_checkpoint_out_of_writer_progress() {
        let fixture = storage_fixture().await;
        let provider = fixture.provider();
        let stream = fixture.stream_id();
        let first = LogFileId::first(stream);
        let (log, index) = provider.create_log_and_index(first).await.unwrap();
        let mut original = IndexedLogWriter::new(first, log, index);
        let records = records(stream.get(), 0, &vec![0; 100_001]).await;
        for record in &records[..100_000] {
            original.write_record(record).unwrap();
        }
        original.sync_data().await.unwrap();
        let checkpoint = StreamCheckpoint::new(original.finalize().unwrap());
        provider.write_checkpoint(&checkpoint).await.unwrap();
        drop(original);

        let initialized = StreamInitializer::initialize(stream, provider)
            .await
            .unwrap();
        let mut writer = IndexedLogWriter::from(initialized);
        assert_eq!(writer.file_id(), first.next());
        assert_eq!(writer.record_count(), 0);
        assert_eq!(writer.expected_record_id(), id(stream.get(), 100_000));
        assert_eq!(writer.buffered_end(), None);
        assert_eq!(writer.flushed_end(), None);
        assert_eq!(writer.synced_end(), None);
        assert!(!writer.is_full());
        let record = &records[100_000];
        let outcome = writer.write_record(record).unwrap();
        assert_eq!(outcome.end(), RecordEndLocation::new(record.id(), 16));
        assert!(!outcome.file_full());
        assert_eq!(writer.expected_record_id(), id(stream.get(), 100_001));
        writer.sync_data().await.unwrap();
        assert_eq!(writer.record_count(), 1);
        assert_eq!(
            writer.synced_end(),
            Some(RecordEndLocation::new(record.id(), 16))
        );
        assert_eq!(
            std::fs::read(provider.log_file_path(first.next())).unwrap(),
            record.as_bytes().as_ref()
        );
        assert_eq!(
            std::fs::read(provider.index_file_path(first.next())).unwrap(),
            [16, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            provider.read_checkpoint(stream).await.unwrap(),
            Some(checkpoint)
        );
    }
}

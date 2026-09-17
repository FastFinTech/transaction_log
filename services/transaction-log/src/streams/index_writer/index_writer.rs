use std::io;

use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
};
use transaction_log_exports::AsyncSyncData;

use super::IndexWriteError;

// Dense index encoding, independent of native struct layout.
pub(in crate::streams) const INDEX_ENTRY_LEN: usize = 8;

/// Buffered append-only encoding of dense index entries to an owned file.
///
/// [`new`](Self::new) accepts an already-open [`File`]. Each synchronous
/// [`write`](Self::write) buffers one little-endian `u64` exclusive log offset.
/// Buffer output, file flushing and durable synchronization are separate,
/// explicit operations, following `RecordWriter`'s completion boundaries.
///
/// This layer knows neither record IDs nor the paired log's progress. The owner
/// establishes correct offsets and file position and completes log output before
/// exposing corresponding index entries. No seeking, validation, file opening,
/// tasks or timers occur here. See the module README for the format and ownership
/// contracts.
///
/// The default file type is the only publicly constructible specialization.
/// The type parameter allows internal deterministic I/O tests without boxing or
/// an extra file trait; it is not a public socket/destination constructor.
pub struct IndexWriter<F = File> {
    file: F,
    // Only the current batch: eight encoded bytes per entry, without a header,
    // initial zero or retained history. Vec capacity survives successful output.
    buffer: Vec<u8>,
    // False across I/O; only success restores it. Cancellation or unwinding must
    // not permit replay of an accepted prefix, even if it ends within an entry.
    usable: bool,
}

impl IndexWriter {
    /// Takes ownership of an already-open file at its established append position.
    ///
    /// The owner supplies an empty file, or a validated/repaired index prefix
    /// whose previous writes have finished flushing. The file must be positioned
    /// after its last entry and exclusively owned for writing. Construction does
    /// no inspection, seeking, I/O or allocation; capacity grows with the batch.
    pub fn new(file: File) -> Self {
        Self::with_file(file)
    }
}

impl<F> IndexWriter<F> {
    // Internal construction also lets pair-level tests inject a scripted file.
    // Keeping this private to streams preserves the file-only public constructor.
    pub(in crate::streams) fn with_file(file: F) -> Self {
        Self {
            file,
            buffer: Vec::new(),
            usable: true,
        }
    }

    /// Synchronously buffers one absolute, exclusive record end in append order.
    ///
    /// Encodes exactly eight little-endian bytes, without I/O. The owner supplies
    /// valid offsets for a contiguous record prefix. Range, increasing-position,
    /// record-count and record-ID validation belong to that owner; checking raw
    /// integers here could not establish agreement with the paired log.
    ///
    /// Capacity grows as needed and is retained after output. A previous I/O
    /// failure rejects the append without changing the buffer.
    #[inline]
    pub fn write(&mut self, end_position: u64) -> Result<(), IndexWriteError> {
        self.check_usable()?;
        self.buffer.extend_from_slice(&end_position.to_le_bytes());
        Ok(())
    }

    /// Borrows the file for observation or file-specific operations.
    ///
    /// Operations through this reference bypass this writer's failure tracking.
    /// The owner must preserve append position, exclusive write ownership and
    /// ordering, including when cloning handles or synchronizing through it.
    pub fn get_ref(&self) -> &F {
        &self.file
    }

    /// Returns the file, discarding buffered entries without implicit output.
    ///
    /// Available after failure so the owner can close or recover the file.
    /// Extraction does not repair a partial entry or authorize resending a batch.
    pub fn into_inner(self) -> F {
        self.file
    }

    #[inline]
    fn check_usable(&self) -> Result<(), IndexWriteError> {
        if self.usable {
            Ok(())
        } else {
            Err(IndexWriteError::Unusable)
        }
    }
}

impl<F: AsyncWrite + Unpin> IndexWriter<F> {
    /// Sends all buffered entries to the file, then clears the reusable buffer.
    ///
    /// An empty batch performs no I/O. Successful acceptance is separate from
    /// completing the file's own buffering: call `flush` afterwards for that.
    /// `Interrupted` writes retry without advancing; short writes continue from
    /// their accepted prefix; zero progress is a terminal `WriteZero` error.
    ///
    /// Failure, panic or cancellation makes the writer unusable. Its complete
    /// batch remains buffered but is not safe to resend. A pending future may be
    /// resumed; dropping an unpolled future changes nothing.
    pub async fn flush_buffer(&mut self) -> Result<(), IndexWriteError> {
        self.check_usable()?;
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.usable = false;
        // Preserve the owned batch until acceptance completes. The cursor lives
        // in this future, including while an underlying write is pending.
        let mut bytes = &self.buffer[..];
        while !bytes.is_empty() {
            match self.file.write(bytes).await {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(written) => bytes = &bytes[written..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        self.buffer.clear();
        self.usable = true;
        Ok(())
    }

    /// Flushes only the file's own buffering, without sending pending entries.
    ///
    /// Call `flush_buffer` first to include those entries. For Tokio files this
    /// waits for previously accepted writes to finish; it does not establish
    /// durability. Even with an empty batch it reaches the file. Failed,
    /// panicking or cancelled flushing makes the writer unusable.
    pub async fn flush(&mut self) -> Result<(), IndexWriteError> {
        self.check_usable()?;
        self.usable = false;
        self.file.flush().await?;
        self.usable = true;
        Ok(())
    }
}

impl<F: AsyncWrite + AsyncSyncData + Unpin> IndexWriter<F> {
    /// Synchronizes previously flushed file data, without sending or flushing entries.
    ///
    /// To include the current batch, call `flush_buffer`, then `flush`, then this
    /// method. Guarantees follow the file/platform's data synchronization; parent
    /// directory metadata and the paired log are the owner's responsibility.
    /// Failure, panic or cancellation makes the writer unusable.
    pub async fn sync_data(&mut self) -> Result<(), IndexWriteError> {
        self.check_usable()?;
        self.usable = false;
        self.file.sync_data().await?;
        self.usable = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::VecDeque,
        future::{Future, poll_fn},
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Operation {
        Write,
        Flush,
        Sync,
    }

    #[derive(Clone, Copy, Debug)]
    enum Step {
        Write(usize),
        Flush,
        Sync,
        Interrupted,
        Pending,
        Error,
        Panic,
        Zero,
    }

    #[derive(Default)]
    struct Observed {
        bytes: Vec<u8>,
        calls: Vec<Operation>,
        steps: VecDeque<Step>,
    }

    // A script makes partial entries and cancellation deterministic. Rc ensures
    // this testing seam does not accidentally impose Send or boxed-future bounds.
    struct ScriptedFile(Rc<RefCell<Observed>>);

    impl ScriptedFile {
        fn poll_operation(&self, operation: Operation, bytes: &[u8]) -> Poll<io::Result<usize>> {
            let mut observed = self.0.borrow_mut();
            observed.calls.push(operation);
            match observed
                .steps
                .pop_front()
                .expect("unexpected file operation")
            {
                Step::Write(limit) => {
                    assert_eq!(operation, Operation::Write);
                    let count = bytes.len().min(limit);
                    observed.bytes.extend_from_slice(&bytes[..count]);
                    Poll::Ready(Ok(count))
                }
                Step::Flush => {
                    assert_eq!(operation, Operation::Flush);
                    Poll::Ready(Ok(0))
                }
                Step::Sync => {
                    assert_eq!(operation, Operation::Sync);
                    Poll::Ready(Ok(0))
                }
                Step::Interrupted => Poll::Ready(Err(io::ErrorKind::Interrupted.into())),
                Step::Pending => Poll::Pending,
                Step::Error => Poll::Ready(Err(io::Error::from_raw_os_error(123))),
                Step::Panic => panic!("scripted file panic"),
                Step::Zero => {
                    assert_eq!(operation, Operation::Write);
                    Poll::Ready(Ok(0))
                }
            }
        }
    }

    impl AsyncWrite for ScriptedFile {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.poll_operation(Operation::Write, bytes)
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_operation(Operation::Flush, &[])
                .map(|result| result.map(|_| ()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            panic!("the index writer must not implicitly shut down its file");
        }
    }

    impl AsyncSyncData for ScriptedFile {
        async fn sync_data(&mut self) -> io::Result<()> {
            poll_fn(|_| self.poll_operation(Operation::Sync, &[]))
                .await
                .map(|_| ())
        }
    }

    fn writer(steps: &[Step]) -> (IndexWriter<ScriptedFile>, Rc<RefCell<Observed>>) {
        let observed = Rc::new(RefCell::new(Observed {
            steps: steps.iter().copied().collect(),
            ..Observed::default()
        }));
        (
            IndexWriter::with_file(ScriptedFile(Rc::clone(&observed))),
            observed,
        )
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    async fn perform(
        writer: &mut IndexWriter<ScriptedFile>,
        operation: Operation,
    ) -> Result<(), IndexWriteError> {
        match operation {
            Operation::Write => writer.flush_buffer().await,
            Operation::Flush => writer.flush().await,
            Operation::Sync => writer.sync_data().await,
        }
    }

    #[tokio::test]
    async fn encodes_literal_offsets_synchronously_and_reuses_the_batch_allocation() {
        let (mut writer, observed) = writer(&[Step::Write(usize::MAX), Step::Write(usize::MAX)]);
        assert_eq!(writer.buffer.capacity(), 0);
        // This encoder preserves all raw u64 values. Its owner, not this layer,
        // establishes whether an offset describes a valid record in a real log.
        for end in [0, 16, 0x0102_0304_0506_0708, u64::MAX] {
            writer.write(end).unwrap();
        }
        let expected = [
            0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1, 255, 255, 255,
            255, 255, 255, 255, 255,
        ];
        assert_eq!(writer.buffer, expected);
        assert!(observed.borrow().calls.is_empty());
        let pointer = writer.buffer.as_ptr();
        let capacity = writer.buffer.capacity();
        writer.flush_buffer().await.unwrap();
        assert_eq!(observed.borrow().bytes, expected);
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.buffer.as_ptr(), pointer);
        assert_eq!(writer.buffer.capacity(), capacity);
        writer.write(35).unwrap();
        writer.flush_buffer().await.unwrap();
        assert_eq!(&observed.borrow().bytes[32..], &[35, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(writer.buffer.as_ptr(), pointer);
        assert_eq!(writer.buffer.capacity(), capacity);
        assert!(observed.borrow().steps.is_empty());
    }

    #[tokio::test]
    async fn buffer_output_file_flush_and_sync_are_separate_and_unpolled_futures_are_inert() {
        let (mut writer, observed) = writer(&[
            Step::Flush,
            Step::Sync,
            Step::Write(usize::MAX),
            Step::Flush,
            Step::Sync,
        ]);
        writer.flush_buffer().await.unwrap();
        assert!(observed.borrow().calls.is_empty());
        writer.write(16).unwrap();
        drop(writer.flush_buffer());
        drop(writer.flush());
        drop(writer.sync_data());
        assert!(observed.borrow().calls.is_empty());
        assert!(writer.usable);
        writer.flush().await.unwrap();
        writer.sync_data().await.unwrap();
        assert!(observed.borrow().bytes.is_empty());
        assert_eq!(writer.buffer.len(), 8);
        writer.flush_buffer().await.unwrap();
        assert_eq!(
            observed.borrow().calls,
            [Operation::Flush, Operation::Sync, Operation::Write]
        );
        writer.flush().await.unwrap();
        writer.sync_data().await.unwrap();
        assert_eq!(
            observed.borrow().calls,
            [
                Operation::Flush,
                Operation::Sync,
                Operation::Write,
                Operation::Flush,
                Operation::Sync
            ]
        );
        assert!(observed.borrow().steps.is_empty());
    }

    #[test]
    fn short_interrupted_and_pending_writes_resume_inside_and_across_entries() {
        let (mut writer, observed) = writer(&[
            Step::Interrupted,
            Step::Interrupted,
            Step::Write(3),
            Step::Pending,
            Step::Interrupted,
            Step::Write(6),
            Step::Pending,
            Step::Write(usize::MAX),
        ]);
        writer.write(16).unwrap();
        writer.write(35).unwrap();
        let mut send = Box::pin(writer.flush_buffer());
        assert!(poll_once(send.as_mut()).is_pending());
        assert_eq!(observed.borrow().bytes, [16, 0, 0]);
        assert!(poll_once(send.as_mut()).is_pending());
        assert_eq!(observed.borrow().bytes, [16, 0, 0, 0, 0, 0, 0, 0, 35]);
        assert!(matches!(poll_once(send.as_mut()), Poll::Ready(Ok(()))));
        drop(send);
        assert!(writer.usable);
        assert!(writer.buffer.is_empty());
        assert_eq!(
            observed.borrow().bytes,
            [16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0]
        );
        assert!(observed.borrow().steps.is_empty());
    }

    #[tokio::test]
    async fn failures_cancellation_and_panics_reject_every_later_operation_without_replay() {
        for operation in [Operation::Write, Operation::Flush, Operation::Sync] {
            for fault in [Step::Error, Step::Pending, Step::Panic] {
                // Check every incomplete output prefix of two entries, including
                // the boundary between them. Flush/sync do not send this batch.
                let prefixes = if operation == Operation::Write {
                    0..16
                } else {
                    0..1
                };
                for prefix in prefixes {
                    let (mut writer, observed) =
                        writer(&[Step::Write(usize::MAX), Step::Flush, Step::Sync]);
                    writer.write(16).unwrap();
                    writer.flush_buffer().await.unwrap();
                    writer.flush().await.unwrap();
                    writer.sync_data().await.unwrap();
                    writer.write(35).unwrap();
                    writer.write(52).unwrap();
                    let pending_bytes = writer.buffer.clone();
                    if prefix > 0 {
                        observed.borrow_mut().steps.push_back(Step::Write(prefix));
                    }
                    observed.borrow_mut().steps.push_back(fault);
                    let mut future = Box::pin(perform(&mut writer, operation));
                    let outcome = catch_unwind(AssertUnwindSafe(|| poll_once(future.as_mut())));
                    match fault {
                        Step::Error => {
                            let Ok(Poll::Ready(Err(IndexWriteError::Io(error)))) = outcome else {
                                panic!("expected original I/O error at {operation:?} / {prefix}");
                            };
                            assert_eq!(error.raw_os_error(), Some(123));
                            assert!(
                                std::error::Error::source(&IndexWriteError::Io(error)).is_some()
                            );
                        }
                        Step::Pending => assert!(matches!(outcome, Ok(Poll::Pending))),
                        Step::Panic => assert!(outcome.is_err()),
                        _ => unreachable!(),
                    }
                    drop(future);
                    assert!(!writer.usable);
                    assert_eq!(writer.buffer, pending_bytes);
                    assert_eq!(&observed.borrow().bytes[8..], &pending_bytes[..prefix]);
                    let calls = observed.borrow().calls.len();
                    assert!(matches!(writer.write(99), Err(IndexWriteError::Unusable)));
                    assert!(matches!(
                        writer.flush_buffer().await,
                        Err(IndexWriteError::Unusable)
                    ));
                    assert!(matches!(
                        writer.flush().await,
                        Err(IndexWriteError::Unusable)
                    ));
                    assert!(matches!(
                        writer.sync_data().await,
                        Err(IndexWriteError::Unusable)
                    ));
                    drop(writer.into_inner());
                    assert_eq!(observed.borrow().calls.len(), calls);
                    assert!(observed.borrow().steps.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn zero_progress_is_terminal_at_every_incomplete_entry_boundary() {
        for prefix in 0..16 {
            let steps = if prefix == 0 {
                vec![Step::Zero]
            } else {
                vec![Step::Write(prefix), Step::Zero]
            };
            let (mut writer, observed) = writer(&steps);
            writer.write(16).unwrap();
            writer.write(35).unwrap();
            assert!(
                matches!(writer.flush_buffer().await, Err(IndexWriteError::Io(error))
                if error.kind() == io::ErrorKind::WriteZero)
            );
            assert_eq!(observed.borrow().bytes.len(), prefix);
            assert_eq!(writer.buffer.len(), 16);
            assert!(matches!(writer.write(52), Err(IndexWriteError::Unusable)));
            assert!(matches!(
                writer.flush_buffer().await,
                Err(IndexWriteError::Unusable)
            ));
            assert!(observed.borrow().steps.is_empty());
        }
    }

    #[test]
    fn pending_file_flush_and_sync_resume_without_sending_newer_entries() {
        for (operation, completion) in [
            (Operation::Flush, Step::Flush),
            (Operation::Sync, Step::Sync),
        ] {
            let (mut writer, observed) = writer(&[Step::Pending, completion]);
            writer.write(16).unwrap();
            let mut future = Box::pin(perform(&mut writer, operation));
            assert!(poll_once(future.as_mut()).is_pending());
            assert!(matches!(poll_once(future.as_mut()), Poll::Ready(Ok(()))));
            drop(future);
            assert!(writer.usable);
            assert_eq!(writer.buffer.len(), 8);
            assert!(observed.borrow().bytes.is_empty());
            assert!(observed.borrow().steps.is_empty());
        }
    }

    #[tokio::test]
    async fn owned_file_appends_preserve_the_prefix_and_drop_discards_only_unsent_entries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("index.idx");
        let file = File::create(&path).await.unwrap();
        let mut writer = IndexWriter::new(file);
        writer.write(16).unwrap();
        writer.write(35).unwrap();
        assert_eq!(writer.get_ref().metadata().await.unwrap().len(), 0);
        writer.flush_buffer().await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            [16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0]
        );
        writer.sync_data().await.unwrap();
        writer.write(99).unwrap();
        drop(writer.into_inner());

        let file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        let mut writer = IndexWriter::new(file);
        writer.write(52).unwrap();
        writer.flush_buffer().await.unwrap();
        writer.flush().await.unwrap();
        writer.sync_data().await.unwrap();
        writer.write(123).unwrap();
        drop(writer);
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            [
                16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
    }

    #[tokio::test]
    async fn a_read_only_file_preserves_its_io_error_and_cannot_be_reused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("read_only.idx");
        tokio::fs::write(&path, [16, 0, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut writer = IndexWriter::new(File::open(&path).await.unwrap());
        writer.write(35).unwrap();
        // Tokio can report the OS error at acceptance or at destination flush.
        let result = match writer.flush_buffer().await {
            Ok(()) => writer.flush().await,
            error => error,
        };
        assert!(matches!(result, Err(IndexWriteError::Io(_))));
        assert!(matches!(writer.write(52), Err(IndexWriteError::Unusable)));
        assert!(matches!(
            writer.sync_data().await,
            Err(IndexWriteError::Unusable)
        ));
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            [16, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}

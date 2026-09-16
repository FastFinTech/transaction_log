use std::{fmt, io};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{AsyncSyncData, RecordBuilder, RecordOutputError, RecordWriteError};
use crate::{Record, RecordId, record::record_protocol::MAX_RECORD_LEN};

/// Compile-time mode for serializing new records through a synchronous callback.
///
/// [`RecordWriter::for_serialization`] selects this mode automatically.
/// It exposes `write(id, callback)` and excludes `write_record(&record)`. Buffer
/// output, destination flushing and optional synchronization remain available.
/// Name it when storing a writer as `RecordWriter<W, SerializeRecords>`.
///
/// This zero-sized marker has no runtime behavior. Writer fields and the common
/// constructor are private; a marker alone cannot create or change a writer's mode.
pub struct SerializeRecords;

/// Compile-time mode for buffering the complete encodings of existing records.
///
/// [`RecordWriter::for_records`] selects this mode automatically. It exposes
/// synchronous `write_record(&record)` and excludes the serialization callback API.
/// Buffer output, destination flushing and optional synchronization remain available.
/// Name it when storing a writer as `RecordWriter<W, ExistingRecords>`.
///
/// This zero-sized marker has no runtime behavior. The writer copies validated
/// bytes without a builder or repeated validation; it never switches append modes.
pub struct ExistingRecords;

/// Synchronous record buffering over an owned, already-open async destination.
///
/// Choose [`for_serialization`](Self::for_serialization) to expose only
/// [`write`](Self::write), or [`for_records`](Self::for_records) to expose only
/// [`write_record`](Self::write_record). Rust infers `Mode` from the constructor;
/// the other append method is unavailable at compile time. Both modes append
/// complete records to one reusable buffer without I/O. [`flush_buffer`](Self::flush_buffer)
/// sends that batch; [`flush`](Self::flush) flushes the destination's own buffering.
/// [`sync_data`](Self::sync_data) synchronizes previously flushed data when `W`
/// also implements [`AsyncSyncData`]. The caller chooses when to perform each step.
///
/// One exclusive owner drives these operations; no worker, queue, timer or `Send`
/// bound is imposed. Pending I/O holds the mutable writer borrow, so another record
/// cannot be built until it completes. Owning a destination does not open it: the
/// caller completes socket handshakes or chooses file opening mode beforehand.
///
/// Serialization errors and panics discard only the current record. Failed,
/// panicking or cancelled I/O makes the writer unusable. Dropping it or extracting
/// the destination with [`into_inner`](Self::into_inner) discards unsent records
/// without flushing, synchronizing or shutting down. See the module README for
/// completion guarantees, failure handling and the hot-path design rationale.
pub struct RecordWriter<W, Mode> {
    // Owned by value: the middle layer governs ordering, while callers choose
    // the destination and retain access to destination-specific operations.
    destination: W,
    // Contains only complete records outside a write callback. Keeping one
    // allocation lets a serializer write fields directly into the final batch;
    // there is no separate payload allocation or per-record buffer handoff.
    buffer: BytesMut,
    // Permission to continue I/O, not a claim that the batch is sent or durable.
    // Set false BEFORE calling/awaiting destination I/O, restoring it only on
    // success. Returning early, cancellation or unwinding leaves it false without
    // a drop guard. While pending, the exclusive borrow prevents another call.
    usable: bool,
    // A zero-sized marker selects the append API at compile time. It is never
    // inspected, and adds no storage or runtime dispatch for either public mode.
    // Private fields and construction prevent callers from substituting a mode.
    _mode: Mode,
}

impl<W, Mode> RecordWriter<W, Mode> {
    // Only the two named public constructors can select a mode. Keeping storage
    // initialization here lets both append APIs share exactly one output lifecycle
    // without a public mode trait, default mode, or runtime selection flag.
    fn with_mode(destination: W, mode: Mode) -> Self {
        Self {
            destination,
            // Capacity, not initialized length. The first record can be built
            // without growth; later builders reserve more if the batch needs it.
            buffer: BytesMut::with_capacity(MAX_RECORD_LEN),
            usable: true,
            _mode: mode,
        }
    }

    /// Borrows the destination, for observation or destination-specific operations.
    ///
    /// For example, after `flush_buffer().await` and `flush().await`, a file owner
    /// can call `sync_all()` through this reference. Prefer the writer's own
    /// [`sync_data`](Self::sync_data) for data synchronization, which also tracks
    /// failures. Operations made through this reference bypass that tracking.
    /// The caller must handle their errors and preserve output order if `W` allows
    /// writing, seeking or handle cloning through a shared reference.
    pub fn get_ref(&self) -> &W {
        &self.destination
    }

    /// Returns the destination without sending, flushing, syncing or shutting down.
    ///
    /// Any unsent buffered records are discarded. Call `flush_buffer` first to
    /// send them. After failed or cancelled output, extracting the destination
    /// does not repair partial framing or establish durability. This method is
    /// still available on an unusable writer so its owner can close or recover
    /// the destination according to application policy.
    pub fn into_inner(self) -> W {
        self.destination
    }

    #[inline]
    fn check_usable(&self) -> Result<(), RecordOutputError> {
        // Retain only terminal state, not a shared/cloned I/O error. The original
        // operation returns its error; later operations cannot safely retry it.
        if self.usable {
            Ok(())
        } else {
            Err(RecordOutputError::Unusable)
        }
    }
}

impl<W> RecordWriter<W, SerializeRecords> {
    /// Takes ownership of a destination for synchronously serializing new records.
    ///
    /// This mode exposes [`write`](Self::write) and has no `write_record` method.
    /// Rust infers [`SerializeRecords`] from this constructor. Complete socket
    /// handshakes or choose file opening mode and position before handing it over.
    ///
    /// Reserves capacity for one maximum record, without initializing record bytes
    /// or performing I/O or scheduling. The buffer grows as records accumulate and
    /// retains capacity after output; neither batch size nor record count is capped.
    pub fn for_serialization(destination: W) -> Self {
        Self::with_mode(destination, SerializeRecords)
    }

    /// Synchronously appends one complete record to the owned buffer.
    ///
    /// A usable writer immediately calls the callback exactly once with a concrete
    /// builder, then finalizes its header and CRC. Serialization uses static
    /// dispatch and no separate payload allocation. Errors and unwinding discard
    /// only this record, preserving all previously buffered records.
    ///
    /// Success means the record is buffered; no destination operation occurs.
    /// Call `flush_buffer` to send accumulated records. The buffer grows as needed
    /// and retains its capacity after output; the size limit applies per record.
    /// A previous output failure rejects the call before invoking the callback.
    /// The callback supplies only opaque payload bytes; the writer derives length
    /// and CRC from them. It does not assign or advance the supplied record ID.
    #[inline]
    pub fn write<F, E>(&mut self, id: RecordId, write_body: F) -> Result<(), RecordWriteError<E>>
    where
        F: FnOnce(&mut RecordBuilder<'_>) -> Result<(), E>,
    {
        self.check_usable()?;
        // A temporary builder appends behind the existing batch. Its drop restores
        // that prefix on every construction failure, including callback unwinding.
        let mut builder = RecordBuilder::new(&mut self.buffer);
        if let Err(error) = write_body(&mut builder) {
            // Preserve a concrete size violation even when the serializer wraps
            // the Write error in its own error type. Otherwise return that type.
            builder.check_error()?;
            return Err(RecordWriteError::Serialize(error));
        }
        // Finalization also catches builder errors swallowed by the callback.
        // Publishing the length exposes header/body/CRC together; it performs no I/O.
        builder.finish(id)?;
        Ok(())
    }
}

impl<W> RecordWriter<W, ExistingRecords> {
    /// Takes ownership of a destination for synchronously copying existing records.
    ///
    /// This mode exposes [`write_record`](Self::write_record) and has no `write`
    /// callback method. Rust infers [`ExistingRecords`] from this constructor.
    /// Complete socket handshakes or choose file opening mode and position first.
    ///
    /// Reserves capacity for one maximum record, without initializing record bytes
    /// or performing I/O or scheduling. Capacity grows with the batch and survives
    /// output. Call [`flush_buffer`](Self::flush_buffer) explicitly to send records;
    /// the selected mode cannot be changed on an existing writer.
    pub fn for_records(destination: W) -> Self {
        Self::with_mode(destination, ExistingRecords)
    }

    /// Synchronously appends an existing record's complete encoding to the buffer.
    ///
    /// Copies the encoded bytes without cloning their handle, revalidating the
    /// record or recalculating CRC. Records retain their append order. No
    /// destination operation occurs until `flush_buffer` is polled.
    /// The record borrow ends on return; the caller may then drop the original.
    /// Returns `Unusable` if earlier I/O failed or was abandoned.
    pub fn write_record(&mut self, record: &Record) -> Result<(), RecordOutputError> {
        self.check_usable()?;
        // Copying is intentional: this append stays synchronous, and its record
        // borrow ends before the batch is sent. RecordReader established validity.
        self.buffer.extend_from_slice(record.as_bytes());
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin, Mode> RecordWriter<W, Mode> {
    /// Writes all buffered records to the destination, then clears the buffer.
    ///
    /// Capacity is retained for reuse. An empty buffer requires no I/O. Success
    /// means `AsyncWrite` accepted all bytes; destination flushing, remote receipt
    /// and file durability are separate concerns. Call `flush` afterwards when
    /// the destination's own buffering must also be flushed.
    ///
    /// Errors, panics or cancellation during output make the writer unusable,
    /// preventing a retry from repeating an already accepted prefix. An unpolled
    /// future leaves the buffer and writer state untouched.
    ///
    /// # Cancellation
    ///
    /// A pending future may be polled again to continue from its cursor. Dropping
    /// it after I/O begins loses that cursor and permanently rejects further I/O.
    /// The full batch remains buffered after failure, but is not safe to resend.
    /// Even an empty-buffer call returns `Unusable` after a previous failure.
    pub async fn flush_buffer(&mut self) -> Result<(), RecordOutputError> {
        self.check_usable()?;
        if self.buffer.is_empty() {
            // Sending nothing must not flush downstream buffers or start an I/O
            // attempt. Destination flush and durable sync remain explicit calls.
            return Ok(());
        }
        // Arm terminal failure before polling the destination, including its
        // first poll. Restoring this in a drop guard would permit prefix replay.
        self.usable = false;
        // Advance only this future's borrowed view. The BytesMut keeps ownership
        // and its full readable length until the entire batch has been accepted.
        let mut bytes = &self.buffer[..];
        while !bytes.is_empty() {
            match self.destination.write(bytes).await {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(written) => bytes = &bytes[written..],
                // Interrupted accepts no bytes; retry the same remaining slice.
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        // AsyncWrite has released its slice borrow. Reuse capacity only now;
        // clearing is not destination flushing, receipt confirmation or durability.
        self.buffer.clear();
        self.usable = true;
        Ok(())
    }

    /// Flushes only the destination's own buffering.
    ///
    /// This does not send records still in the writer's buffer; call `flush_buffer`
    /// first to include those records. File durability requires a separate
    /// `sync_data` or destination-specific `sync_all` call. A failed, panicking or
    /// cancelled flush makes this writer unusable, just like incomplete output.
    /// Unlike an empty `flush_buffer`, this calls the destination even when the
    /// writer's buffer is empty: earlier accepted bytes may still be pending there.
    pub async fn flush(&mut self) -> Result<(), RecordOutputError> {
        self.check_usable()?;
        self.usable = false;
        // Do not call flush_buffer here. Applications can drain our batch more
        // often than they flush a destination, and choose both cadences explicitly.
        self.destination.flush().await?;
        self.usable = true;
        Ok(())
    }
}

impl<W: AsyncWrite + AsyncSyncData + Unpin, Mode> RecordWriter<W, Mode> {
    /// Synchronizes previously flushed destination data to persistent storage.
    ///
    /// Available only for destinations implementing [`AsyncSyncData`], including
    /// Tokio files. This neither sends the writer's buffered records nor calls
    /// the destination's `flush`. To include pending records, explicitly call
    /// `flush_buffer`, then `flush`, then this method.
    ///
    /// A failure returns the original I/O error and makes the writer unusable.
    /// Panics or cancellation during synchronization also make it unusable because
    /// durability is uncertain. An unpolled future leaves the writer unchanged.
    /// Successful synchronization follows the destination's platform guarantees
    /// and need not include all file metadata. It does not acknowledge replication.
    pub async fn sync_data(&mut self) -> Result<(), RecordOutputError> {
        self.check_usable()?;
        // Reuse the terminal I/O policy even though sync sends no record bytes:
        // after an incomplete sync, continuing must be an application recovery
        // decision. The bool adds no state or extra checks to serialization.
        self.usable = false;
        // The bound selects this capability at compile time. No runtime type
        // check, implicit flush, or automatic sync on other write paths occurs.
        self.destination.sync_data().await?;
        self.usable = true;
        Ok(())
    }
}

impl<W, Mode> fmt::Debug for RecordWriter<W, Mode> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Report lifecycle state without exposing payloads or requiring W: Debug.
        formatter
            .debug_struct("RecordWriter")
            .field("usable", &self.usable)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        future::{Future, poll_fn},
        io::Write,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use tokio::io::{AsyncReadExt, AsyncSeekExt, BufWriter};

    use super::*;
    use crate::{
        RecordBuildError, RecordReader, SequenceNumber, StreamId,
        record::record_protocol::{
            MAX_PAYLOAD_LEN,
            test_data::{FRAME, encode_frame},
        },
    };

    #[derive(Default)]
    struct Observed {
        bytes: Vec<u8>,
        addresses: Vec<usize>,
        flushes: usize,
        shutdowns: usize,
        dropped: bool,
        resume_output: bool,
        sync_calls: usize,
        synced: Vec<Vec<u8>>,
        resume_sync: bool,
        resume_flush: bool,
    }

    #[derive(Clone, Copy)]
    enum SyncBehavior {
        Ready,
        Pending,
        Error,
        Panic,
    }

    // An owned, deliberately !Send destination. No runtime, threads or LocalSet
    // are needed to poll it. Shared state is only test instrumentation.
    struct Destination {
        observed: Rc<RefCell<Observed>>,
        max_write: usize,
        interrupt_once: bool,
        fail_after: Option<usize>,
        pending_after: Option<usize>,
        fail_flush: bool,
        pending_flush: bool,
        panic_flush: bool,
        panic_write: bool,
        sync_behavior: SyncBehavior,
    }

    impl Destination {
        fn new() -> Self {
            Self {
                observed: Rc::default(),
                max_write: usize::MAX,
                interrupt_once: false,
                fail_after: None,
                pending_after: None,
                fail_flush: false,
                pending_flush: false,
                panic_flush: false,
                panic_write: false,
                sync_behavior: SyncBehavior::Ready,
            }
        }
    }

    impl AsyncWrite for Destination {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            assert!(!self.panic_write, "intentional destination panic");
            if self.interrupt_once {
                self.interrupt_once = false;
                return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
            }
            let mut observed = self.observed.borrow_mut();
            if self
                .fail_after
                .is_some_and(|limit| observed.bytes.len() >= limit)
            {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "original output error",
                )));
            }
            if self
                .pending_after
                .is_some_and(|limit| observed.bytes.len() >= limit)
                && !observed.resume_output
            {
                return Poll::Pending;
            }
            let count = bytes.len().min(self.max_write);
            observed.addresses.push(bytes.as_ptr() as usize);
            observed.bytes.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.observed.borrow_mut().flushes += 1;
            assert!(!self.panic_flush, "intentional flush panic");
            if self.pending_flush && !self.observed.borrow().resume_flush {
                return Poll::Pending;
            }
            Poll::Ready(if self.fail_flush {
                Err(io::Error::other("original flush error"))
            } else {
                Ok(())
            })
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.observed.borrow_mut().shutdowns += 1;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncSyncData for Destination {
        async fn sync_data(&mut self) -> io::Result<()> {
            self.observed.borrow_mut().sync_calls += 1;
            poll_fn(|_| {
                let mut observed = self.observed.borrow_mut();
                match self.sync_behavior {
                    SyncBehavior::Pending if !observed.resume_sync => return Poll::Pending,
                    SyncBehavior::Error => {
                        return Poll::Ready(Err(io::Error::other("original sync error")));
                    }
                    SyncBehavior::Panic => panic!("intentional sync panic"),
                    _ => {}
                }
                let bytes = observed.bytes.clone();
                observed.synced.push(bytes);
                Poll::Ready(Ok(()))
            })
            .await
        }
    }

    impl Drop for Destination {
        fn drop(&mut self) {
            self.observed.borrow_mut().dropped = true;
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum WriteStep {
        Accept(usize),
        Interrupt,
        Pending,
        Zero,
        Error,
        Panic,
    }

    // A finite script makes partial-output tests deterministic and checks that
    // no extra destination call happens after completion or terminal failure.
    struct ScriptedDestination {
        inner: Destination,
        steps: VecDeque<WriteStep>,
    }

    impl ScriptedDestination {
        fn new(steps: impl IntoIterator<Item = WriteStep>) -> Self {
            Self {
                inner: Destination::new(),
                steps: steps.into_iter().collect(),
            }
        }
    }

    impl AsyncWrite for ScriptedDestination {
        fn poll_write(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.steps.pop_front().expect("unexpected extra write") {
                WriteStep::Accept(count) => {
                    self.inner.max_write = count;
                    Pin::new(&mut self.inner).poll_write(context, bytes)
                }
                WriteStep::Interrupt => Poll::Ready(Err(io::ErrorKind::Interrupted.into())),
                WriteStep::Pending => {
                    // The next scripted step is immediately available on repoll.
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
                WriteStep::Zero => Poll::Ready(Ok(0)),
                WriteStep::Error => Poll::Ready(Err(io::Error::other("scripted write failure"))),
                WriteStep::Panic => panic!("scripted write panic"),
            }
        }

        fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(context)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(context)
        }
    }

    impl AsyncSyncData for ScriptedDestination {
        async fn sync_data(&mut self) -> io::Result<()> {
            self.inner.sync_data().await
        }
    }

    fn id(stream: u16, sequence: u64) -> RecordId {
        RecordId::new(
            StreamId::new(stream).unwrap(),
            SequenceNumber::new(sequence),
        )
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    async fn decoded_record(frame: &[u8]) -> Record {
        // Test fixtures enter through the real validation boundary; no unchecked
        // Record construction or production encoder supplies expected bytes.
        let mut reader = RecordReader::new(frame);
        assert!(reader.wait_to_read().await.unwrap());
        reader.try_read_next().unwrap().unwrap()
    }

    async fn record_with_body(id: RecordId, body: &[u8]) -> Record {
        decoded_record(&encode_frame(id, body)).await
    }

    async fn golden_record() -> Record {
        decoded_record(FRAME).await
    }

    #[test]
    fn writes_are_synchronous_and_an_owned_local_destination_needs_no_runtime() {
        let destination = Destination::new();
        let observed = Rc::clone(&destination.observed);
        let mut writer = RecordWriter::for_serialization(destination);
        let called = Rc::new(Cell::new(false));
        writer
            .write(id(0, 1), |body| {
                called.set(true);
                body.write_all(b"local")
            })
            .unwrap();
        assert!(called.get());
        assert!(observed.borrow().bytes.is_empty());
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"local"));
        {
            let mut flush = Box::pin(writer.flush_buffer());
            assert!(matches!(poll_once(flush.as_mut()), Poll::Ready(Ok(()))));
        }
        assert_eq!(observed.borrow().bytes, encode_frame(id(0, 1), b"local"));
        assert_eq!(observed.borrow().flushes, 0);
        assert!(!observed.borrow().dropped);
        let destination = writer.into_inner();
        assert!(Rc::ptr_eq(&observed, &destination.observed));
        assert_eq!(observed.borrow().shutdowns, 0);
        drop(destination);
        assert!(observed.borrow().dropped);
    }

    #[tokio::test]
    async fn batches_grow_past_one_record_and_reuse_capacity_after_explicit_output() {
        let mut writer = RecordWriter::for_serialization(Destination::new());
        let bodies = [
            Vec::new(),
            vec![0x5a; 2048],
            vec![0xa5; MAX_PAYLOAD_LEN],
            vec![9; 8],
        ];
        let expected = bodies
            .iter()
            .enumerate()
            .flat_map(|(index, body)| encode_frame(id(index as u16, index as u64), body).to_vec())
            .collect::<Vec<_>>();
        assert!(expected.len() > MAX_RECORD_LEN);

        let mut allocation = None;
        for round in 0..2 {
            for (index, body) in bodies.iter().enumerate() {
                writer
                    .write(id(index as u16, index as u64), |builder| {
                        // Many field-sized writes, with the limit applied per record.
                        for field in body.chunks(8) {
                            builder.write_all(field)?;
                        }
                        Ok::<_, io::Error>(())
                    })
                    .unwrap();
            }
            assert_eq!(&writer.buffer[..], expected);
            assert_eq!(
                writer.get_ref().observed.borrow().bytes.len(),
                round * expected.len()
            );
            let current = (writer.buffer.as_ptr() as usize, writer.buffer.capacity());
            if let Some(previous) = allocation {
                assert_eq!(current, previous);
            }
            allocation = Some(current);
            writer.flush_buffer().await.unwrap();
            assert!(writer.buffer.is_empty());
            assert_eq!(
                (writer.buffer.as_ptr() as usize, writer.buffer.capacity()),
                current
            );
            // Repeated/empty output must not replay any bytes or flush the sink.
            writer.flush_buffer().await.unwrap();
            assert_eq!(
                writer.get_ref().observed.borrow().addresses.len(),
                round + 1
            );
        }
        {
            let observed = writer.get_ref().observed.borrow();
            assert_eq!(observed.bytes, expected.repeat(2));
            assert_eq!(observed.flushes, 0);
        }

        let mut reader = RecordReader::new(&expected[..]);
        let mut index = 0;
        while reader.wait_to_read().await.unwrap() {
            while let Some(record) = reader.try_read_next().unwrap() {
                assert_eq!(record.id(), id(index as u16, index as u64));
                assert_eq!(record.body(), bodies[index]);
                index += 1;
            }
        }
        assert_eq!(index, bodies.len());
    }

    #[tokio::test]
    async fn existing_records_join_the_batch_in_order_and_need_not_outlive_the_append() {
        let record = golden_record().await;
        let mut writer = RecordWriter::for_records(Destination::new());
        writer
            .write_record(&record_with_body(id(0, 1), b"before").await)
            .unwrap();
        writer.write_record(&record).unwrap();
        assert_eq!(record.as_bytes().as_ref(), FRAME);
        drop(record);
        writer
            .write_record(&record_with_body(id(0, 2), b"after").await)
            .unwrap();
        let expected = [
            encode_frame(id(0, 1), b"before").as_ref(),
            FRAME,
            encode_frame(id(0, 2), b"after").as_ref(),
        ]
        .concat();
        assert_eq!(&writer.buffer[..], expected);
        assert!(writer.get_ref().observed.borrow().bytes.is_empty());
        let address = writer.buffer.as_ptr() as usize;
        writer.flush_buffer().await.unwrap();
        let observed = writer.get_ref().observed.borrow();
        assert_eq!(observed.addresses, [address]);
        assert_eq!(observed.bytes, expected);
    }

    #[tokio::test]
    async fn maximum_existing_records_can_be_copied_repeatedly_without_changing_ids_or_limits() {
        let record_id = RecordId::new(StreamId::MAX, SequenceNumber::MAX);
        let payload = vec![0xd7; MAX_PAYLOAD_LEN];
        let frame = encode_frame(record_id, &payload);
        let record = {
            let mut reader = RecordReader::new(frame.as_ref());
            assert!(reader.wait_to_read().await.unwrap());
            reader.try_read_next().unwrap().unwrap()
        };
        let mut writer = RecordWriter::for_records(Vec::new());
        // Reusing the same ID is intentional: this layer neither advances IDs
        // (which would overflow here) nor enforces stream sequence continuity.
        writer.write_record(&record).unwrap();
        writer.write_record(&record).unwrap();
        assert_eq!(record.as_bytes().as_ref(), frame.as_ref());
        drop(record);
        writer
            .write_record(&record_with_body(id(0, 0), b"").await)
            .unwrap();
        assert_eq!(writer.buffer.len(), 2 * MAX_RECORD_LEN + 16);
        assert!(writer.get_ref().is_empty());
        writer.flush_buffer().await.unwrap();
        assert_eq!(
            writer.get_ref().as_slice(),
            [
                frame.as_ref(),
                frame.as_ref(),
                encode_frame(id(0, 0), b"").as_ref()
            ]
            .concat()
        );

        // A shorter following batch must reuse storage without leaking old bytes.
        let address = writer.buffer.as_ptr();
        let capacity = writer.buffer.capacity();
        writer
            .write_record(&record_with_body(id(0, 1), b"short").await)
            .unwrap();
        writer.flush_buffer().await.unwrap();
        assert_eq!(writer.buffer.as_ptr(), address);
        assert_eq!(writer.buffer.capacity(), capacity);
        assert_eq!(
            writer.into_inner(),
            [
                frame.as_ref(),
                frame.as_ref(),
                encode_frame(id(0, 0), b"").as_ref(),
                encode_frame(id(0, 1), b"short").as_ref()
            ]
            .concat()
        );
    }

    #[tokio::test]
    async fn construction_errors_and_unwind_preserve_preceding_buffered_records() {
        let mut writer = RecordWriter::for_serialization(Vec::new());
        writer
            .write(id(0, 1), |body| body.write_all(b"first"))
            .unwrap();
        let prefix = encode_frame(id(0, 1), b"first");
        let error = writer
            .write(id(0, 2), |body| {
                body.write_all(b"discard").unwrap();
                Err(123_u32)
            })
            .unwrap_err();
        assert!(matches!(error, RecordWriteError::Serialize(123)));
        assert_eq!(&writer.buffer[..], prefix);

        let too_large = vec![0; MAX_PAYLOAD_LEN + 1];
        for callback_fails in [false, true] {
            let error = writer
                .write(id(0, 2), |body| {
                    let _ = body.write_all(&too_large);
                    if callback_fails { Err(456_u32) } else { Ok(()) }
                })
                .unwrap_err();
            assert!(matches!(
                error,
                RecordWriteError::Build(RecordBuildError::PayloadTooLarge)
            ));
            assert_eq!(&writer.buffer[..], prefix);
        }
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                writer.write(id(0, 2), |body| -> io::Result<()> {
                    body.write_all(b"unfinished")?;
                    panic!("intentional callback panic");
                })
            }))
            .is_err()
        );
        assert!(writer.usable);
        assert_eq!(&writer.buffer[..], prefix);
        assert!(writer.get_ref().is_empty());
        writer
            .write(id(0, 2), |body| body.write_all(b"last"))
            .unwrap();
        writer.flush_buffer().await.unwrap();
        assert_eq!(
            writer.into_inner(),
            [prefix, encode_frame(id(0, 2), b"last")].concat()
        );
    }

    #[tokio::test]
    async fn rejecting_a_fully_written_maximum_payload_preserves_the_batch_and_reuses_dirty_storage()
     {
        let payload = vec![0xe3; MAX_PAYLOAD_LEN];
        let prefix = encode_frame(id(0, 1), b"accepted");
        for unwind in [false, true] {
            let mut writer = RecordWriter::for_serialization(Vec::new());
            writer
                .write(id(0, 1), |body| body.write_all(b"accepted"))
                .unwrap();
            let old_capacity = writer.buffer.capacity();
            let result = catch_unwind(AssertUnwindSafe(|| {
                writer.write(id(0, 2), |body| {
                    body.write_all(&payload).unwrap();
                    body.flush().unwrap(); // A serializer flush must not publish it.
                    if unwind {
                        panic!("callback fails after filling the payload");
                    }
                    Err(123_u32)
                })
            }));
            if unwind {
                assert!(result.is_err());
            } else {
                assert!(matches!(result, Ok(Err(RecordWriteError::Serialize(123)))));
            }
            assert!(writer.usable);
            assert!(writer.buffer.capacity() > old_capacity);
            assert_eq!(&writer.buffer[..], prefix);
            assert!(writer.get_ref().is_empty());
            let address = writer.buffer.as_ptr();
            let capacity = writer.buffer.capacity();
            // The shorter replacements must initialize their own headers/CRCs
            // and expose none of the large abandoned payload still in capacity.
            writer.write(id(0, 2), |_| Ok::<_, io::Error>(())).unwrap();
            writer
                .write(id(0, 3), |body| body.write_all(b"next"))
                .unwrap();
            writer.flush_buffer().await.unwrap();
            assert_eq!(writer.buffer.as_ptr(), address);
            assert_eq!(writer.buffer.capacity(), capacity);
            assert_eq!(
                writer.into_inner(),
                [
                    prefix.as_ref(),
                    encode_frame(id(0, 2), b"").as_ref(),
                    encode_frame(id(0, 3), b"next").as_ref()
                ]
                .concat()
            );
        }
    }

    #[tokio::test]
    async fn partial_writes_and_interruption_preserve_batch_bytes_and_order() {
        let record = golden_record().await;
        let mut destination = Destination::new();
        destination.max_write = 3;
        destination.interrupt_once = true;
        let mut writer = RecordWriter::for_records(destination);
        writer.write_record(&record).unwrap();
        writer
            .write_record(&record_with_body(id(0, 1), b"next").await)
            .unwrap();
        let expected = [FRAME, encode_frame(id(0, 1), b"next").as_ref()].concat();
        let address = writer.buffer.as_ptr() as usize;
        writer.flush_buffer().await.unwrap();
        writer.flush().await.unwrap();
        let observed = writer.get_ref().observed.borrow();
        assert_eq!(observed.bytes, expected);
        assert_eq!(
            observed.addresses,
            (0..expected.len())
                .step_by(3)
                .map(|offset| address + offset)
                .collect::<Vec<_>>()
        );
        assert_eq!(observed.flushes, 1);
        assert_eq!(observed.shutdowns, 0);
    }

    #[test]
    fn pending_buffer_output_resumes_from_its_cursor_and_retains_the_allocation() {
        let mut destination = Destination::new();
        destination.max_write = 3;
        destination.pending_after = Some(3);
        let observed = Rc::clone(&destination.observed);
        let mut writer = RecordWriter::for_serialization(destination);
        writer
            .write(id(0, 1), |body| body.write_all(b"resume"))
            .unwrap();
        let address = writer.buffer.as_ptr() as usize;
        {
            let mut flush = Box::pin(writer.flush_buffer());
            assert!(poll_once(flush.as_mut()).is_pending());
            assert_eq!(
                observed.borrow().bytes,
                encode_frame(id(0, 1), b"resume")[..3]
            );
            observed.borrow_mut().resume_output = true;
            assert!(matches!(poll_once(flush.as_mut()), Poll::Ready(Ok(()))));
        }
        assert!(writer.usable);
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.buffer.as_ptr() as usize, address);
        let observed = observed.borrow();
        assert_eq!(observed.bytes, encode_frame(id(0, 1), b"resume"));
        assert_eq!(&observed.addresses[..2], &[address, address + 3]);
    }

    #[tokio::test]
    async fn every_partial_batch_boundary_preserves_bytes_and_rejects_replay_after_failure() {
        let record = golden_record().await;
        let expected = [FRAME, encode_frame(id(0, 1), b"").as_ref()].concat();
        // Every prefix includes header, body and CRC positions, the exact boundary
        // between records, and all but the final byte. Completion is tested below.
        for accepted in 0..expected.len() {
            for stop in [
                WriteStep::Error,
                WriteStep::Zero,
                WriteStep::Pending,
                WriteStep::Panic,
            ] {
                let mut steps = Vec::new();
                if accepted != 0 {
                    steps.push(WriteStep::Accept(accepted));
                }
                steps.push(stop);
                let destination = ScriptedDestination::new(steps);
                let observed = Rc::clone(&destination.inner.observed);
                let mut writer = RecordWriter::for_records(destination);
                writer.write_record(&record).unwrap();
                writer
                    .write_record(&record_with_body(id(0, 1), b"").await)
                    .unwrap();
                {
                    let mut send = Box::pin(writer.flush_buffer());
                    let result = catch_unwind(AssertUnwindSafe(|| poll_once(send.as_mut())));
                    match stop {
                        WriteStep::Error | WriteStep::Zero => {
                            let Ok(Poll::Ready(Err(RecordOutputError::Io(error)))) = result else {
                                panic!("expected I/O error at byte {accepted}: {stop:?}");
                            };
                            assert_eq!(
                                error.kind(),
                                if matches!(stop, WriteStep::Zero) {
                                    io::ErrorKind::WriteZero
                                } else {
                                    io::ErrorKind::Other
                                }
                            );
                        }
                        WriteStep::Pending => assert!(matches!(result, Ok(Poll::Pending))),
                        WriteStep::Panic => assert!(result.is_err()),
                        _ => unreachable!(),
                    }
                } // Dropping a pending future abandons its partial-write cursor.
                assert!(!writer.usable, "byte {accepted}, {stop:?}");
                assert_eq!(&writer.buffer[..], expected);
                assert_eq!(observed.borrow().bytes, expected[..accepted]);
                assert!(writer.get_ref().steps.is_empty());
                assert!(matches!(
                    writer.write_record(&record),
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.flush_buffer().await,
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.flush().await,
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.sync_data().await,
                    Err(RecordOutputError::Unusable)
                ));
                // Recovery can reclaim ownership, but cannot silently replay or
                // flush the remaining batch while extracting/dropping the writer.
                let destination = writer.into_inner();
                assert!(!observed.borrow().dropped);
                drop(destination);
                let observed = observed.borrow();
                assert!(observed.dropped);
                assert_eq!(observed.bytes, expected[..accepted]);
                assert_eq!(observed.flushes, 0);
                assert_eq!(observed.sync_calls, 0);
                assert_eq!(observed.shutdowns, 0);
            }
        }
    }

    #[tokio::test]
    async fn repeated_interruptions_and_pending_writes_resume_across_record_boundaries() {
        let record = golden_record().await;
        let expected = [FRAME, encode_frame(id(0, 1), b"").as_ref()].concat();
        let destination = ScriptedDestination::new([
            WriteStep::Interrupt,
            WriteStep::Interrupt,
            WriteStep::Accept(2),
            WriteStep::Interrupt,
            WriteStep::Pending,
            WriteStep::Accept(FRAME.len() - 2),
            WriteStep::Pending,
            WriteStep::Interrupt,
            WriteStep::Interrupt,
            WriteStep::Accept(expected.len() - FRAME.len() - 1),
            WriteStep::Pending,
            WriteStep::Accept(1),
        ]);
        let observed = Rc::clone(&destination.inner.observed);
        let mut writer = RecordWriter::for_records(destination);
        writer.write_record(&record).unwrap();
        writer
            .write_record(&record_with_body(id(0, 1), b"").await)
            .unwrap();
        let address = writer.buffer.as_ptr() as usize;
        let capacity = writer.buffer.capacity();
        {
            let mut send = Box::pin(writer.flush_buffer());
            for accepted in [2, FRAME.len(), expected.len() - 1] {
                assert!(poll_once(send.as_mut()).is_pending());
                assert_eq!(observed.borrow().bytes, expected[..accepted]);
            }
            assert!(matches!(poll_once(send.as_mut()), Poll::Ready(Ok(()))));
        }
        assert!(writer.usable);
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.buffer.as_ptr() as usize, address);
        assert_eq!(writer.buffer.capacity(), capacity);
        assert!(writer.get_ref().steps.is_empty());
        let observed = observed.borrow();
        assert_eq!(observed.bytes, expected);
        assert_eq!(
            observed.addresses,
            [
                address,
                address + 2,
                address + FRAME.len(),
                address + expected.len() - 1
            ]
        );
        assert_eq!(observed.flushes, 0);
        assert_eq!(observed.sync_calls, 0);
    }

    #[tokio::test]
    async fn output_errors_preserve_the_batch_and_original_error_and_reject_further_work() {
        // Exercise the same output lifecycle through each public constructor.
        // Only the append and its post-failure rejection differ between modes.
        async fn check<Mode>(writer: &mut RecordWriter<Destination, Mode>, zero: bool) {
            let RecordOutputError::Io(error) = writer.flush_buffer().await.unwrap_err() else {
                panic!("expected original I/O error")
            };
            assert_eq!(
                error.kind(),
                if zero {
                    io::ErrorKind::WriteZero
                } else {
                    io::ErrorKind::BrokenPipe
                }
            );
            if !zero {
                assert_eq!(error.to_string(), "original output error");
            }
            assert_eq!(&writer.buffer[..], FRAME);
            assert!(matches!(
                writer.flush_buffer().await,
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.flush().await,
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.sync_data().await,
                Err(RecordOutputError::Unusable)
            ));
            let observed = writer.get_ref().observed.borrow();
            assert_eq!(observed.bytes, FRAME[..if zero { 0 } else { 3 }]);
            assert_eq!(observed.flushes, 0);
            assert_eq!(observed.sync_calls, 0);
        }

        let record = golden_record().await;
        for zero in [false, true] {
            let failing_destination = || {
                let mut destination = Destination::new();
                destination.max_write = if zero { 0 } else { 3 };
                destination.fail_after = Some(3);
                destination
            };

            let mut serialized = RecordWriter::for_serialization(failing_destination());
            serialized
                .write(record.id(), |body| body.write_all(record.body()))
                .unwrap();
            check(&mut serialized, zero).await;
            assert!(matches!(
                serialized.write(id(0, 1), |_| -> io::Result<()> {
                    panic!("callback invoked after output failure");
                }),
                Err(RecordWriteError::Output(RecordOutputError::Unusable))
            ));
            assert_eq!(&serialized.buffer[..], FRAME);

            let mut copied = RecordWriter::for_records(failing_destination());
            copied.write_record(&record).unwrap();
            check(&mut copied, zero).await;
            assert!(matches!(
                copied.write_record(&record),
                Err(RecordOutputError::Unusable)
            ));
            assert_eq!(&copied.buffer[..], FRAME);
        }
    }

    #[tokio::test]
    async fn cancelling_pending_buffer_output_never_replays_an_accepted_prefix() {
        let record = golden_record().await;
        for accepted in [0, 3] {
            let mut destination = Destination::new();
            destination.max_write = 3;
            destination.pending_after = Some(accepted);
            let mut writer = RecordWriter::for_records(destination);
            writer.write_record(&record).unwrap();
            {
                let mut flush = Box::pin(writer.flush_buffer());
                assert!(poll_once(flush.as_mut()).is_pending());
            }
            // The cursor was dropped; another flush must not restart from zero.
            assert!(!writer.usable);
            assert_eq!(&writer.buffer[..], FRAME);
            assert!(matches!(
                writer.flush_buffer().await,
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.write_record(&record),
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.flush().await,
                Err(RecordOutputError::Unusable)
            ));
            assert_eq!(writer.get_ref().observed.borrow().bytes, FRAME[..accepted]);
        }
    }

    #[test]
    fn unpolled_buffer_output_preserves_the_batch_and_output_panics_make_it_unusable() {
        let mut writer = RecordWriter::for_serialization(Destination::new());
        writer
            .write(id(0, 1), |body| body.write_all(b"body"))
            .unwrap();
        drop(writer.flush_buffer());
        assert!(writer.usable);
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"body"));
        assert!(writer.get_ref().observed.borrow().bytes.is_empty());
        writer.destination.panic_write = true;
        {
            let mut flush = Box::pin(writer.flush_buffer());
            assert!(catch_unwind(AssertUnwindSafe(|| poll_once(flush.as_mut()))).is_err());
        }
        assert!(!writer.usable);
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"body"));
        assert!(writer.get_ref().observed.borrow().bytes.is_empty());
    }

    #[tokio::test]
    async fn empty_buffer_output_does_not_touch_the_destination() {
        let mut destination = Destination::new();
        destination.panic_write = true;
        destination.fail_flush = true;
        let mut writer = RecordWriter::for_serialization(destination);
        writer.flush_buffer().await.unwrap();
        assert!(writer.usable);
        assert_eq!(writer.get_ref().observed.borrow().flushes, 0);
    }

    #[tokio::test]
    async fn buffer_output_and_destination_flushing_are_separate_operations() {
        let destination = Destination::new();
        let observed = Rc::clone(&destination.observed);
        let mut writer =
            RecordWriter::for_serialization(BufWriter::with_capacity(4096, destination));
        writer
            .write(id(0, 1), |body| body.write_all(b"buffered downstream"))
            .unwrap();
        let expected = encode_frame(id(0, 1), b"buffered downstream");
        writer.flush().await.unwrap();
        assert_eq!(&writer.buffer[..], expected);
        assert!(writer.get_ref().buffer().is_empty());
        assert!(observed.borrow().bytes.is_empty());

        writer.flush_buffer().await.unwrap();
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.get_ref().buffer(), expected);
        assert!(observed.borrow().bytes.is_empty());
        assert_eq!(observed.borrow().flushes, 1);

        writer.flush().await.unwrap();
        assert_eq!(observed.borrow().bytes, expected);
        assert_eq!(observed.borrow().flushes, 2);
        drop(writer);
        assert_eq!(observed.borrow().flushes, 2);
        assert_eq!(observed.borrow().shutdowns, 0);
        assert!(observed.borrow().dropped);
    }

    #[test]
    fn drop_and_into_inner_discard_unsent_records_without_implicit_output() {
        for extract in [false, true] {
            let destination = Destination::new();
            let observed = Rc::clone(&destination.observed);
            let mut writer = RecordWriter::for_serialization(destination);
            writer
                .write(id(0, 1), |body| body.write_all(b"unsent"))
                .unwrap();
            if extract {
                let destination = writer.into_inner();
                assert!(!observed.borrow().dropped);
                drop(destination);
            } else {
                drop(writer);
            }
            let observed = observed.borrow();
            assert!(observed.dropped);
            assert!(observed.bytes.is_empty());
            assert_eq!(observed.flushes, 0);
            assert_eq!(observed.shutdowns, 0);
        }
    }

    #[tokio::test]
    async fn downstream_flush_failure_keeps_newer_records_buffered_and_never_replays_older_bytes() {
        let record = golden_record().await;
        let destination = ScriptedDestination::new([WriteStep::Accept(3), WriteStep::Error]);
        let observed = Rc::clone(&destination.inner.observed);
        let mut writer = RecordWriter::for_records(BufWriter::with_capacity(4096, destination));
        writer.write_record(&record).unwrap();
        writer.flush_buffer().await.unwrap();
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.get_ref().buffer(), FRAME);
        assert!(observed.borrow().bytes.is_empty());
        writer
            .write_record(&record_with_body(id(0, 1), b"newer").await)
            .unwrap();
        let RecordOutputError::Io(error) = writer.flush().await.unwrap_err() else {
            panic!("expected failure writing the downstream buffer");
        };
        assert_eq!(error.to_string(), "scripted write failure");
        assert!(!writer.usable);
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"newer"));
        assert_eq!(observed.borrow().bytes, FRAME[..3]);
        assert!(matches!(
            writer.flush_buffer().await,
            Err(RecordOutputError::Unusable)
        ));
        assert!(matches!(
            writer.flush().await,
            Err(RecordOutputError::Unusable)
        ));
        drop(writer.into_inner());
        let observed = observed.borrow();
        assert_eq!(observed.bytes, FRAME[..3]);
        assert!(observed.dropped);
        assert_eq!(observed.shutdowns, 0);
    }

    #[tokio::test]
    async fn unpolled_and_resumed_destination_flushes_leave_newer_records_buffered() {
        let mut destination = Destination::new();
        destination.pending_flush = true;
        let observed = Rc::clone(&destination.observed);
        let mut writer = RecordWriter::for_serialization(destination);
        writer
            .write(id(0, 1), |body| body.write_all(b"sent"))
            .unwrap();
        writer.flush_buffer().await.unwrap();
        writer
            .write(id(0, 2), |body| body.write_all(b"unsent"))
            .unwrap();
        let unsent = encode_frame(id(0, 2), b"unsent");
        drop(writer.flush());
        assert!(writer.usable);
        assert_eq!(&writer.buffer[..], unsent);
        assert_eq!(observed.borrow().flushes, 0);
        {
            let mut flush = Box::pin(writer.flush());
            assert!(poll_once(flush.as_mut()).is_pending());
            assert_eq!(observed.borrow().bytes, encode_frame(id(0, 1), b"sent"));
            observed.borrow_mut().resume_flush = true;
            assert!(matches!(poll_once(flush.as_mut()), Poll::Ready(Ok(()))));
        }
        assert!(writer.usable);
        assert_eq!(&writer.buffer[..], unsent);
        assert_eq!(observed.borrow().flushes, 2);
        writer.flush_buffer().await.unwrap();
        assert_eq!(
            observed.borrow().bytes,
            [encode_frame(id(0, 1), b"sent"), unsent].concat()
        );
    }

    #[tokio::test]
    async fn destination_flush_panic_preserves_unsent_records_and_blocks_all_further_work() {
        let record = golden_record().await;
        let mut writer = RecordWriter::for_serialization(Destination::new());
        writer
            .write(record.id(), |body| body.write_all(record.body()))
            .unwrap();
        writer.flush_buffer().await.unwrap();
        writer.flush().await.unwrap();
        // A failure after an earlier successful flush must still be terminal.
        writer.destination.panic_flush = true;
        writer
            .write(id(0, 1), |body| body.write_all(b"unsent"))
            .unwrap();
        {
            let mut flush = Box::pin(writer.flush());
            assert!(catch_unwind(AssertUnwindSafe(|| poll_once(flush.as_mut()))).is_err());
        }
        assert!(!writer.usable);
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"unsent"));
        assert!(matches!(
            writer.write(id(0, 2), |_| -> io::Result<()> {
                panic!("callback invoked after flush panic");
            }),
            Err(RecordWriteError::Output(RecordOutputError::Unusable))
        ));
        assert!(matches!(
            writer.flush_buffer().await,
            Err(RecordOutputError::Unusable)
        ));
        assert!(matches!(
            writer.flush().await,
            Err(RecordOutputError::Unusable)
        ));
        assert!(matches!(
            writer.sync_data().await,
            Err(RecordOutputError::Unusable)
        ));
        let observed = writer.get_ref().observed.borrow();
        assert_eq!(observed.bytes, FRAME);
        assert_eq!(observed.flushes, 2);
        assert_eq!(observed.sync_calls, 0);
    }

    #[tokio::test]
    async fn failed_or_cancelled_destination_flush_makes_the_writer_unusable() {
        for cancel in [false, true] {
            let mut destination = Destination::new();
            destination.fail_flush = !cancel;
            destination.pending_flush = cancel;
            let mut writer = RecordWriter::for_serialization(destination);
            writer
                .write(id(0, 1), |body| body.write_all(b"finished record"))
                .unwrap();
            writer.flush_buffer().await.unwrap();
            writer
                .write(id(0, 2), |body| body.write_all(b"unsent"))
                .unwrap();
            if cancel {
                let mut flush = Box::pin(writer.flush());
                assert!(poll_once(flush.as_mut()).is_pending());
            } else {
                let RecordOutputError::Io(error) = writer.flush().await.unwrap_err() else {
                    panic!("expected I/O error")
                };
                assert_eq!(error.to_string(), "original flush error");
            }
            assert!(matches!(
                writer.flush().await,
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.flush_buffer().await,
                Err(RecordOutputError::Unusable)
            ));
            assert!(matches!(
                writer.sync_data().await,
                Err(RecordOutputError::Unusable)
            ));
            assert_eq!(writer.get_ref().observed.borrow().flushes, 1);
            assert_eq!(writer.get_ref().observed.borrow().sync_calls, 0);
            assert_eq!(&writer.buffer[..], encode_frame(id(0, 2), b"unsent"));
        }
    }

    #[tokio::test]
    async fn synchronization_is_separate_from_buffer_output_and_destination_flushing() {
        let mut writer = RecordWriter::for_serialization(Destination::new());
        let expected = encode_frame(id(0, 1), b"first").to_vec();
        writer
            .write(id(0, 1), |body| body.write_all(b"first"))
            .unwrap();

        // Synchronizing cannot publish the record still in our BytesMut.
        writer.sync_data().await.unwrap();
        assert_eq!(&writer.buffer[..], expected);
        assert_eq!(
            writer.get_ref().observed.borrow().synced,
            [Vec::<u8>::new()]
        );
        assert_eq!(writer.get_ref().observed.borrow().flushes, 0);

        writer.flush_buffer().await.unwrap();
        writer.flush().await.unwrap();
        writer.sync_data().await.unwrap();
        assert!(writer.usable);
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.get_ref().observed.borrow().sync_calls, 2);
        assert_eq!(writer.get_ref().observed.borrow().flushes, 1);

        // Later appends stay buffered while earlier output can be synced again.
        writer
            .write(id(0, 2), |body| body.write_all(b"later"))
            .unwrap();
        writer.sync_data().await.unwrap();
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 2), b"later"));
        let observed = writer.get_ref().observed.borrow();
        assert_eq!(observed.synced, [Vec::new(), expected.clone(), expected]);
        assert_eq!(observed.flushes, 1);
    }

    #[test]
    fn unpolled_sync_is_inert_and_pending_local_sync_can_complete() {
        let mut destination = Destination::new();
        destination.sync_behavior = SyncBehavior::Pending;
        let observed = Rc::clone(&destination.observed);
        let mut writer = RecordWriter::for_serialization(destination);
        writer
            .write(id(0, 1), |body| body.write_all(b"pending"))
            .unwrap();
        drop(writer.sync_data());
        assert!(writer.usable);
        assert_eq!(observed.borrow().sync_calls, 0);
        {
            let mut sync = Box::pin(writer.sync_data());
            assert!(poll_once(sync.as_mut()).is_pending());
            assert_eq!(observed.borrow().sync_calls, 1);
            assert!(observed.borrow().synced.is_empty());
            observed.borrow_mut().resume_sync = true;
            assert!(matches!(poll_once(sync.as_mut()), Poll::Ready(Ok(()))));
        }
        assert!(writer.usable);
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"pending"));
        assert_eq!(observed.borrow().sync_calls, 1);
        assert_eq!(observed.borrow().synced, [Vec::<u8>::new()]);
        writer.write(id(0, 2), |_| Ok::<_, io::Error>(())).unwrap();
    }

    #[tokio::test]
    async fn sync_errors_cancellation_and_panics_reject_further_work_without_draining() {
        let record = golden_record().await;
        for behavior in [
            SyncBehavior::Error,
            SyncBehavior::Pending,
            SyncBehavior::Panic,
        ] {
            for completed_sync in [false, true] {
                let mut writer = RecordWriter::for_records(Destination::new());
                writer.write_record(&record).unwrap();
                writer.flush_buffer().await.unwrap();
                writer.flush().await.unwrap();
                if completed_sync {
                    writer.sync_data().await.unwrap();
                }
                // A previous successful durable boundary must not mask a later failure.
                writer.destination.sync_behavior = behavior;
                writer
                    .write_record(&record_with_body(id(0, 1), b"unsent").await)
                    .unwrap();
                {
                    let mut sync = Box::pin(writer.sync_data());
                    match behavior {
                        SyncBehavior::Error => {
                            let Poll::Ready(Err(RecordOutputError::Io(error))) =
                                poll_once(sync.as_mut())
                            else {
                                panic!("expected original synchronization error");
                            };
                            assert_eq!(error.kind(), io::ErrorKind::Other);
                            assert_eq!(error.to_string(), "original sync error");
                        }
                        SyncBehavior::Pending => assert!(poll_once(sync.as_mut()).is_pending()),
                        SyncBehavior::Panic => {
                            assert!(
                                catch_unwind(AssertUnwindSafe(|| poll_once(sync.as_mut())))
                                    .is_err()
                            );
                        }
                        SyncBehavior::Ready => unreachable!(),
                    }
                }
                assert!(!writer.usable);
                assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"unsent"));
                assert!(matches!(
                    writer.write_record(&record),
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.flush_buffer().await,
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.flush().await,
                    Err(RecordOutputError::Unusable)
                ));
                assert!(matches!(
                    writer.sync_data().await,
                    Err(RecordOutputError::Unusable)
                ));
                let observed = writer.get_ref().observed.borrow();
                assert_eq!(observed.bytes, FRAME);
                assert_eq!(observed.flushes, 1);
                assert_eq!(observed.sync_calls, 1 + usize::from(completed_sync));
                assert_eq!(observed.synced.len(), usize::from(completed_sync));
                if completed_sync {
                    assert_eq!(observed.synced[0], FRAME);
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_sync_rejects_serialization_without_invoking_the_callback() {
        let mut writer = RecordWriter::for_serialization(Destination::new());
        writer
            .write(id(0, 1), |body| body.write_all(b"unsent"))
            .unwrap();
        writer.destination.sync_behavior = SyncBehavior::Error;
        assert!(matches!(
            writer.sync_data().await,
            Err(RecordOutputError::Io(_))
        ));
        assert!(matches!(
            writer.write(id(0, 2), |_| -> io::Result<()> {
                panic!("callback invoked after failed synchronization");
            }),
            Err(RecordWriteError::Output(RecordOutputError::Unusable))
        ));
        assert_eq!(&writer.buffer[..], encode_frame(id(0, 1), b"unsent"));
        assert!(writer.get_ref().observed.borrow().bytes.is_empty());
    }

    #[tokio::test]
    async fn both_modes_write_files_and_leave_durability_to_the_owner() {
        async fn check<Mode>(mut writer: RecordWriter<tokio::fs::File, Mode>) {
            writer.flush_buffer().await.unwrap();
            writer.flush().await.unwrap();
            writer.sync_data().await.unwrap();
            let mut file = writer.into_inner();
            file.rewind().await.unwrap();
            let mut stored = Vec::new();
            file.read_to_end(&mut stored).await.unwrap();
            assert_eq!(
                stored,
                [encode_frame(id(0, 1), b"file body").as_ref(), FRAME].concat()
            );
            let mut reader = RecordReader::new(&stored[..]);
            assert!(reader.wait_to_read().await.unwrap());
            assert_eq!(
                reader.try_read_next().unwrap().unwrap().body(),
                b"file body"
            );
            assert_eq!(
                reader.try_read_next().unwrap().unwrap().as_bytes().as_ref(),
                FRAME
            );
            assert!(reader.try_read_next().unwrap().is_none());
            assert!(!reader.wait_to_read().await.unwrap());
        }

        let record = golden_record().await;
        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut serialized = RecordWriter::for_serialization(file);
        serialized
            .write(id(0, 1), |body| body.write_all(b"file body"))
            .unwrap();
        serialized
            .write(record.id(), |body| body.write_all(record.body()))
            .unwrap();
        check(serialized).await;

        let file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
        let mut copied = RecordWriter::for_records(file);
        copied
            .write_record(&record_with_body(id(0, 1), b"file body").await)
            .unwrap();
        copied.write_record(&record).unwrap();
        check(copied).await;
    }

    #[tokio::test]
    async fn owned_socket_is_handed_over_after_handshake_and_closed_by_the_owner() {
        use tokio::net::{TcpListener, TcpStream};
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let send = async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(b"hello").await.unwrap();
            let mut response = [0; 5];
            socket.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"ready");
            let mut writer = RecordWriter::for_serialization(socket);
            writer
                .write(id(0, 1), |body| body.write_all(&[0x55; 2048]))
                .unwrap();
            writer.flush_buffer().await.unwrap();
            writer.flush().await.unwrap();
            let mut socket = writer.into_inner();
            socket.shutdown().await.unwrap();
        };
        let receive = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 5];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"hello");
            socket.write_all(b"ready").await.unwrap();
            let mut reader = RecordReader::new(socket);
            assert!(reader.wait_to_read().await.unwrap());
            assert_eq!(
                reader.try_read_next().unwrap().unwrap().body(),
                &[0x55; 2048]
            );
            assert!(reader.try_read_next().unwrap().is_none());
            assert!(!reader.wait_to_read().await.unwrap());
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(send, receive);
        })
        .await
        .unwrap();
    }
}

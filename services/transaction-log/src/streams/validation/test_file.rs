//! Explicit file controls shared by the log and index validator tests.

use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    future::{Future, poll_fn},
    io::{self, SeekFrom},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, ready},
};

use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek, AsyncWrite, AsyncWriteExt, ReadBuf},
};
use transaction_log_exports::AsyncSyncData;

use super::ValidationFile;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileOperation {
    Length,
    Truncate(u64),
    /// A write starting after this many bytes have been accepted by this wrapper.
    Write(usize),
    Flush,
    Sync,
}

/// One file's explicitly shared observations and injected behavior.
#[derive(Debug, Default)]
pub(super) struct FileProbe {
    /// Entries to length/truncate/sync, and poll attempts for writes/flushes.
    pub(super) started: RefCell<Vec<FileOperation>>,
    /// Operations that returned success; a write means acceptance, not durability.
    pub(super) completed: RefCell<Vec<FileOperation>>,
    pub(super) pause_at: Option<FileOperation>,
    pub(super) fail_at: Option<FileOperation>,
    pub(super) max_write: Option<usize>,
    pub(super) bytes_written: Cell<usize>,
    pub(super) paused: Cell<bool>,
    pub(super) resume: Cell<bool>,
    // Retain the underlying handle only in tests, so accepted Tokio writes can
    // be quiesced before inspecting bytes after an injected failure/drop.
    pub(super) abandoned_file: RefCell<Option<File>>,
}

impl FileProbe {
    fn before(&self, operation: FileOperation) -> Poll<io::Result<()>> {
        if self.pause_at == Some(operation) && !self.resume.get() {
            self.paused.set(true);
            // Tests drive their futures on one thread and explicitly poll again
            // after resuming. No background task needs waking.
            return Poll::Pending;
        }
        if self.fail_at == Some(operation) {
            return Poll::Ready(Err(io::Error::from_raw_os_error(123)));
        }
        Poll::Ready(Ok(()))
    }

    pub(super) async fn quiesce(&self) {
        let file = self.abandoned_file.borrow_mut().take();
        if let Some(mut file) = file {
            file.flush().await.unwrap();
        }
    }
}

/// Forwards successful operations to a real file, with explicit pauses and errors.
///
/// Length, truncation and synchronization can pause/fail before their underlying
/// calls. Writes can be shortened and paused/failed at a chosen accepted byte count;
/// flushes can pause/fail before reaching the file. Reads and seeks pass through.
/// No recovery decisions or validation logic belong in this wrapper.
#[derive(Debug)]
pub(super) struct TestFile {
    file: Option<File>,
    probe: Rc<FileProbe>,
}

impl TestFile {
    pub(super) fn new(file: File, probe: Rc<FileProbe>) -> Self {
        Self {
            file: Some(file),
            probe,
        }
    }
}

impl ValidationFile for TestFile {
    async fn length(&mut self) -> io::Result<u64> {
        self.probe.started.borrow_mut().push(FileOperation::Length);
        poll_fn(|_| self.probe.before(FileOperation::Length)).await?;
        let length = self.file.as_ref().unwrap().metadata().await?.len();
        self.probe
            .completed
            .borrow_mut()
            .push(FileOperation::Length);
        Ok(length)
    }

    async fn set_len(&mut self, length: u64) -> io::Result<()> {
        let operation = FileOperation::Truncate(length);
        self.probe.started.borrow_mut().push(operation);
        poll_fn(|_| self.probe.before(operation)).await?;
        self.file.as_ref().unwrap().set_len(length).await?;
        self.probe.completed.borrow_mut().push(operation);
        Ok(())
    }
}

impl AsyncSyncData for TestFile {
    async fn sync_data(&mut self) -> io::Result<()> {
        self.probe.started.borrow_mut().push(FileOperation::Sync);
        poll_fn(|_| self.probe.before(FileOperation::Sync)).await?;
        self.file.as_ref().unwrap().sync_data().await?;
        self.probe.completed.borrow_mut().push(FileOperation::Sync);
        Ok(())
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        *self.probe.abandoned_file.borrow_mut() = self.file.take();
    }
}

impl AsyncRead for TestFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.file.as_mut().unwrap()).poll_read(cx, output)
    }
}

impl AsyncSeek for TestFile {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        Pin::new(self.file.as_mut().unwrap()).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(self.file.as_mut().unwrap()).poll_complete(cx)
    }
}

impl AsyncWrite for TestFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let previous = self.probe.bytes_written.get();
        let operation = FileOperation::Write(previous);
        self.probe.started.borrow_mut().push(operation);
        ready!(self.probe.before(operation))?;
        let length = bytes.len().min(self.probe.max_write.unwrap_or(bytes.len()));
        let written =
            ready!(Pin::new(self.file.as_mut().unwrap()).poll_write(cx, &bytes[..length]))?;
        self.probe.completed.borrow_mut().push(operation);
        self.probe.bytes_written.set(previous + written);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.probe.started.borrow_mut().push(FileOperation::Flush);
        ready!(self.probe.before(FileOperation::Flush))?;
        ready!(Pin::new(self.file.as_mut().unwrap()).poll_flush(cx))?;
        self.probe.completed.borrow_mut().push(FileOperation::Flush);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.file.as_mut().unwrap()).poll_shutdown(cx)
    }
}

/// Drives either validator to its configured gate without timing sleeps.
pub(super) async fn wait_for_pause<F: Future>(probe: &FileProbe, mut validation: Pin<&mut F>)
where
    F::Output: Debug,
{
    poll_fn(|cx| {
        if let Poll::Ready(result) = validation.as_mut().poll(cx) {
            panic!("validation completed before the paused operation: {result:?}");
        }
        if probe.paused.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

use std::{future::Future, io};

use tokio::io::{AsyncRead, AsyncSeek};
use transaction_log_exports::AsyncSyncData;

/// Shared file capabilities needed for log and index recovery.
///
/// Implemented for [`tokio::fs::File`]. File wrappers, including controlled test
/// files, can implement this trait and be passed directly to
/// [`super::LogFileValidator::validate`] or [`super::IndexFileValidator::validate`].
/// Index recovery additionally requires [`tokio::io::AsyncWrite`]; log recovery
/// does not write record bytes. Reading, seeking and data
/// synchronization use their existing traits; only length operations are added.
/// All operations must refer to the same underlying file and cursor.
///
/// Operations return concrete futures without boxing or a `Send` requirement.
/// Implementations must defer I/O until polled and return the original I/O error
/// on failure. Cancellation need not stop underlying I/O. The recovery caller
/// supplies exclusive access and finishes flushing earlier writes before validation.
pub trait ValidationFile: AsyncRead + AsyncSeek + AsyncSyncData + Unpin {
    /// Returns the file's byte length without changing its contents or cursor.
    fn length(&mut self) -> impl Future<Output = io::Result<u64>>;

    /// Sets the exact byte length without repositioning the cursor.
    ///
    /// Shrinking removes the tail; extending adds zero bytes. Success means the
    /// length change has completed, not merely been scheduled. This does not
    /// establish durability; validation synchronizes separately after output.
    fn set_len(&mut self, length: u64) -> impl Future<Output = io::Result<()>>;
}

impl ValidationFile for tokio::fs::File {
    async fn length(&mut self) -> io::Result<u64> {
        Ok(self.metadata().await?.len())
    }

    async fn set_len(&mut self, length: u64) -> io::Result<()> {
        tokio::fs::File::set_len(self, length).await
    }
}

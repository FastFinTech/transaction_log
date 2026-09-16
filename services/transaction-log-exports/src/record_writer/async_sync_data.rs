use std::{future::Future, io};

/// Optional asynchronous data-synchronization capability for a destination.
///
/// Implemented for [`tokio::fs::File`]. Custom storage wrappers and test
/// destinations can implement it to expose [`super::RecordWriter::sync_data`].
/// This capability is separate from [`tokio::io::AsyncWrite`]; buffers and
/// sockets do not acquire it merely by accepting writes.
///
/// [`super::RecordWriter`] uses this trait through a generic bound, so selecting
/// the capability requires no runtime check or virtual call. The opaque future
/// does not require boxing or allocation. No `Send` bound is imposed on the
/// destination or future; implementations may use state local to their executor.
/// Callers cannot assume an arbitrary implementation's sync future is `Send`.
///
/// Exclusive mutable access allows wrappers to track synchronization state and
/// matches the writer's single-owner API, even though Tokio's file method itself
/// accepts a shared reference. This trait does not add a timer or choose a durable
/// sync cadence. The module README describes implementor and caller contracts.
pub trait AsyncSyncData {
    /// Synchronizes previously flushed data to the destination's persistent storage.
    ///
    /// To include pending records, callers must drain their own buffers and flush
    /// destination buffering first. Synchronizing older output while newer records
    /// remain buffered is valid; those newer records are outside this operation.
    /// Implementations must not implicitly drain a wrapper's pending write buffer.
    /// File metadata need not all be synchronized; guarantees follow the underlying
    /// storage and platform, which may implement this like `sync_all`.
    ///
    /// Success must represent completion of the underlying data-sync operation,
    /// not just scheduling it. Test implementations may simulate that completion.
    /// Returns the underlying I/O error if synchronization fails. Failure or cancellation
    /// leaves completion uncertain; dropping the future need not stop underlying
    /// I/O. Implementations should defer I/O until their future is polled.
    // Keep the future concrete and deliberately omit + Send. Implementors can use
    // native async fn; an async-trait macro would unnecessarily box this operation.
    fn sync_data(&mut self) -> impl Future<Output = io::Result<()>>;
}

impl AsyncSyncData for tokio::fs::File {
    async fn sync_data(&mut self) -> io::Result<()> {
        // Qualify the inherent method so this cannot recurse into our trait
        // implementation. Tokio supplies its file I/O scheduling and OS semantics;
        // this adapter adds neither an AsyncWrite flush nor another worker.
        tokio::fs::File::sync_data(self).await
    }
}

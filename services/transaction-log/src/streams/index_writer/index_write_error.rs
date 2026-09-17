use std::io;

use thiserror::Error;

/// A failed index-file operation or attempted reuse after incomplete output.
///
/// The original failure preserves its concrete I/O source. Later calls return
/// `Unusable`: partially accepted entries must not be replayed without recovery.
#[derive(Debug, Error)]
pub enum IndexWriteError {
    /// Index-file output, destination flushing or data synchronization failed.
    #[error("index file I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Earlier I/O failed, panicked or was cancelled after it began.
    #[error("index writer is unusable after incomplete or failed output")]
    Unusable,
}

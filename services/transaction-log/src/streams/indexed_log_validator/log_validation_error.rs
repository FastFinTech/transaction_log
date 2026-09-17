use crate::storage::StorageFileOpenError;
use crate::streams::{IndexWriteError, IndexedLogWriteError};
use std::io;
use thiserror::Error;
use transaction_log_exports::RecordReadError;

/// An operation failure or invalid recovery precondition, not a corrupt suffix.
#[derive(Debug, Error)]
pub enum LogValidationError {
    /// Provider failed to open the requested log/index path.
    #[error(transparent)]
    Open(#[from] StorageFileOpenError),
    /// The supplied trusted endpoint cannot describe this file's prefix.
    #[error("invalid validation start: {0}")]
    InvalidStart(&'static str),
    /// Existing index bytes do not support the supplied trusted prefix.
    #[error(
        "trusted index prefix is unavailable or inconsistent; supply an earlier trusted boundary"
    )]
    TrustedIndexUnavailable,
    /// Log-file metadata, seeking, truncation, flushing or synchronization failed.
    #[error("log file operation failed: {0}")]
    LogIo(#[source] io::Error),
    /// Index-file metadata, reading, seeking, truncation or synchronization failed.
    #[error("index file operation failed: {0}")]
    IndexIo(#[source] io::Error),
    /// Reader failed operationally; this is never classified as corrupt data.
    #[error("record reading failed: {0}")]
    Read(#[source] RecordReadError),
    /// Index repair output failed.
    #[error(transparent)]
    IndexWrite(#[from] IndexWriteError),
    /// Synchronizing the resumed writer failed during handover.
    #[error(transparent)]
    Writer(#[from] IndexedLogWriteError),
    /// File lengths changed while exclusive recovery access was required.
    #[error("file lengths changed during validation or before repair/handover")]
    FilesChanged,
    /// Validation must finish successfully before this operation is available.
    #[error("validate the pair before repair or handover")]
    NotValidated,
    /// Incomplete I/O has made this validator unusable; reopen before recovery.
    #[error("validator is unusable after incomplete or failed I/O")]
    Unusable,
    /// The index has not yet been repaired through the accepted log boundary.
    #[error("repair the index before handover")]
    IndexRepairRequired,
    /// The caller must explicitly authorize removal of the invalid log suffix.
    #[error("explicitly truncate the invalid log tail before handover")]
    LogRepairRequired,
    /// A full file must be handed over as a completed file, not a writable one.
    #[error("file is full; use into_completed")]
    Full,
    /// A partial file cannot be handed over as complete.
    #[error("file is not full; use into_writer")]
    NotFull,
}

use std::io;

use thiserror::Error;

use crate::streams::IndexWriteError;

/// Failure while checking a trusted index entry or replacing its following suffix.
#[derive(Debug, Error)]
pub enum IndexFileValidationError {
    /// Index-file metadata, seeking, reading, truncation or synchronization failed.
    #[error("index file operation failed: {0}")]
    Io(#[from] io::Error),
    /// The caller-certified entry is absent or contains another log position.
    #[error(
        "trusted index entry {entry_index} must contain {expected_position}, but contains {actual_position:?}"
    )]
    TrustedIndexMismatch {
        /// Zero-based dense-index entry selected by the trusted record ID.
        entry_index: u64,
        /// Caller-certified exclusive log end.
        expected_position: u64,
        /// Stored value, or `None` when the complete entry is absent.
        actual_position: Option<u64>,
    },
    /// The supplied suffix would exceed the entries assigned to one log file.
    #[error("trusted prefix and supplied suffix contain {count} entries; maximum is {maximum}")]
    TooManyEntries {
        /// Combined trusted-prefix and supplied-suffix entry count.
        count: u64,
        /// Maximum entries assigned to one index file.
        maximum: u64,
    },
    /// Index suffix output failed.
    #[error(transparent)]
    Write(#[from] IndexWriteError),
}

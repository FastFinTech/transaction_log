use std::io;

use thiserror::Error;
use transaction_log_exports::RecordReadError;

/// Invalid recovery input or an operational failure, never permission to truncate.
#[derive(Debug, Error)]
pub enum LogFileValidationError {
    /// The caller-supplied trusted endpoint cannot describe this file's prefix.
    #[error("invalid log validation start: {0}")]
    InvalidStart(&'static str),
    /// File metadata, seeking, truncation or synchronization failed.
    #[error("log file operation failed: {0}")]
    Io(#[from] io::Error),
    /// Record reading failed operationally, rather than finding corrupt content.
    #[error("log record reading failed: {0}")]
    Read(#[source] RecordReadError),
}

use thiserror::Error;
use transaction_log_exports::{RecordId, RecordReadError};

/// Content that invalidates the suffix after the last accepted record.
///
/// File I/O failures are separate operation errors and never authorize truncation.
#[derive(Debug, Error)]
pub enum LogTailError {
    /// Framing, stream domain or CRC validation failed at the next record.
    #[error("invalid record encoding: {0}")]
    Record(#[source] RecordReadError),
    /// The next record belongs to another stream or breaks sequence continuity.
    #[error("expected record {expected:?}, received {actual:?}")]
    UnexpectedRecordId {
        /// Required stream and sequence.
        expected: RecordId,
        /// Identity found in the file.
        actual: RecordId,
    },
    /// Bytes follow the last of this file's 100,000 assigned records.
    #[error("bytes follow the complete assigned record range")]
    ExtraData,
}

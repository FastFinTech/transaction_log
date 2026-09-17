use thiserror::Error;
use transaction_log_exports::{RecordId, RecordOutputError};

use crate::streams::IndexWriteError;

/// A rejected append/finalization or failed output to an indexed log pair.
///
/// Record and finalization precondition errors leave the writer open. Output
/// failures retain the original source error and make the entire pair unusable;
/// subsequent operations return `Unusable` without performing more I/O.
#[derive(Debug, Error)]
pub enum IndexedLogWriteError {
    /// The record does not have the next ID assigned to this file.
    #[error("expected record {expected:?}, received {actual:?}")]
    UnexpectedRecordId {
        /// Required stream and sequence number.
        expected: RecordId,
        /// Identity of the rejected record.
        actual: RecordId,
    },
    /// All record positions assigned to the file have been buffered.
    #[error("indexed log is full")]
    Full,
    /// Finalization was requested before all assigned records were appended.
    #[error("cannot finalize an indexed log containing only {record_count} records")]
    NotFull {
        /// Number of records in the validated prefix plus buffered appends.
        record_count: u64,
    },
    /// The full pair has not completed durable synchronization.
    #[error("cannot finalize before both log and index are synchronized")]
    NotSynchronized,
    /// Finalization has already ended this writer's output lifecycle.
    #[error("indexed log is finalized")]
    Finalized,
    /// Earlier output did not complete; recovery is required before reuse.
    #[error("indexed log is unusable after incomplete or failed output")]
    Unusable,
    /// Buffer output, flushing or synchronization of the record data failed.
    #[error("log output failed: {0}")]
    Log(#[source] RecordOutputError),
    /// Writing, flushing or synchronization of the index failed.
    #[error("index output failed: {0}")]
    Index(#[source] IndexWriteError),
}

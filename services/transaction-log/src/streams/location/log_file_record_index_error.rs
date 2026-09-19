use thiserror::Error;

use super::LogFileId;

/// A zero-based record index lies outside the configured log-file range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "record index {index} exceeds maximum supported index {maximum_index} for log file {file_id:?}"
)]
#[cfg_attr(not(test), allow(dead_code))] // The record_id_at helper is currently used only by tests.
pub(crate) struct LogFileRecordIndexError {
    pub(super) file_id: LogFileId,
    pub(super) index: u64,
    pub(super) maximum_index: u64,
}

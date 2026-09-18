use super::LogTailError;
use crate::streams::index_writer::INDEX_ENTRY_LEN;
use crate::streams::{LogFileId, RECORDS_PER_FILE, RecordEndLocation};
use getset::{CopyGetters, Getters};

/// Immutable findings from a completed validation pass, before any repair.
///
/// The accepted prefix includes the caller's trusted starting prefix. A report
/// describes content, not durable synchronization or permission to serve a file.
/// Its original lengths and diagnostics remain unchanged after repairs.
#[derive(Debug, CopyGetters, Getters)]
pub struct LogValidationReport {
    /// File that was inspected.
    #[getset(get_copy = "pub")]
    pub(super) file_id: LogFileId,
    /// Last accepted record, or `None` for an empty valid prefix.
    #[getset(get_copy = "pub")]
    pub(super) end: Option<RecordEndLocation>,
    /// Trusted plus newly validated consecutive records.
    #[getset(get_copy = "pub")]
    pub(super) record_count: u64,
    /// Log byte length observed before scanning.
    #[getset(get_copy = "pub")]
    pub(super) log_length: u64,
    /// Index byte length observed before scanning, including any partial entry.
    #[getset(get_copy = "pub")]
    pub(super) index_length: u64,
    /// Checkpoint-certified entries plus consecutive matching scanned suffix entries.
    #[getset(get_copy = "pub")]
    pub(super) matching_index_entries: u64,
    /// First invalid record or extra-file suffix, if one was found.
    #[getset(get = "pub")]
    pub(super) tail_error: Option<LogTailError>,
}

impl LogValidationReport {
    /// Exclusive byte end of the valid log prefix; zero for an empty prefix.
    pub fn valid_log_length(&self) -> u64 {
        self.end.map_or(0, |end| end.position())
    }

    /// Whether all 100,000 assigned records are present in the accepted prefix.
    pub fn is_full(&self) -> bool {
        self.record_count == RECORDS_PER_FILE
    }

    /// Whether the original index needs replacement or truncation of its suffix.
    pub fn needs_index_repair(&self) -> bool {
        self.matching_index_entries != self.record_count
            || self.index_length != self.record_count * INDEX_ENTRY_LEN as u64
    }
}

use getset::CopyGetters;

use crate::streams::RecordEndLocation;

/// Snapshot of one successfully buffered log record and its index entry.
///
/// This reports acceptance into the writer's buffers, not flushing, durable
/// synchronization or checkpoint publication. Later appends do not change it.
/// Field access uses read-only getters; the pair writer constructs the result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct AppendOutcome {
    /// Accepted record and its exclusive byte end in this log file.
    pub(super) end: RecordEndLocation,
    /// Whether this accepted record filled the file's assigned record capacity.
    /// The owner must synchronize and finalize before switching to another pair.
    /// A later append to this writer is still rejected with `Full`.
    pub(super) file_full: bool,
}

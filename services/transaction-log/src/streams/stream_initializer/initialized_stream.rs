use getset::{CopyGetters, Getters};
use tokio::fs::File;

use crate::streams::{LogFileId, RecordEndLocation};

/// Initialization result owning one stream's active log/index pair.
///
/// `end` describes the returned file only. An empty file after a completed file
/// has no local endpoint even though the stream checkpoint covers earlier records.
/// Recovered records are synchronized, and handles are positioned for appending.
/// Newly created empty files are not explicitly synchronized; later writes follow
/// the stream owner's durability schedule. There is no public unchecked constructor.
/// Consume with [`crate::streams::IndexedLogWriter::from`] to prepare the writer.
/// Preserve contents, cursors and exclusive write ownership until that handover;
/// borrowed file getters do not authorize mutation of the initialized pair.
/// Fields are visible within `streams` for named ownership transfer. Stream
/// components constructing this value must establish the same guarantees.
#[derive(Debug, CopyGetters, Getters)]
pub struct InitializedStream {
    /// Identity of the returned files, including when no record exists in them.
    #[getset(get_copy = "pub")]
    pub(in crate::streams) file_id: LogFileId,
    /// Owned active log, positioned after its accepted records or at zero when empty.
    #[getset(get = "pub")]
    pub(in crate::streams) log: File,
    /// Owned active index, positioned after its accepted entries or at zero when empty.
    #[getset(get = "pub")]
    pub(in crate::streams) index: File,
    /// Last accepted record in this file, or `None` for an empty active pair.
    #[getset(get_copy = "pub")]
    pub(in crate::streams) end: Option<RecordEndLocation>,
}

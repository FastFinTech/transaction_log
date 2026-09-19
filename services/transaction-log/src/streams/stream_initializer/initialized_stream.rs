use getset::{CopyGetters, Getters};
use tokio::fs::File;

use crate::streams::{LogFileId, RecordEndLocation};

/// Planned initialization result owning one stream's active log/index pair.
///
/// `end` describes the returned file only. An empty file after a completed file
/// has no local endpoint even though the stream checkpoint covers earlier records.
/// The initializer will establish synchronized contents and append-positioned
/// handles before returning this value. There is no public unchecked constructor.
#[derive(Debug, CopyGetters, Getters)]
pub struct InitializedStream {
    /// Identity of the returned files, including when no record exists in them.
    #[getset(get_copy = "pub")]
    pub(super) file_id: LogFileId,
    /// Owned active log, positioned after its accepted records or at zero when empty.
    #[getset(get = "pub")]
    pub(super) log: File,
    /// Owned active index, positioned after its accepted entries or at zero when empty.
    #[getset(get = "pub")]
    pub(super) index: File,
    /// Last accepted record in this file, or `None` for an empty active pair.
    #[getset(get_copy = "pub")]
    pub(super) end: Option<RecordEndLocation>,
}

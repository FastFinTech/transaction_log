use crate::streams::{LogFileId, RecordEndLocation};
use getset::CopyGetters;

/// Completion metadata for a full validated and synchronized log/index pair.
///
/// Created only by successful validator handover. Both files have been closed;
/// the owner can publish this boundary and open the next file. This does not
/// publish a checkpoint, synchronize directory entries, rename files or create
/// a historical reader. It describes this recovery completion, not a guarantee
/// against subsequent external modification or deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct CompletedIndexedLog {
    /// The last assigned record and its exclusive log end.
    #[getset(get_copy = "pub")]
    end: RecordEndLocation,
}

impl CompletedIndexedLog {
    pub(super) fn new(end: RecordEndLocation) -> Self {
        Self { end }
    }

    /// Identity of the completed file, derived from its endpoint.
    pub fn file_id(&self) -> LogFileId {
        self.end.log_file_id()
    }
}

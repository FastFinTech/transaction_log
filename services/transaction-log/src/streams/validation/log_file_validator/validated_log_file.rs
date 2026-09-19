use getset::{CopyGetters, Getters};
use tokio::fs::File;

use crate::streams::{LogTailError, RecordEndLocation};

/// An owned log file after successful validation, tail removal and synchronization.
///
/// The original trusted prefix remains caller-certified. Newly accepted records
/// have passed encoding, CRC, stream and sequence checks. The index has not been
/// inspected or written, and no checkpoint or stream readiness has been published.
/// Only the validator can construct this result.
/// `F` is the concrete input file type, defaulting to [`tokio::fs::File`].
#[derive(Debug, CopyGetters, Getters)]
pub struct ValidatedLogFile<F = File> {
    /// Synchronized file whose ownership can be extracted with `into_inner`.
    pub(super) file: F,
    /// Last accepted record, including the trusted prefix; `None` for an empty log.
    #[getset(get_copy = "pub")]
    pub(super) validated_end: Option<RecordEndLocation>,
    /// Absolute exclusive ends of newly validated records, excluding trusted ones.
    pub(super) suffix_ends: Vec<u64>,
    /// Content violation whose tail was successfully removed, or `None` if clean.
    #[getset(get = "pub")]
    pub(super) removed_tail: Option<LogTailError>,
}

impl<F> ValidatedLogFile<F> {
    /// Offsets to pass to `IndexFileValidator::validate` using the same trusted start.
    ///
    /// Each offset is the absolute exclusive byte end of one newly accepted record.
    /// An empty slice means no new records were accepted; the trusted prefix may
    /// still be nonempty. Both validators receive the original checkpoint-certified
    /// endpoint; this result's final endpoint must not replace it in the index call.
    pub fn suffix_ends(&self) -> &[u64] {
        &self.suffix_ends
    }

    /// Returns the owned log file, discarding the findings without additional I/O.
    ///
    /// The cursor may reflect reader read-ahead. The caller must seek explicitly
    /// before reading or appending; extraction adds no index or checkpoint guarantee.
    pub fn into_inner(self) -> F {
        self.file
    }
}

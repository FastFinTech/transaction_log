use thiserror::Error;

use super::LogFileId;

/// Invalid endpoints for inclusive enumeration of log-file identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LogFileIdRangeError {
    /// The endpoints belong to different streams.
    #[error("log-file range endpoints belong to different streams: {first:?} through {last:?}")]
    DifferentStreams {
        /// Requested first file.
        first: LogFileId,
        /// Requested last file.
        last: LogFileId,
    },
    /// The last file precedes the first within the same stream.
    #[error("log-file range is reversed: {first:?} through {last:?}")]
    ReversedFiles {
        /// Requested first file.
        first: LogFileId,
        /// Requested last file.
        last: LogFileId,
    },
}

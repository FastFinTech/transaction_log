use thiserror::Error;
use transaction_log_exports::{SequenceNumber, StreamId};

use super::LogFileId;

/// Inconsistent endpoints supplied to [`super::RecordRangeLocation::new`].
///
/// These errors describe the range representation. File/index I/O, actual
/// record validation and agreement with readable storage are separate concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecordRangeLocationError {
    /// Range endpoints belong to different logical streams.
    #[error("record range starts in stream {start_stream_id} and ends in stream {end_stream_id}")]
    DifferentStreams {
        /// Stream containing the requested first record.
        start_stream_id: StreamId,
        /// Stream containing the requested last record.
        end_stream_id: StreamId,
    },
    /// The requested last sequence precedes the first sequence.
    #[error(
        "record range starts at sequence {start_sequence_number} after its end {end_sequence_number}"
    )]
    ReversedRecords {
        /// Requested first sequence.
        start_sequence_number: SequenceNumber,
        /// Requested last sequence.
        end_sequence_number: SequenceNumber,
    },
    /// The last file's known byte end is at or before its known byte start.
    #[error(
        "record range has an empty or reversed byte span {start_position}..{end_position} in {log_file_id:?}"
    )]
    InvalidByteSpan {
        /// File containing the invalid bounded span or prefix.
        log_file_id: LogFileId,
        /// Supplied start for a single file, or zero for a later file's prefix.
        start_position: u64,
        /// Supplied exclusive end in this same file.
        end_position: u64,
    },
}

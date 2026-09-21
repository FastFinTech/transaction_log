use thiserror::Error;
use transaction_log_exports::{SequenceNumber, StreamId};

use super::{LogFileId, LogFilePosition};

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
        "record range has an empty or reversed byte span {}..{} in {log_file_id:?}",
        .start_position.get(), .end_position.get()
    )]
    InvalidByteSpan {
        /// File containing the invalid bounded span or prefix.
        log_file_id: LogFileId,
        /// Supplied start for a single file, or zero for a later file's prefix.
        start_position: LogFilePosition,
        /// Supplied exclusive end in this same file.
        end_position: LogFilePosition,
    },
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::StreamId;

    use super::{LogFileId, LogFilePosition, RecordRangeLocationError};

    #[test]
    fn invalid_byte_span_displays_offsets_as_full_width_numbers() {
        let file = LogFileId::first(StreamId::MIN);
        let error = RecordRangeLocationError::InvalidByteSpan {
            log_file_id: file,
            start_position: LogFilePosition::new(u64::MAX),
            end_position: LogFilePosition::new(4_294_967_296),
        };

        assert_eq!(
            error.to_string(),
            format!(
                "record range has an empty or reversed byte span 18446744073709551615..4294967296 in {file:?}"
            )
        );
    }
}

use getset::CopyGetters;
use transaction_log_exports::RecordId;

use super::LogFileId;

/// The exclusive byte end of a record within its assigned log file.
///
/// `record_id` identifies the last included record, and `position` is immediately
/// after its complete encoding, including the CRC. It is the value of that
/// record's dense-index entry. It can precede physical EOF when newer records
/// follow, and it stays in this record's file even at a file boundary.
///
/// This immutable metadata value stores a resolver's result without inspecting
/// storage. It establishes no record validity, file existence or durability and
/// is not a stream checkpoint. Pair it with [`super::RecordStartLocation`] to
/// construct a [`super::RecordRangeLocation`]. The distinct endpoint types prevent
/// accidentally swapping their roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct RecordEndLocation {
    /// Record whose complete encoding ends at this exclusive position.
    record_id: RecordId,
    /// Exclusive byte offset from the beginning of that record's log file.
    position: u64,
}

impl RecordEndLocation {
    /// Stores the supplied identity and position without validation or file I/O.
    ///
    /// The file/index resolver must establish that this is the actual end of
    /// the record. The model does not substitute partial numeric validation for
    /// reading storage. [`super::RecordRangeLocation::new`] checks this endpoint's
    /// relationship to a start, including rejecting a zero end position.
    pub const fn new(record_id: RecordId, position: u64) -> Self {
        Self {
            record_id,
            position,
        }
    }

    /// Returns the file containing this record and its exclusive end position.
    ///
    /// It does not advance to the next record or file, so the helper remains
    /// usable at the maximum sequence number. No file identity is stored twice.
    pub fn log_file_id(self) -> LogFileId {
        LogFileId::from_record_id(self.record_id)
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{SequenceNumber, StreamId};

    use super::{RecordEndLocation, RecordId};

    #[test]
    fn exclusive_ends_stay_in_the_last_included_records_file() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, position, file_number) in [
                (0, 16, 0),
                // Completing file zero does not move its endpoint into file one.
                (99_999, 6_553_500_000, 0),
                (100_000, 16, 1),
                (u64::MAX, 3_382_654_560, 184_467_440_737_095),
            ] {
                let record_id = RecordId::new(stream_id, SequenceNumber::new(sequence));
                let location = RecordEndLocation::new(record_id, position);

                assert_eq!(location.record_id(), record_id);
                assert_eq!(location.position(), position);
                assert_eq!(location.log_file_id().stream_id(), stream_id);
                assert_eq!(location.log_file_id().file_number().get(), file_number);
            }
        }
    }
}

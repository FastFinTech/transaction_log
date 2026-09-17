use getset::CopyGetters;
use transaction_log_exports::RecordId;

use super::LogFileId;

/// The inclusive byte start of a record within its assigned log file.
///
/// `record_id` identifies the record that starts here, even when its position
/// was resolved from the preceding record's dense-index entry. The first record
/// in a file starts at zero; positions never carry over from the preceding file.
///
/// This immutable metadata value stores a resolver's result without inspecting
/// storage. It establishes no record validity, file existence or durability.
/// Pair it with [`super::RecordEndLocation`] to construct a
/// [`super::RecordRangeLocation`], which checks the relationship between endpoints.
/// The distinct endpoint types prevent accidentally swapping their roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct RecordStartLocation {
    /// Record whose first encoded byte is at this position.
    record_id: RecordId,
    /// Inclusive byte offset from the beginning of that record's log file.
    position: u64,
}

impl RecordStartLocation {
    /// Stores the supplied identity and position without validation or file I/O.
    ///
    /// The file/index resolver must establish that the position is the actual
    /// start of this record. A raw offset and ID alone cannot establish that
    /// relationship. This constructor does not substitute partial numeric checks
    /// for reading storage, or repeat validation on every metadata access.
    pub const fn new(record_id: RecordId, position: u64) -> Self {
        Self {
            record_id,
            position,
        }
    }

    /// Returns the file containing this record and its inclusive start position.
    ///
    /// The file identity is derived from the record ID rather than stored twice.
    /// No index lookup or offset arithmetic is performed.
    pub fn log_file_id(self) -> LogFileId {
        LogFileId::from_record_id(self.record_id)
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{SequenceNumber, StreamId};

    use super::{RecordId, RecordStartLocation};

    #[test]
    fn starts_keep_the_target_identity_across_file_and_numeric_boundaries() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, position, file_number) in [
                (0, 0, 0),
                (99_999, 6_553_434_465, 0),
                // The preceding record ends in file zero; this start belongs to one.
                (100_000, 0, 1),
                (u64::MAX, 3_382_589_025, 184_467_440_737_095),
            ] {
                let record_id = RecordId::new(stream_id, SequenceNumber::new(sequence));
                let location = RecordStartLocation::new(record_id, position);

                assert_eq!(location.record_id(), record_id);
                assert_eq!(location.position(), position);
                assert_eq!(location.log_file_id().stream_id(), stream_id);
                assert_eq!(location.log_file_id().file_number().get(), file_number);
            }
        }
    }
}

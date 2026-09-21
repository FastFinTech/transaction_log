use getset::CopyGetters;
use serde::{Deserialize, Serialize};
use transaction_log_exports::{RecordId, RecordLength};

use super::{LogFileId, RecordEndLocation};

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
///
/// Serde uses named `record_id` and `position` fields. Deserialization validates
/// the nested identifiers and the `u64` position, and rejects missing, duplicate
/// or unknown object fields. It does not verify the position against storage.
/// The endpoint's role comes from its Rust type or enclosing field, not a type
/// tag in the serialized data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters, Serialize, Deserialize)]
#[getset(get_copy = "pub")]
#[serde(deny_unknown_fields)]
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

    /// Derives this record's exclusive end from its total encoded length.
    ///
    /// Adds the header, payload and CRC length to this inclusive start position.
    /// The record ID and assigned file stay the same, including for the last
    /// record in a file. Advancing to another record is a separate operation.
    ///
    /// The caller must supply this record's length. This computes metadata only;
    /// it does not inspect storage or establish record validity or durability.
    /// The length's protocol bounds are already established by [`RecordLength`].
    ///
    /// # Panics
    ///
    /// Panics if the resulting byte position exceeds `u64::MAX`, in debug and
    /// release. A valid resolved log-file position cannot overflow this way.
    #[inline]
    pub const fn to_end(self, record_length: RecordLength) -> RecordEndLocation {
        let position = self
            .position
            .checked_add(record_length.get())
            .expect("record end position exceeds u64::MAX");
        RecordEndLocation::new(self.record_id, position)
    }

    /// Derives the next record's inclusive start using this record's encoded length.
    ///
    /// Advances the record ID and adds the length within the same file. When the
    /// next record belongs to another file, its start position is zero.
    /// The caller supplies this record's length, not the next record's length.
    /// This derives metadata only and does not prove either record exists.
    ///
    /// Delegates to [`Self::to_end`] and [`RecordEndLocation::next_record_start`]
    /// so byte arithmetic and file rotation retain their existing contracts.
    ///
    /// # Panics
    ///
    /// Panics on byte-position overflow or sequence exhaustion through those
    /// helpers, in both debug and release.
    #[inline]
    pub fn to_next(self, record_length: RecordLength) -> Self {
        self.to_end(record_length).next_record_start()
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{SequenceNumber, StreamId};

    use super::{RecordId, RecordLength, RecordStartLocation};

    #[test]
    fn to_end_adds_the_encoded_length_and_keeps_the_same_record_and_file() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, start_position, length, end_position, file_number) in [
                (0, 0, RecordLength::MIN, 16, 0),
                (0, 0, RecordLength::MAX, 65_535, 0),
                (42, 100, RecordLength::new(21).unwrap(), 121, 0),
                (99_999, 6_553_434_465, RecordLength::MAX, 6_553_500_000, 0),
                (100_000, 0, RecordLength::MIN, 16, 1),
            ] {
                let record_id = RecordId::new(stream_id, SequenceNumber::new(sequence));
                let end = RecordStartLocation::new(record_id, start_position).to_end(length);

                assert_eq!(end.record_id(), record_id);
                assert_eq!(end.position(), end_position);
                assert_eq!(end.log_file_id().stream_id(), stream_id);
                assert_eq!(end.log_file_id().file_number().get(), file_number);
            }
        }
    }

    #[test]
    fn to_end_rejects_byte_position_overflow_without_wrapping() {
        // Raw metadata can contain offsets that no real log file can reach.
        let record_id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
        let start = RecordStartLocation::new(record_id, u64::MAX - 16);
        assert_eq!(start.to_end(RecordLength::MIN).position(), u64::MAX);

        for position in [u64::MAX - 15, u64::MAX] {
            let start = RecordStartLocation::new(record_id, position);
            assert!(std::panic::catch_unwind(|| start.to_end(RecordLength::MIN)).is_err());
        }
    }

    #[test]
    fn to_next_advances_the_record_and_resets_the_position_at_file_rotation() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, position, length, next_sequence, next_position) in [
                (0, 0, RecordLength::MIN, 1, 16),
                (0, 0, RecordLength::MAX, 1, 65_535),
                (42, 100, RecordLength::new(21).unwrap(), 43, 121),
                (
                    99_998,
                    6_553_368_930,
                    RecordLength::MAX,
                    99_999,
                    6_553_434_465,
                ),
                (99_999, 6_553_434_465, RecordLength::MAX, 100_000, 0),
                (100_000, 0, RecordLength::MIN, 100_001, 16),
            ] {
                let start = RecordStartLocation::new(
                    RecordId::new(stream_id, SequenceNumber::new(sequence)),
                    position,
                );
                let expected = RecordStartLocation::new(
                    RecordId::new(stream_id, SequenceNumber::new(next_sequence)),
                    next_position,
                );

                assert_eq!(start.to_next(length), expected);
            }
        }
    }

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

    #[test]
    fn json_uses_named_fields_and_round_trips_through_io() {
        let json =
            r#"{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553434465}"#;
        let location = RecordStartLocation::new(
            RecordId::new(StreamId::new(42).unwrap(), SequenceNumber::new(99_999)),
            6_553_434_465,
        );
        let mut bytes = Vec::new();
        serde_json::to_writer(&mut bytes, &location).unwrap();
        assert_eq!(bytes, json.as_bytes());
        assert_eq!(
            serde_json::from_reader::<_, RecordStartLocation>(bytes.as_slice()).unwrap(),
            location
        );
    }

    #[test]
    fn json_preserves_numeric_limits_without_claiming_storage_validity() {
        // These are metadata limits, not a claim that such offsets exist in a file.
        for (json, record_id, position) in [
            (
                r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":0}"#,
                RecordId::new(StreamId::MIN, SequenceNumber::MIN),
                0,
            ),
            (
                r#"{"record_id":{"stream_id":4095,"sequence_number":18446744073709551615},"position":18446744073709551615}"#,
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
                u64::MAX,
            ),
        ] {
            let location = RecordStartLocation::new(record_id, position);
            assert_eq!(serde_json::to_string(&location).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<RecordStartLocation>(json).unwrap(),
                location
            );
        }
    }

    #[test]
    fn json_rejects_incomplete_ambiguous_and_invalid_metadata() {
        for json in [
            r#"{}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0}}"#,
            r#"{"position":0}"#,
            r#"{"record_id":null,"position":0}"#,
            r#"{"record_id":{"stream_id":4096,"sequence_number":0},"position":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":-1},"position":0}"#,
            r#"{"record_id":{"stream_id":0},"position":0}"#,
            r#"{"record_id":{"stream_id":0,"stream_id":1,"sequence_number":0},"position":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":-1}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":1.5}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":18446744073709551616}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":null}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":"0"}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":0,"position":16}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"record_id":{"stream_id":1,"sequence_number":0},"position":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0,"extra":0},"position":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":0,"extra":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":0"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":0} trailing"#,
        ] {
            assert!(
                serde_json::from_str::<RecordStartLocation>(json).is_err(),
                "{json}"
            );
        }
    }
}

use getset::CopyGetters;
use serde::{Deserialize, Serialize};
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
///
/// Serde uses named `record_id` and `position` fields. Deserialization validates
/// the nested identifiers and the `u64` position, and rejects missing, duplicate
/// or unknown object fields. It does not verify the position against storage.
/// The endpoint's role comes from its Rust type or enclosing field, not a type
/// tag in the serialized data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters, Serialize, Deserialize)]
#[getset(get_copy = "pub")]
#[serde(deny_unknown_fields)]
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

    #[test]
    fn json_uses_named_fields_and_round_trips_through_io() {
        let json =
            r#"{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553500000}"#;
        let location = RecordEndLocation::new(
            RecordId::new(StreamId::new(42).unwrap(), SequenceNumber::new(99_999)),
            6_553_500_000,
        );
        let mut bytes = Vec::new();
        serde_json::to_writer(&mut bytes, &location).unwrap();
        assert_eq!(bytes, json.as_bytes());
        assert_eq!(
            serde_json::from_reader::<_, RecordEndLocation>(bytes.as_slice()).unwrap(),
            location
        );
    }

    #[test]
    fn json_preserves_numeric_limits_without_claiming_storage_validity() {
        // Zero cannot be a real record end; accepting it here leaves storage
        // validation to the resolver, just as the metadata constructor does.
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
            let location = RecordEndLocation::new(record_id, position);
            assert_eq!(serde_json::to_string(&location).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<RecordEndLocation>(json).unwrap(),
                location
            );
        }
    }

    #[test]
    fn json_rejects_incomplete_ambiguous_and_invalid_metadata() {
        for json in [
            r#"{}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0}}"#,
            r#"{"position":16}"#,
            r#"{"record_id":null,"position":16}"#,
            r#"{"record_id":{"stream_id":4096,"sequence_number":0},"position":16}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":-1},"position":16}"#,
            r#"{"record_id":{"stream_id":0},"position":16}"#,
            r#"{"record_id":{"stream_id":0,"stream_id":1,"sequence_number":0},"position":16}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":-1}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":1.5}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":18446744073709551616}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":null}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":"16"}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":16,"position":32}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"record_id":{"stream_id":1,"sequence_number":0},"position":16}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0,"extra":0},"position":16}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":16,"extra":0}"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":16"#,
            r#"{"record_id":{"stream_id":0,"sequence_number":0},"position":16} trailing"#,
        ] {
            assert!(
                serde_json::from_str::<RecordEndLocation>(json).is_err(),
                "{json}"
            );
        }
    }
}

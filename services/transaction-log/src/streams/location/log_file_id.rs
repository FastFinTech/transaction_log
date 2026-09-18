use getset::CopyGetters;
use serde::{Deserialize, Serialize};
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

use super::{LogFileNumber, RECORDS_PER_FILE};

/// Identifies one stream's log file by its zero-based sequence range.
///
/// Fields are read-only. A record maps to file number
/// `sequence_number / RECORDS_PER_FILE`, independently of record byte lengths.
/// File numbers cannot exceed [`LogFileNumber::MAX`]. Derived ordering compares
/// stream ID first and then file number, without implying cross-stream chronology.
///
/// Range helpers describe assigned positions, not records known to exist. This
/// value performs no I/O and establishes neither file existence nor completeness.
/// Its native Rust layout is not a serialized filename or on-disk format.
/// Serde represents it with named `stream_id` and `file_number` integer fields.
/// Deserialization validates both domains and rejects missing, duplicate or
/// unknown fields. JSON and compatible binary formats share that validation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, CopyGetters, Serialize, Deserialize,
)]
#[getset(get_copy = "pub")]
#[serde(deny_unknown_fields)]
pub struct LogFileId {
    /// Logical stream whose records belong to this file.
    stream_id: StreamId,
    /// Zero-based file number within the stream, bounded by the sequence domain.
    file_number: LogFileNumber,
}

impl LogFileId {
    /// Combines already validated identifiers without further validation or I/O.
    ///
    /// Filesystem existence and file contents remain storage responsibilities.
    pub const fn new(stream_id: StreamId, file_number: LogFileNumber) -> Self {
        Self {
            stream_id,
            file_number,
        }
    }

    /// Finds the file range assigned to a record, without I/O or validation.
    ///
    /// Every representable record ID maps to a valid file ID. This does not prove
    /// that the record has been stored, nor that earlier records are present.
    #[inline]
    pub fn from_record_id(record_id: RecordId) -> Self {
        Self::new(
            record_id.stream_id(),
            LogFileNumber::from_sequence_number(record_id.sequence_number()),
        )
    }

    /// Returns the first record ID assigned to this file's range.
    ///
    /// The validated file-number bound guarantees that multiplication fits in
    /// `u64`; getters need not repeat the constructor's range check.
    #[inline]
    pub const fn first_record_id(self) -> RecordId {
        RecordId::new(
            self.stream_id,
            SequenceNumber::new(self.file_number.get() * RECORDS_PER_FILE),
        )
    }

    /// Returns the last representable record ID assigned to this file's range.
    ///
    /// Ordinary files end 99,999 positions after their first ID. The final range
    /// ends at [`SequenceNumber::MAX`] and contains only 51,616 positions. This
    /// numeric endpoint does not declare a partially filled file complete/sealed.
    #[inline]
    pub const fn last_record_id(self) -> RecordId {
        let first = self.file_number.get() * RECORDS_PER_FILE;
        // Only the terminal range reaches the sequence-domain boundary. Capping
        // that endpoint preserves a usable range for every valid RecordId.
        let last = first.saturating_add(RECORDS_PER_FILE - 1);
        RecordId::new(self.stream_id, SequenceNumber::new(last))
    }

    /// Returns the next file ID in the same stream, leaving this value unchanged.
    ///
    /// This does not create a file, seal the current file, or coordinate writers.
    ///
    /// # Panics
    ///
    /// Panics at [`LogFileNumber::MAX`] in both debug and release builds.
    /// There is no successor range containing any representable sequence number.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self::new(self.stream_id, self.file_number.next())
    }
}

#[cfg(test)]
mod tests {
    use super::{LogFileId, LogFileNumber, RecordId, SequenceNumber, StreamId};

    #[test]
    fn json_preserves_named_fields_and_domain_endpoints() {
        for (json, stream_id, file_number) in [
            (r#"{"stream_id":0,"file_number":0}"#, StreamId::MIN, 0),
            (
                r#"{"stream_id":4095,"file_number":184467440737095}"#,
                StreamId::MAX,
                184_467_440_737_095,
            ),
        ] {
            let id = LogFileId::new(stream_id, LogFileNumber::new(file_number).unwrap());
            assert_eq!(serde_json::to_string(&id).unwrap(), json);
            assert_eq!(serde_json::from_str::<LogFileId>(json).unwrap(), id);
        }
    }

    #[test]
    fn json_rejects_out_of_domain_values_through_existing_validation() {
        for number in [184_467_440_737_096, u64::MAX] {
            let json = format!(r#"{{"stream_id":0,"file_number":{number}}}"#);
            let error = serde_json::from_str::<LogFileId>(&json).unwrap_err();
            let expected = LogFileNumber::new(number).unwrap_err();
            assert!(error.to_string().contains(&expected.to_string()));
        }
        let error =
            serde_json::from_str::<LogFileId>(r#"{"stream_id":4096,"file_number":0}"#).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&StreamId::new(4096).unwrap_err().to_string())
        );
    }

    #[test]
    fn json_rejects_missing_duplicate_unknown_and_mistyped_fields() {
        for json in [
            r#"{"stream_id":0}"#,
            r#"{"file_number":0}"#,
            r#"{"stream_id":0,"stream_id":1,"file_number":0}"#,
            r#"{"stream_id":0,"file_number":0,"file_number":1}"#,
            r#"{"stream_id":0,"file_number":0,"extra":0}"#,
            r#"{"stream_id":0,"file_number":-1}"#,
            r#"{"stream_id":0,"file_number":1.5}"#,
            r#"{"stream_id":0,"file_number":18446744073709551616}"#,
            r#"{"stream_id":0,"file_number":null}"#,
            r#"{"stream_id":0,"file_number":"0"}"#,
        ] {
            assert!(serde_json::from_str::<LogFileId>(json).is_err(), "{json}");
        }
    }

    #[test]
    fn maps_record_ids_across_file_and_integer_boundaries() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            for (sequence, file_number) in [
                (0, 0),
                (99_999, 0),
                (100_000, 1),
                (199_999, 1),
                (200_000, 2),
                (4_294_967_295, 42_949),
                (4_294_967_296, 42_949),
                (18_446_744_073_709_499_999, 184_467_440_737_094),
                (18_446_744_073_709_500_000, 184_467_440_737_095),
                (u64::MAX, 184_467_440_737_095),
            ] {
                let record = RecordId::new(stream, SequenceNumber::new(sequence));
                let file = LogFileId::from_record_id(record);
                assert_eq!(file.stream_id(), stream);
                assert_eq!(file.file_number().get(), file_number);
            }
        }
    }

    #[test]
    fn range_endpoints_match_assigned_sequences() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            for (number, first, last) in [
                (0, 0, 99_999),
                (1, 100_000, 199_999),
                (
                    184_467_440_737_094,
                    18_446_744_073_709_400_000,
                    18_446_744_073_709_499_999,
                ),
                (184_467_440_737_095, 18_446_744_073_709_500_000, u64::MAX),
            ] {
                let file = LogFileId::new(stream, LogFileNumber::new(number).unwrap());
                assert_eq!(
                    file.first_record_id(),
                    RecordId::new(stream, SequenceNumber::new(first))
                );
                assert_eq!(
                    file.last_record_id(),
                    RecordId::new(stream, SequenceNumber::new(last))
                );
            }
        }
    }

    #[test]
    fn successor_preserves_stream_and_produces_adjacent_ranges() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            for number in [0, 1, 184_467_440_737_094] {
                let file = LogFileId::new(stream, LogFileNumber::new(number).unwrap());
                let next = file.next();
                assert_eq!(next.stream_id(), stream);
                assert_eq!(next.file_number().get(), number + 1);
                assert_eq!(next.first_record_id(), file.last_record_id().next());
            }
        }
    }

    #[test]
    #[should_panic(expected = "log file number overflow")]
    fn successor_panics_when_no_sequence_range_remains() {
        let file = LogFileId::from_record_id(RecordId::new(StreamId::MIN, SequenceNumber::MAX));
        let _ = file.next();
    }
}

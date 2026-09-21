use std::iter::FusedIterator;

use getset::CopyGetters;
use serde::{Deserialize, Serialize};
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

use super::{LogFileIdRangeError, LogFileNumber, LogFileRecordIndexError, RECORDS_PER_FILE};

/// Identifies one stream's log file by its zero-based sequence range.
///
/// Fields are read-only. A record maps to file number
/// `sequence_number / RECORDS_PER_FILE`, independently of record byte lengths.
/// File numbers cannot exceed [`LogFileNumber::MAX`]. File IDs have no ordering
/// traits: compare file numbers only after establishing that streams match.
///
/// Range helpers describe assigned positions, not records known to exist. This
/// value performs no I/O and establishes neither file existence nor completeness.
/// Its native Rust layout is not a serialized filename or on-disk format.
/// Serde represents it with named `stream_id` and `file_number` integer fields.
/// Deserialization validates both domains and rejects missing, duplicate or
/// unknown fields. JSON and compatible binary formats share that validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, CopyGetters, Serialize, Deserialize)]
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

    /// Returns file zero for the given stream, whose first assigned sequence is zero.
    ///
    /// This constructs a logical identity; it does not search for the first existing file.
    pub const fn first(stream_id: StreamId) -> Self {
        Self::new(stream_id, LogFileNumber::MIN)
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

    /// Counts assigned record positions from the beginning of the record's file
    /// through `record_id`, including `record_id` itself.
    ///
    /// This is an associated function because it answers a property of the
    /// record's naturally assigned file; it does not compare the record with a
    /// caller-supplied [`LogFileId`]. Every valid [`RecordId`] maps to exactly one
    /// file, so the operation is infallible and needs no mismatch error.
    ///
    /// The count is one-based. Sequence zero returns `1`, sequence 99,999 returns
    /// `100_000`, and sequence 100,000 returns `1` for the next file. Formally it
    /// is `sequence_number % RECORDS_PER_FILE + 1`. The terminal numeric file is
    /// shorter than an ordinary file, so [`SequenceNumber::MAX`] returns `51_616`.
    ///
    /// This value is neither the stream-wide sequence number, a zero-based record
    /// index, nor a byte position. It describes the range position assigned by
    /// the storage layout; it does not prove that the record or file exists, that
    /// preceding records are present, or that any bytes have been validated or
    /// made durable.
    #[inline]
    pub(crate) fn record_count_through(record_id: RecordId) -> u64 {
        record_id.sequence_number().get() % RECORDS_PER_FILE + 1
    }

    /// Returns the record ID at a zero-based position in this file's assigned range.
    ///
    /// The file supplies the stream ID and the first assigned sequence number.
    /// Index zero therefore returns [`Self::first_record_id`]. In an ordinary
    /// file, index 99,999 returns [`Self::last_record_id`]. This method describes
    /// assigned identities only; it performs no I/O and does not prove that the
    /// file or record exists, that preceding records are present, or that any
    /// bytes have been validated or made durable.
    ///
    /// The index is checked against the configured 100,000-position file range
    /// before sequence addition, so an invalid index cannot silently select a
    /// record in the following ordinary file. Sequence-number exhaustion follows
    /// the application's existing panic policy.
    ///
    /// # Errors
    ///
    /// Returns [`LogFileRecordIndexError`] when `index` is not below
    /// [`RECORDS_PER_FILE`].
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))] // Currently used only by location and validation tests.
    pub(crate) fn record_id_at(self, index: u64) -> Result<RecordId, LogFileRecordIndexError> {
        let maximum_index = RECORDS_PER_FILE - 1;
        if index > maximum_index {
            return Err(LogFileRecordIndexError {
                file_id: self,
                index,
                maximum_index,
            });
        }
        let first = self.first_record_id().sequence_number().get();
        let sequence = first
            .checked_add(index)
            .expect("record sequence number exhausted");
        Ok(RecordId::new(self.stream_id, SequenceNumber::new(sequence)))
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

    /// Enumerates this file through `last`, including both endpoints.
    ///
    /// The iterator owns its bounds, allocates nothing and performs no I/O.
    /// It stops before requesting the final file's successor, including at
    /// [`LogFileNumber::MAX`], and remains exhausted after the last item.
    /// Enumerated IDs do not establish that the files exist.
    ///
    /// # Errors
    ///
    /// Returns [`LogFileIdRangeError`] for different streams or reversed file bounds.
    pub fn iter_to(
        self,
        last: Self,
    ) -> Result<impl FusedIterator<Item = Self>, LogFileIdRangeError> {
        if self.stream_id != last.stream_id {
            return Err(LogFileIdRangeError::DifferentStreams { first: self, last });
        }
        if self.file_number > last.file_number {
            return Err(LogFileIdRangeError::ReversedFiles { first: self, last });
        }
        Ok(std::iter::successors(Some(self), move |file| {
            if *file == last {
                None
            } else {
                Some(file.next())
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{LogFileId, LogFileNumber, RecordId, SequenceNumber, StreamId};

    #[test]
    fn iteration_is_inclusive_and_preserves_the_stream() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            let first = LogFileId::new(stream, LogFileNumber::new(999).unwrap());
            let last = LogFileId::new(stream, LogFileNumber::new(1001).unwrap());
            let ids = first.iter_to(last).unwrap().collect::<Vec<_>>();
            assert_eq!(
                ids.iter()
                    .map(|id| id.file_number().get())
                    .collect::<Vec<_>>(),
                [999, 1000, 1001]
            );
            assert!(ids.iter().all(|id| id.stream_id() == stream));
            assert_eq!(first.iter_to(first).unwrap().collect::<Vec<_>>(), [first]);
        }
    }

    #[test]
    fn iteration_is_lazy_and_stops_without_advancing_the_terminal_file() {
        let first = LogFileId::new(StreamId::MAX, LogFileNumber::MIN);
        let last = LogFileId::new(StreamId::MAX, LogFileNumber::MAX);
        assert_eq!(
            first
                .iter_to(last)
                .unwrap()
                .take(3)
                .map(|id| id.file_number().get())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let penultimate = LogFileId::new(
            StreamId::MAX,
            LogFileNumber::new(LogFileNumber::MAX.get() - 1).unwrap(),
        );
        let mut iterator = penultimate.iter_to(last).unwrap();
        assert_eq!(iterator.next(), Some(penultimate));
        assert_eq!(iterator.next(), Some(last));
        assert_eq!(iterator.next(), None);
        assert_eq!(iterator.next(), None);
        assert_eq!(last.iter_to(last).unwrap().collect::<Vec<_>>(), [last]);
    }

    #[test]
    fn iteration_rejects_different_streams_and_reversed_files() {
        use super::LogFileIdRangeError;

        let first = LogFileId::new(StreamId::MIN, LogFileNumber::new(2).unwrap());
        let other_stream = LogFileId::new(StreamId::MAX, LogFileNumber::new(3).unwrap());
        assert!(
            matches!(first.iter_to(other_stream), Err(LogFileIdRangeError::DifferentStreams { first: actual_first, last }) if actual_first == first && last == other_stream)
        );
        let earlier = LogFileId::new(StreamId::MIN, LogFileNumber::new(1).unwrap());
        assert!(
            matches!(first.iter_to(earlier), Err(LogFileIdRangeError::ReversedFiles { first: actual_first, last }) if actual_first == first && last == earlier)
        );
    }

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
    fn counts_inclusively_within_the_naturally_assigned_file() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            for (sequence, count) in [
                (0, 1),
                (1, 2),
                (99_998, 99_999),
                (99_999, 100_000),
                (100_000, 1),
                (100_001, 2),
                (199_999, 100_000),
                (200_000, 1),
                (18_446_744_073_709_499_999, 100_000),
                (18_446_744_073_709_500_000, 1),
                (u64::MAX, 51_616),
            ] {
                let record_id = RecordId::new(stream, SequenceNumber::new(sequence));
                assert_eq!(LogFileId::record_count_through(record_id), count);
            }
        }
    }

    #[test]
    fn locates_zero_based_record_ids_and_rejects_indexes_outside_each_file() {
        for stream in [StreamId::MIN, StreamId::MAX] {
            let ordinary = LogFileId::new(stream, LogFileNumber::new(1).unwrap());
            for (index, sequence) in [(0, 100_000), (1, 100_001), (99_999, 199_999)] {
                assert_eq!(
                    ordinary.record_id_at(index).unwrap(),
                    RecordId::new(stream, SequenceNumber::new(sequence))
                );
            }
            for index in [100_000, u64::MAX] {
                let error = ordinary.record_id_at(index).unwrap_err();
                assert_eq!(error.file_id, ordinary);
                assert_eq!(error.index, index);
                assert_eq!(error.maximum_index, 99_999);
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

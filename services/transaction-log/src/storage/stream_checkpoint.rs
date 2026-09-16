use getset::CopyGetters;
use serde::{Deserialize, Serialize};
use transaction_log_exports::RecordId;

use super::LogFileId;

/// An immutable snapshot of the end of one stream's checkpointed record prefix.
///
/// The last record ID identifies both the stream and the log file containing the
/// endpoint. The end position is a byte offset within that file, immediately after
/// the complete encoded record, including its CRC. It is not an index-file offset
/// or a byte count across the whole stream.
///
/// A checkpoint published for recovery must describe a contiguous, validated and
/// durable prefix on this replica. This model stores that boundary; constructing
/// it does not validate records, inspect files, synchronize data or persist anything.
/// Those guarantees belong to the future checkpoint/recovery owner.
///
/// Use `Option<StreamCheckpoint>` when no checkpoint exists. Sequence zero is a
/// valid record ID, not an empty-stream sentinel.
///
/// Serde supports human-readable JSON with `last_record_id` (a nested object with
/// integer `stream_id` and `sequence_number`) and integer `end_position` fields.
/// Deserialization checks the stream ID's domain and numeric types, and rejects
/// missing, duplicate or unknown fields. These metadata checks do not establish
/// that the checkpoint agrees with storage or is safe to use for recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters, Serialize, Deserialize)]
#[getset(get_copy = "pub")]
#[serde(deny_unknown_fields)]
pub struct StreamCheckpoint {
    /// Last complete record included in the checkpoint, including its stream ID.
    last_record_id: RecordId,
    /// Exclusive byte offset after that record in the file from [`Self::log_file_id`].
    ///
    /// This may precede the file's physical length when an uncheckpointed tail
    /// follows it. A `u64` preserves positions beyond 4 GiB on every target.
    end_position: u64,
}

impl StreamCheckpoint {
    /// Stores a checkpoint boundary supplied by its owner without performing I/O.
    ///
    /// The caller supplies the actual end offset of `last_record_id` in its log
    /// file. The model cannot establish that relationship or the prefix's validity
    /// and durability from these two values alone, so construction performs no
    /// partial validation. Persistence and validation of loaded checkpoints are
    /// separate responsibilities, not implemented by this scaffold.
    pub const fn new(last_record_id: RecordId, end_position: u64) -> Self {
        Self {
            last_record_id,
            end_position,
        }
    }

    /// Returns the log file containing the checkpoint's last record and end position.
    ///
    /// At a file boundary this still identifies the completed file, not its
    /// successor. No increment is needed, including at the maximum sequence number.
    pub fn log_file_id(self) -> LogFileId {
        LogFileId::from_record_id(self.last_record_id)
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{SequenceNumber, StreamId};

    use super::{RecordId, StreamCheckpoint};

    #[test]
    fn endpoints_stay_with_their_record_file_across_rotation_and_sequence_limits() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, end_position, file_number) in [
                (0, 16, 0),
                // 100,000 maximum-size records exceed a u32 file offset.
                (99_999, 6_553_500_000, 0),
                (100_000, 16, 1),
                // The terminal range has 51,616 positions; no successor is needed.
                (u64::MAX, 825_856, 184_467_440_737_095),
            ] {
                let last_record_id = RecordId::new(stream_id, SequenceNumber::new(sequence));
                let checkpoint = StreamCheckpoint::new(last_record_id, end_position);

                assert_eq!(checkpoint.last_record_id(), last_record_id);
                assert_eq!(checkpoint.end_position(), end_position);
                assert_eq!(checkpoint.log_file_id().stream_id(), stream_id);
                assert_eq!(checkpoint.log_file_id().file_number().get(), file_number);
            }
        }
    }

    #[test]
    fn json_has_a_readable_stable_shape_and_round_trips_through_io() {
        let json = r#"{
  "last_record_id": {
    "stream_id": 42,
    "sequence_number": 99999
  },
  "end_position": 6553500000
}"#;
        let checkpoint = StreamCheckpoint::new(
            RecordId::new(StreamId::new(42).unwrap(), SequenceNumber::new(99_999)),
            6_553_500_000,
        );
        let mut bytes = Vec::new();
        serde_json::to_writer_pretty(&mut bytes, &checkpoint).unwrap();
        assert_eq!(bytes, json.as_bytes());
        let decoded: StreamCheckpoint = serde_json::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn json_preserves_maximum_identifiers_and_large_positions_exactly() {
        let json = r#"{"last_record_id":{"stream_id":4095,"sequence_number":18446744073709551615},"end_position":3382654560}"#;
        // 51,616 maximum-size records in the terminal file range.
        let checkpoint = StreamCheckpoint::new(
            RecordId::new(StreamId::MAX, SequenceNumber::MAX),
            3_382_654_560,
        );
        assert_eq!(serde_json::to_string(&checkpoint).unwrap(), json);
        assert_eq!(
            serde_json::from_str::<StreamCheckpoint>(json).unwrap(),
            checkpoint
        );
    }

    #[test]
    fn json_rejects_incomplete_ambiguous_and_invalid_metadata() {
        for json in [
            r#"{}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0}}"#,
            r#"{"end_position":16}"#,
            r#"{"last_record_id":{"stream_id":4096,"sequence_number":0},"end_position":16}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":-1},"end_position":16}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":-1}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":1.5}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":18446744073709551616}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":null}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":"16"}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":16,"end_position":32}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"last_record_id":{"stream_id":1,"sequence_number":0},"end_position":16}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0,"extra":0},"end_position":16}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":16,"extra":0}"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":16"#,
            r#"{"last_record_id":{"stream_id":0,"sequence_number":0},"end_position":16} trailing"#,
        ] {
            assert!(
                serde_json::from_str::<StreamCheckpoint>(json).is_err(),
                "{json}"
            );
        }
    }
}

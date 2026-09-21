use getset::CopyGetters;
use serde::{Deserialize, Serialize};

use crate::streams::RecordEndLocation;

/// An immutable snapshot of the end of one stream's checkpointed record prefix.
///
/// Its [`RecordEndLocation`] identifies the last included record and the exclusive
/// byte position after its complete encoding, including the CRC, in its log file.
/// Use `checkpoint.end().record_id()`, `checkpoint.end().position()` and
/// `checkpoint.end().log_file_id()` to inspect the boundary. Endpoint behavior
/// belongs to that type rather than duplicate checkpoint accessors.
/// The position is a [`crate::streams::LogFilePosition`]; its raw byte offset is
/// available through `checkpoint.end().position().get()`.
///
/// A checkpoint published for recovery certifies a contiguous prefix of validated
/// records AND their correct index entries on this replica. Its owner must not
/// advance it until both are established, and must synchronize the covered log
/// data, then its index data, before durably publishing the checkpoint. Recovery
/// trusts the certified prefix instead of revalidating it. This model only stores
/// the boundary: construction performs no validation, synchronization or I/O.
/// Those guarantees belong to the checkpoint/recovery owner.
///
/// Use `Option<StreamCheckpoint>` when no checkpoint exists. Sequence zero is a
/// valid record ID, not an empty-stream sentinel.
///
/// Serde uses a named `end` field containing the endpoint's `record_id` and
/// `position` fields. Deserialization delegates identifier and numeric checks to
/// the endpoint, and rejects missing, duplicate or unknown object fields at every
/// level. These metadata checks do not establish that the checkpoint agrees with
/// storage or is safe to use for recovery.
/// [`crate::storage::StorageProvider::checkpoint_file_path`] supplies its per-stream path;
/// the model itself does not choose a location or perform file I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters, Serialize, Deserialize)]
#[getset(get_copy = "pub")]
#[serde(deny_unknown_fields)]
pub struct StreamCheckpoint {
    /// Last included record and its exclusive byte end in that record's log file.
    ///
    /// This may precede the file's physical length when an uncheckpointed tail
    /// follows it. The endpoint's `LogFilePosition` preserves the full `u64`
    /// offset, including positions beyond 4 GiB, on every target.
    end: RecordEndLocation,
}

impl StreamCheckpoint {
    /// Stores a checkpoint boundary supplied by its owner without performing I/O.
    ///
    /// The caller supplies the actual end of the last checkpointed record. An
    /// endpoint alone cannot establish agreement with storage or the prefix's
    /// validity and durability, so wrapping it performs no partial validation.
    /// Persistence and validation of loaded checkpoints are separate
    /// responsibilities of storage and the recovery owner.
    pub const fn new(end: RecordEndLocation) -> Self {
        Self { end }
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

    use super::{RecordEndLocation, StreamCheckpoint};
    use crate::streams::LogFilePosition;

    #[test]
    fn checkpoint_preserves_the_supplied_endpoint() {
        for stream_id in [StreamId::MIN, StreamId::MAX] {
            for (sequence, position) in [
                (0, 16),
                // 100,000 maximum-size records exceed a u32 file offset.
                (99_999, 6_553_500_000),
                (100_000, 16),
                // The terminal range has 51,616 positions; no successor is needed.
                (u64::MAX, 825_856),
            ] {
                let end = RecordEndLocation::new(
                    RecordId::new(stream_id, SequenceNumber::new(sequence)),
                    LogFilePosition::new(position),
                );
                let checkpoint = StreamCheckpoint::new(end);
                assert_eq!(checkpoint.end(), end);
            }
        }
    }

    #[test]
    fn json_has_a_readable_stable_shape_and_round_trips_through_io() {
        let json = r#"{
  "end": {
    "record_id": {
      "stream_id": 42,
      "sequence_number": 99999
    },
    "position": 6553500000
  }
}"#;
        let checkpoint = StreamCheckpoint::new(RecordEndLocation::new(
            RecordId::new(StreamId::new(42).unwrap(), SequenceNumber::new(99_999)),
            LogFilePosition::new(6_553_500_000),
        ));
        let mut bytes = Vec::new();
        serde_json::to_writer_pretty(&mut bytes, &checkpoint).unwrap();
        assert_eq!(bytes, json.as_bytes());
        let decoded: StreamCheckpoint = serde_json::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded, checkpoint);
    }

    #[test]
    fn json_preserves_maximum_identifiers_and_large_positions_exactly() {
        let json = r#"{"end":{"record_id":{"stream_id":4095,"sequence_number":18446744073709551615},"position":3382654560}}"#;
        // 51,616 maximum-size records in the terminal file range.
        let checkpoint = StreamCheckpoint::new(RecordEndLocation::new(
            RecordId::new(StreamId::MAX, SequenceNumber::MAX),
            LogFilePosition::new(3_382_654_560),
        ));
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
            r#"{"end":null}"#,
            r#"{"end":{}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0}}}"#,
            r#"{"end":{"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":4096,"sequence_number":0},"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":-1},"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":-1}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":1.5}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":18446744073709551616}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":null}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":"16"}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16,"position":32}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"record_id":{"stream_id":1,"sequence_number":0},"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16},"end":{"record_id":{"stream_id":1,"sequence_number":0},"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0,"extra":0},"position":16}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16,"extra":0}}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16},"extra":0}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}"#,
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}} trailing"#,
        ] {
            assert!(
                serde_json::from_str::<StreamCheckpoint>(json).is_err(),
                "{json}"
            );
        }
    }
}

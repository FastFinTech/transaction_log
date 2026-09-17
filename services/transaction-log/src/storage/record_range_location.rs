use std::iter::FusedIterator;

use getset::CopyGetters;

use super::{
    LogFileId, LogFileRange, RecordEndLocation, RecordRangeLocationError, RecordStartLocation,
};

/// Outer byte boundaries of an inclusive range of records in one stream.
///
/// A [`RecordStartLocation`] and [`RecordEndLocation`] keep each record identity
/// attached to its byte boundary and prevent swapping endpoint roles. Equal
/// endpoint record IDs describe a single record. Larger ranges can span files;
/// each position is relative to its own endpoint's file, not to the whole stream.
///
/// [`Self::iter`] lazily describes each file's portion, leaving intermediate file
/// ends for the future reader to resolve. No file list, index data or file handle
/// is retained. Construction and iteration allocate nothing and perform no I/O.
///
/// This is immutable location metadata, not proof of stored record validity or
/// durability. The file/index resolver must establish that the supplied positions
/// match the requested records and are readable. Owning this value does not keep
/// files open or prevent retention, replacement or truncation. Its native layout
/// is not a persisted representation, and no separate single-record model is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct RecordRangeLocation {
    /// First included record and its inclusive byte start in its own log file.
    start: RecordStartLocation,
    /// Last included record and its exclusive byte end, including its CRC.
    ///
    /// This position can be less than the start's position across files. Their
    /// difference is a byte count only when both endpoints belong to one file.
    end: RecordEndLocation,
}

impl RecordRangeLocation {
    /// Stores resolved endpoints after checking their internal ordering.
    ///
    /// The supplied byte positions must come from the file/index resolver. This
    /// constructor checks the metadata relationships needed by the iterator;
    /// it cannot match positions to actual record boundaries, encoded lengths,
    /// CRCs or readable file extents. Those checks remain with the resolver.
    ///
    /// ```
    /// use transaction_log::storage::{RecordEndLocation, RecordRangeLocation, RecordStartLocation};
    /// use transaction_log_exports::{RecordId, SequenceNumber, StreamId};
    ///
    /// let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
    /// let start = RecordStartLocation::new(id, 0);
    /// let end = RecordEndLocation::new(id, 16);
    /// let range = RecordRangeLocation::new(start, end)?;
    /// assert_eq!(range.start().position(), 0);
    /// assert_eq!(range.end().record_id(), id);
    /// assert_eq!(range.iter().count(), 1);
    /// # Ok::<(), transaction_log::storage::RecordRangeLocationError>(())
    /// ```
    ///
    /// Endpoint roles cannot be swapped:
    ///
    /// ```compile_fail,E0308
    /// use transaction_log::storage::{RecordEndLocation, RecordRangeLocation, RecordStartLocation};
    /// use transaction_log_exports::{RecordId, SequenceNumber, StreamId};
    ///
    /// let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
    /// let start = RecordStartLocation::new(id, 0);
    /// let end = RecordEndLocation::new(id, 16);
    /// let _ = RecordRangeLocation::new(end, start);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`RecordRangeLocationError`] for different streams, a last sequence
    /// before the first, a zero end position, or an end at/before the start within
    /// a single file. Positions belonging to different files are not compared.
    pub fn new(
        start: RecordStartLocation,
        end: RecordEndLocation,
    ) -> Result<Self, RecordRangeLocationError> {
        let start_record_id = start.record_id();
        let end_record_id = end.record_id();
        if start_record_id.stream_id() != end_record_id.stream_id() {
            return Err(RecordRangeLocationError::DifferentStreams {
                start_stream_id: start_record_id.stream_id(),
                end_stream_id: end_record_id.stream_id(),
            });
        }
        if start_record_id.sequence_number() > end_record_id.sequence_number() {
            return Err(RecordRangeLocationError::ReversedRecords {
                start_sequence_number: start_record_id.sequence_number(),
                end_sequence_number: end_record_id.sequence_number(),
            });
        }

        let start_file = start.log_file_id();
        let end_file = end.log_file_id();
        // A later file's prefix starts at zero, independently of the first
        // file's start. Comparing offsets across files would reject valid ranges.
        let end_file_start = if start_file == end_file {
            start.position()
        } else {
            0
        };
        if end.position() <= end_file_start {
            return Err(RecordRangeLocationError::InvalidByteSpan {
                log_file_id: end_file,
                start_position: end_file_start,
                end_position: end.position(),
            });
        }

        Ok(Self { start, end })
    }

    /// Iterates over `(log_file_id, range)` in ascending file-number order.
    ///
    /// One file yields [`LogFileRange::FileRange`], including a single-record
    /// request. Multiple files yield a [`LogFileRange::FilePostfix`], zero or
    /// more [`LogFileRange::EntireFile`] items and a [`LogFileRange::FilePrefix`].
    /// The outer positions are preserved exactly; the reader resolves missing
    /// file ends when opening each file, rather than this iterator doing I/O.
    ///
    /// Iteration owns a copy of these endpoints, allocates nothing, and advances
    /// one file per item. It works across the full sequence domain without
    /// materializing that range. It remains exhausted after the last file, even
    /// at [`super::LogFileNumber::MAX`], and never increments the final record ID.
    pub fn iter(self) -> impl FusedIterator<Item = (LogFileId, LogFileRange)> {
        let first = self.start.log_file_id();
        let last = self.end.log_file_id();
        std::iter::successors(Some(first), move |file| {
            // Stop before asking for a successor, which may not exist at MAX.
            if *file == last {
                None
            } else {
                Some(file.next())
            }
        })
        .map(move |file| {
            let range = if first == last {
                LogFileRange::FileRange {
                    start_position: self.start.position(),
                    end_position: self.end.position(),
                }
            } else if file == first {
                LogFileRange::FilePostfix {
                    start_position: self.start.position(),
                }
            } else if file == last {
                LogFileRange::FilePrefix {
                    end_position: self.end.position(),
                }
            } else {
                LogFileRange::EntireFile
            };
            (file, range)
        })
    }
}

#[cfg(test)]
mod tests {
    use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

    use super::{
        LogFileId, LogFileRange, RecordEndLocation, RecordRangeLocation, RecordRangeLocationError,
        RecordStartLocation,
    };
    use crate::storage::LogFileNumber;

    #[test]
    fn equal_ids_locate_one_record_including_empty_and_maximum_payloads() {
        for (stream_id, sequence, start, end, file_number) in [
            (StreamId::MIN, 0, 0, 16, 0),
            (StreamId::MAX, 99_999, 6_553_434_465, 6_553_500_000, 0),
            (StreamId::MIN, 100_000, 0, 2_064, 1),
            (
                StreamId::MAX,
                u64::MAX,
                3_382_589_025,
                3_382_654_560,
                184_467_440_737_095,
            ),
        ] {
            let id = RecordId::new(stream_id, SequenceNumber::new(sequence));
            let start_location = RecordStartLocation::new(id, start);
            let end_location = RecordEndLocation::new(id, end);
            let location = RecordRangeLocation::new(start_location, end_location).unwrap();
            let file_id = LogFileId::new(stream_id, LogFileNumber::new(file_number).unwrap());

            assert_eq!(location.start(), start_location);
            assert_eq!(location.end(), end_location);
            assert_eq!(location.start().log_file_id(), file_id);
            assert_eq!(location.end().log_file_id(), file_id);
            assert_eq!(
                location.iter().collect::<Vec<_>>(),
                [(
                    file_id,
                    LogFileRange::FileRange {
                        start_position: start,
                        end_position: end
                    }
                ),]
            );
        }
    }

    #[test]
    fn partial_range_in_one_file_preserves_both_outer_positions() {
        let start = RecordId::new(StreamId::MIN, SequenceNumber::new(100_010));
        let end = RecordId::new(StreamId::MIN, SequenceNumber::new(100_020));
        // An inclusive request for records 10..=20 covers eleven records.
        let location = RecordRangeLocation::new(
            RecordStartLocation::new(start, 160),
            RecordEndLocation::new(end, 336),
        )
        .unwrap();
        assert_eq!(
            location.iter().collect::<Vec<_>>(),
            [(
                LogFileId::new(StreamId::MIN, LogFileNumber::new(1).unwrap()),
                LogFileRange::FileRange {
                    start_position: 160,
                    end_position: 336
                }
            ),]
        );
    }

    #[test]
    fn multi_file_range_yields_postfix_whole_files_and_prefix() {
        let stream_id = StreamId::new(42).unwrap();
        let start = RecordId::new(stream_id, SequenceNumber::new(99_998));
        let end = RecordId::new(stream_id, SequenceNumber::new(300_001));
        let location = RecordRangeLocation::new(
            RecordStartLocation::new(start, 1_599_968),
            RecordEndLocation::new(end, 32),
        )
        .unwrap();
        let actual: Vec<_> = location
            .iter()
            .map(|(id, range)| {
                assert_eq!(id.stream_id(), stream_id);
                (id.file_number().get(), range)
            })
            .collect();
        assert_eq!(
            actual,
            [
                (
                    0,
                    LogFileRange::FilePostfix {
                        start_position: 1_599_968
                    }
                ),
                (1, LogFileRange::EntireFile),
                (2, LogFileRange::EntireFile),
                (3, LogFileRange::FilePrefix { end_position: 32 }),
            ]
        );
        assert_eq!(location.end().position(), 32);
        assert_eq!(location.end().log_file_id().file_number().get(), 3);
    }

    #[test]
    fn adjacent_boundary_records_need_no_entire_file_item() {
        let start = RecordId::new(StreamId::MIN, SequenceNumber::new(99_999));
        let end = RecordId::new(StreamId::MIN, SequenceNumber::new(100_000));
        let location = RecordRangeLocation::new(
            RecordStartLocation::new(start, 1_599_984),
            RecordEndLocation::new(end, 16),
        )
        .unwrap();
        let actual: Vec<_> = location
            .iter()
            .map(|(id, range)| (id.file_number().get(), range))
            .collect();
        assert_eq!(
            actual,
            [
                (
                    0,
                    LogFileRange::FilePostfix {
                        start_position: 1_599_984
                    }
                ),
                (1, LogFileRange::FilePrefix { end_position: 16 }),
            ]
        );
    }

    #[test]
    fn exact_file_endpoints_preserve_explicit_bounds() {
        let start = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
        for (last_sequence, expected) in [
            (
                99_999,
                vec![(
                    0,
                    LogFileRange::FileRange {
                        start_position: 0,
                        end_position: 1_600_000,
                    },
                )],
            ),
            (
                199_999,
                vec![
                    (0, LogFileRange::FilePostfix { start_position: 0 }),
                    (
                        1,
                        LogFileRange::FilePrefix {
                            end_position: 1_600_000,
                        },
                    ),
                ],
            ),
        ] {
            let end = RecordId::new(StreamId::MIN, SequenceNumber::new(last_sequence));
            let location = RecordRangeLocation::new(
                RecordStartLocation::new(start, 0),
                RecordEndLocation::new(end, 1_600_000),
            )
            .unwrap();
            let actual: Vec<_> = location
                .iter()
                .map(|(id, range)| (id.file_number().get(), range))
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn iterator_stops_at_the_terminal_file_and_can_be_created_again() {
        let start = RecordId::new(
            StreamId::MAX,
            SequenceNumber::new(18_446_744_073_709_499_999),
        );
        let end = RecordId::new(StreamId::MAX, SequenceNumber::MAX);
        let location = RecordRangeLocation::new(
            RecordStartLocation::new(start, 1_599_984),
            RecordEndLocation::new(end, 825_856),
        )
        .unwrap();
        let mut iter = location.iter();
        assert_eq!(
            iter.next().unwrap().0.file_number().get(),
            184_467_440_737_094
        );
        assert_eq!(
            iter.next(),
            Some((
                LogFileId::new(StreamId::MAX, LogFileNumber::MAX),
                LogFileRange::FilePrefix {
                    end_position: 825_856
                }
            ))
        );
        for _ in 0..3 {
            assert_eq!(iter.next(), None);
        }
        assert_eq!(location.iter().count(), 2);
        assert_eq!(
            location.iter().nth(1).unwrap().0.file_number(),
            LogFileNumber::MAX
        );
    }

    #[test]
    fn full_sequence_domain_can_be_iterated_progressively_without_a_file_list() {
        let start = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
        let end = RecordId::new(StreamId::MIN, SequenceNumber::MAX);
        let location = RecordRangeLocation::new(
            RecordStartLocation::new(start, 0),
            RecordEndLocation::new(end, 825_856),
        )
        .unwrap();
        // There are 184,467,440,737,096 files in the assigned range. Constructing
        // it and taking three items must not enumerate or allocate all of them.
        let first_three: Vec<_> = location
            .iter()
            .take(3)
            .map(|(id, range)| (id.file_number().get(), range))
            .collect();
        assert_eq!(
            first_three,
            [
                (0, LogFileRange::FilePostfix { start_position: 0 }),
                (1, LogFileRange::EntireFile),
                (2, LogFileRange::EntireFile),
            ]
        );
        assert_eq!(
            location.end().log_file_id().file_number(),
            LogFileNumber::MAX
        );
    }

    #[test]
    fn different_streams_and_reversed_sequences_are_rejected() {
        let start = RecordId::new(StreamId::MIN, SequenceNumber::new(20));
        let other_stream = RecordId::new(StreamId::MAX, SequenceNumber::new(20));
        assert_eq!(
            RecordRangeLocation::new(
                RecordStartLocation::new(start, 320),
                RecordEndLocation::new(other_stream, 336),
            )
            .unwrap_err(),
            RecordRangeLocationError::DifferentStreams {
                start_stream_id: StreamId::MIN,
                end_stream_id: StreamId::MAX,
            }
        );
        let earlier = RecordId::new(StreamId::MIN, SequenceNumber::new(19));
        assert_eq!(
            RecordRangeLocation::new(
                RecordStartLocation::new(start, 320),
                RecordEndLocation::new(earlier, 336),
            )
            .unwrap_err(),
            RecordRangeLocationError::ReversedRecords {
                start_sequence_number: SequenceNumber::new(20),
                end_sequence_number: SequenceNumber::new(19),
            }
        );
        // The same rejection must hold when the reversed endpoints cross files.
        let first = RecordId::new(StreamId::MIN, SequenceNumber::new(100_000));
        let last = RecordId::new(StreamId::MIN, SequenceNumber::new(99_999));
        assert!(matches!(
            RecordRangeLocation::new(
                RecordStartLocation::new(first, 0),
                RecordEndLocation::new(last, 1_600_000),
            ),
            Err(RecordRangeLocationError::ReversedRecords { .. })
        ));
    }

    #[test]
    fn empty_and_reversed_known_byte_spans_report_the_affected_file() {
        let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
        for (start, end) in [(0, 0), (16, 16), (32, 16), (u64::MAX, 16)] {
            assert_eq!(
                RecordRangeLocation::new(
                    RecordStartLocation::new(id, start),
                    RecordEndLocation::new(id, end),
                )
                .unwrap_err(),
                RecordRangeLocationError::InvalidByteSpan {
                    log_file_id: LogFileId::new(StreamId::MIN, LogFileNumber::MIN),
                    start_position: start,
                    end_position: end,
                }
            );
        }
        let later = RecordId::new(StreamId::MIN, SequenceNumber::new(100_000));
        assert_eq!(
            RecordRangeLocation::new(
                RecordStartLocation::new(id, 0),
                RecordEndLocation::new(later, 0),
            )
            .unwrap_err(),
            RecordRangeLocationError::InvalidByteSpan {
                log_file_id: LogFileId::new(StreamId::MIN, LogFileNumber::new(1).unwrap()),
                start_position: 0,
                end_position: 0,
            }
        );
    }
}

use getset::CopyGetters;

use super::{SequenceNumber, StreamId};

/// The stream and sequence number that together identify a record.
///
/// Sequence numbers are local to a stream. Derived ordering compares stream ID
/// first and then sequence number; it does not define chronology across streams.
/// Construction does not check uniqueness or continuity against stored records.
///
/// Fields are read-only. Construct an ID with [`Self::new`], read its components
/// with [`Self::stream_id`] and [`Self::sequence_number`], and obtain a successor
/// with [`Self::next`]. The accessors copy values without allocation or validation.
///
/// This is an owned Rust value whose memory layout is compiler-selected.
/// Its wire representation is defined separately by [`super::record_protocol`].
///
/// ```compile_fail,E0616
/// use transaction_log_exports::{RecordId, SequenceNumber, StreamId};
///
/// let mut id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
/// id.sequence_number = SequenceNumber::MAX;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct RecordId {
    /// Logical stream containing the record, shared by its replicas.
    stream_id: StreamId,
    /// Position identifier assigned by the command handler within that stream.
    sequence_number: SequenceNumber,
}

impl RecordId {
    /// Combines already typed identifiers without allocation or further validation.
    pub const fn new(stream_id: StreamId, sequence_number: SequenceNumber) -> Self {
        Self {
            stream_id,
            sequence_number,
        }
    }

    /// Returns the next record ID in the same stream.
    ///
    /// The original ID is unchanged. This method never wraps to zero or advances
    /// the stream ID.
    ///
    /// This is checked arithmetic, not sequence assignment or continuity
    /// validation. It does not reserve the returned ID or coordinate writers.
    /// The already validated stream ID is reused without further validation.
    ///
    /// # Panics
    ///
    /// Panics if the sequence number is [`SequenceNumber::MAX`]. The increment
    /// is checked in both debug and release builds.
    ///
    /// ```
    /// use transaction_log_exports::{RecordId, SequenceNumber, StreamId};
    ///
    /// let current = RecordId::new(StreamId::MIN, SequenceNumber::new(41));
    /// let next = current.next();
    /// assert_eq!(next.stream_id(), current.stream_id());
    /// assert_eq!(next.sequence_number().get(), 42);
    /// ```
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        let value = self
            .sequence_number
            .get()
            .checked_add(1)
            .expect("record sequence number overflow");
        Self::new(self.stream_id, SequenceNumber::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::{RecordId, SequenceNumber, StreamId};

    #[test]
    fn successor_preserves_stream_and_advances_across_numeric_boundaries() {
        for stream_id in [StreamId::MIN, StreamId::new(42).unwrap(), StreamId::MAX] {
            for (current, expected) in [
                (0, 1),
                (1, 2),
                (65_535, 65_536),
                (4_294_967_295, 4_294_967_296),
                (u64::MAX - 1, u64::MAX),
            ] {
                let id = RecordId::new(stream_id, SequenceNumber::new(current));
                assert_eq!(
                    id.next(),
                    RecordId::new(stream_id, SequenceNumber::new(expected))
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "record sequence number overflow")]
    fn successor_panics_on_exhausted_sequence() {
        let last = RecordId::new(StreamId::MIN, SequenceNumber::MAX);
        let _ = last.next();
    }
}

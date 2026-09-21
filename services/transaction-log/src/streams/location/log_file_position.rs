use serde::{Deserialize, Serialize};
use transaction_log_exports::RecordLength;

/// A byte offset within a log file, distinct from a length or index-file offset.
///
/// Every `u64` value is representable, including zero. This is numeric metadata:
/// it does not identify a file, prove that the position exists or establish a
/// record boundary. The enclosing location determines whether it denotes an
/// inclusive start or an exclusive end. Ordering compares only the byte offsets;
/// callers must establish that they belong to the same file when needed.
///
/// Extract the raw offset explicitly with [`Self::get`] at I/O or encoding
/// boundaries. The transparent native layout does not define a file encoding.
/// Serde represents this metadata as a single `u64`, preserving the numeric
/// position field in serialized record locations without adding storage validation.
///
/// ```
/// use transaction_log::streams::LogFilePosition;
/// use transaction_log_exports::RecordLength;
///
/// assert_eq!(LogFilePosition::START.get(), 0);
/// let position = LogFilePosition::new(4_294_967_296);
/// let end = position.advance(RecordLength::MIN);
/// assert_eq!(end.get(), 4_294_967_312);
/// assert_eq!(end.retreat(RecordLength::MIN), position);
/// assert_eq!(position.get(), 4_294_967_296);
/// ```
///
/// An encoded record length cannot be passed as a log position:
///
/// ```compile_fail,E0308
/// use transaction_log::streams::LogFilePosition;
/// use transaction_log_exports::RecordLength;
///
/// fn accept_position(position: LogFilePosition) {}
/// accept_position(RecordLength::MIN);
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogFilePosition(u64);

impl LogFilePosition {
    /// Byte zero, the start of a log file.
    ///
    /// This names an offset only; it does not identify or open a file.
    pub const START: Self = Self(0);

    /// Stores a raw byte offset without validation or file I/O.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the byte offset without checking it against storage.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns a position one encoded record length ahead, leaving `self` unchanged.
    ///
    /// Adds the complete header, payload and CRC length within the same file.
    /// This does not advance a record ID, rotate files or establish a record boundary.
    /// The length's protocol bounds are already guaranteed by [`RecordLength`].
    ///
    /// # Panics
    ///
    /// Panics if the resulting offset exceeds `u64::MAX`, in debug and release.
    #[inline]
    #[must_use]
    pub const fn advance(self, length: RecordLength) -> Self {
        Self(
            self.0
                .checked_add(length.get())
                .expect("log file position overflow"),
        )
    }

    /// Returns a position one encoded record length behind, leaving `self` unchanged.
    ///
    /// Subtracts the complete header, payload and CRC length within the same file.
    /// This does not discover the preceding record or its length, change record IDs
    /// or cross into a previous file. The caller must supply the appropriate length.
    ///
    /// # Panics
    ///
    /// Panics if the length exceeds this offset, in debug and release.
    /// Subtracting exactly the offset returns position zero.
    #[inline]
    #[must_use]
    pub const fn retreat(self, length: RecordLength) -> Self {
        Self(
            self.0
                .checked_sub(length.get())
                .expect("log file position underflow"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{LogFilePosition, RecordLength};

    #[test]
    fn preserves_offsets_without_narrowing_or_imposing_storage_bounds() {
        assert_eq!(LogFilePosition::START.get(), 0);
        const ABOVE_FOUR_GIB: LogFilePosition = LogFilePosition::new(4_294_967_296);
        assert_eq!(ABOVE_FOUR_GIB.get(), 4_294_967_296);

        for value in [0, 1, 65_535, 6_553_500_000, u64::MAX] {
            assert_eq!(LogFilePosition::new(value).get(), value);
        }
    }

    #[test]
    fn moves_by_the_encoded_length_without_mutating_the_original() {
        const END: LogFilePosition = LogFilePosition::new(123).advance(RecordLength::MIN);
        const START: LogFilePosition = END.retreat(RecordLength::MIN);
        assert_eq!(END.get(), 139);
        assert_eq!(START.get(), 123);

        for (start, length, end) in [
            (0, RecordLength::MIN, 16),
            (0, RecordLength::MAX, 65_535),
            (1, RecordLength::MAX, 65_536),
            (42, RecordLength::new(21).unwrap(), 63),
            (4_294_967_295, RecordLength::MIN, 4_294_967_311),
            (u64::MAX - 65_535, RecordLength::MAX, u64::MAX),
        ] {
            let start_position = LogFilePosition::new(start);
            let end_position = LogFilePosition::new(end);
            assert_eq!(start_position.advance(length), end_position);
            assert_eq!(end_position.retreat(length), start_position);
            assert_eq!(start_position.get(), start);
            assert_eq!(end_position.get(), end);
        }
    }

    #[test]
    fn advancing_past_the_numeric_limit_panics_instead_of_wrapping() {
        for (position, length) in [
            (u64::MAX - 15, RecordLength::MIN),
            (u64::MAX - 65_534, RecordLength::MAX),
            (u64::MAX, RecordLength::MIN),
        ] {
            let position = LogFilePosition::new(position);
            assert!(std::panic::catch_unwind(|| position.advance(length)).is_err());
        }
    }

    #[test]
    fn retreating_before_zero_panics_instead_of_wrapping() {
        for (position, length) in [
            (0, RecordLength::MIN),
            (15, RecordLength::MIN),
            (65_534, RecordLength::MAX),
        ] {
            let position = LogFilePosition::new(position);
            assert!(std::panic::catch_unwind(|| position.retreat(length)).is_err());
        }
    }
}

use std::fmt;

use serde::{Deserialize, Serialize};
use transaction_log_exports::SequenceNumber;

use super::LogFileNumberError;

/// Number of consecutive sequence positions assigned to each ordinary log file.
///
/// This is a storage-layout constant, not a per-process tuning parameter. The
/// final range ends at `u64::MAX` and has fewer representable positions.
pub const RECORDS_PER_FILE: u64 = 100_000;

/// A zero-based log-file number in `0..=184_467_440_737_095`, stored as a `u64`.
///
/// The bound is `u64::MAX / RECORDS_PER_FILE`: every valid number names a range
/// with at least one representable sequence, and multiplying it by the record
/// count fits in `u64`. A number alone does not identify a stream or prove that
/// a file exists; combine it with a stream ID in [`super::LogFileId`].
///
/// Raw values are checked by [`Self::new`] or [`TryFrom<u64>`]. Conversion from
/// [`SequenceNumber`] is infallible because division establishes the bound.
/// Serde represents the number as an integer and loads it through the same checked
/// conversion. Its transparent native layout does not define a file encoding.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
// Deserialization must preserve the domain, not merely accept any raw u64.
#[serde(try_from = "u64", into = "u64")]
pub struct LogFileNumber(u64);

impl LogFileNumber {
    /// First file number, zero, containing sequence zero.
    pub const MIN: Self = Self(0);
    /// Last file number, whose range contains the final 51,616 sequence positions.
    pub const MAX: Self = Self(u64::MAX / RECORDS_PER_FILE);

    /// Validates a raw file number without filesystem access.
    ///
    /// # Errors
    ///
    /// Returns [`LogFileNumberError`] containing `value` if it exceeds [`Self::MAX`].
    /// It is not clamped, truncated or interpreted as a sequence number.
    pub const fn new(value: u64) -> Result<Self, LogFileNumberError> {
        if value > Self::MAX.0 {
            return Err(LogFileNumberError::new(value));
        }
        Ok(Self(value))
    }

    /// Returns the file number assigned to a sequence, without further validation.
    ///
    /// This groups the sequence by the fixed record count; it does not establish
    /// that the record or file exists. Equivalent to [`From<SequenceNumber>`].
    #[inline]
    pub const fn from_sequence_number(sequence_number: SequenceNumber) -> Self {
        // Dividing any u64 by the record count proves the bound. Construct the
        // wrapper directly rather than repeat the raw-value range check.
        Self(sequence_number.get() / RECORDS_PER_FILE)
    }

    /// Returns the validated integer without checking its range again.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next file number, leaving the original value unchanged.
    ///
    /// # Panics
    ///
    /// Panics at [`Self::MAX`] in debug and release builds. There is no successor
    /// range containing a representable sequence; file numbers never wrap.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        // The domain ends well before u64 overflow, so checked_add alone would
        // allow file numbers whose assigned sequences cannot be represented.
        assert!(self.0 < Self::MAX.0, "log file number overflow");
        Self(self.0 + 1)
    }
}

impl TryFrom<u64> for LogFileNumber {
    type Error = LogFileNumberError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LogFileNumber> for u64 {
    #[inline]
    fn from(value: LogFileNumber) -> Self {
        value.get()
    }
}

impl From<SequenceNumber> for LogFileNumber {
    #[inline]
    fn from(value: SequenceNumber) -> Self {
        Self::from_sequence_number(value)
    }
}

impl fmt::Display for LogFileNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::{LogFileNumber, RECORDS_PER_FILE, SequenceNumber};

    #[test]
    fn raw_construction_accepts_only_addressable_file_numbers() {
        assert_eq!(RECORDS_PER_FILE, 100_000);
        assert_eq!(LogFileNumber::MIN.get(), 0);
        assert_eq!(LogFileNumber::MAX.get(), 184_467_440_737_095);
        assert_eq!(size_of::<LogFileNumber>(), size_of::<u64>());
        assert_eq!(align_of::<LogFileNumber>(), align_of::<u64>());

        for value in [
            0,
            1,
            4_294_967_296,
            184_467_440_737_094,
            184_467_440_737_095,
        ] {
            let number = LogFileNumber::new(value).unwrap();
            assert_eq!(number.get(), value);
            assert_eq!(u64::from(number), value);
            assert_eq!(LogFileNumber::try_from(value), Ok(number));
            assert_eq!(number.to_string(), value.to_string());
        }
        assert_eq!(
            format!("{:015}", LogFileNumber::new(1_234).unwrap()),
            "000000000001234"
        );

        for value in [184_467_440_737_096, u64::MAX - 1, u64::MAX] {
            let error = LogFileNumber::new(value).unwrap_err();
            assert_eq!(error.value(), value);
            assert_eq!(LogFileNumber::try_from(value), Err(error));
        }
        assert_eq!(
            LogFileNumber::new(184_467_440_737_096)
                .unwrap_err()
                .to_string(),
            "log file number 184467440737096 is outside the supported range 0..=184467440737095"
        );
    }

    #[test]
    fn groups_sequences_at_file_and_integer_boundaries() {
        for (sequence, expected) in [
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
            let sequence = SequenceNumber::new(sequence);
            let number = LogFileNumber::from_sequence_number(sequence);
            assert_eq!(number.get(), expected);
            assert_eq!(LogFileNumber::from(sequence), number);
        }
    }

    #[test]
    fn json_preserves_integer_shape_and_enforces_the_domain() {
        for (json, number) in [
            ("0", LogFileNumber::MIN),
            ("184467440737095", LogFileNumber::MAX),
        ] {
            assert_eq!(serde_json::to_string(&number).unwrap(), json);
            assert_eq!(serde_json::from_str::<LogFileNumber>(json).unwrap(), number);
        }
        for json in [
            "184467440737096",
            "18446744073709551615",
            "18446744073709551616",
            "-1",
            "1.5",
            "null",
            "\"0\"",
        ] {
            assert!(
                serde_json::from_str::<LogFileNumber>(json).is_err(),
                "{json}"
            );
        }
        let error = serde_json::from_str::<LogFileNumber>("184467440737096").unwrap_err();
        let expected = LogFileNumber::new(184_467_440_737_096).unwrap_err();
        assert!(error.to_string().contains(&expected.to_string()));
    }

    #[test]
    fn successor_advances_up_to_the_domain_limit() {
        for (value, expected) in [
            (0, 1),
            (4_294_967_295, 4_294_967_296),
            (184_467_440_737_094, 184_467_440_737_095),
        ] {
            assert_eq!(LogFileNumber::new(value).unwrap().next().get(), expected);
        }
    }

    #[test]
    #[should_panic(expected = "log file number overflow")]
    fn successor_panics_at_the_domain_limit() {
        let _ = LogFileNumber::MAX.next();
    }
}

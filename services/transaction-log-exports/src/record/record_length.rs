use super::{RecordLengthError, record_protocol as protocol};

/// Total encoded record length, including header, payload and CRC, in bytes.
///
/// Values are in `16..=65_535`, stored as a `u64` for arithmetic with file positions.
/// The encoded length field remains two bytes. This is a single record's
/// length, not its payload length, a file position or a batch size. The bounds
/// come from [`protocol::MIN_RECORD_LEN`] and [`protocol::MAX_RECORD_LEN`].
/// Checked construction validates the number only; it does not validate bytes,
/// framing or a checksum. [`Record::length`](super::Record::length) returns this
/// type using the record's established validity without repeating the check.
///
/// ```
/// use transaction_log_exports::RecordLength;
///
/// let length = RecordLength::new(21)?;
/// assert_eq!(length.get(), 21);
/// assert!(RecordLength::new(15).is_err());
/// assert!(RecordLength::new(65_536).is_err());
/// assert_eq!(RecordLength::MIN.get(), 16);
/// assert_eq!(RecordLength::MAX.get(), 65_535);
/// # Ok::<(), transaction_log_exports::RecordLengthError>(())
/// ```
///
/// Raw construction cannot bypass validation outside the record module:
///
/// ```compile_fail,E0423
/// use transaction_log_exports::RecordLength;
/// let length = RecordLength(0);
/// ```
///
/// ```compile_fail,E0624
/// use transaction_log_exports::RecordLength;
/// let length = unsafe { RecordLength::new_unchecked(21) };
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordLength(u64);

impl RecordLength {
    /// Minimum encoded record length: the protocol header and CRC with no payload.
    pub const MIN: Self = Self(protocol::MIN_RECORD_LEN as u64);
    /// Maximum encoded record length, bounded by the protocol's two-byte length field.
    pub const MAX: Self = Self(protocol::MAX_RECORD_LEN as u64);

    /// Validates a raw encoded record length.
    ///
    /// # Errors
    ///
    /// Returns [`RecordLengthError`] preserving `value` when it is outside the
    /// protocol's minimum and maximum encoded lengths.
    pub const fn new(value: u64) -> Result<Self, RecordLengthError> {
        if value < Self::MIN.0 || value > Self::MAX.0 {
            return Err(RecordLengthError::new(value));
        }
        Ok(Self(value))
    }

    /// Returns the validated byte count without checking its range again.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Wraps the length of an already-validated, immutable record.
    ///
    /// Only [`Record::length`](super::Record::length) may use this constructor.
    /// Other consumers of raw lengths must use [`Self::new`] or [`TryFrom<u64>`].
    /// It is restricted to the record module and performs no validation.
    ///
    /// # Safety
    ///
    /// `value` must be in [`Self::MIN`]..=[`Self::MAX`], obtained from a record
    /// whose complete framing and exact encoded extent have already been validated.
    /// Consumers of this value may rely on that range without further checks.
    #[inline]
    pub(super) const unsafe fn new_unchecked(value: u64) -> Self {
        Self(value)
    }
}

impl TryFrom<u64> for RecordLength {
    type Error = RecordLengthError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{RecordLength, protocol};

    #[test]
    fn validates_every_wire_length_and_preserves_rejected_values() {
        for value in 0..=65_535u64 {
            if value < 16 {
                let error = RecordLength::new(value).unwrap_err();
                assert_eq!(error.value(), value);
                assert_eq!(RecordLength::try_from(value), Err(error));
            } else {
                let length = RecordLength::new(value).unwrap();
                assert_eq!(length.get(), value);
                assert_eq!(RecordLength::try_from(value), Ok(length));
            }
        }
    }

    #[test]
    fn rejects_oversized_lengths_without_narrowing_the_input() {
        for value in [65_536, 65_552, u64::from(u32::MAX), u64::MAX] {
            let error = RecordLength::new(value).unwrap_err();
            assert_eq!(error.value(), value);
            assert_eq!(RecordLength::try_from(value), Err(error));
        }
    }

    #[test]
    fn limits_follow_the_protocol_and_checked_construction_works_in_constants() {
        const LENGTH: RecordLength = match RecordLength::new(21) {
            Ok(length) => length,
            Err(_) => panic!("the fixture length must be valid"),
        };
        assert_eq!(LENGTH.get(), 21);
        assert_eq!(RecordLength::MIN.get(), 16);
        assert_eq!(RecordLength::MAX.get(), 65_535);
        assert_eq!(RecordLength::MIN.get(), protocol::MIN_RECORD_LEN as u64);
        assert_eq!(RecordLength::MAX.get(), protocol::MAX_RECORD_LEN as u64);
    }
}

use getset::CopyGetters;
use thiserror::Error;

use super::RecordLength;

/// A raw byte count rejected by checked encoded-record length construction.
///
/// The rejected input is preserved without clamping or adjustment. This reports
/// a numeric length error, not the validity of any record's bytes or checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, CopyGetters)]
#[getset(get_copy = "pub")]
#[error(
    "record length {value} is outside the supported encoded range {}..={} bytes",
    RecordLength::MIN.get(),
    RecordLength::MAX.get()
)]
pub struct RecordLengthError {
    /// Rejected raw byte count.
    value: u64,
}

impl RecordLengthError {
    /// Preserves a value rejected by the checked constructor.
    pub(super) const fn new(value: u64) -> Self {
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::RecordLength;

    #[test]
    fn describes_the_rejected_length_and_full_encoded_range() {
        let error = RecordLength::new(15).unwrap_err();
        assert_eq!(error.value(), 15);
        assert_eq!(
            error.to_string(),
            "record length 15 is outside the supported encoded range 16..=65535 bytes"
        );
        assert!(std::error::Error::source(&error).is_none());

        let error = RecordLength::new(u64::MAX).unwrap_err();
        assert_eq!(error.value(), u64::MAX);
        assert_eq!(
            error.to_string(),
            "record length 18446744073709551615 is outside the supported encoded range 16..=65535 bytes"
        );
    }
}

use getset::CopyGetters;
use thiserror::Error;

use super::LogFileNumber;

/// A raw file number with no corresponding representable sequence range.
///
/// Returned by [`LogFileNumber::new`] and its checked conversion. The rejected
/// raw number is preserved by [`Self::value`]. This does not report invalid stream
/// IDs, missing files, corrupt contents or other filesystem errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, CopyGetters)]
#[getset(get_copy = "pub")]
#[error(
    "log file number {value} is outside the supported range 0..={}",
    LogFileNumber::MAX
)]
pub struct LogFileNumberError {
    /// Rejected raw file number, without clamping or truncation.
    value: u64,
}

impl LogFileNumberError {
    /// Preserves a number already rejected by file-number validation.
    pub(super) const fn new(value: u64) -> Self {
        Self { value }
    }
}

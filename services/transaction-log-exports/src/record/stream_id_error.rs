use getset::CopyGetters;
use thiserror::Error;

use super::StreamId;

/// A raw stream ID outside the supported logical stream range.
///
/// Returned by [`StreamId::new`] and `StreamId::try_from`. The reader exposes it
/// through [`RecordReadError::InvalidStreamId`](crate::RecordReadError::InvalidStreamId).
/// It reports a domain range violation, not malformed framing or authorization.
/// The rejected value is read-only and available through [`Self::value`].
///
/// ```compile_fail,E0616
/// use transaction_log_exports::StreamId;
///
/// let mut error = StreamId::new(4096).unwrap_err();
/// error.value = 0;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, CopyGetters)]
#[getset(get_copy = "pub")]
#[error(
    "stream ID {value} is outside the supported range 0..={}",
    StreamId::MAX
)]
pub struct StreamIdError {
    /// The rejected raw integer, preserved without clamping or truncation.
    value: u16,
}

impl StreamIdError {
    /// Preserves an integer already rejected by stream ID validation.
    pub(crate) const fn new(value: u16) -> Self {
        Self { value }
    }
}

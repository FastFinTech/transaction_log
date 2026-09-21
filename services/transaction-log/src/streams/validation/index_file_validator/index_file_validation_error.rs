use std::io;

use thiserror::Error;

use crate::streams::{IndexWriteError, LogFilePosition};

/// Failure while checking a trusted index entry or replacing its following suffix.
#[derive(Debug, Error)]
pub enum IndexFileValidationError {
    /// Index-file metadata, seeking, reading, truncation or synchronization failed.
    #[error("index file operation failed: {0}")]
    Io(#[from] io::Error),
    /// The caller-certified entry is absent or contains another log position.
    #[error(
        "trusted index entry {entry_index} must contain {}, but contains {:?}",
        .expected_position.get(), .actual_position.map(LogFilePosition::get)
    )]
    TrustedIndexMismatch {
        /// Zero-based dense-index entry selected by the trusted record ID.
        entry_index: u64,
        /// Caller-certified exclusive log end.
        expected_position: LogFilePosition,
        /// Stored value, or `None` when the complete entry is absent.
        actual_position: Option<LogFilePosition>,
    },
    /// The supplied suffix would exceed the entries assigned to one log file.
    #[error("trusted prefix and supplied suffix contain {count} entries; maximum is {maximum}")]
    TooManyEntries {
        /// Combined trusted-prefix and supplied-suffix entry count.
        count: u64,
        /// Maximum entries assigned to one index file.
        maximum: u64,
    },
    /// Index suffix output failed.
    #[error(transparent)]
    Write(#[from] IndexWriteError),
}

#[cfg(test)]
mod tests {
    use super::{IndexFileValidationError, LogFilePosition};

    #[test]
    fn trusted_mismatch_displays_numeric_offsets_and_distinguishes_absence() {
        for (actual_position, expected_message) in [
            (
                None,
                "trusted index entry 2 must contain 4294967296, but contains None",
            ),
            (
                Some(LogFilePosition::new(u64::MAX)),
                "trusted index entry 2 must contain 4294967296, but contains Some(18446744073709551615)",
            ),
        ] {
            let error = IndexFileValidationError::TrustedIndexMismatch {
                entry_index: 2,
                expected_position: LogFilePosition::new(4_294_967_296),
                actual_position,
            };
            assert_eq!(error.to_string(), expected_message);
        }
    }
}

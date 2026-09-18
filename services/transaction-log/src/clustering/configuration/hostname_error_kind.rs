use thiserror::Error;

use super::Hostname;

/// A configured-hostname validation failure, independent of its original input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HostnameErrorKind {
    /// No characters remain after trimming.
    #[error("hostname must not be empty")]
    Empty,
    /// The name exceeds the limit excluding its optional final dot.
    #[error(
        "hostname exceeds {} bytes excluding the final root dot",
        Hostname::MAX_NAME_LENGTH
    )]
    TooLong,
    /// An IP address was supplied instead of a DNS hostname.
    #[error("IP literals are not configured hostnames")]
    IpLiteral,
    /// A dot-separated label is empty.
    #[error("label {label_index} is empty")]
    EmptyLabel {
        /// Zero-based index of the empty label, excluding an optional root dot.
        label_index: usize,
    },
    /// One label exceeds its byte limit.
    #[error("label {label_index} exceeds {} bytes", Hostname::MAX_LABEL_LENGTH)]
    LabelTooLong {
        /// Zero-based index of the overlength label.
        label_index: usize,
    },
    /// A character is outside ASCII letters, digits, hyphens and label separators.
    #[error("invalid character at byte {byte_index}")]
    InvalidCharacter {
        /// Zero-based byte offset in the trimmed input before lowercasing.
        byte_index: usize,
    },
    /// A label starts or ends with a hyphen.
    #[error("label {label_index} must start and end with an ASCII letter or digit")]
    InvalidBoundary {
        /// Zero-based index of the label with an invalid boundary.
        label_index: usize,
    },
}

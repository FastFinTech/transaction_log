use thiserror::Error;

use super::MemberId;

/// The member-name rule that failed, independent of the rejected input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MemberIdErrorKind {
    /// The trimmed name is shorter than the minimum byte length.
    #[error(
        "trimmed name must contain at least {} bytes",
        MemberId::MIN_NAME_LENGTH
    )]
    TooShort,
    /// The trimmed name exceeds the maximum byte length.
    #[error("name exceeds {} bytes", MemberId::MAX_NAME_LENGTH)]
    TooLong,
    /// A byte is outside ASCII letters, digits, hyphens and underscores.
    #[error("invalid character at byte {byte_index}; use ASCII letters, digits, '-' or '_'")]
    InvalidCharacter {
        /// Zero-based byte offset of the first invalid character in the trimmed name.
        byte_index: usize,
    },
    /// The first or last character is a hyphen or underscore.
    #[error("name must start and end with an ASCII letter or digit")]
    InvalidBoundary,
}

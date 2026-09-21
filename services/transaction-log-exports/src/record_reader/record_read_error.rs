use std::io;

use thiserror::Error;

use crate::record::{StreamIdError, record_protocol as protocol};

/// Malformed wire data or a failed source read.
#[derive(Debug, Error)]
pub enum RecordReadError {
    /// Input ended before a complete header was available.
    #[error(
        "record header requires {} bytes, got {actual} before end of input",
        protocol::HEADER_LEN
    )]
    TruncatedHeader {
        /// Number of header bytes received.
        actual: usize,
    },

    /// The encoded length cannot contain the fixed header and CRC trailer.
    #[error(
        "record length {declared} is smaller than the {} bytes required for its header and CRC trailer",
        protocol::MIN_RECORD_LEN
    )]
    InvalidLength {
        /// Rejected length from the wire header.
        declared: u16,
    },

    /// The encoded stream ID is outside the supported logical-stream domain.
    #[error("record header contains an invalid stream ID: {0}")]
    InvalidStreamId(#[from] StreamIdError),

    /// Input ended after a valid header but before the complete record arrived.
    #[error("record requires {expected} bytes, got {actual} before end of input")]
    TruncatedRecord {
        /// Total encoded record length declared by the header.
        expected: usize,
        /// Number of encoded record bytes received.
        actual: usize,
    },

    /// The stored checksum differs from CRC-32C over the header and payload.
    #[error("record CRC-32C mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch {
        /// Checksum decoded from the trailer.
        stored: u32,
        /// Checksum calculated over the preceding record bytes.
        computed: u32,
    },

    /// Original source I/O error.
    #[error("failed to read a record: {0}")]
    Io(#[from] io::Error),

    /// A previous validation or I/O error made the reader terminal.
    #[error("record reader cannot continue after an earlier error")]
    ReaderFailed,
}

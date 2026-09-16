use std::io;

use thiserror::Error;

use crate::record::record_protocol::MAX_PAYLOAD_LEN;

/// A rejected operation while constructing a record.
///
/// Each writer-owned record builder constructs one record and retains its first error.
/// An unsuccessful write leaves the buffer unchanged, and subsequent writes on
/// that builder fail with the same error. Finalization also rejects the record
/// and rolls it back. A fresh builder starts with no error, even when it borrows
/// the same buffer.
/// Earlier complete records remain in the writer's buffer. These errors describe
/// record construction, not destination I/O or application payload validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecordBuildError {
    /// Appending bytes would exceed this record's payload limit, excluding the
    /// header, CRC trailer and any earlier records in the batch.
    #[error("record body exceeds the maximum payload length of {MAX_PAYLOAD_LEN} bytes")]
    PayloadTooLarge,
}

impl From<RecordBuildError> for io::Error {
    fn from(error: RecordBuildError) -> Self {
        // Serializers expect io::Write errors. Preserve the typed cause for
        // inspection while identifying this as invalid input, not failed I/O.
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

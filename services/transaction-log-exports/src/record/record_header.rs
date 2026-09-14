use getset::CopyGetters;

use super::RecordId;

/// An owned, read-only snapshot of an already-validated record's header.
///
/// Obtain a snapshot through [`Record::get_header`](super::Record::get_header),
/// then read its native-endian fields through [`Self::length`] and [`Self::id`].
/// The snapshot can outlive the record without retaining its byte buffer.
///
/// Its native memory layout is compiler-selected. Use
/// [`record_protocol::HEADER_LEN`](super::record_protocol::HEADER_LEN) for the
/// serialized size; native padding is never written to the wire or file.
///
/// ```compile_fail,E0616
/// use transaction_log_exports::RecordHeader;
///
/// fn change_length(mut header: RecordHeader) {
///     header.length = 17;
/// }
/// ```
///
/// Construction is restricted to the record module:
///
/// ```compile_fail,E0624
/// use transaction_log_exports::{RecordHeader, RecordId, SequenceNumber, StreamId};
///
/// let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
/// let header = RecordHeader::new(16, id);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
#[getset(get_copy = "pub")]
pub struct RecordHeader {
    /// Total encoded header, body, and CRC trailer length in bytes.
    ///
    /// The value is in `16..=65_535`, as established by record validation.
    length: u16,
    /// The stream and sequence number identifying the record.
    id: RecordId,
}

impl RecordHeader {
    /// Copies an already-validated record's length and identity into a snapshot.
    ///
    /// The caller must supply the validated encoded length in `16..=65_535` and
    /// the corresponding record ID. Construction stays within the record module
    /// so the hot path can reuse those guarantees without allocation or repeated
    /// validation.
    #[inline]
    pub(super) const fn new(length: u16, id: RecordId) -> Self {
        Self { length, id }
    }
}

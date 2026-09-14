use bytes::Bytes;

use super::record_protocol as protocol;
use super::{RecordHeader, RecordId, SequenceNumber, StreamId};

/// A completed, immutable record that owns a shared handle to its encoded bytes.
///
/// [`RecordReader`](crate::RecordReader) validates the encoded length, stream ID,
/// and CRC before constructing a record. Accessors rely on that guarantee and
/// perform no further integrity validation.
///
/// Cloning shares the byte storage. A record can outlive the receive buffer it
/// was split from, though a small record may retain a larger backing allocation.
///
/// See [`record_protocol`](super::record_protocol) for the wire and file format.
/// [`RecordHeader`] is a decoded value, not a view cast onto the encoded bytes.
/// Use [`protocol::HEADER_LEN`] for the encoded header length, rather than
/// deriving the wire format from `size_of::<RecordHeader>()`.
#[derive(Debug, Clone)]
pub struct Record {
    // Invariant: immutable bytes contain exactly one record, with a size in
    // protocol::MIN_RECORD_LEN..=protocol::MAX_RECORD_LEN, a length equal to
    // bytes.len(), a stream ID in 0..StreamId::COUNT, and a verified CRC-32C
    // trailer covering the header and body.
    bytes: Bytes,
}

impl Record {
    /// Wraps the reader's fully validated frame without repeating its checks.
    ///
    /// # Safety
    ///
    /// `bytes` must contain exactly one complete record: a byte length in
    /// [`protocol::MIN_RECORD_LEN`]..=[`protocol::MAX_RECORD_LEN`], its
    /// little-endian length equal to `bytes.len()`, a stream ID in
    /// `0..StreamId::COUNT`, and a correct CRC-32C trailer over all preceding bytes.
    /// Getters rely on these guarantees, including for unchecked byte access.
    pub(crate) unsafe fn from_validated_bytes(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Decodes all header fields into an independently owned value.
    ///
    /// The snapshot retains no byte storage and can outlive this record. Reuse it
    /// when several field accesses are needed. Length comes from the validated
    /// byte handle; identity fields are decoded without allocation or validation.
    #[inline]
    pub fn get_header(&self) -> RecordHeader {
        RecordHeader::new(self.length(), self.id())
    }

    /// Returns the encoded length including the header, body, and CRC trailer.
    ///
    /// The result is in `16..=65_535`. This reads the byte handle's length; the
    /// reader has already established equality with the encoded length field.
    #[inline]
    pub fn length(&self) -> u16 {
        // The reader guarantees this slice's length equals the encoded u16 length.
        self.bytes.len() as u16
    }

    /// Returns the owned stream-and-sequence pair identifying this record.
    ///
    /// This decodes the two identity fields without retaining storage or
    /// checking stream sequence continuity. See [`Self::get_header`] to also
    /// obtain the total encoded length in one owned snapshot.
    #[inline]
    pub fn id(&self) -> RecordId {
        RecordId::new(self.stream_id(), self.sequence_number())
    }

    /// Returns the validated logical stream ID without repeating its range check.
    ///
    /// The immutable encoded field was validated before this record was created.
    /// Its address need not be aligned to the native integer type.
    #[inline]
    pub fn stream_id(&self) -> StreamId {
        // SAFETY: This immutable record has a fully validated header.
        let header = unsafe { protocol::read::header_unchecked(&self.bytes) };
        let value = protocol::read::stream_id(header);
        // SAFETY: RecordReader validated this field before freezing the batch.
        // Record owns immutable bytes, so the ID cannot have changed since.
        unsafe { StreamId::new_unchecked(value) }
    }

    /// Returns the stream-local sequence number encoded in this record.
    ///
    /// Every raw `u64` value is representable. Acceptance as the next sequence in
    /// a stream is a separate, stateful decision made by ingestion or replay.
    #[inline]
    pub fn sequence_number(&self) -> SequenceNumber {
        // SAFETY: This immutable record has a fully validated header.
        let header = unsafe { protocol::read::header_unchecked(&self.bytes) };
        SequenceNumber::new(protocol::read::sequence_number(header))
    }

    /// Reads the stored checksum from the trailer without computing or verifying it.
    ///
    /// The reader already verified CRC-32C over the complete header and payload.
    /// The checksum is returned as a native-endian value, with no storage retained.
    #[inline]
    pub fn crc(&self) -> u32 {
        // SAFETY: The reader validated the complete immutable frame, including
        // enough bytes for the trailer. No length or CRC check is repeated here.
        unsafe { protocol::read::crc(&self.bytes) }
    }

    /// Borrows the opaque payload, excluding the header and CRC trailer.
    ///
    /// The slice may be empty and contains at most
    /// [`protocol::MAX_PAYLOAD_LEN`] bytes. Its lifetime is tied to this record.
    /// No payload bytes are copied or interpreted.
    #[inline]
    pub fn body(&self) -> &[u8] {
        &self.bytes[protocol::HEADER_LEN..self.bytes.len() - protocol::CRC_LEN]
    }

    /// Borrows the entire encoded record, suitable for sending or storing.
    /// Clone this `Bytes` handle to retain or slice the storage independently.
    #[inline]
    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the underlying byte handle without copying or cloning it.
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

/// Borrows the complete encoding for APIs accepting a byte slice.
///
/// Unlike [`Record::body`], the result includes the header and CRC trailer.
/// Unlike cloning [`Record::as_bytes`], borrowing does not retain storage
/// independently of the record.
impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{Record, RecordHeader, RecordId, SequenceNumber, StreamId};
    use crate::record::record_protocol::test_data::{FRAME, encode_frame, reference_crc};
    use bytes::Bytes;

    #[test]
    fn exposes_encoded_fields_and_an_independently_owned_header() {
        let record = fixture_record();
        let pointer = record.as_bytes().as_ptr();

        assert_eq!(record.length(), 21);
        assert_eq!(record.stream_id(), StreamId::new(0x0708).unwrap());
        assert_eq!(
            record.sequence_number(),
            SequenceNumber::new(0x1112_1314_1516_1718)
        );
        assert_eq!(record.crc(), 0x8a51_c744);
        assert_eq!(record.body(), b"hello");
        assert_eq!(record.as_ref(), FRAME);
        assert_eq!(record.body().as_ptr().addr(), pointer.addr() + 12);

        let header = record.get_header();
        assert_eq!(header.id(), record.id());
        let bytes = record.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(bytes.as_ref(), FRAME);
        drop(bytes);
        assert_eq!(header.length(), 21);
        assert_eq!(header.id().stream_id(), StreamId::new(0x0708).unwrap());
        assert_eq!(header.id().sequence_number().get(), 0x1112_1314_1516_1718);
    }

    #[test]
    fn clones_and_retained_byte_handles_share_storage_and_outlive_the_record() {
        let record = fixture_record();
        let clone = record.clone();
        let bytes = record.as_bytes().clone();
        assert_eq!(clone.as_bytes().as_ptr(), record.as_bytes().as_ptr());
        assert_eq!(bytes.as_ptr(), record.as_bytes().as_ptr());
        drop(record);
        assert_eq!(clone.body(), b"hello");
        assert_eq!(clone.crc(), 0x8a51_c744);
        drop(clone);
        assert_eq!(bytes.as_ref(), FRAME);
    }

    #[test]
    fn reads_all_fields_at_every_byte_alignment() {
        // Consecutive 21-byte frames visit all eight address residues, regardless
        // of the allocation's initial alignment. No reader or realignment is used.
        let storage = Bytes::from(FRAME.repeat(8));
        let mut alignments = [false; 8];
        let expected_id = RecordId::new(
            StreamId::new(0x0708).unwrap(),
            SequenceNumber::new(0x1112_1314_1516_1718),
        );
        for index in 0..8 {
            let bytes = storage.slice(index * FRAME.len()..(index + 1) * FRAME.len());
            let pointer = bytes.as_ptr();
            alignments[pointer.addr() % 8] = true;
            // SAFETY: This exact slice is the known valid 21-byte fixture,
            // including its supported stream ID and independently verified CRC.
            let record = unsafe { Record::from_validated_bytes(bytes) };
            assert_eq!(record.length(), 21);
            assert_eq!(record.stream_id(), expected_id.stream_id());
            assert_eq!(record.sequence_number(), expected_id.sequence_number());
            assert_eq!(record.id(), expected_id);
            assert_eq!(record.get_header(), RecordHeader::new(21, expected_id));
            assert_eq!(record.crc(), 0x8a51_c744);
            assert_eq!(record.body(), b"hello");
            assert_eq!(record.as_ref(), FRAME);
            assert_eq!(record.as_bytes().as_ptr(), pointer);
            assert_eq!(record.body().as_ptr().addr(), pointer.addr() + 12);
        }
        assert!(alignments.into_iter().all(|seen| seen));
    }

    #[test]
    fn exposes_empty_and_large_payloads_and_identifier_boundaries() {
        for body_len in [0, 1, 7, 8, 9, 255, 256, 65_518, 65_519] {
            let body: Vec<u8> = (0..body_len).map(|index| index as u8).collect();
            for id in [
                RecordId::new(StreamId::MIN, SequenceNumber::MIN),
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
            ] {
                let frame = encode_frame(id, &body);
                let crc = reference_crc(&frame[..12 + body_len]);
                // SAFETY: The independent encoder produces an exact frame with
                // typed IDs and a valid CRC; these inputs are at most 65,535 bytes.
                let record = unsafe { Record::from_validated_bytes(frame) };
                assert_eq!(record.length(), 16 + body_len as u16);
                assert_eq!(record.stream_id(), id.stream_id());
                assert_eq!(record.sequence_number(), id.sequence_number());
                assert_eq!(record.id(), id);
                assert_eq!(
                    record.get_header(),
                    RecordHeader::new(16 + body_len as u16, id)
                );
                assert_eq!(record.body(), body);
                assert_eq!(record.crc(), crc);
            }
        }
    }

    fn fixture_record() -> Record {
        // SAFETY: FRAME is a complete, known valid 21-byte frame with a supported
        // stream ID and a known CRC. Copy it so lifetime tests own an allocation.
        unsafe { Record::from_validated_bytes(Bytes::copy_from_slice(FRAME)) }
    }
}

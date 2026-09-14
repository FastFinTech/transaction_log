//! Encoded record layout and codecs shared by records, readers, and writers.
//!
//! All integers are little-endian, with no padding. The CRC-32C trailer covers
//! the complete header and body. These helpers interpret bytes; RecordReader
//! owns framing, stream ID validation, and checksum verification.
//!
//! See the parent [`record`](super) module for the maintained wire/file contract,
//! layout table, ownership requirements, and hot-path rationale. The constants
//! here define production byte sizes. The inline `read` and `write` modules
//! group crate-private operations by direction; shared test fixtures stay at this
//! module's root. Their contracts and rationale are covered by the parent README.
//!
//! The decoded Rust types can contain alignment padding. Use these encoded
//! sizes when interpreting bytes, rather than `size_of` on those types.

const LENGTH_LEN: usize = 2;
const STREAM_ID_LEN: usize = 2;
const SEQUENCE_NUMBER_LEN: usize = 8;

const LENGTH_OFFSET: usize = 0;
const STREAM_ID_OFFSET: usize = LENGTH_OFFSET + LENGTH_LEN;
const SEQUENCE_NUMBER_OFFSET: usize = STREAM_ID_OFFSET + STREAM_ID_LEN;

/// Encoded header size, independent of the decoded Rust struct's layout.
/// The 12 bytes contain total length, stream ID, and sequence number.
pub const HEADER_LEN: usize = SEQUENCE_NUMBER_OFFSET + SEQUENCE_NUMBER_LEN;
/// Size of the trailing little-endian CRC-32C field.
/// These four bytes are excluded from checksum computation.
pub const CRC_LEN: usize = 4;
/// Minimum encoded record size: a header and CRC trailer, with an empty body.
/// Lengths below these 16 bytes are rejected by the reader.
pub const MIN_RECORD_LEN: usize = HEADER_LEN + CRC_LEN;
/// Maximum encoded record size, imposed by the two-byte length field.
/// This is a fixed 65,535-byte limit per record, not per buffer or batch.
pub const MAX_RECORD_LEN: usize = u16::MAX as usize;
/// Maximum payload size after accounting for the header and CRC trailer.
/// A writer must reject payloads above these 65,519 bytes before narrowing length.
pub const MAX_PAYLOAD_LEN: usize = MAX_RECORD_LEN - MIN_RECORD_LEN;

/// Reads shared by immutable records and input validation.
///
/// Field decoders return raw values; RecordReader owns framing, stream-ID checks,
/// and comparison of stored and computed checksums. Unsafe entry points rely on
/// byte extents already established by their callers, without repeating checks.
pub(crate) mod read {
    use super::{CRC_LEN, HEADER_LEN, LENGTH_OFFSET, SEQUENCE_NUMBER_OFFSET, STREAM_ID_OFFSET};

    /// Decodes total encoded length without checking the protocol minimum or extent.
    /// The complete header array proves only that the field's bytes are available.
    #[inline]
    pub(crate) fn length(header: &[u8; HEADER_LEN]) -> u16 {
        u16::from_le_bytes(read_field(header, LENGTH_OFFSET))
    }

    /// Decodes the raw stream ID without applying the domain range restriction.
    /// The reader validates it before record getters can wrap it as a typed ID.
    #[inline]
    pub(crate) fn stream_id(header: &[u8; HEADER_LEN]) -> u16 {
        u16::from_le_bytes(read_field(header, STREAM_ID_OFFSET))
    }

    /// Decodes the raw stream-local sequence; every `u64` bit pattern is representable.
    /// This helper has no per-stream state and does not check sequence continuity.
    #[inline]
    pub(crate) fn sequence_number(header: &[u8; HEADER_LEN]) -> u64 {
        u64::from_le_bytes(read_field(header, SEQUENCE_NUMBER_OFFSET))
    }

    /// Decodes the checksum stored in the final bytes without recomputing or comparing it.
    ///
    /// # Safety
    ///
    /// `frame` must contain at least `CRC_LEN` bytes. The caller establishes that
    /// those final bytes are the intended trailer; no semantic validation is done.
    #[inline]
    pub(crate) unsafe fn crc(frame: &[u8]) -> u32 {
        // SAFETY: the caller guarantees the trailer extent. The slice supplies live,
        // initialized bytes; read_unaligned requires no u32 alignment or reference.
        unsafe {
            u32::from_le(
                frame
                    .as_ptr()
                    .add(frame.len() - CRC_LEN)
                    .cast::<u32>()
                    .read_unaligned(),
            )
        }
    }

    /// Computes the checksum over a complete frame, excluding its CRC trailer.
    ///
    /// The caller establishes the frame extent first. The encoded length is not
    /// consulted, and the stored checksum is not compared. Readers use this with
    /// [`crc`] to validate complete input. Writers with an uninitialized trailer
    /// use [`super::write::crc`]; they must not create a slice exposing those
    /// uninitialized bytes.
    ///
    /// # Panics
    ///
    /// Panics if fewer than [`CRC_LEN`] bytes are supplied.
    #[inline]
    pub(crate) fn compute_crc(frame: &[u8]) -> u32 {
        crc32c::crc32c(&frame[..frame.len() - CRC_LEN])
    }

    /// Borrows the fixed-size header without repeating the caller's length check.
    ///
    /// # Safety
    ///
    /// `frame` must contain at least `HEADER_LEN` bytes.
    /// The returned array still contains raw, potentially invalid field values;
    /// borrowing it does not establish any semantic record invariant.
    #[inline]
    pub(crate) unsafe fn header_unchecked(frame: &[u8]) -> &[u8; HEADER_LEN] {
        // SAFETY: The caller guarantees the extent. The slice supplies initialized
        // bytes and the borrow lifetime. A u8 array has alignment 1 and accepts all
        // bit patterns; no reference to a potentially unaligned native header is made.
        unsafe { &*frame.as_ptr().cast::<[u8; HEADER_LEN]>() }
    }

    /// Extracts a field using protocol constants; inlined constant bounds allow the
    /// optimizer to eliminate slicing and conversion checks in release getters.
    ///
    /// # Panics
    ///
    /// Panics if `offset..offset + N` is outside the header. Callers use fixed valid
    /// offsets and widths; neither is supplied by untrusted input.
    #[inline]
    fn read_field<const N: usize>(header: &[u8; HEADER_LEN], offset: usize) -> [u8; N] {
        header[offset..offset + N].try_into().unwrap()
    }
}

/// Initializes records in exclusive, possibly uninitialized storage.
///
/// The caller establishes capacity, initialized payload length, and exclusive
/// access. These operations use the shared layout to fill the header and trailer;
/// they do not grow storage, publish buffer length, or perform input validation.
pub(crate) mod write {
    use std::slice;

    use super::{CRC_LEN, LENGTH_OFFSET, SEQUENCE_NUMBER_OFFSET, STREAM_ID_OFFSET};
    use crate::RecordId;

    /// Initializes the final length and identity using the encoded header layout.
    ///
    /// The writer establishes the total length from its bounded body. The typed ID
    /// already carries a valid stream ID; this performs no repeated validation and
    /// does not construct a native `RecordHeader` or copy its memory representation.
    /// Every header byte receives its final value without reading the previous bytes.
    ///
    /// # Safety
    ///
    /// `header` must be valid for writes to `HEADER_LEN` contiguous bytes, with no
    /// overlapping references or concurrent accesses during the call. Those bytes
    /// may be uninitialized, and no integer alignment is required. This writes the
    /// supplied fields without validating length or establishing the frame extent.
    #[inline]
    pub(crate) unsafe fn header(header: *mut u8, length: u16, id: RecordId) {
        // SAFETY: the caller provides the entire writable header. These fixed-width
        // fields cover it exactly; unaligned writes initialize bytes without reading
        // them or forming references to uninitialized or unaligned integers.
        unsafe {
            header
                .add(LENGTH_OFFSET)
                .cast::<u16>()
                .write_unaligned(length.to_le());
            header
                .add(STREAM_ID_OFFSET)
                .cast::<u16>()
                .write_unaligned(id.stream_id().get().to_le());
            header
                .add(SEQUENCE_NUMBER_OFFSET)
                .cast::<u64>()
                .write_unaligned(id.sequence_number().get().to_le());
        }
    }

    /// Computes CRC-32C over the finalized header and payload and initializes its trailer.
    ///
    /// `record_len` includes the four-byte trailer. Its previous contents are neither
    /// read nor included in the checksum. Length comes from the writer's bounded byte
    /// count, not from decoding the header again. This does not validate the frame or
    /// change the owning buffer's length.
    ///
    /// # Safety
    ///
    /// `record_len` must be in `MIN_RECORD_LEN..=MAX_RECORD_LEN`. `record` must cover
    /// that many contiguous bytes in one live allocation: the finalized header and
    /// payload must be initialized and readable, and the final `CRC_LEN` bytes must
    /// be writable but may be uninitialized. The encoded length must equal
    /// `record_len`. No overlapping references or concurrent accesses may be used
    /// during the call. No integer alignment is required.
    #[inline]
    pub(crate) unsafe fn crc(record: *mut u8, record_len: usize) {
        let content_len = record_len - CRC_LEN;
        let crc = {
            // SAFETY: the caller guarantees the initialized header/payload extent in
            // one allocation; the protocol bound also fits isize. This shared slice
            // excludes the potentially uninitialized trailer and ends before the store.
            let content = unsafe { slice::from_raw_parts(record, content_len) };
            crc32c::crc32c(content)
        };
        // SAFETY: the caller provides the four writable trailer bytes immediately
        // after the content. No shared view remains. The unaligned little-endian
        // store initializes the trailer without reading its previous contents.
        unsafe {
            record
                .add(content_len)
                .cast::<u32>()
                .write_unaligned(crc.to_le());
        }
    }
}

#[cfg(test)]
pub(crate) mod test_data {
    use bytes::Bytes;

    use crate::RecordId;

    // Header, "hello", and its independently computed CRC-32C trailer. Shared
    // by record and reader tests without using the production encoding helpers.
    pub(crate) const FRAME: &[u8] = &[
        0x15, 0, 0x08, 0x07, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, b'h', b'e', b'l',
        b'l', b'o', 0x44, 0xc7, 0x51, 0x8a,
    ];

    /// Builds test input from the documented format, without production helpers.
    pub(crate) fn encode_frame(id: RecordId, body: &[u8]) -> Bytes {
        let length = body
            .len()
            .checked_add(16)
            .and_then(|length| u16::try_from(length).ok())
            .expect("record exceeds 65,535 bytes");
        let mut frame = Vec::with_capacity(usize::from(length));
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(&id.stream_id().get().to_le_bytes());
        frame.extend_from_slice(&id.sequence_number().get().to_le_bytes());
        frame.extend_from_slice(body);
        frame.extend_from_slice(&reference_crc(&frame).to_le_bytes());
        Bytes::from(frame)
    }

    /// Independent bitwise CRC-32C reference, shared by all record tests.
    pub(crate) fn reference_crc(bytes: &[u8]) -> u32 {
        let mut crc = u32::MAX;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 == 0 {
                    crc >> 1
                } else {
                    (crc >> 1) ^ 0x82f6_3b78
                };
            }
        }
        !crc
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::slice;

    use super::test_data::{FRAME, encode_frame, reference_crc};
    use super::*;
    use crate::{RecordId, SequenceNumber, StreamId};

    #[test]
    fn decodes_documented_field_sizes_offsets_and_little_endian_values() {
        assert_eq!(HEADER_LEN, 12);
        assert_eq!(CRC_LEN, 4);
        assert_eq!(MIN_RECORD_LEN, 16);
        assert_eq!(MAX_RECORD_LEN, 65_535);
        assert_eq!(MAX_PAYLOAD_LEN, 65_519);

        // Distinct bytes detect swapped fields, offsets, byte order and truncation.
        // Raw protocol decoding accepts all bit patterns, before reader validation.
        let header = [
            0x02, 0x01, 0xef, 0xbe, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
        ];
        assert_eq!(read::length(&header), 0x0102);
        assert_eq!(read::stream_id(&header), 0xbeef);
        assert_eq!(read::sequence_number(&header), 0x1112_1314_1516_1718);
        // SAFETY: all four checksum bytes are present and initialized.
        assert_eq!(unsafe { read::crc(&[0x78, 0x56, 0x34, 0x12]) }, 0x1234_5678);

        for (byte, length, stream_id, sequence, crc) in [
            (0, 0, 0, 0, 0),
            (0xff, u16::MAX, u16::MAX, u64::MAX, u32::MAX),
        ] {
            let header = [byte; 12];
            assert_eq!(read::length(&header), length);
            assert_eq!(read::stream_id(&header), stream_id);
            assert_eq!(read::sequence_number(&header), sequence);
            // SAFETY: all four checksum bytes are present and initialized.
            assert_eq!(unsafe { read::crc(&[byte; 4]) }, crc);
        }
    }

    #[test]
    fn encodes_header_using_documented_field_offsets_and_byte_order() {
        for (length, id, expected) in [
            (
                16,
                RecordId::new(StreamId::MIN, SequenceNumber::MIN),
                [16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                65_535,
                RecordId::new(StreamId::MAX, SequenceNumber::MAX),
                [
                    0xff, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ],
            ),
            (
                0x1234,
                RecordId::new(
                    StreamId::new(0x0708).unwrap(),
                    SequenceNumber::new(0x1112_1314_1516_1718),
                ),
                [
                    0x34, 0x12, 0x08, 0x07, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
                ],
            ),
        ] {
            for offset in 0..8 {
                let mut storage = [MaybeUninit::new(0xa5_u8); 27];
                storage[offset..offset + 12].fill(MaybeUninit::uninit());
                // SAFETY: this pointer covers the 12 writable header bytes inside
                // storage, with no active views. The header is deliberately
                // uninitialized; prefix and suffix guards must remain unchanged.
                unsafe {
                    write::header(storage.as_mut_ptr().cast::<u8>().add(offset), length, id);
                }
                // SAFETY: write::header initialized all 12 header bytes. Every
                // other byte was initialized as a guard before the call.
                let bytes = storage.map(|byte| unsafe { byte.assume_init() });
                assert_eq!(&bytes[..offset], &[0xa5; 7][..offset]);
                assert_eq!(&bytes[offset..offset + 12], &expected);
                assert!(bytes[offset + 12..].iter().all(|&byte| byte == 0xa5));
            }
        }
    }

    #[test]
    fn initializes_crc_in_uninitialized_trailers_at_every_alignment() {
        for body_len in [0, 1, 7, 8, 9, 65_519] {
            let body: Vec<u8> = (0..body_len).map(|index| index as u8).collect();
            let frame = encode_frame(RecordId::new(StreamId::MAX, SequenceNumber::MAX), &body);
            let content_len = frame.len() - 4;
            for offset in 0..8 {
                let mut storage = vec![MaybeUninit::new(0xa5_u8); offset + frame.len() + 8];
                for (destination, &byte) in storage[offset..offset + content_len]
                    .iter_mut()
                    .zip(&frame[..content_len])
                {
                    destination.write(byte);
                }
                storage[offset + content_len..offset + frame.len()].fill(MaybeUninit::uninit());
                // SAFETY: independent encoding supplies a finalized header and
                // initialized payload with a matching length in protocol bounds.
                // The full allocation pointer permits writing the four trailing
                // bytes, which are deliberately uninitialized. No view is active.
                unsafe {
                    write::crc(storage.as_mut_ptr().cast::<u8>().add(offset), frame.len());
                }
                // SAFETY: header, payload, and guards were initialized above;
                // write::crc initialized the only remaining bytes, its trailer.
                let bytes =
                    unsafe { slice::from_raw_parts(storage.as_ptr().cast::<u8>(), storage.len()) };
                assert!(bytes[..offset].iter().all(|&byte| byte == 0xa5));
                assert_eq!(&bytes[offset..offset + frame.len()], frame.as_ref());
                assert_eq!(&bytes[offset + frame.len()..], &[0xa5; 8]);
            }
        }
    }

    #[test]
    fn computes_crc_for_known_frames_and_binary_payloads() {
        assert_eq!(reference_crc(b""), 0);
        assert_eq!(reference_crc(b"123456789"), 0xe306_9283);
        assert_eq!(reference_crc(&FRAME[..17]), 0x8a51_c744);
        assert_eq!(read::compute_crc(FRAME), 0x8a51_c744);
        assert_eq!(
            encode_frame(
                RecordId::new(
                    StreamId::new(0x0708).unwrap(),
                    SequenceNumber::new(0x1112_1314_1516_1718),
                ),
                b"hello",
            )
            .as_ref(),
            FRAME
        );

        for body_len in [0, 1, 7, 8, 9, 255, 256, 65_518, 65_519] {
            let body: Vec<u8> = (0..body_len).map(|index| index as u8).collect();
            let frame = encode_frame(RecordId::new(StreamId::MAX, SequenceNumber::MAX), &body);
            let expected = reference_crc(&frame[..12 + body_len]);
            assert_eq!(read::compute_crc(&frame), expected);
        }
    }

    #[test]
    fn crc_covers_every_header_and_body_byte_and_excludes_the_trailer() {
        for offset in 0..FRAME.len() {
            for bit in 0..8 {
                let mut altered = FRAME.to_vec();
                altered[offset] ^= 1 << bit;
                // This tests checksum coverage over a known extent, even when
                // the altered bytes would fail the reader's header validation.
                let computed = read::compute_crc(&altered);
                if offset < 17 {
                    assert_ne!(computed, 0x8a51_c744, "offset {offset}, bit {bit}");
                } else {
                    assert_eq!(computed, 0x8a51_c744, "offset {offset}, bit {bit}");
                }
            }
        }
    }
    #[test]
    #[should_panic(expected = "record exceeds 65,535 bytes")]
    fn test_encoder_rejects_oversized_payload_before_narrowing_the_length() {
        let body = vec![0; 65_520];
        encode_frame(RecordId::new(StreamId::MIN, SequenceNumber::MIN), &body);
    }
}

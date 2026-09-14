#![doc = include_str!("README.md")]
#![deny(missing_docs)]

#[allow(clippy::module_inception)] // Keep Record in its own type-named file.
mod record;
mod record_header;
mod record_id;
pub mod record_protocol;
mod sequence_number;
mod stream_id;
mod stream_id_error;

pub use record::Record;
pub use record_header::RecordHeader;
pub use record_id::RecordId;
pub use sequence_number::SequenceNumber;
pub use stream_id::StreamId;
pub use stream_id_error::StreamIdError;

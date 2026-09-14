//! Shared public types and APIs for the Transaction Log service.

pub mod record;
pub mod record_reader;
pub mod record_writer;

pub use record::{Record, RecordHeader, RecordId, SequenceNumber, StreamId, StreamIdError};
pub use record_reader::{RecordReadError, RecordReader};
pub use record_writer::{RecordBuildError, RecordWriteError, RecordWriter};

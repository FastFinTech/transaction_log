#![doc = include_str!("../README.md")]

pub mod record;
pub mod record_reader;
pub mod record_writer;

pub use record::{
    Record, RecordHeader, RecordId, RecordLength, RecordLengthError, SequenceNumber, StreamId,
    StreamIdError,
};
pub use record_reader::{RecordReadError, RecordReader};
pub use record_writer::{
    AsyncSyncData, ExistingRecords, RecordBuildError, RecordBuilder, RecordOutputError,
    RecordWriteError, RecordWriter, SerializeRecords,
};

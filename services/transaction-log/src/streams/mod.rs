#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod index_writer;
mod indexed_log_validator;
mod indexed_log_writer;
pub mod location;
mod stream_checkpoint;

pub use index_writer::{IndexWriteError, IndexWriter};
pub use indexed_log_validator::{
    CompletedIndexedLog, IndexedLogValidator, LogTailError, LogValidationError, LogValidationReport,
};
pub use indexed_log_writer::{IndexedLogWriteError, IndexedLogWriter};
pub use location::{
    LogFileId, LogFileNumber, LogFileNumberError, LogFileRange, RECORDS_PER_FILE,
    RecordEndLocation, RecordRangeLocation, RecordRangeLocationError, RecordStartLocation,
};
pub use stream_checkpoint::StreamCheckpoint;

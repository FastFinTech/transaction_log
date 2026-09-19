#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod index_writer;
mod indexed_log_writer;
pub mod location;
mod stream_checkpoint;
pub mod validation;

pub use index_writer::{IndexWriteError, IndexWriter};
pub use indexed_log_writer::{IndexedLogWriteError, IndexedLogWriter};
pub use location::{
    LogFileId, LogFileIdRangeError, LogFileNumber, LogFileNumberError, LogFileRange,
    RECORDS_PER_FILE, RecordEndLocation, RecordRangeLocation, RecordRangeLocationError,
    RecordStartLocation,
};
pub use stream_checkpoint::StreamCheckpoint;
pub use validation::{
    IndexFileValidationError, IndexFileValidator, IndexedLogValidationError, IndexedLogValidator,
    LogFileValidationError, LogFileValidator, LogTailError, ValidatedFilePair, ValidatedLogFile,
    ValidationFile,
};

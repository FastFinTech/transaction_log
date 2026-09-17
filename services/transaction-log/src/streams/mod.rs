#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod index_writer;
mod indexed_log_validator;
mod indexed_log_writer;

pub use index_writer::{IndexWriteError, IndexWriter};
pub use indexed_log_validator::{
    CompletedIndexedLog, IndexedLogValidator, LogTailError, LogValidationError, LogValidationReport,
};
pub use indexed_log_writer::{IndexedLogWriteError, IndexedLogWriter};

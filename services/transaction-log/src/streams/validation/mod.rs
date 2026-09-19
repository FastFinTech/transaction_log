#![doc = include_str!("README.md")]

mod index_file_validator;
mod indexed_log_validator;
mod log_file_validator;
mod validation_file;

#[cfg(test)]
mod test_file;

pub use index_file_validator::{IndexFileValidationError, IndexFileValidator};
pub use indexed_log_validator::{
    IndexedLogValidationError, IndexedLogValidator, ValidatedFilePair,
};
pub use log_file_validator::{
    LogFileValidationError, LogFileValidator, LogTailError, ValidatedLogFile,
};
pub use validation_file::ValidationFile;

#![doc = include_str!("README.md")]

mod log_file_validation_error;
#[allow(clippy::module_inception)] // Keep LogFileValidator in its own type-named file.
mod log_file_validator;
mod log_tail_error;
mod validated_log_file;

pub use log_file_validation_error::LogFileValidationError;
pub use log_file_validator::LogFileValidator;
pub use log_tail_error::LogTailError;
pub use validated_log_file::ValidatedLogFile;

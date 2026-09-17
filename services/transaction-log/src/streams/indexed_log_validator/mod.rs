#![doc = include_str!("README.md")]

mod completed_indexed_log;
#[allow(clippy::module_inception)] // Keep IndexedLogValidator in its own type-named file.
mod indexed_log_validator;
mod log_tail_error;
mod log_validation_error;
mod log_validation_report;

pub use completed_indexed_log::CompletedIndexedLog;
pub use indexed_log_validator::IndexedLogValidator;
pub use log_tail_error::LogTailError;
pub use log_validation_error::LogValidationError;
pub use log_validation_report::LogValidationReport;

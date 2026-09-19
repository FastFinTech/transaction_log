#![doc = include_str!("README.md")]

mod indexed_log_validation_error;
#[allow(clippy::module_inception)] // Keep IndexedLogValidator in its own type-named file.
mod indexed_log_validator;
mod validated_file_pair;

pub use indexed_log_validation_error::IndexedLogValidationError;
pub use indexed_log_validator::IndexedLogValidator;
pub use validated_file_pair::ValidatedFilePair;

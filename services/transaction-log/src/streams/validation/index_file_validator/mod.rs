#![doc = include_str!("README.md")]

mod index_file_validation_error;
#[allow(clippy::module_inception)] // Keep IndexFileValidator in its own type-named file.
mod index_file_validator;

pub use index_file_validation_error::IndexFileValidationError;
pub use index_file_validator::IndexFileValidator;

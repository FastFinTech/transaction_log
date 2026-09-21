#![doc = include_str!("README.md")]

mod record_read_error;
#[allow(clippy::module_inception)] // Keep RecordReader in its own type-named file.
mod record_reader;

pub use record_read_error::RecordReadError;
pub use record_reader::RecordReader;

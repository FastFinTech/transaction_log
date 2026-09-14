#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod record_build_error;
mod record_builder;
mod record_write_error;
#[allow(clippy::module_inception)] // Keep RecordWriter in its own type-named file.
mod record_writer;

pub use record_build_error::RecordBuildError;
pub use record_write_error::RecordWriteError;
pub use record_writer::RecordWriter;

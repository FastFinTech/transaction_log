#![doc = include_str!("README.md")]

mod indexed_log_write_error;
#[allow(clippy::module_inception)] // Keep IndexedLogWriter in its own type-named file.
mod indexed_log_writer;
mod indexed_log_writer_state;

pub use indexed_log_write_error::IndexedLogWriteError;
pub use indexed_log_writer::IndexedLogWriter;

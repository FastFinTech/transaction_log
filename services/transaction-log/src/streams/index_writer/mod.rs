#![doc = include_str!("README.md")]

mod index_write_error;
#[allow(clippy::module_inception)] // Keep IndexWriter in its own type-named file.
mod index_writer;

pub use index_write_error::IndexWriteError;
pub use index_writer::IndexWriter;

pub(super) use index_writer::INDEX_ENTRY_LEN;

#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod async_sync_data;
mod record_build_error;
mod record_builder;
mod record_output_error;
mod record_write_error;
#[allow(clippy::module_inception)] // Keep RecordWriter in its own type-named file.
mod record_writer;

pub use async_sync_data::AsyncSyncData;
pub use record_build_error::RecordBuildError;
pub use record_builder::RecordBuilder;
pub use record_output_error::RecordOutputError;
pub use record_write_error::RecordWriteError;
pub use record_writer::{ExistingRecords, RecordWriter, SerializeRecords};

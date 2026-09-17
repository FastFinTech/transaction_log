#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod log_file_id;
mod log_file_number;
mod log_file_number_error;
mod log_file_range;
mod record_end_location;
mod record_range_location;
mod record_range_location_error;
mod record_start_location;
mod storage_config;
mod storage_provider;
mod storage_provider_error;
mod stream_checkpoint;

pub use log_file_id::LogFileId;
pub use log_file_number::{LogFileNumber, RECORDS_PER_FILE};
pub use log_file_number_error::LogFileNumberError;
pub use log_file_range::LogFileRange;
pub use record_end_location::RecordEndLocation;
pub use record_range_location::RecordRangeLocation;
pub use record_range_location_error::RecordRangeLocationError;
pub use record_start_location::RecordStartLocation;
pub use storage_config::StorageConfig;
pub use storage_provider::StorageProvider;
pub use storage_provider_error::StorageProviderError;
pub use stream_checkpoint::StreamCheckpoint;

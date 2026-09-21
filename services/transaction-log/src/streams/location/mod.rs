#![doc = include_str!("README.md")]

mod log_file_id;
mod log_file_id_range_error;
mod log_file_number;
mod log_file_number_error;
mod log_file_position;
mod log_file_range;
mod log_file_record_index_error;
mod record_end_location;
mod record_range_location;
mod record_range_location_error;
mod record_start_location;

pub use log_file_id::LogFileId;
pub use log_file_id_range_error::LogFileIdRangeError;
pub use log_file_number::{LogFileNumber, RECORDS_PER_FILE};
pub use log_file_number_error::LogFileNumberError;
pub use log_file_position::LogFilePosition;
pub use log_file_range::LogFileRange;
pub(crate) use log_file_record_index_error::LogFileRecordIndexError;
pub use record_end_location::RecordEndLocation;
pub use record_range_location::RecordRangeLocation;
pub use record_range_location_error::RecordRangeLocationError;
pub use record_start_location::RecordStartLocation;

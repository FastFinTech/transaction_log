#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod bounded_frame;
mod message_read_error;
mod message_reader;
mod message_write_error;
mod message_writer;
mod protocol;

pub use message_read_error::MessageReadError;
pub use message_reader::MessageReadExt;
pub use message_write_error::MessageWriteError;
pub use message_writer::MessageWriteExt;
pub use protocol::{LENGTH_PREFIX_LEN, MAX_MESSAGE_BODY_LEN};

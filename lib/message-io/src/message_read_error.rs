use std::io;

use thiserror::Error;

use crate::MAX_MESSAGE_BODY_LEN;

/// Failure to receive and decode exactly one framed message.
#[derive(Debug, Error)]
pub enum MessageReadError {
    /// Clean EOF before any byte of the next prefix was consumed.
    #[error("end of message stream")]
    EndOfStream,
    /// Original transport error; truncation uses `UnexpectedEof`.
    #[error("message read failed: {0}")]
    Io(#[from] io::Error),
    /// The advertised body length exceeds the limit; its body was not read.
    #[error("message body length {length} exceeds {MAX_MESSAGE_BODY_LEN} bytes")]
    TooLarge {
        /// Rejected length from the wire prefix.
        length: u32,
    },
    /// Original Postcard error from the complete received body.
    #[error("message decoding failed: {0}")]
    Decode(#[source] postcard::Error),
    /// Decoding left bytes inside the frame, instead of consuming exactly one value.
    #[error("message contains {count} trailing bytes")]
    TrailingBytes {
        /// Number of unconsumed body bytes.
        count: usize,
    },
}

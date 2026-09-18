use std::io;

use thiserror::Error;

use crate::MAX_MESSAGE_BODY_LEN;

/// Failure to encode or write one complete framed message.
#[derive(Debug, Error)]
pub enum MessageWriteError {
    /// Encoding attempted to exceed the body limit; no destination bytes were written.
    #[error("message body exceeds {MAX_MESSAGE_BODY_LEN} bytes")]
    TooLarge,
    /// Original Postcard serialization error; no destination bytes were written.
    #[error("message encoding failed: {0}")]
    Encode(#[source] postcard::Error),
    /// Original destination error; an unknown partial frame may have been accepted.
    #[error("message write failed: {0}")]
    Io(#[from] io::Error),
}

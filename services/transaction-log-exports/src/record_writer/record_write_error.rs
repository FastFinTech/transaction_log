use thiserror::Error;

use super::RecordBuildError;

/// A record rejected before it could be appended to a writer's completed bytes.
///
/// Earlier records and buffer capacity are retained, and the writer can be used
/// again immediately. A builder error takes precedence over a callback error
/// from the same invocation, so exceeding the payload limit is reported
/// consistently whether the callback propagates or ignores the failed write.
/// These errors do not describe socket output or remote acceptance.
#[derive(Debug, Error)]
pub enum RecordWriteError<E> {
    /// The body-writing callback failed independently of a rejected buffer write.
    #[error("record body callback failed: {0}")]
    BodyWrite(#[source] E),
    /// Record construction rejected the payload, including an ignored write error.
    #[error(transparent)]
    Build(#[from] RecordBuildError),
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use super::RecordWriteError;

    #[test]
    fn retains_the_typed_callback_error_as_its_source() {
        let error = RecordWriteError::BodyWrite(io::Error::other("invalid body"));
        assert_eq!(
            error.to_string(),
            "record body callback failed: invalid body"
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::Other
        );
    }
}

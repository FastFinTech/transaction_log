use thiserror::Error;

use super::{RecordBuildError, RecordOutputError};

/// Failure to serialize or append one complete record to the buffer.
///
/// Construction failures discard only this record, preserving earlier buffered
/// records and leaving the writer usable. A retained builder error takes
/// precedence over a callback error. `write` performs no I/O; it can report an
/// output error only when a previous output operation left the writer unusable.
/// `E` is the callback's own error type; callers need no library-specific error
/// conversion or boxed serialization error. Callback side effects outside the
/// builder are not rolled back, and a callback panic is not caught by the writer.
#[derive(Debug, Error)]
pub enum RecordWriteError<E> {
    /// The builder rejected a write, even if the callback ignored that error.
    #[error(transparent)]
    Build(#[from] RecordBuildError),
    /// The serialization callback failed; its concrete error is preserved.
    #[error("record serialization failed: {0}")]
    Serialize(#[source] E),
    /// A previous failed, panicking or cancelled send, flush or sync made the
    /// writer unusable; the serialization callback was not invoked.
    #[error(transparent)]
    Output(#[from] RecordOutputError),
}

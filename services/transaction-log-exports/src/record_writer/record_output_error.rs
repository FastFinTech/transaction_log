use std::io;

use thiserror::Error;

/// Failure while sending, flushing, or synchronizing destination data.
///
/// The operation that fails returns the original I/O error. Subsequent operations
/// return `Unusable`, as does output attempted after cancelling an in-progress
/// buffer output, destination flush, or data synchronization. This prevents replay
/// of an accepted prefix or continued writes after uncertain synchronization.
/// The writer does not retain or clone the first error. Callers that need its
/// kind or source must retain the original result; `Unusable` reports only the
/// terminal lifecycle state. Output is not automatically retried or recovered.
#[derive(Debug, Error)]
pub enum RecordOutputError {
    /// The destination reported an I/O error; output or durability may be incomplete.
    #[error("record output failed: {0}")]
    Io(#[from] io::Error),
    /// Earlier buffer output, destination flushing or synchronization failed,
    /// panicked, or was cancelled before completion. No I/O was attempted now.
    #[error("record writer is unusable after incomplete or failed output")]
    Unusable,
}

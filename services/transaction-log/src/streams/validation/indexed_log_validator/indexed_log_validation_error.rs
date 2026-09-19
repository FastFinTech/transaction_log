use thiserror::Error;

use crate::{
    storage::StorageError,
    streams::{IndexFileValidationError, LogFileValidationError},
};

/// Failure opening, recovering or positioning one log/index pair.
///
/// An error returns no validated pair. Earlier changes may already have completed;
/// the underlying storage, log or index error remains available to the caller.
#[derive(Debug, Error)]
pub enum IndexedLogValidationError {
    /// Opening a log or index failed, preserving its path and original cause.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Log validation, tail removal, synchronization or final positioning failed.
    #[error(transparent)]
    Log(#[from] LogFileValidationError),
    /// Index validation, replacement, synchronization or final positioning failed.
    #[error(transparent)]
    Index(#[from] IndexFileValidationError),
}

use std::{
    io,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Provider failure preserving the requested path and concrete underlying cause.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Filesystem operation failed.
    #[error("storage I/O failure at {}: {source}", path.display())]
    Io {
        /// Requested file or directory path.
        path: PathBuf,
        /// Original filesystem error.
        #[source]
        source: io::Error,
    },
    /// JSON encoding or decoding failed, including model deserialization validation.
    #[error("invalid storage JSON at {}: {source}", path.display())]
    Json {
        /// Metadata path.
        path: PathBuf,
        /// Original Serde error.
        #[source]
        source: serde_json::Error,
    },
    /// Metadata exceeds an operation's bounded-read limit.
    #[error("storage metadata at {} exceeds {max_bytes} bytes", path.display())]
    TooLarge {
        /// Rejected metadata path.
        path: PathBuf,
        /// Maximum accepted file size, in bytes.
        max_bytes: u64,
    },
}

impl StorageError {
    pub(super) fn io(path: PathBuf, source: io::Error) -> Self {
        Self::Io { path, source }
    }

    /// Borrows the requested file or directory path for any provider failure.
    pub fn path(&self) -> &Path {
        match self {
            Self::Io { path, .. } | Self::Json { path, .. } | Self::TooLarge { path, .. } => path,
        }
    }
}

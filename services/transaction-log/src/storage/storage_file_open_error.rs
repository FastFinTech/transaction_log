use std::{
    io,
    path::{Path, PathBuf},
};

use thiserror::Error;

/// Failure to open a particular log or index path, preserving its OS error.
#[derive(Debug, Error)]
#[error("failed to open storage file {}: {source}", path.display())]
pub struct StorageFileOpenError {
    path: PathBuf,
    #[source]
    source: io::Error,
}

impl StorageFileOpenError {
    pub(super) fn new(path: PathBuf, source: io::Error) -> Self {
        Self { path, source }
    }

    /// Absolute path requested by the provider.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

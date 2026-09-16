use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// A directory-creation failure during [`StorageProvider::initialize`](super::StorageProvider::initialize).
///
/// The requested directory is available through [`Self::path`]. Its parent may
/// be the component that prevented creation. The underlying [`io::Error`] is
/// preserved as the standard [`std::error::Error::source`] for diagnostics.
#[derive(Debug, Error)]
#[error("failed to create storage directory {}: {source}", path.display())]
pub struct StorageProviderError {
    path: PathBuf,
    #[source]
    source: io::Error,
}

impl StorageProviderError {
    /// Records the directory requested from the filesystem and its failure.
    pub(super) fn new(path: PathBuf, source: io::Error) -> Self {
        Self { path, source }
    }

    /// Borrows the full directory path whose creation was requested.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

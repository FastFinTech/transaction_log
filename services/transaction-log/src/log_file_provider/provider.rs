use std::path::{Path, PathBuf};

/// Holds the root directory for the application's stream log files.
///
/// This is a scaffold: constructing a provider only stores the path.
/// File naming, opening, and other filesystem operations are not implemented.
#[derive(Debug)]
pub struct LogFileProvider {
    root_directory: PathBuf,
}

impl LogFileProvider {
    /// Takes ownership of the root directory path for stream files.
    ///
    /// The path is stored as supplied, including relative paths. This performs
    /// no filesystem I/O: it neither creates nor validates the directory, and
    /// does not canonicalize the path. Construction is therefore infallible.
    pub fn new(root_directory: PathBuf) -> Self {
        Self { root_directory }
    }

    /// Borrows the configured root directory without allocating or copying it.
    pub fn root_directory(&self) -> &Path {
        &self.root_directory
    }
}

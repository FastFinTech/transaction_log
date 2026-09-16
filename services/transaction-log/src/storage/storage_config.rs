use std::io;
use std::path::{self, Path, PathBuf};

/// Immutable startup configuration owned by a [`StorageProvider`](super::StorageProvider).
///
/// The root is made absolute at construction so later working-directory changes
/// cannot redirect storage. Directory grouping and filename rules are fixed by
/// the provider's storage layout rather than configurable independently per run.
#[derive(Debug)]
pub struct StorageConfig {
    root_directory: PathBuf,
}

impl StorageConfig {
    /// Resolves and owns the storage root without creating or inspecting it.
    ///
    /// Relative paths use the current working directory at this call. Resolution
    /// follows [`std::path::absolute`]; it does not canonicalize symlinks, require
    /// an existing directory, or establish permission to write there. Native path
    /// characters are preserved without converting the root to a UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if the path is empty, otherwise rejected
    /// by absolute-path resolution, or the current directory cannot be obtained.
    pub fn new(root_directory: PathBuf) -> io::Result<Self> {
        Ok(Self {
            root_directory: path::absolute(root_directory)?,
        })
    }

    /// Borrows the absolute storage root without allocation.
    pub fn root_directory(&self) -> &Path {
        &self.root_directory
    }
}

#[cfg(test)]
mod tests {
    use super::StorageConfig;
    use std::path::PathBuf;

    #[test]
    fn resolves_a_relative_root_against_the_current_directory() {
        // Do not change the process-wide working directory in parallel tests.
        let current = std::env::current_dir().unwrap();
        let relative = PathBuf::from("transaction-log-test-root").join("data");
        let config = StorageConfig::new(relative.clone()).unwrap();

        assert!(config.root_directory().is_absolute());
        assert_eq!(config.root_directory(), current.join(relative));
    }

    #[test]
    fn accepts_a_nonexistent_root_without_creating_it() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("log data 東京").join("replica");
        let config = StorageConfig::new(root.clone()).unwrap();

        assert_eq!(config.root_directory(), root);
        assert!(!root.exists());
        assert_eq!(temporary.path().read_dir().unwrap().count(), 0);
    }

    #[test]
    fn rejects_an_empty_root() {
        assert!(StorageConfig::new(PathBuf::new()).is_err());
    }
}

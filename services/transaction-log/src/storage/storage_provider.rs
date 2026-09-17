use std::path::{Path, PathBuf};

use transaction_log_exports::StreamId;

use super::{LogFileId, StorageConfig, StorageProviderError};

/// Owns storage configuration, constructs paths, and initializes stream directories.
///
/// Paths follow the fixed decimal layout documented in this module's README.
/// Path construction is synchronous and performs no filesystem access. Call
/// [`Self::initialize`] during startup to ensure all stream base directories exist.
/// Opening files and creating deeper log/index range directories belong to future
/// file operations; this provider does not yet manage open handles.
///
/// The provider is immutable and can be shared through `Arc<StorageProvider>`.
/// It has no path cache, locks, background tasks or initialization flag.
#[derive(Debug)]
pub struct StorageProvider {
    config: StorageConfig,
}

impl StorageProvider {
    /// Takes ownership of startup configuration without performing filesystem I/O.
    ///
    /// The configuration has already resolved the root to an absolute path.
    /// Construction does not create directories or prove that storage is writable.
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }

    /// Borrows the absolute storage root without allocating or copying it.
    pub fn root_directory(&self) -> &Path {
        self.config.root_directory()
    }

    /// Ensures the root and all supported stream base directories exist.
    ///
    /// Creates missing parents and `streams/0000` through `streams/4095` using
    /// Tokio filesystem operations. Existing directories and their contents are
    /// left intact. No range directories, log, index or checkpoint files are created.
    ///
    /// Call during startup before starting file operations. Repeated calls are
    /// allowed. Failure or cancellation can leave some directories created; a
    /// retry accepts that progress rather than rolling it back. Success does not
    /// validate existing records, claim exclusive ownership, or durably sync
    /// directory entries. Future file operations must still handle I/O failures.
    ///
    /// # Errors
    ///
    /// Returns [`StorageProviderError`] at the first directory that cannot be
    /// created, preserving the requested path and the underlying I/O error.
    pub async fn initialize(&self) -> Result<(), StorageProviderError> {
        for stream_id in StreamId::all() {
            let path = self.stream_directory(stream_id);
            // create_dir_all accepts existing directories and creates missing
            // parents. A separate existence check would add I/O and a race.
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|source| StorageProviderError::new(path, source))?;
        }
        Ok(())
    }

    /// Constructs the absolute `.log` path assigned to a file ID.
    ///
    /// Returns a newly allocated path whether or not it exists and whether or not
    /// initialization has run. This performs no I/O or validation of file contents.
    pub fn log_file_path(&self, id: LogFileId) -> PathBuf {
        self.file_path(id, "log")
    }

    /// Constructs the absolute `.idx` path paired with a file ID's log file.
    ///
    /// The directory and numeric basename match [`Self::log_file_path`]. This
    /// allocates a path but neither creates nor inspects an index file.
    pub fn index_file_path(&self, id: LogFileId) -> PathBuf {
        self.file_path(id, "idx")
    }

    /// Constructs the absolute `checkpoint.json` path for one stream's checkpoint.
    ///
    /// The checkpoint belongs to the stream, so its path stays in the stream base
    /// directory as the last checkpointed record advances across log files. Returns
    /// an owned path without creating directories or loading, writing or validating
    /// checkpoint data. The path is available before initialization.
    pub fn checkpoint_file_path(&self, stream_id: StreamId) -> PathBuf {
        self.stream_directory(stream_id).join("checkpoint.json")
    }

    /// Shared base layout for initialization and all file-path methods.
    fn stream_directory(&self, stream_id: StreamId) -> PathBuf {
        self.root_directory()
            .join("streams")
            .join(format!("{stream_id:04}"))
    }

    /// Keeps directory grouping and numeric filenames identical for log/index pairs.
    fn file_path(&self, id: LogFileId, extension: &str) -> PathBuf {
        let file_number = format!("{:015}", id.file_number());
        let mut path = self.stream_directory(id.stream_id());

        // Valid file numbers fit in 15 ASCII decimal digits. Their first twelve
        // digits form four range directories; the final three select one of up
        // to 1,000 log/index pairs in that leaf. Keep the full number as basename.
        for start in (0..12).step_by(3) {
            path.push(&file_number[start..start + 3]);
        }
        path.push(format!("{file_number}.{extension}"));
        path
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::io;
    use std::path::Path;

    use crate::storage::LogFileNumber;

    use super::{LogFileId, StorageConfig, StorageProvider, StreamId};

    #[test]
    fn paths_match_the_layout_at_every_group_boundary() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("log data 東京").join("replica");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());

        for (number, relative) in [
            (0, "000/000/000/000/000000000000000"),
            (999, "000/000/000/000/000000000000999"),
            (1_000, "000/000/000/001/000000000001000"),
            (1_234, "000/000/000/001/000000000001234"),
            (999_999, "000/000/000/999/000000000999999"),
            (1_000_000, "000/000/001/000/000000001000000"),
            (999_999_999, "000/000/999/999/000000999999999"),
            (1_000_000_000, "000/001/000/000/000001000000000"),
            (999_999_999_999, "000/999/999/999/000999999999999"),
            (1_000_000_000_000, "001/000/000/000/001000000000000"),
            (184_467_440_737_095, "184/467/440/737/184467440737095"),
        ] {
            for (stream, directory) in [(0, "0000"), (42, "0042"), (4095, "4095")] {
                let id = LogFileId::new(
                    StreamId::new(stream).unwrap(),
                    LogFileNumber::new(number).unwrap(),
                );
                let expected = root.join("streams").join(directory).join(relative);
                assert_eq!(provider.log_file_path(id), expected.with_extension("log"));
                assert_eq!(provider.index_file_path(id), expected.with_extension("idx"));
            }
        }

        // Constructors and path queries have no filesystem side effects.
        assert!(!root.exists());
        assert_eq!(temporary.path().read_dir().unwrap().count(), 0);
    }

    #[test]
    fn checkpoint_paths_use_stream_bases_without_filesystem_access() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("log data 東京").join("replica");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());

        for (stream, relative) in [
            (0, "streams/0000/checkpoint.json"),
            (42, "streams/0042/checkpoint.json"),
            (4095, "streams/4095/checkpoint.json"),
        ] {
            assert_eq!(
                provider.checkpoint_file_path(StreamId::new(stream).unwrap()),
                root.join(relative),
            );
        }
        assert!(!root.exists());
        assert_eq!(temporary.path().read_dir().unwrap().count(), 0);
    }

    #[test]
    fn relative_configuration_produces_absolute_paths() {
        let current = std::env::current_dir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new("log data".into()).unwrap());
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);

        assert_eq!(provider.root_directory(), current.join("log data"));
        assert_eq!(
            provider.log_file_path(id),
            current.join("log data/streams/0000/000/000/000/000/000000000000000.log")
        );
        assert_eq!(
            provider.checkpoint_file_path(StreamId::MIN),
            current.join("log data/streams/0000/checkpoint.json"),
        );
    }

    #[cfg(unix)]
    #[test]
    fn preserves_non_utf8_root_components() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary
            .path()
            .join(OsString::from_vec(b"log-\xff".to_vec()));
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);

        assert_eq!(
            provider.log_file_path(id),
            root.join("streams/0000/000/000/000/000/000000000000000.log")
        );
        assert_eq!(
            provider.checkpoint_file_path(StreamId::MIN),
            root.join("streams/0000/checkpoint.json"),
        );
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn initialization_creates_only_stream_bases_and_preserves_existing_contents() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("new").join("replica");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());

        provider.initialize().await.unwrap();

        let streams = root.join("streams");
        assert_eq!(root.read_dir().unwrap().count(), 1);
        assert_eq!(streams.read_dir().unwrap().count(), 4096);
        for stream in 0..4096 {
            let directory = streams.join(format!("{stream:04}"));
            assert!(directory.is_dir());
            assert_eq!(directory.read_dir().unwrap().count(), 0);
        }

        // Simulate files already present on restart. Initialization must neither
        // inspect record validity nor overwrite or remove any existing contents.
        let id = LogFileId::new(
            StreamId::new(42).unwrap(),
            LogFileNumber::new(1_234).unwrap(),
        );
        let log = provider.log_file_path(id);
        let index = provider.index_file_path(id);
        let checkpoint = provider.checkpoint_file_path(id.stream_id());
        assert!(!log.parent().unwrap().exists());
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(&log, b"existing log contents").unwrap();
        fs::write(&index, b"existing index contents").unwrap();
        fs::write(&checkpoint, b"existing checkpoint contents").unwrap();
        let unrelated = root.join("existing-metadata");
        fs::write(&unrelated, b"keep me").unwrap();

        provider.initialize().await.unwrap();

        assert_eq!(fs::read(log).unwrap(), b"existing log contents");
        assert_eq!(fs::read(index).unwrap(), b"existing index contents");
        assert_eq!(
            fs::read(checkpoint).unwrap(),
            b"existing checkpoint contents"
        );
        assert_eq!(fs::read(unrelated).unwrap(), b"keep me");
        assert_eq!(streams.read_dir().unwrap().count(), 4096);
    }

    #[tokio::test]
    async fn initialization_reports_a_stream_collision_and_can_retry_partial_progress() {
        let temporary = tempfile::tempdir().unwrap();
        let provider =
            StorageProvider::new(StorageConfig::new(temporary.path().to_owned()).unwrap());
        let streams = temporary.path().join("streams");
        fs::create_dir(&streams).unwrap();
        let collision = streams.join("0007");
        fs::write(&collision, b"not a directory").unwrap();

        let error = provider.initialize().await.unwrap_err();

        assert_eq!(error.path(), collision);
        let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert!(error.to_string().contains(&collision.display().to_string()));
        assert!(error.to_string().contains(&source.to_string()));
        assert_eq!(fs::read(&collision).unwrap(), b"not a directory");
        for stream in 0..7 {
            assert!(streams.join(format!("{stream:04}")).is_dir());
        }
        assert!(!streams.join("0008").exists());

        fs::remove_file(collision).unwrap();
        provider.initialize().await.unwrap();
        assert_eq!(streams.read_dir().unwrap().count(), 4096);
        assert!(streams.join("4095").is_dir());
    }

    #[tokio::test]
    async fn initialization_reports_parent_collisions_without_overwriting_them() {
        for blocked in [Path::new("replica"), Path::new("replica/streams")] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().join("replica");
            let collision = temporary.path().join(blocked);
            fs::create_dir_all(collision.parent().unwrap()).unwrap();
            fs::write(&collision, b"preserve this file").unwrap();
            let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());

            let error = provider.initialize().await.unwrap_err();

            assert_eq!(error.path(), root.join("streams/0000"));
            assert!(error.source().unwrap().is::<io::Error>());
            assert_eq!(fs::read(collision).unwrap(), b"preserve this file");
        }
    }
}

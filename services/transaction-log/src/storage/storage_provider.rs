use super::{StorageConfig, StorageError};
use crate::clustering::configuration::EstablishedClusteringConfiguration;
use crate::streams::{LogFileId, LogFileNumber, StreamCheckpoint};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use transaction_log_exports::StreamId;

const FILE_NAME: &str = "clustering.json";
const PENDING_FILE_NAME: &str = ".clustering.pending";

/// Owns storage configuration, constructs paths, and initializes stream directories.
///
/// Paths follow the fixed decimal layout documented in this module's README.
/// Path construction is synchronous and performs no filesystem access. Call
/// [`Self::initialize`] during startup to ensure all stream base directories exist.
/// Validation opens existing logs and opens or creates their repairable indexes.
/// The caller owns returned handles; deeper directory creation and file rotation
/// remain owner responsibilities. The provider does not cache open handles.
/// Established clustering configuration is loaded/published directly through
/// Serde. UUID assignment and deployment-configuration comparison belong to
/// application lifecycle policy, not these storage operations.
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
    /// Returns [`StorageError`] at the first directory that cannot be
    /// created, preserving the requested path and the underlying I/O error.
    pub async fn initialize(&self) -> Result<(), StorageError> {
        for stream_id in StreamId::all() {
            let path = self.stream_directory(stream_id);
            // create_dir_all accepts existing directories and creates missing
            // parents. A separate existence check would add I/O and a race.
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|source| StorageError::io(path, source))?;
        }
        Ok(())
    }

    /// Constructs the absolute `.log` path assigned to a file ID.
    ///
    /// Returns a newly allocated path whether or not it exists and whether or not
    /// initialization has run. This performs no I/O or validation of file contents.
    pub fn log_file_path(&self, id: LogFileId) -> PathBuf {
        self.log_or_index_file_path(id, "log")
    }

    /// Constructs the absolute `.idx` path paired with a file ID's log file.
    ///
    /// The directory and numeric basename match [`Self::log_file_path`]. This
    /// allocates a path but neither creates nor inspects an index file.
    pub fn index_file_path(&self, id: LogFileId) -> PathBuf {
        self.log_or_index_file_path(id, "idx")
    }

    /// Finds the highest existing canonical `.log` file for one stream.
    ///
    /// Searches range directories in descending order, backtracking through empty
    /// branches. An absent stream directory or a tree without logs returns `None`.
    /// Indexes, unrelated names, misplaced logs and symbolic links are ignored.
    /// Empty log files count; contents, continuity and checkpoints are not checked.
    ///
    /// The caller must exclude concurrent tree changes. Directory enumeration and
    /// entry-type failures propagate with their paths. This creates nothing and
    /// retains only pending siblings along the fixed-depth search, not all log IDs.
    pub async fn maximum_log_file(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<LogFileId>, StorageError> {
        struct PendingDirectory {
            /// Directory to enumerate next: the stream base or a range directory.
            path: PathBuf,
            /// Number of three-digit range components below the stream base.
            /// Zero is the stream base; four is a leaf containing log/index files.
            depth: usize,
            /// Concatenated range components interpreted as a number, without
            /// the file number's remaining digits. For `000/000/000/001`, this
            /// is 1 at depth four, covering file numbers 1,000 through 1,999.
            /// At depth zero it is 0 and no range component has been selected.
            prefix: u64,
        }

        /// Enumerates valid child ranges in descending order. Only an absent
        /// stream base is empty; failures in discovered branches propagate.
        async fn range_directories(
            directory: &PendingDirectory,
        ) -> Result<Vec<PendingDirectory>, StorageError> {
            let mut entries = match tokio::fs::read_dir(&directory.path).await {
                Ok(entries) => entries,
                Err(source) if directory.depth == 0 && source.kind() == io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(source) => return Err(StorageError::io(directory.path.clone(), source)),
            };
            let mut children = Vec::new();
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|source| StorageError::io(directory.path.clone(), source))?
            {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if name.len() != 3 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
                    continue;
                }
                let prefix = directory.prefix * 1000 + name.parse::<u64>().unwrap();
                let first_file = prefix * 1000u64.pow(4 - directory.depth as u32);
                if first_file > LogFileNumber::MAX.get() {
                    continue;
                }
                let kind = entry
                    .file_type()
                    .await
                    .map_err(|source| StorageError::io(entry.path(), source))?;
                if kind.is_dir() {
                    children.push(PendingDirectory {
                        path: entry.path(),
                        depth: directory.depth + 1,
                        prefix,
                    });
                }
            }
            children.sort_unstable_by_key(|directory| std::cmp::Reverse(directory.prefix));
            Ok(children)
        }

        /// Selects the highest canonical regular log in one leaf directory.
        async fn maximum_log_in_directory(
            provider: &StorageProvider,
            stream_id: StreamId,
            path: &Path,
        ) -> Result<Option<LogFileId>, StorageError> {
            let mut entries = tokio::fs::read_dir(path)
                .await
                .map_err(|source| StorageError::io(path.to_owned(), source))?;
            let mut maximum = None;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|source| StorageError::io(path.to_owned(), source))?
            {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                let Some(number) = name.strip_suffix(".log") else {
                    continue;
                };
                if number.len() != 15 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
                    continue;
                }
                let Ok(number) = LogFileNumber::new(number.parse().unwrap()) else {
                    continue;
                };
                let id = LogFileId::new(stream_id, number);
                if provider.log_file_path(id) != entry.path() {
                    continue;
                }
                let kind = entry
                    .file_type()
                    .await
                    .map_err(|source| StorageError::io(entry.path(), source))?;
                // All candidates are constructed with this search's stream ID.
                if kind.is_file()
                    && maximum
                        .is_none_or(|previous: LogFileId| id.file_number() > previous.file_number())
                {
                    maximum = Some(id);
                }
            }
            Ok(maximum)
        }

        let mut pending = vec![PendingDirectory {
            path: self.stream_directory(stream_id),
            depth: 0,
            prefix: 0,
        }];
        while let Some(directory) = pending.pop() {
            if directory.depth == 4 {
                if let Some(id) = maximum_log_in_directory(self, stream_id, &directory.path).await?
                {
                    return Ok(Some(id));
                }
            } else {
                let children = range_directories(&directory).await?;
                // Reverse descending children so pop() visits the highest first.
                pending.extend(children.into_iter().rev());
            }
        }
        Ok(None)
    }

    /// Opens an existing log for validation and explicitly requested tail repair.
    ///
    /// Uses read/write access without creating, truncating or append mode. The
    /// validator must seek to verified boundaries before handing over for writes.
    /// No directories are created. The caller must exclude other writers during
    /// validation, repair and handover; this operation does not acquire a lock.
    pub async fn open_log_for_validation(
        &self,
        id: LogFileId,
    ) -> Result<tokio::fs::File, StorageError> {
        let path = self.log_file_path(id);
        tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|source| StorageError::io(path, source))
    }

    /// Opens the paired index for inspection and repair, creating it if missing.
    ///
    /// Existing bytes are preserved. Uses read/write access without append mode
    /// so repair can replace an incorrect suffix. Parent directories must already
    /// exist. Creating a missing empty index establishes no validated entries or
    /// durable directory publication. Open the authoritative log successfully
    /// first, so a missing log does not leave a newly created orphan index.
    pub async fn open_index_for_repair(
        &self,
        id: LogFileId,
    ) -> Result<tokio::fs::File, StorageError> {
        let path = self.index_file_path(id);
        tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(|source| StorageError::io(path, source))
    }

    /// Removes a log and its paired index, accepting already-absent files.
    ///
    /// Removes the log first, then the index; it never removes directories.
    /// Other errors stop cleanup, so partial deletion is possible and retryable.
    /// Requires exclusive recovery ownership. Unix synchronizes the affected
    /// directory and its ancestors through the configured root after deletion;
    /// portable directory durability is not provided on Windows.
    pub async fn remove_log_and_index(&self, id: LogFileId) -> Result<(), StorageError> {
        let mut removed = false;
        for path in [self.log_file_path(id), self.index_file_path(id)] {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => removed = true,
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Err(source) => return Err(StorageError::io(path, source)),
            }
        }
        if removed {
            self.sync_log_file_directory(id)?;
        }
        Ok(())
    }

    /// Synchronizes a pair's directory entries, not its log/index data.
    ///
    /// Blocking Unix I/O synchronizes its parent and ancestors through the
    /// configured root. Windows has no portable directory-sync implementation.
    /// The caller must synchronize both files separately before checkpointing.
    pub fn sync_log_file_directory(&self, id: LogFileId) -> Result<(), StorageError> {
        let path = self.log_file_path(id);
        self.sync_directories_to_root(path.parent().unwrap())
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

    /// Reads one stream's checkpoint, returning `None` when its path is absent.
    ///
    /// Reads at most 4 KiB plus one byte to detect oversized metadata. Malformed
    /// JSON and other I/O failures are errors. Creates or modifies nothing.
    /// Deserialization validates the model; the recovery owner must check the
    /// checkpoint's stream identity and agreement with the actual log/index pair.
    pub async fn read_checkpoint(
        &self,
        stream_id: StreamId,
    ) -> Result<Option<StreamCheckpoint>, StorageError> {
        use tokio::io::AsyncReadExt;

        const MAX_CHECKPOINT_BYTES: u64 = 4096;

        let path = self.checkpoint_file_path(stream_id);
        let file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(StorageError::io(path, source)),
        };
        let mut bytes = Vec::new();
        file.take(MAX_CHECKPOINT_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|source| StorageError::io(path.clone(), source))?;
        if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
            return Err(StorageError::TooLarge {
                path,
                max_bytes: MAX_CHECKPOINT_BYTES,
            });
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| StorageError::Json { path, source })
    }

    /// Publishes or replaces a supplied checkpoint using synchronized staging JSON.
    ///
    /// Blocking startup I/O requires an existing stream directory and exclusive
    /// ownership. The caller certifies and synchronizes the covered log/index
    /// prefix first. This operation does not compare checkpoints or validate logs.
    /// `.checkpoint.pending` is never loaded as a checkpoint and may be overwritten
    /// after an interrupted attempt. After syncing its contents, rename replaces
    /// `checkpoint.json`; Unix then syncs ancestors through the configured root.
    /// Windows directory durability and the root's own parent are not synchronized.
    /// Errors may leave staging or an already-published checkpoint; reread before
    /// recovery. There is no async cancellation point within this operation.
    pub fn write_checkpoint(&self, checkpoint: &StreamCheckpoint) -> Result<(), StorageError> {
        let stream_id = checkpoint.end().record_id().stream_id();
        let path = self.checkpoint_file_path(stream_id);
        let bytes = serde_json::to_vec(checkpoint).map_err(|source| StorageError::Json {
            path: path.clone(),
            source,
        })?;
        let directory = self.stream_directory(stream_id);
        let pending = directory.join(".checkpoint.pending");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&pending)
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        file.write_all(&bytes)
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        file.sync_all()
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        drop(file);
        fs::rename(&pending, &path).map_err(|source| StorageError::io(path, source))?;
        self.sync_directories_to_root(&directory)
    }

    /// Loads stored clustering settings, returning `None` only when the file is absent.
    ///
    /// Performs bounded blocking startup I/O without creating directories or assigning
    /// a UUID. Malformed or unreadable metadata is an error, never fresh storage.
    pub fn read_clustering_configuration(
        &self,
    ) -> Result<Option<EstablishedClusteringConfiguration>, StorageError> {
        const MAX_CLUSTERING_CONFIGURATION_BYTES: u64 = 4096;

        let path = self.root_directory().join(FILE_NAME);
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let pending = self.root_directory().join(PENDING_FILE_NAME);
                match fs::metadata(&pending) {
                    Ok(_) => {
                        return Err(StorageError::io(
                            pending,
                            io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                "unpublished clustering configuration requires recovery",
                            ),
                        ));
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                    Err(source) => return Err(StorageError::io(pending, source)),
                }
            }
            Err(source) => return Err(StorageError::io(path, source)),
        };
        let mut bytes = Vec::new();
        file.take(MAX_CLUSTERING_CONFIGURATION_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| StorageError::io(path.clone(), source))?;
        if bytes.len() as u64 > MAX_CLUSTERING_CONFIGURATION_BYTES {
            return Err(StorageError::TooLarge {
                path,
                max_bytes: MAX_CLUSTERING_CONFIGURATION_BYTES,
            });
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| StorageError::Json { path, source })
    }

    /// Publishes first-load configuration and UUID without replacing existing metadata.
    ///
    /// Creates the root if necessary, synchronizes a new staging file and publishes
    /// it with a no-overwrite hard link. Requires hard-link support and exclusive
    /// ownership of the root. Existing published or unpublished staging files cause
    /// an error; callers must inspect interrupted writes rather than reassign UUIDs.
    ///
    /// Blocking startup I/O has no async cancellation point. Errors can leave staging
    /// or published metadata: reread before deciding how to recover. Unix syncs the
    /// root directory; Windows has no portable directory-durability guarantee here.
    /// Parent directories are not recursively synchronized. This does not generate
    /// a UUID, check peer agreement, enforce a process lock or establish readiness.
    pub fn write_clustering_configuration(
        &self,
        configuration: &EstablishedClusteringConfiguration,
    ) -> Result<(), StorageError> {
        let root = self.root_directory();
        let path = root.join(FILE_NAME);
        let bytes = serde_json::to_vec(configuration).map_err(|source| StorageError::Json {
            path: path.clone(),
            source,
        })?;
        fs::create_dir_all(root).map_err(|source| StorageError::io(root.to_owned(), source))?;
        let pending = root.join(PENDING_FILE_NAME);
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        file.write_all(&bytes)
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        file.sync_all()
            .map_err(|source| StorageError::io(pending.clone(), source))?;
        drop(file);
        fs::hard_link(&pending, &path).map_err(|source| StorageError::io(path, source))?;
        #[cfg(unix)]
        fs::File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| StorageError::io(root.to_owned(), source))?;
        fs::remove_file(&pending).map_err(|source| StorageError::io(pending, source))?;
        #[cfg(unix)]
        fs::File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| StorageError::io(root.to_owned(), source))?;
        Ok(())
    }
    /// Synchronizes directory links inside this provider's configured root only.
    fn sync_directories_to_root(&self, directory: &Path) -> Result<(), StorageError> {
        #[cfg(unix)]
        for path in directory.ancestors() {
            fs::File::open(path)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| StorageError::io(path.to_owned(), source))?;
            if path == self.root_directory() {
                break;
            }
        }
        #[cfg(not(unix))]
        let _ = directory;
        Ok(())
    }

    /// Shared base layout for initialization and all file-path methods.
    fn stream_directory(&self, stream_id: StreamId) -> PathBuf {
        self.root_directory()
            .join("streams")
            .join(format!("{stream_id:04}"))
    }

    /// Keeps directory grouping and numeric filenames identical for log/index pairs.
    fn log_or_index_file_path(&self, id: LogFileId, extension: &str) -> PathBuf {
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

    use crate::streams::LogFileNumber;

    use super::{LogFileId, StorageConfig, StorageProvider, StreamId};

    #[tokio::test]
    async fn maximum_log_search_backtracks_past_empty_and_index_only_branches() {
        let temporary = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap());
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            None
        );
        assert!(!temporary.path().join("streams").exists());
        let stream = temporary.path().join("streams/0000");
        fs::create_dir_all(stream.join("184/467/440/737")).unwrap();
        fs::write(stream.join("184/467/440/737/184467440737095.idx"), b"index").unwrap();
        fs::create_dir_all(stream.join("001/000/000/000")).unwrap();
        let leaf = stream.join("000/000/000/001");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(leaf.join("000000000001234.log"), b"").unwrap();
        fs::write(leaf.join("000000000001999.log"), b"unvalidated").unwrap();
        fs::create_dir_all(stream.join("000/000/000/002")).unwrap();
        let expected = LogFileId::new(StreamId::MIN, LogFileNumber::new(1999).unwrap());
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            Some(expected)
        );
        assert_eq!(
            fs::read(provider.log_file_path(expected)).unwrap(),
            b"unvalidated"
        );
        assert_eq!(
            provider.maximum_log_file(StreamId::MAX).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn maximum_log_search_handles_every_directory_carry_and_terminal_number() {
        let temporary = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap());
        for number in [
            0,
            999,
            1000,
            999_999,
            1_000_000,
            999_999_999,
            1_000_000_000,
            999_999_999_999,
            1_000_000_000_000,
            LogFileNumber::MAX.get(),
        ] {
            let id = LogFileId::new(StreamId::MAX, LogFileNumber::new(number).unwrap());
            let path = provider.log_file_path(id);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"").unwrap();
            assert_eq!(
                provider.maximum_log_file(StreamId::MAX).await.unwrap(),
                Some(id)
            );
        }
    }

    #[tokio::test]
    async fn maximum_log_search_ignores_noncanonical_entries_and_reports_io_errors() {
        let temporary = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap());
        let stream = temporary.path().join("streams/0000");
        let leaf = stream.join("000/000/000/000");
        fs::create_dir_all(&leaf).unwrap();
        for name in [
            "1.log",
            "00000000000000x.log",
            "000000000001000.log",
            "000000000000999.idx",
            "999999999999999.log",
            "notes",
        ] {
            fs::write(leaf.join(name), b"preserve").unwrap();
        }
        fs::create_dir(leaf.join("000000000000998.log")).unwrap();
        fs::create_dir_all(stream.join("junk/000/000/000")).unwrap();
        fs::create_dir_all(stream.join("999/999/999/999")).unwrap();
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            None
        );
        fs::write(leaf.join("000000000000000.log"), b"").unwrap();
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            Some(LogFileId::new(StreamId::MIN, LogFileNumber::MIN))
        );

        let obstructed = temporary.path().join("streams/4095");
        fs::write(&obstructed, b"not a directory").unwrap();
        let error = provider.maximum_log_file(StreamId::MAX).await.unwrap_err();
        assert_eq!(error.path(), obstructed);
        assert!(matches!(error, super::StorageError::Io { .. }));
    }

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

    #[tokio::test]
    async fn validation_opens_preserve_existing_bytes_and_index_creation_is_explicit() {
        let temporary = tempfile::tempdir().unwrap();
        let provider =
            StorageProvider::new(StorageConfig::new(temporary.path().to_owned()).unwrap());
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
        let log = provider.log_file_path(id);
        let index = provider.index_file_path(id);
        let error = provider.open_log_for_validation(id).await.unwrap_err();
        assert_eq!(error.path(), log);
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(!log.parent().unwrap().exists());
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(&log, b"unchanged log").unwrap();
        drop(provider.open_log_for_validation(id).await.unwrap());
        assert!(!index.exists());
        drop(provider.open_index_for_repair(id).await.unwrap());
        assert_eq!(fs::read(&index).unwrap(), b"");
        fs::write(&index, b"unchanged index").unwrap();
        drop(provider.open_index_for_repair(id).await.unwrap());
        assert_eq!(fs::read(&log).unwrap(), b"unchanged log");
        assert_eq!(fs::read(&index).unwrap(), b"unchanged index");
    }

    #[tokio::test]
    async fn index_open_errors_identify_the_path_and_preserve_colliding_directories() {
        let temporary = tempfile::tempdir().unwrap();
        let provider =
            StorageProvider::new(StorageConfig::new(temporary.path().to_owned()).unwrap());
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
        let index = provider.index_file_path(id);
        fs::create_dir_all(&index).unwrap();
        fs::write(index.join("keep"), b"existing content").unwrap();
        let error = provider.open_index_for_repair(id).await.unwrap_err();
        assert_eq!(error.path(), index);
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .is_some()
        );
        assert_eq!(fs::read(index.join("keep")).unwrap(), b"existing content");
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

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_publication_replaces_metadata_and_reuses_interrupted_staging() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let path = provider.checkpoint_file_path(StreamId::MIN);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let first_json =
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#;
        let next_json =
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":1},"position":32}}"#;
        let first: StreamCheckpoint = serde_json::from_str(first_json).unwrap();
        let next: StreamCheckpoint = serde_json::from_str(next_json).unwrap();
        provider.write_checkpoint(&first).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), first_json);
        let pending = path.parent().unwrap().join(".checkpoint.pending");
        fs::write(&pending, b"interrupted metadata").unwrap();
        assert_eq!(
            provider.read_checkpoint(StreamId::MIN).await.unwrap(),
            Some(first)
        );
        provider.write_checkpoint(&next).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), next_json);
        assert_eq!(
            provider.read_checkpoint(StreamId::MIN).await.unwrap(),
            Some(next)
        );
        assert!(!pending.exists());

        fs::create_dir(&pending).unwrap();
        let error = provider.write_checkpoint(&first).unwrap_err();
        assert_eq!(error.path(), pending);
        assert_eq!(fs::read_to_string(path).unwrap(), next_json);
    }

    #[tokio::test]
    async fn checkpoint_publication_requires_existing_parents_and_does_not_load_staging() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("absent");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
        let checkpoint: StreamCheckpoint = serde_json::from_str(
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#,
        )
        .unwrap();
        assert!(provider.write_checkpoint(&checkpoint).is_err());
        assert!(!root.exists());
        let path = provider.checkpoint_file_path(StreamId::MIN);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let pending = path.parent().unwrap().join(".checkpoint.pending");
        fs::write(pending, b"never certified").unwrap();
        assert_eq!(provider.read_checkpoint(StreamId::MIN).await.unwrap(), None);
    }

    #[tokio::test]
    async fn pair_removal_is_idempotent_handles_orphan_indexes_and_preserves_collisions() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
        provider.remove_log_and_index(file).await.unwrap();
        assert!(!directory.path().join("streams").exists());
        let log = provider.log_file_path(file);
        let index = provider.index_file_path(file);
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(&index, b"orphan").unwrap();
        provider.remove_log_and_index(file).await.unwrap();
        assert!(!index.exists());
        fs::write(&log, b"discard").unwrap();
        fs::write(&index, b"discard").unwrap();
        provider.remove_log_and_index(file).await.unwrap();
        provider.remove_log_and_index(file).await.unwrap();
        assert!(!log.exists());
        assert!(!index.exists());
        assert!(log.parent().unwrap().exists());
        fs::write(&log, b"discard").unwrap();
        fs::create_dir(&index).unwrap();
        let error = provider.remove_log_and_index(file).await.unwrap_err();
        assert_eq!(error.path(), index);
        assert!(!log.exists());
        assert!(index.is_dir());
    }

    #[tokio::test]
    async fn reads_literal_checkpoint_and_enforces_the_inclusive_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let path = provider.checkpoint_file_path(StreamId::MAX);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let json = r#"{"end":{"record_id":{"stream_id":4095,"sequence_number":18446744073709551615},"position":3382654560}}"#;
        let mut bytes = json.as_bytes().to_vec();
        bytes.resize(4096, b' ');
        fs::write(&path, &bytes).unwrap();
        let checkpoint = provider
            .read_checkpoint(StreamId::MAX)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.end().record_id().stream_id(), StreamId::MAX);
        assert_eq!(
            checkpoint.end().record_id().sequence_number().get(),
            u64::MAX
        );
        assert_eq!(checkpoint.end().position(), 3_382_654_560);
        bytes.push(b' ');
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(provider.read_checkpoint(StreamId::MAX).await,
            Err(StorageError::TooLarge { path: failed, max_bytes: 4096 }) if failed == path));
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn distinguishes_absence_from_invalid_json_and_io_failures() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let path = provider.checkpoint_file_path(StreamId::MIN);
        assert_eq!(provider.read_checkpoint(StreamId::MIN).await.unwrap(), None);
        assert!(!path.parent().unwrap().exists());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        for json in [
            "",
            "{}",
            "null",
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}} trailing"#,
        ] {
            fs::write(&path, json).unwrap();
            assert!(matches!(provider.read_checkpoint(StreamId::MIN).await,
                Err(StorageError::Json { path: failed, .. }) if failed == path));
            assert_eq!(fs::read_to_string(&path).unwrap(), json);
        }
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(matches!(provider.read_checkpoint(StreamId::MIN).await,
            Err(StorageError::Io { path: failed, .. }) if failed == path));
    }
}

#[cfg(test)]
mod clustering_configuration_tests {
    use super::*;
    use crate::{
        clustering::configuration::{
            ClusterMembership, ClusteringConfiguration, Hostname, NodeName,
        },
        storage::StorageConfig,
    };
    use uuid::Uuid;

    #[test]
    fn absent_read_has_no_side_effects_and_both_modes_round_trip() {
        for node in [
            None,
            Some(NodeName::Master),
            Some(NodeName::Replica1),
            Some(NodeName::Replica2),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("data");
            let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
            assert!(provider.read_clustering_configuration().unwrap().is_none());
            assert!(!root.exists());
            let config = match node {
                None => ClusteringConfiguration::Single,
                Some(node) => ClusteringConfiguration::Cluster(ClusterMembership::new(
                    node,
                    Hostname::parse("cluster.example".into()).unwrap(),
                )),
            };
            let stored = EstablishedClusteringConfiguration::new(Uuid::from_u128(1), config);
            provider.write_clustering_configuration(&stored).unwrap();
            assert_eq!(
                provider.read_clustering_configuration().unwrap(),
                Some(stored)
            );
            assert!(!root.join(PENDING_FILE_NAME).exists());
        }
    }

    #[test]
    fn reads_independent_fixture_and_refuses_overwrite() {
        let temp = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temp.path().to_owned()).unwrap());
        let path = temp.path().join(FILE_NAME);
        let fixture = br#"{"cluster_id":"00000000-0000-0000-0000-000000000001","clustering_configuration":"single"}"#;
        fs::write(&path, fixture).unwrap();
        let loaded = provider.read_clustering_configuration().unwrap().unwrap();
        assert_eq!(
            loaded,
            EstablishedClusteringConfiguration::new(
                Uuid::from_u128(1),
                ClusteringConfiguration::Single
            )
        );
        let replacement = EstablishedClusteringConfiguration::new(
            Uuid::from_u128(2),
            ClusteringConfiguration::Single,
        );
        assert!(
            provider
                .write_clustering_configuration(&replacement)
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), fixture);
    }

    #[test]
    fn malformed_oversized_and_interrupted_metadata_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temp.path().to_owned()).unwrap());
        let path = temp.path().join(FILE_NAME);
        for fixture in [
            b"".as_slice(),
            b"{}",
            b"null",
            br#"{"cluster_id":"bad","clustering_configuration":"single"}"#,
        ] {
            fs::write(&path, fixture).unwrap();
            assert!(matches!(
                provider.read_clustering_configuration(),
                Err(StorageError::Json { .. })
            ));
            assert_eq!(fs::read(&path).unwrap(), fixture);
        }
        fs::write(&path, vec![b' '; 4097]).unwrap();
        match provider.read_clustering_configuration().unwrap_err() {
            StorageError::TooLarge {
                path: rejected,
                max_bytes,
            } => {
                assert_eq!(rejected, path);
                assert_eq!(max_bytes, 4096);
            }
            error => panic!("expected size-limit error, got {error:?}"),
        }
        fs::remove_file(path).unwrap();
        fs::write(temp.path().join(PENDING_FILE_NAME), b"partial").unwrap();
        assert!(matches!(
            provider.read_clustering_configuration(),
            Err(StorageError::Io { .. })
        ));
        assert!(
            provider
                .write_clustering_configuration(&EstablishedClusteringConfiguration::new(
                    Uuid::from_u128(1),
                    ClusteringConfiguration::Single
                ))
                .is_err()
        );
        assert_eq!(
            fs::read(temp.path().join(PENDING_FILE_NAME)).unwrap(),
            b"partial"
        );
    }
}

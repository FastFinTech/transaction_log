use super::{StorageConfig, StorageError};
use crate::clustering::configuration::EstablishedClusteringConfiguration;
use crate::streams::{LogFileId, LogFileNumber, StreamCheckpoint};
use std::{
    io,
    ops::RangeInclusive,
    path::{Path, PathBuf},
};
use transaction_log_exports::StreamId;

/// Owns storage configuration, constructs paths, and initializes stream directories.
///
/// Paths follow the fixed decimal layout documented in this module's README.
/// Async construction ensures the root and all stream base directories exist.
/// Path queries are synchronous and perform no filesystem access.
/// Creates fresh log/index pairs and opens existing files for validation or repair.
/// The caller owns returned handles, file rotation and recovery coordination.
/// The provider does not cache open handles.
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
    /// Takes ownership of configuration and creates the root and all stream bases.
    ///
    /// Creates missing parents and `streams/0000` through `streams/4095` using
    /// Tokio filesystem operations. Existing directories and their contents are
    /// left intact. No range directories, log, index or checkpoint files are created.
    ///
    /// Returns a provider only after every directory is established. Constructing
    /// another provider for the same root is allowed. Failure or cancellation can
    /// leave some directories created; a retry accepts that progress rather than
    /// rolling it back. An unpolled future performs no I/O. Success does not
    /// validate existing records, claim exclusive ownership, or durably sync
    /// directory entries. Future file operations must still handle I/O failures.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] at the first directory that cannot be
    /// created, preserving the requested path and the underlying I/O error.
    pub async fn new(config: StorageConfig) -> Result<Self, StorageError> {
        let provider = Self { config };
        for stream_id in StreamId::all() {
            let path = provider.stream_directory(stream_id);
            // create_dir_all accepts existing directories and creates missing
            // parents. A separate existence check would add I/O and a race.
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|source| StorageError::io(path, source))?;
        }
        Ok(provider)
    }

    /// Borrows the absolute storage root without allocating or copying it.
    pub fn root_directory(&self) -> &Path {
        self.config.root_directory()
    }

    /// Clears test data while retaining the root and initialized stream bases.
    ///
    /// Test-only: requires exclusive access, closed files and an intact base
    /// layout. Removes metadata, staging, range directories and unrelated entries.
    /// Retained directories must be real directories; links are never traversed.
    /// No durable synchronization is needed for disposable test data. An error
    /// can leave partial cleanup; retry after resolving the cause.
    /// Await completion before releasing the fixture: dropping this future does
    /// not stop an already spawned cleanup task.
    #[cfg(test)]
    pub(crate) async fn clear_all(&mut self) -> Result<(), StorageError> {
        use std::{collections::HashSet, fs};

        fn clear_directory(path: &Path, retained: &HashSet<PathBuf>) -> Result<(), StorageError> {
            let entries =
                fs::read_dir(path).map_err(|source| StorageError::io(path.to_owned(), source))?;
            for entry in entries {
                let entry = entry.map_err(|source| StorageError::io(path.to_owned(), source))?;
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .map_err(|source| StorageError::io(path.clone(), source))?;
                if retained.contains(&path) {
                    if !file_type.is_dir() {
                        return Err(StorageError::io(
                            path,
                            io::Error::new(
                                io::ErrorKind::NotADirectory,
                                "test base is not a directory",
                            ),
                        ));
                    }
                    clear_directory(&path, retained)?;
                } else {
                    // Entries come directly from this root's directory traversal.
                    // file_type does not follow links, nor does remove_dir_all.
                    let result = if file_type.is_dir() {
                        fs::remove_dir_all(&path)
                    } else {
                        fs::remove_file(&path)
                    };
                    result.map_err(|source| StorageError::io(path, source))?;
                }
            }
            Ok(())
        }

        let root = self.root_directory().to_owned();
        let mut retained: HashSet<_> = StreamId::all()
            .map(|stream_id| self.stream_directory(stream_id))
            .collect();
        retained.insert(root.join("streams"));

        // Batch the scan into one blocking task, rather than dispatching thousands
        // of separate filesystem operations for the mostly empty stream bases.
        tokio::task::spawn_blocking(move || clear_directory(&root, &retained))
            .await
            .expect("test storage cleanup task panicked")
    }

    /// Constructs the absolute `.log` path assigned to a file ID.
    ///
    /// Returns a newly allocated path whether or not the file exists.
    /// This performs no I/O or validation of file contents.
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

    /// Constructs the absolute `checkpoint.json` path for one stream's checkpoint.
    ///
    /// The checkpoint belongs to the stream, so its path stays in the stream base
    /// directory as the last checkpointed record advances across log files. Returns
    /// an owned path without creating directories or loading, writing or validating
    /// checkpoint data. Construction creates its parent directory, not the checkpoint.
    pub fn checkpoint_file_path(&self, stream_id: StreamId) -> PathBuf {
        const FILE_NAME: &str = "checkpoint.json";

        self.stream_directory(stream_id).join(FILE_NAME)
    }

    /// Loads stored clustering settings, returning `None` when the published file is absent.
    ///
    /// Uses Tokio for bounded startup I/O without creating directories or assigning
    /// a UUID. Malformed or unreadable metadata is an error, never fresh storage.
    /// Pending staging is ignored, whether or not the published file exists.
    pub async fn read_clustering_configuration(
        &self,
    ) -> Result<Option<EstablishedClusteringConfiguration>, StorageError> {
        const MAX_CLUSTERING_CONFIGURATION_BYTES: u64 = 4096;

        let path = self.clustering_configuration_file_path();
        read_json(&path, MAX_CLUSTERING_CONFIGURATION_BYTES).await
    }

    /// Atomically writes the supplied clustering configuration, replacing existing metadata.
    ///
    /// Writes and synchronizes staging in the root established by construction,
    /// then renames it over the destination. Requires exclusive root ownership. The
    /// clustering lifecycle caller is responsible for write-once establishment
    /// and preserving the established UUID and deployment configuration.
    ///
    /// Uses Tokio filesystem I/O. Errors/cancellation can leave staging or published
    /// metadata: quiesce outstanding I/O and reread before deciding how to recover.
    /// Unix syncs the root directory after renaming the staging file;
    /// Windows has no portable directory-durability guarantee here.
    /// Parent directories are not recursively synchronized. This does not generate
    /// a UUID, check peer agreement, enforce a process lock or establish readiness.
    pub async fn write_clustering_configuration(
        &self,
        configuration: &EstablishedClusteringConfiguration,
    ) -> Result<(), StorageError> {
        let path = self.clustering_configuration_file_path();
        write_json_atomic(&path, configuration).await
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
        const MAX_CHECKPOINT_BYTES: u64 = 4096;

        let path = self.checkpoint_file_path(stream_id);
        read_json(&path, MAX_CHECKPOINT_BYTES).await
    }

    /// Publishes or replaces a supplied checkpoint using synchronized staging JSON.
    ///
    /// Uses Tokio filesystem I/O and requires an existing, durably established
    /// stream directory and exclusive ownership. The caller certifies and
    /// synchronizes the covered log/index prefix and its directory entries first.
    /// This operation does not compare checkpoints or validate logs.
    /// `checkpoint.json.pending` is never loaded as a checkpoint. Staging is created
    /// or truncated before writing. After flushing and syncing it,
    /// rename replaces `checkpoint.json`; Unix then syncs only the stream directory.
    /// Windows has no directory-durability guarantee here.
    /// Errors/cancellation may leave staging or an already-published checkpoint;
    /// quiesce outstanding I/O and reread before recovery or another publication.
    pub async fn write_checkpoint(
        &self,
        checkpoint: &StreamCheckpoint,
    ) -> Result<(), StorageError> {
        let stream_id = checkpoint.end().record_id().stream_id();
        let path = self.checkpoint_file_path(stream_id);
        write_json_atomic(&path, checkpoint).await
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

    /// Creates a new empty log/index pair and any missing parent directories.
    ///
    /// Returns `(log, index)`, both readable/writable and positioned at byte zero,
    /// without append mode. Creates the log first, then the index, using
    /// `create_new` for each: existing files, even empty ones or orphan indexes,
    /// are never opened, truncated or removed. The caller must exclude concurrent
    /// access to this pair; creating two files is not an atomic operation.
    ///
    /// Errors or cancellation can leave directories and a newly created log,
    /// or both files. There is no rollback or automatic retry. Quiesce outstanding
    /// I/O and inspect/recover partial creation before another attempt.
    /// This operation does not synchronize files or directory entries, publish a
    /// checkpoint, or establish stream continuity. Those remain caller duties.
    /// An unpolled future performs no I/O.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] with the requested parent directory, log path
    /// or index path and the original filesystem error for the failed operation.
    pub async fn create_log_and_index(
        &self,
        id: LogFileId,
    ) -> Result<(tokio::fs::File, tokio::fs::File), StorageError> {
        let log_path = self.log_file_path(id);
        let index_path = self.index_file_path(id);
        let directory = log_path.parent().unwrap();
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|source| StorageError::io(directory.to_owned(), source))?;

        let log = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)
            .await
            .map_err(|source| StorageError::io(log_path, source))?;
        let index = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&index_path)
            .await
            .map_err(|source| StorageError::io(index_path, source))?;
        Ok((log, index))
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
    /// so repair can replace an incorrect suffix. The parent directory hierarchy
    /// must already exist and be durably established. On Unix, synchronizes the
    /// immediate parent after opening, including when the index already exists:
    /// it may have been created by an interrupted recovery. Windows has no
    /// directory-durability guarantee here. Index contents must still be validated
    /// and synchronized by the caller before checkpoint publication.
    ///
    /// Open the authoritative log successfully first, so a missing log does not
    /// leave a newly created orphan index. Errors/cancellation can leave a newly
    /// created index; there is no rollback. Directory-sync failures retain the
    /// directory path and original I/O cause.
    pub async fn open_index_for_repair(
        &self,
        id: LogFileId,
    ) -> Result<tokio::fs::File, StorageError> {
        let path = self.index_file_path(id);
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(|source| StorageError::io(path.clone(), source))?;
        sync_directory(path.parent().unwrap()).await?;
        Ok(file)
    }

    /// Removes an inclusive range of log/index pairs from one stream.
    ///
    /// Validates the range before I/O, then visits files in ascending order,
    /// deleting each log before its index. Already-absent files are accepted.
    /// Directories and entries outside the range are preserved.
    ///
    /// After all deletions, Unix opens and synchronizes each distinct parent
    /// directory once through Tokio, without synchronizing ancestors. Existing
    /// parents are synced even when the files were already absent, allowing a
    /// repeat after interrupted deletion/synchronization. Missing parents are
    /// skipped. Windows has no directory-durability guarantee here.
    ///
    /// Requires exclusive recovery access and a durably established directory
    /// hierarchy. Errors stop the operation and preserve the failing path and
    /// cause. Errors/cancellation can leave partial deletion or unsynchronized
    /// changes; quiesce outstanding I/O before repeating the original range.
    ///
    /// # Errors
    ///
    /// Different streams or reversed endpoints return an `InvalidInput` I/O
    /// error at the first log's path, containing the original range error.
    /// Filesystem errors retain their original causes; no checkpoint is published.
    pub async fn remove_log_and_index(
        &self,
        range: RangeInclusive<LogFileId>,
    ) -> Result<(), StorageError> {
        let (first, last) = range.into_inner();
        let files = first.iter_to(last).map_err(|source| {
            StorageError::io(
                self.log_file_path(first),
                io::Error::new(io::ErrorKind::InvalidInput, source),
            )
        })?;
        #[cfg(unix)]
        let mut directories = std::collections::BTreeSet::new();
        for id in files {
            let log = self.log_file_path(id);
            #[cfg(unix)]
            directories.insert(log.parent().unwrap().to_owned());
            for path in [log, self.index_file_path(id)] {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => return Err(StorageError::io(path, source)),
                }
            }
        }
        #[cfg(unix)]
        for path in directories {
            match sync_directory(&path).await {
                Ok(()) => {}
                Err(StorageError::Io { source, .. })
                    if source.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Constructs the stored clustering configuration path without filesystem I/O.
    fn clustering_configuration_file_path(&self) -> PathBuf {
        const FILE_NAME: &str = "clustering.json";

        self.root_directory().join(FILE_NAME)
    }

    /// Shared base layout for initialization and stream file-path methods.
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

/// Reads an owned JSON value using Tokio, returning `None` only for a `NotFound` open.
///
/// The byte limit is inclusive. Reads at most one byte beyond it to detect oversized
/// input before deserializing. Other I/O failures and JSON/model validation failures
/// preserve the requested path and original cause. An empty file is invalid JSON.
/// Creates or modifies nothing and does not inspect pending files; recovery policy
/// belongs to the caller. Cancellation produces no value or file modifications.
async fn read_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    max_bytes: u64,
) -> Result<Option<T>, StorageError> {
    use tokio::io::AsyncReadExt;

    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(StorageError::io(path.to_owned(), source)),
    };
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| StorageError::io(path.to_owned(), source))?;
    if bytes.len() as u64 > max_bytes {
        return Err(StorageError::TooLarge {
            path: path.to_owned(),
            max_bytes,
        });
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| StorageError::Json {
            path: path.to_owned(),
            source,
        })
}

/// Serializes before I/O, then atomically replaces the destination with complete JSON.
///
/// The provider supplies an absolute file path with an existing parent directory
/// and excludes concurrent access to the destination and its `.pending` sibling.
/// This helper creates no directories. Ancestor durability belongs to the caller;
/// Unix synchronizes only the immediate parent, and Windows skips directory sync.
/// Creates or truncates the pending file before writing. The caller owns any
/// write-once policy.
///
/// Atomic publication does not imply rollback: errors/cancellation can leave a
/// partial staging file or a published destination. Quiesce outstanding I/O and
/// inspect storage before another attempt. Failures preserve their path and cause.
async fn write_json_atomic<T: serde::Serialize + ?Sized>(
    path: &Path,
    value: &T,
) -> Result<(), StorageError> {
    use tokio::io::AsyncWriteExt;

    let bytes = serde_json::to_vec(value).map_err(|source| StorageError::Json {
        path: path.to_owned(),
        source,
    })?;
    let pending = path.with_added_extension("pending");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pending)
        .await
        .map_err(|source| StorageError::io(pending.clone(), source))?;
    file.write_all(&bytes)
        .await
        .map_err(|source| StorageError::io(pending.clone(), source))?;
    // Surface pending write failures before syncing or publishing the file.
    file.flush()
        .await
        .map_err(|source| StorageError::io(pending.clone(), source))?;
    file.sync_all()
        .await
        .map_err(|source| StorageError::io(pending.clone(), source))?;
    drop(file);

    tokio::fs::rename(&pending, path)
        .await
        .map_err(|source| StorageError::io(path.to_owned(), source))?;

    sync_directory(path.parent().unwrap()).await?;
    Ok(())
}

/// Synchronizes one directory through Tokio on Unix; a no-op on other platforms.
///
/// The caller selects an existing directory. No ancestors are synchronized and
/// no entries are created. Missing-directory errors are preserved for the caller
/// to interpret. Errors retain the directory path and original I/O cause.
#[cfg_attr(not(unix), allow(unused_variables))]
async fn sync_directory(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    tokio::fs::File::open(path)
        .await
        .map_err(|source| StorageError::io(path.to_owned(), source))?
        .sync_all()
        .await
        .map_err(|source| StorageError::io(path.to_owned(), source))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::io;
    use std::path::Path;

    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use crate::{
        storage::test_support::storage_fixture,
        streams::{LogFileIdRangeError, LogFileNumber},
    };

    use super::{LogFileId, StorageConfig, StorageError, StorageProvider, StreamId};

    #[test]
    fn unpolled_construction_creates_nothing() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("absent");
        let construction = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
        assert!(!root.exists());
        drop(construction);
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn construction_creates_only_stream_bases_and_preserves_existing_contents() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("new").join("replica");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap())
            .await
            .unwrap();

        let streams = root.join("streams");
        assert_eq!(root.read_dir().unwrap().count(), 1);
        assert_eq!(streams.read_dir().unwrap().count(), 4096);
        for stream in 0..4096 {
            let directory = streams.join(format!("{stream:04}"));
            assert!(directory.is_dir());
            assert_eq!(directory.read_dir().unwrap().count(), 0);
        }

        // Simulate files already present on restart. Construction must neither
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

        drop(provider);
        let reopened = StorageProvider::new(StorageConfig::new(root.clone()).unwrap())
            .await
            .unwrap();
        assert_eq!(reopened.root_directory(), root);

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
    async fn clear_all_preserves_stream_bases_and_removes_all_test_contents() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let mut provider = StorageProvider::new(StorageConfig::new(root.into()).unwrap())
            .await
            .unwrap();

        for stream in [0, 7, 4095] {
            let stream_id = StreamId::new(stream).unwrap();
            let id = LogFileId::new(stream_id, LogFileNumber::MIN);
            let log = provider.log_file_path(id);
            fs::create_dir_all(log.parent().unwrap()).unwrap();
            fs::write(log, b"log").unwrap();
            fs::write(provider.index_file_path(id), b"index").unwrap();
            let checkpoint = provider.checkpoint_file_path(stream_id);
            fs::write(&checkpoint, b"checkpoint").unwrap();
            fs::write(checkpoint.with_added_extension("pending"), b"staging").unwrap();
        }
        for name in ["clustering.json", "clustering.json.pending", "notes.txt"] {
            fs::write(root.join(name), b"metadata").unwrap();
        }
        // Include unrelated directories and a checkpoint replaced by a directory.
        for name in [
            "extra/nested",
            "streams/4096/nested",
            "streams/0008/checkpoint.json",
        ] {
            let directory = root.join(name);
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("extra"), b"data").unwrap();
        }

        for _ in 0..2 {
            provider.clear_all().await.unwrap();

            assert_eq!(root.read_dir().unwrap().count(), 1);
            assert_eq!(root.join("streams").read_dir().unwrap().count(), 4096);
            for stream in 0..4096 {
                let directory = root.join(format!("streams/{stream:04}"));
                assert!(directory.is_dir());
                assert_eq!(directory.read_dir().unwrap().count(), 0);
            }
        }
        // The same provider remains usable after clearing, without reconstruction.
        let checkpoint = provider.checkpoint_file_path(StreamId::MIN);
        fs::write(&checkpoint, b"new test data").unwrap();
        provider.clear_all().await.unwrap();
        assert!(!checkpoint.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn clear_all_does_not_follow_links_outside_the_root() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("keep");
        fs::write(&sentinel, b"outside data").unwrap();
        let mut provider =
            StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap())
                .await
                .unwrap();
        let base = provider.stream_directory(StreamId::MIN);
        symlink(outside.path(), base.join("linked-directory")).unwrap();
        symlink(&sentinel, temporary.path().join("linked-file")).unwrap();

        provider.clear_all().await.unwrap();

        assert_eq!(base.read_dir().unwrap().count(), 0);
        assert_eq!(fs::read(&sentinel).unwrap(), b"outside data");

        // A base replaced by a link violates the fixture's intact-layout contract.
        fs::remove_dir(&base).unwrap();
        symlink(outside.path(), &base).unwrap();
        let error = provider.clear_all().await.unwrap_err();
        assert_eq!(error.path(), base);
        assert_eq!(fs::read(sentinel).unwrap(), b"outside data");
    }

    #[tokio::test]
    async fn construction_reports_a_stream_collision_and_can_retry_partial_progress() {
        let temporary = tempfile::tempdir().unwrap();
        let streams = temporary.path().join("streams");
        fs::create_dir(&streams).unwrap();
        let collision = streams.join("0007");
        fs::write(&collision, b"not a directory").unwrap();

        let error = StorageProvider::new(StorageConfig::new(temporary.path().to_owned()).unwrap())
            .await
            .unwrap_err();

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
        let provider =
            StorageProvider::new(StorageConfig::new(temporary.path().to_owned()).unwrap())
                .await
                .unwrap();
        assert_eq!(provider.root_directory(), temporary.path());
        assert_eq!(streams.read_dir().unwrap().count(), 4096);
        assert!(streams.join("4095").is_dir());
    }

    #[tokio::test]
    async fn construction_reports_parent_collisions_without_overwriting_them() {
        for blocked in [
            Path::new("data"),
            Path::new("data/replica"),
            Path::new("data/replica/streams"),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().join("data/replica");
            let collision = temporary.path().join(blocked);
            fs::create_dir_all(collision.parent().unwrap()).unwrap();
            fs::write(&collision, b"preserve this file").unwrap();
            let error = StorageProvider::new(StorageConfig::new(root.clone()).unwrap())
                .await
                .unwrap_err();

            assert_eq!(error.path(), root.join("streams/0000"));
            assert!(error.source().unwrap().is::<io::Error>());
            assert_eq!(fs::read(collision).unwrap(), b"preserve this file");
        }
    }

    #[tokio::test]
    async fn paths_match_the_layout_at_every_group_boundary() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let root = provider.root_directory();

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

        // Probe an exclusively claimed stream for the no-I/O guarantee; the
        // fixed boundary IDs above may belong to concurrently running cases.
        provider.log_file_path(LogFileId::new(stream_id, LogFileNumber::MIN));
        provider.index_file_path(LogFileId::new(stream_id, LogFileNumber::MIN));
        assert_eq!(
            provider
                .stream_directory(stream_id)
                .read_dir()
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn checkpoint_paths_use_stream_bases_without_creating_files() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let root = provider.root_directory();

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
        assert!(!provider.checkpoint_file_path(stream_id).exists());
    }

    #[tokio::test]
    async fn relative_configuration_produces_absolute_paths() {
        let current = std::env::current_dir().unwrap();
        let temporary = tempfile::tempdir_in(&current).unwrap();
        let relative = temporary
            .path()
            .strip_prefix(&current)
            .unwrap()
            .join("log data");
        let root = current.join(&relative);
        let provider = StorageProvider::new(StorageConfig::new(relative).unwrap())
            .await
            .unwrap();
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);

        assert_eq!(provider.root_directory(), root);
        assert_eq!(
            provider.log_file_path(id),
            root.join("streams/0000/000/000/000/000/000000000000000.log")
        );
        assert_eq!(
            provider.checkpoint_file_path(StreamId::MIN),
            root.join("streams/0000/checkpoint.json"),
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preserves_non_utf8_root_components() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let temporary = tempfile::tempdir().unwrap();
        let root = temporary
            .path()
            .join(OsString::from_vec(b"log-\xff".to_vec()));
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap())
            .await
            .unwrap();
        let id = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);

        assert_eq!(
            provider.log_file_path(id),
            root.join("streams/0000/000/000/000/000/000000000000000.log")
        );
        assert_eq!(
            provider.checkpoint_file_path(StreamId::MIN),
            root.join("streams/0000/checkpoint.json"),
        );
        assert!(root.is_dir());
    }

    #[tokio::test]
    async fn maximum_log_search_backtracks_past_empty_and_index_only_branches() {
        let temporary = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            None
        );
        let stream = temporary.path().join("streams/0000");
        assert_eq!(stream.read_dir().unwrap().count(), 0);
        // Directory disappearance still has the same empty-search result.
        fs::remove_dir(&stream).unwrap();
        assert_eq!(
            provider.maximum_log_file(StreamId::MIN).await.unwrap(),
            None
        );
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
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
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
            let id = LogFileId::new(stream_id, LogFileNumber::new(number).unwrap());
            let path = provider.log_file_path(id);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"").unwrap();
            assert_eq!(
                provider.maximum_log_file(stream_id).await.unwrap(),
                Some(id)
            );
        }
    }

    #[tokio::test]
    async fn maximum_log_search_ignores_noncanonical_entries_and_reports_io_errors() {
        let temporary = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(temporary.path().into()).unwrap())
            .await
            .unwrap();
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
        fs::remove_dir(&obstructed).unwrap();
        fs::write(&obstructed, b"not a directory").unwrap();
        let error = provider.maximum_log_file(StreamId::MAX).await.unwrap_err();
        assert_eq!(error.path(), obstructed);
        assert!(matches!(error, super::StorageError::Io { .. }));
    }

    #[tokio::test]
    async fn unpolled_pair_creation_leaves_the_stream_empty() {
        let fixture = storage_fixture().await;
        let provider = fixture.provider();
        let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
        let creation = provider.create_log_and_index(id);
        drop(creation);
        assert_eq!(
            provider
                .stream_directory(id.stream_id())
                .read_dir()
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn pair_creation_returns_empty_seekable_read_write_files_and_preserves_neighbors() {
        let fixture = storage_fixture().await;
        let other = storage_fixture().await;
        let provider = fixture.provider();
        let checkpoint = provider.checkpoint_file_path(fixture.stream_id());
        let other_checkpoint = provider.checkpoint_file_path(other.stream_id());
        fs::write(&checkpoint, b"existing checkpoint").unwrap();
        fs::write(&other_checkpoint, b"other stream").unwrap();

        // Reuse a leaf, cross its boundary, and create all deeper range levels.
        for number in [0, 999, 1_000, 1_000_000_000_000] {
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::new(number).unwrap());
            let (mut log, mut index) = provider.create_log_and_index(id).await.unwrap();
            for (file, path) in [
                (&mut log, provider.log_file_path(id)),
                (&mut index, provider.index_file_path(id)),
            ] {
                assert_eq!(file.metadata().await.unwrap().len(), 0);
                assert_eq!(file.stream_position().await.unwrap(), 0);
                file.write_all(b"abc").await.unwrap();
                file.flush().await.unwrap();
                file.seek(io::SeekFrom::Start(1)).await.unwrap();
                file.write_all(b"X").await.unwrap();
                file.flush().await.unwrap();
                file.seek(io::SeekFrom::Start(0)).await.unwrap();
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"aXc"); // Seeking really controls writes, not append mode.
                assert_eq!(fs::read(path).unwrap(), b"aXc");
            }
        }
        for number in [0, 999, 1_000, 1_000_000_000_000] {
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::new(number).unwrap());
            assert_eq!(fs::read(provider.log_file_path(id)).unwrap(), b"aXc");
            assert_eq!(fs::read(provider.index_file_path(id)).unwrap(), b"aXc");
        }
        assert_eq!(fs::read(checkpoint).unwrap(), b"existing checkpoint");
        assert_eq!(fs::read(other_checkpoint).unwrap(), b"other stream");
    }

    #[tokio::test]
    async fn pair_creation_preserves_existing_logs_and_never_touches_their_indexes() {
        for bytes in [b"".as_slice(), b"existing log"] {
            for has_index in [false, true] {
                let fixture = storage_fixture().await;
                let provider = fixture.provider();
                let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
                let log = provider.log_file_path(id);
                let index = provider.index_file_path(id);
                fs::create_dir_all(log.parent().unwrap()).unwrap();
                fs::write(&log, bytes).unwrap();
                if has_index {
                    fs::write(&index, b"existing index").unwrap();
                }

                let error = provider.create_log_and_index(id).await.unwrap_err();
                assert_eq!(error.path(), log);
                assert_eq!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .kind(),
                    io::ErrorKind::AlreadyExists
                );
                assert_eq!(fs::read(log).unwrap(), bytes);
                if has_index {
                    assert_eq!(fs::read(index).unwrap(), b"existing index");
                } else {
                    assert!(!index.exists());
                }
            }
        }
    }

    #[tokio::test]
    async fn pair_creation_preserves_orphan_indexes_and_leaves_partial_creation_for_recovery() {
        for bytes in [b"".as_slice(), b"orphan index"] {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
            let log = provider.log_file_path(id);
            let index = provider.index_file_path(id);
            fs::create_dir_all(index.parent().unwrap()).unwrap();
            fs::write(&index, bytes).unwrap();

            let error = provider.create_log_and_index(id).await.unwrap_err();
            assert_eq!(error.path(), index);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<io::Error>()
                    .unwrap()
                    .kind(),
                io::ErrorKind::AlreadyExists
            );
            assert_eq!(fs::read(&log).unwrap(), b"");
            assert_eq!(fs::read(&index).unwrap(), bytes);

            // A retry must not silently adopt or overwrite the incomplete pair.
            let error = provider.create_log_and_index(id).await.unwrap_err();
            assert_eq!(error.path(), log);
            assert_eq!(fs::read(log).unwrap(), b"");
            assert_eq!(fs::read(index).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn pair_creation_reports_parent_obstructions_without_modifying_them() {
        for depth in 0..4 {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
            let mut blocked = provider.stream_directory(id.stream_id());
            for _ in 0..=depth {
                blocked.push("000");
            }
            fs::create_dir_all(blocked.parent().unwrap()).unwrap();
            fs::write(&blocked, b"not a directory").unwrap();
            let log = provider.log_file_path(id);

            let error = provider.create_log_and_index(id).await.unwrap_err();
            assert_eq!(error.path(), log.parent().unwrap());
            assert!(error.source().unwrap().is::<io::Error>());
            assert_eq!(fs::read(blocked).unwrap(), b"not a directory");
            assert!(!log.exists());
            assert!(!provider.index_file_path(id).exists());
        }
    }

    #[tokio::test]
    async fn pair_creation_preserves_directory_collisions_at_either_file_path() {
        for block_index in [false, true] {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
            let log = provider.log_file_path(id);
            let index = provider.index_file_path(id);
            let blocked = if block_index { &index } else { &log };
            fs::create_dir_all(blocked).unwrap();
            let sentinel = blocked.join("keep");
            fs::write(&sentinel, b"preserve me").unwrap();

            let error = provider.create_log_and_index(id).await.unwrap_err();
            assert_eq!(error.path(), blocked);
            assert!(error.source().unwrap().is::<io::Error>());
            assert_eq!(fs::read(sentinel).unwrap(), b"preserve me");
            if block_index {
                assert_eq!(fs::read(log).unwrap(), b"");
            } else {
                assert!(!index.exists());
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pair_creation_rejects_dangling_file_links_without_creating_their_targets() {
        for link_index in [false, true] {
            let fixture = storage_fixture().await;
            let provider = fixture.provider();
            let id = LogFileId::new(fixture.stream_id(), LogFileNumber::MIN);
            let log = provider.log_file_path(id);
            let index = provider.index_file_path(id);
            let linked = if link_index { &index } else { &log };
            fs::create_dir_all(linked.parent().unwrap()).unwrap();
            let target = provider
                .stream_directory(id.stream_id())
                .join("must-not-create");
            std::os::unix::fs::symlink(&target, linked).unwrap();

            let error = provider.create_log_and_index(id).await.unwrap_err();
            assert_eq!(error.path(), linked);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<io::Error>()
                    .unwrap()
                    .kind(),
                io::ErrorKind::AlreadyExists
            );
            assert_eq!(fs::read_link(linked).unwrap(), target);
            assert!(!target.exists());
            if link_index {
                assert_eq!(fs::read(log).unwrap(), b"");
            } else {
                assert!(!index.exists());
            }
        }
    }

    #[tokio::test]
    async fn validation_opens_preserve_existing_bytes_and_index_creation_is_explicit() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let id = LogFileId::new(stream_id, LogFileNumber::MIN);
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
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let id = LogFileId::new(stream_id, LogFileNumber::MIN);
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

    #[tokio::test]
    async fn pair_removal_is_idempotent_handles_orphan_indexes_and_preserves_collisions() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let file = LogFileId::new(stream_id, LogFileNumber::MIN);
        provider.remove_log_and_index(file..=file).await.unwrap();
        assert_eq!(
            provider
                .stream_directory(stream_id)
                .read_dir()
                .unwrap()
                .count(),
            0
        );
        let log = provider.log_file_path(file);
        let index = provider.index_file_path(file);
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(&index, b"orphan").unwrap();
        provider.remove_log_and_index(file..=file).await.unwrap();
        assert!(!index.exists());
        fs::write(&log, b"discard").unwrap();
        fs::write(&index, b"discard").unwrap();
        provider.remove_log_and_index(file..=file).await.unwrap();
        provider.remove_log_and_index(file..=file).await.unwrap();
        assert!(!log.exists());
        assert!(!index.exists());
        assert!(log.parent().unwrap().exists());
        fs::write(&log, b"discard").unwrap();
        fs::create_dir(&index).unwrap();
        let error = provider
            .remove_log_and_index(file..=file)
            .await
            .unwrap_err();
        assert_eq!(error.path(), index);
        assert!(!log.exists());
        assert!(index.is_dir());
    }

    #[tokio::test]
    async fn pair_range_removal_is_inclusive_across_directories_and_preserves_neighbors() {
        let test_stream = storage_fixture().await;
        let other_test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let file = |number| LogFileId::new(stream_id, LogFileNumber::new(number).unwrap());
        let first = file(999);
        let last = file(1002);
        let discarded = [
            provider.log_file_path(first),
            provider.index_file_path(first),
            provider.log_file_path(file(1000)),
            provider.index_file_path(last),
        ];
        let other_stream = LogFileId::new(other_test_stream.stream_id(), first.file_number());
        let preserved = [
            provider.log_file_path(file(998)),
            provider.index_file_path(file(998)),
            provider.log_file_path(file(1003)),
            provider.index_file_path(file(1003)),
            provider.log_file_path(other_stream),
            provider.index_file_path(other_stream),
            provider.checkpoint_file_path(stream_id),
        ];
        for path in discarded.iter().chain(&preserved) {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"original contents").unwrap();
        }

        // Includes a log-only pair, an absent pair and an index-only pair.
        provider.remove_log_and_index(first..=last).await.unwrap();
        provider.remove_log_and_index(first..=last).await.unwrap();

        for number in 999..=1002 {
            assert!(!provider.log_file_path(file(number)).exists());
            assert!(!provider.index_file_path(file(number)).exists());
        }
        for path in preserved {
            assert_eq!(fs::read(path).unwrap(), b"original contents");
        }
        for path in discarded {
            assert!(path.parent().unwrap().is_dir());
        }
    }

    #[tokio::test]
    async fn pair_range_removal_rejects_invalid_bounds_before_deleting_anything() {
        let test_stream = storage_fixture().await;
        let other_test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let first = LogFileId::new(stream_id, LogFileNumber::new(2).unwrap());
        let earlier = LogFileId::new(stream_id, LogFileNumber::new(1).unwrap());
        let other_stream = LogFileId::new(other_test_stream.stream_id(), first.file_number());
        for id in [first, earlier, other_stream] {
            let log = provider.log_file_path(id);
            fs::create_dir_all(log.parent().unwrap()).unwrap();
            fs::write(log, b"preserve log").unwrap();
            fs::write(provider.index_file_path(id), b"preserve index").unwrap();
        }

        for (last, expected) in [
            (
                earlier,
                LogFileIdRangeError::ReversedFiles {
                    first,
                    last: earlier,
                },
            ),
            (
                other_stream,
                LogFileIdRangeError::DifferentStreams {
                    first,
                    last: other_stream,
                },
            ),
        ] {
            let StorageError::Io { path, source } = provider
                .remove_log_and_index(first..=last)
                .await
                .unwrap_err()
            else {
                panic!("expected a path-bearing I/O error");
            };
            assert_eq!(path, provider.log_file_path(first));
            assert_eq!(source.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                source
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<LogFileIdRangeError>(),
                Some(&expected)
            );
        }

        for id in [first, earlier, other_stream] {
            assert_eq!(
                fs::read(provider.log_file_path(id)).unwrap(),
                b"preserve log"
            );
            assert_eq!(
                fs::read(provider.index_file_path(id)).unwrap(),
                b"preserve index"
            );
        }
    }
}

#[cfg(test)]
mod clustering_configuration_tests {
    use super::*;
    use crate::{
        clustering::configuration::{
            ClusterMembership, ClusteringConfiguration, Hostname, NodeName,
        },
        storage::test_support::{remove_test_path, report_cleanup_error, shared_storage},
    };
    use std::fs;
    use tokio::sync::{Mutex, MutexGuard};
    use uuid::Uuid;

    /// Cleans root-wide metadata before releasing its exclusive test lock.
    struct ClusteringFixture {
        paths: [PathBuf; 2],
        _guard: MutexGuard<'static, ()>,
    }

    impl Drop for ClusteringFixture {
        fn drop(&mut self) {
            for path in &self.paths {
                if let Err(error) = remove_test_path(path) {
                    report_cleanup_error(error);
                }
            }
        }
    }

    // Clustering metadata is root-wide. Serialize only these short metadata
    // cases; stream tests keep using their independently claimed directories.
    async fn clustering_fixture() -> (&'static StorageProvider, ClusteringFixture) {
        static METADATA: Mutex<()> = Mutex::const_new(());
        let provider = shared_storage().await;
        let guard = METADATA.lock().await;
        let path = provider.clustering_configuration_file_path();
        (
            provider,
            ClusteringFixture {
                paths: [path.clone(), path.with_added_extension("pending")],
                _guard: guard,
            },
        )
    }

    #[tokio::test]
    async fn metadata_cleanup_runs_before_unlocking_on_success_and_panic() {
        for panicking in [false, true] {
            let (provider, guard) = clustering_fixture().await;
            let path = provider.clustering_configuration_file_path();
            let pending = path.with_added_extension("pending");
            assert!(!path.exists());
            assert!(!pending.exists());
            fs::write(&path, b"test metadata").unwrap();
            fs::write(&pending, b"staging").unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _guard = guard;
                if panicking {
                    panic!("original metadata test failure");
                }
            }));
            assert_eq!(result.is_err(), panicking);
            // Reacquire before inspecting the shared paths: another test may
            // run between our scopes, but its guard must also clean before exit.
            let (_provider, _guard) = clustering_fixture().await;
            assert!(!path.exists());
            assert!(!pending.exists());
        }
    }

    #[tokio::test]
    async fn absent_read_has_no_side_effects_and_both_modes_round_trip() {
        for node in [
            None,
            Some(NodeName::Master),
            Some(NodeName::Replica1),
            Some(NodeName::Replica2),
        ] {
            let (provider, _guard) = clustering_fixture().await;
            let root = provider.root_directory();
            assert!(
                provider
                    .read_clustering_configuration()
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(!root.join("clustering.json").exists());
            assert_eq!(root.read_dir().unwrap().count(), 1);
            let config = match node {
                None => ClusteringConfiguration::Single,
                Some(node) => ClusteringConfiguration::Cluster(ClusterMembership::new(
                    node,
                    Hostname::parse("cluster.example".into()).unwrap(),
                )),
            };
            let stored = EstablishedClusteringConfiguration::new(Uuid::from_u128(1), config);
            provider
                .write_clustering_configuration(&stored)
                .await
                .unwrap();
            assert_eq!(
                provider.read_clustering_configuration().await.unwrap(),
                Some(stored)
            );
            assert!(!root.join("clustering.json.pending").exists());
        }
    }

    #[tokio::test]
    async fn reads_independent_fixture_and_replaces_it_when_requested() {
        let (provider, _guard) = clustering_fixture().await;
        let root = provider.root_directory();
        let path = root.join("clustering.json");
        let fixture = br#"{"cluster_id":"00000000-0000-0000-0000-000000000001","clustering_configuration":"single"}"#;
        let mut bytes = fixture.to_vec();
        bytes.resize(4096, b' ');
        fs::write(&path, &bytes).unwrap();
        let pending = root.join("clustering.json.pending");
        fs::write(&pending, b"interrupted staging").unwrap();
        let loaded = provider
            .read_clustering_configuration()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded,
            EstablishedClusteringConfiguration::new(
                Uuid::from_u128(1),
                ClusteringConfiguration::Single
            )
        );
        // Published metadata takes precedence over staging and accepts exactly 4 KiB.
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read(&pending).unwrap(), b"interrupted staging");
        let replacement = EstablishedClusteringConfiguration::new(
            Uuid::from_u128(2),
            ClusteringConfiguration::Single,
        );
        provider
            .write_clustering_configuration(&replacement)
            .await
            .unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            br#"{"cluster_id":"00000000-0000-0000-0000-000000000002","clustering_configuration":"single"}"#
        );
        assert_eq!(
            provider.read_clustering_configuration().await.unwrap(),
            Some(replacement)
        );
    }

    #[tokio::test]
    async fn reads_reject_invalid_metadata_and_writes_replace_stale_staging() {
        let (provider, _guard) = clustering_fixture().await;
        let root = provider.root_directory();
        let path = root.join("clustering.json");
        for fixture in [
            b"".as_slice(),
            b"{}",
            b"null",
            br#"{"cluster_id":"bad","clustering_configuration":"single"}"#,
        ] {
            fs::write(&path, fixture).unwrap();
            assert!(matches!(
                provider.read_clustering_configuration().await,
                Err(StorageError::Json { .. })
            ));
            assert_eq!(fs::read(&path).unwrap(), fixture);
        }
        fs::write(&path, vec![b' '; 4097]).unwrap();
        match provider.read_clustering_configuration().await.unwrap_err() {
            StorageError::TooLarge {
                path: rejected,
                max_bytes,
            } => {
                assert_eq!(rejected, path);
                assert_eq!(max_bytes, 4096);
            }
            error => panic!("expected size-limit error, got {error:?}"),
        }
        fs::remove_file(&path).unwrap();
        let pending = root.join("clustering.json.pending");
        fs::write(&pending, b"partial").unwrap();
        assert_eq!(
            provider.read_clustering_configuration().await.unwrap(),
            None
        );
        assert!(!path.exists());
        assert_eq!(fs::read(&pending).unwrap(), b"partial");
        let stored = EstablishedClusteringConfiguration::new(
            Uuid::from_u128(1),
            ClusteringConfiguration::Single,
        );
        provider
            .write_clustering_configuration(&stored)
            .await
            .unwrap();
        assert!(!root.join("clustering.json.pending").exists());
        assert_eq!(
            provider.read_clustering_configuration().await.unwrap(),
            Some(stored)
        );
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::storage::test_support::storage_fixture;
    use std::fs;

    #[tokio::test]
    async fn reads_literal_checkpoint_and_enforces_the_inclusive_size_limit() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // Keep the independent boundary-value fixture unchanged. Storage parses
        // metadata; matching its stream to the requested path is the initializer's job.
        let json = r#"{"end":{"record_id":{"stream_id":4095,"sequence_number":18446744073709551615},"position":3382654560}}"#;
        let mut bytes = json.as_bytes().to_vec();
        bytes.resize(4096, b' ');
        fs::write(&path, &bytes).unwrap();
        let checkpoint = provider.read_checkpoint(stream_id).await.unwrap().unwrap();
        assert_eq!(checkpoint.end().record_id().stream_id(), StreamId::MAX);
        assert_eq!(
            checkpoint.end().record_id().sequence_number().get(),
            u64::MAX
        );
        assert_eq!(checkpoint.end().position(), 3_382_654_560);
        bytes.push(b' ');
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(provider.read_checkpoint(stream_id).await,
            Err(StorageError::TooLarge { path: failed, max_bytes: 4096 }) if failed == path));
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn distinguishes_absence_from_invalid_json_and_io_failures() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        assert_eq!(provider.read_checkpoint(stream_id).await.unwrap(), None);
        assert_eq!(path.parent().unwrap().read_dir().unwrap().count(), 0);
        for json in [
            "",
            "{}",
            "null",
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}} trailing"#,
        ] {
            fs::write(&path, json).unwrap();
            assert!(matches!(provider.read_checkpoint(stream_id).await,
                Err(StorageError::Json { path: failed, .. }) if failed == path));
            assert_eq!(fs::read_to_string(&path).unwrap(), json);
        }
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(matches!(provider.read_checkpoint(stream_id).await,
            Err(StorageError::Io { path: failed, .. }) if failed == path));
    }

    #[tokio::test]
    async fn checkpoint_publication_replaces_metadata_and_truncates_interrupted_staging() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let first_json = format!(
            r#"{{"end":{{"record_id":{{"stream_id":{stream_id},"sequence_number":0}},"position":16}}}}"#
        );
        let next_json = format!(
            r#"{{"end":{{"record_id":{{"stream_id":{stream_id},"sequence_number":1}},"position":32}}}}"#
        );
        let first: StreamCheckpoint = serde_json::from_str(&first_json).unwrap();
        let next: StreamCheckpoint = serde_json::from_str(&next_json).unwrap();
        provider.write_checkpoint(&first).await.unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), first_json);
        let pending = path.parent().unwrap().join("checkpoint.json.pending");
        fs::write(&pending, b"interrupted metadata").unwrap();
        assert_eq!(
            provider.read_checkpoint(stream_id).await.unwrap(),
            Some(first)
        );
        provider.write_checkpoint(&next).await.unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), next_json);
        assert_eq!(
            provider.read_checkpoint(stream_id).await.unwrap(),
            Some(next)
        );
        assert!(!pending.exists());

        fs::create_dir(&pending).unwrap();
        let collision = pending.join("preserve");
        fs::write(&collision, b"existing contents").unwrap();
        let error = provider.write_checkpoint(&first).await.unwrap_err();
        assert_eq!(error.path(), pending);
        assert_eq!(fs::read_to_string(path).unwrap(), next_json);
        assert_eq!(fs::read(collision).unwrap(), b"existing contents");
    }

    #[tokio::test]
    async fn checkpoint_publication_requires_existing_parents_and_does_not_load_staging() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("absent");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap())
            .await
            .unwrap();
        let checkpoint: StreamCheckpoint = serde_json::from_str(
            r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#,
        )
        .unwrap();
        let path = provider.checkpoint_file_path(StreamId::MIN);
        fs::remove_dir(path.parent().unwrap()).unwrap();
        assert!(provider.write_checkpoint(&checkpoint).await.is_err());
        assert!(!path.parent().unwrap().exists());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let pending = path.parent().unwrap().join("checkpoint.json.pending");
        fs::write(pending, b"never certified").unwrap();
        assert_eq!(provider.read_checkpoint(StreamId::MIN).await.unwrap(), None);
    }

    #[tokio::test]
    async fn checkpoint_rename_failure_preserves_staging_and_allows_retry() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let json = format!(
            r#"{{"end":{{"record_id":{{"stream_id":{stream_id},"sequence_number":7}},"position":128}}}}"#
        );
        let checkpoint: StreamCheckpoint = serde_json::from_str(&json).unwrap();
        let path = provider.checkpoint_file_path(stream_id);
        let pending = path.parent().unwrap().join("checkpoint.json.pending");
        fs::create_dir_all(&path).unwrap();
        let collision = path.join("preserve");
        fs::write(&collision, b"existing contents").unwrap();

        let StorageError::Io {
            path: failed,
            source,
        } = provider.write_checkpoint(&checkpoint).await.unwrap_err()
        else {
            panic!("expected the rename's I/O error");
        };
        assert_eq!(failed, path);
        assert_ne!(source.kind(), io::ErrorKind::NotFound);
        assert_eq!(fs::read_to_string(&pending).unwrap(), json);
        assert_eq!(fs::read(&collision).unwrap(), b"existing contents");

        fs::remove_file(collision).unwrap();
        fs::remove_dir(&path).unwrap();
        provider.write_checkpoint(&checkpoint).await.unwrap();
        assert_eq!(
            provider.read_checkpoint(stream_id).await.unwrap(),
            Some(checkpoint)
        );
        assert!(!pending.exists());
    }
}

#[cfg(test)]
mod json_read_tests {
    use super::*;
    use std::{error::Error, fs};

    #[tokio::test]
    async fn missing_files_return_none_without_creating_paths_or_inspecting_staging() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("absent");
        let path = parent.join("document.json");
        assert_eq!(read_json::<u64>(&path, 16).await.unwrap(), None);
        assert!(!parent.exists());

        fs::create_dir(&parent).unwrap();
        assert_eq!(read_json::<u64>(&path, 16).await.unwrap(), None);
        assert!(!path.exists());

        let pending = path.with_added_extension("pending");
        fs::write(&pending, b"interrupted staging").unwrap();
        assert_eq!(read_json::<u64>(&path, 16).await.unwrap(), None);
        assert!(!path.exists());
        assert_eq!(fs::read(&pending).unwrap(), b"interrupted staging");
    }

    #[tokio::test]
    async fn reads_owned_values_from_independent_json() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct Document {
            name: String,
            values: Vec<u32>,
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        let bytes = br#"{"name":"caf\u00e9","values":[1,2,3]}"#;
        fs::write(&path, bytes).unwrap();

        assert_eq!(
            read_json::<Document>(&path, 128).await.unwrap(),
            Some(Document {
                name: "café".to_owned(),
                values: vec![1, 2, 3],
            })
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);

        fs::write(&path, b"null").unwrap();
        assert_eq!(
            read_json::<Option<u64>>(&path, 4).await.unwrap(),
            Some(None)
        );
    }

    #[tokio::test]
    async fn enforces_inclusive_byte_limit_before_deserializing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        // Whitespace still contributes to the file's byte limit.
        fs::write(&path, b"123 ").unwrap();
        for limit in [4, 5, u64::MAX] {
            assert_eq!(read_json::<u64>(&path, limit).await.unwrap(), Some(123));
        }
        for limit in [0, 3] {
            let StorageError::TooLarge {
                path: failed,
                max_bytes,
            } = read_json::<u64>(&path, limit).await.unwrap_err()
            else {
                panic!("expected the size-limit error");
            };
            assert_eq!(failed, path);
            assert_eq!(max_bytes, limit);
        }

        // Oversized malformed input must report the size limit, not a JSON error.
        fs::write(&path, b"not JSON").unwrap();
        assert!(matches!(
            read_json::<u64>(&path, 4).await,
            Err(StorageError::TooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn invalid_json_preserves_the_path_and_serde_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        for bytes in [
            b"".as_slice(),
            b" \n",
            b"{",
            b"true false",
            b"\"invalid \xff\"",
        ] {
            fs::write(&path, bytes).unwrap();
            let error = read_json::<serde_json::Value>(&path, 64).await.unwrap_err();
            assert_eq!(error.path(), path);
            assert!(error.source().unwrap().is::<serde_json::Error>());
            let StorageError::Json { source, .. } = error else {
                panic!("expected the JSON error for {bytes:?}");
            };
            assert!(source.is_eof() || source.is_syntax());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn deserialization_enforces_the_target_types_validation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        // This is valid JSON, but invalid for the requested type.
        fs::write(&path, b"0").unwrap();
        let StorageError::Json {
            path: failed,
            source,
        } = read_json::<std::num::NonZeroU64>(&path, 1)
            .await
            .unwrap_err()
        else {
            panic!("expected model validation to fail");
        };
        assert_eq!(failed, path);
        assert!(source.is_data());
    }

    #[tokio::test]
    async fn filesystem_errors_preserve_the_path_and_io_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        fs::create_dir(&path).unwrap();
        let error = read_json::<u64>(&path, 16).await.unwrap_err();
        assert_eq!(error.path(), path);
        assert!(error.source().unwrap().is::<io::Error>());
        let StorageError::Io { source, .. } = error else {
            panic!("expected a filesystem error");
        };
        // Opening a directory fails on Windows; reading it fails on Unix.
        assert_ne!(source.kind(), io::ErrorKind::NotFound);
        assert!(source.raw_os_error().is_some());
        assert!(path.is_dir());
    }
}

#[cfg(test)]
mod json_write_tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn publishes_and_replaces_documents_with_or_without_stale_staging() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("document.json");
        let pending = directory.path().join("document.json.pending");
        write_json_atomic(&path, "original document").await.unwrap();
        assert_eq!(fs::read(&path).unwrap(), br#""original document""#);
        assert!(!pending.exists());

        write_json_atomic(&path, "replacement").await.unwrap();
        assert_eq!(fs::read(&path).unwrap(), br#""replacement""#);
        assert!(!pending.exists());

        // Shorter JSON must leave neither the old document nor a staging suffix.
        fs::write(&pending, b"interrupted staging contents").unwrap();
        write_json_atomic(&path, "x").await.unwrap();
        assert_eq!(fs::read(&path).unwrap(), br#""x""#);
        assert!(!pending.exists());
    }

    #[tokio::test]
    async fn serialization_failure_changes_no_files() {
        // Serde supports this map, but JSON cannot encode an array as an object key.
        let invalid_json = std::collections::BTreeMap::from([(vec![1, 2], 3)]);
        for existing_files in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let parent = directory.path().join("metadata");
            let path = parent.join("document.json");
            let pending = parent.join("document.json.pending");
            if existing_files {
                fs::create_dir(&parent).unwrap();
                fs::write(&path, b"original document").unwrap();
                fs::write(&pending, b"original staging").unwrap();
            }

            let StorageError::Json {
                path: failed,
                source,
            } = write_json_atomic(&path, &invalid_json).await.unwrap_err()
            else {
                panic!("expected the serialization error");
            };
            assert_eq!(failed, path);
            assert_eq!(source.to_string(), "key must be a string");
            if existing_files {
                assert_eq!(fs::read(&path).unwrap(), b"original document");
                assert_eq!(fs::read(&pending).unwrap(), b"original staging");
            } else {
                assert!(!parent.exists());
            }
        }
    }

    #[tokio::test]
    async fn writing_does_not_create_parent_directories() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("absent");
        let path = parent.join("document.json");
        let StorageError::Io {
            path: failed,
            source,
        } = write_json_atomic(&path, &[1, 2, 3][..]).await.unwrap_err()
        else {
            panic!("expected the missing parent's I/O error");
        };
        assert_eq!(failed, parent.join("document.json.pending"));
        assert_eq!(source.kind(), io::ErrorKind::NotFound);
        assert!(!parent.exists());
    }
}

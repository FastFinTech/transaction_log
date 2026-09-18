use super::{StorageConfig, StorageError};
use crate::clustering::configuration::EstablishedClusteringConfiguration;
use crate::streams::LogFileId;
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use transaction_log_exports::StreamId;
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

    /// Loads stored clustering settings, returning `None` only when the file is absent.
    ///
    /// Performs bounded blocking startup I/O without creating directories or assigning
    /// a UUID. Malformed or unreadable metadata is an error, never fresh storage.
    pub fn read_clustering_configuration(
        &self,
    ) -> Result<Option<EstablishedClusteringConfiguration>, StorageError> {
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
        file.take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| StorageError::io(path.clone(), source))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(StorageError::TooLarge {
                path,
                max_bytes: MAX_BYTES,
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
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::io;
    use std::path::Path;

    use crate::streams::LogFileNumber;

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

const FILE_NAME: &str = "clustering.json";
const PENDING_FILE_NAME: &str = ".clustering.pending";
const MAX_BYTES: u64 = 4096;

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
        fs::write(&path, vec![b' '; MAX_BYTES as usize + 1]).unwrap();
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

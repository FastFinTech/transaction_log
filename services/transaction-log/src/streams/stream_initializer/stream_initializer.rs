use std::io;

use anyhow::{Result, bail, ensure};
use transaction_log_exports::StreamId;

use super::InitializedStream;
use crate::{
    storage::{StorageError, StorageProvider},
    streams::{
        IndexedLogValidationError, IndexedLogValidator, LogFileId, RecordEndLocation,
        StreamCheckpoint, ValidatedFilePair,
    },
};

/// Stateless entry point for initializing one stream's active log/index pair.
///
/// Loading, discovery, pair validation, cleanup and pair preparation are implemented.
/// Final handover is implemented; checkpoint publication remains scaffolded.
/// Startup does not call it.
pub struct StreamInitializer;

impl StreamInitializer {
    /// Initializes one stream and returns its active files and file-local endpoint.
    ///
    /// Creates private state and runs checkpoint loading, discovery, validation,
    /// cleanup, pair preparation and checkpoint advancement in that order. Each
    /// fallible step must succeed before the next starts. Finally consumes the
    /// state to return the active pair.
    /// The caller must provide exclusive recovery access to this stream.
    ///
    /// # Panics
    ///
    /// Currently panics in checkpoint advancement after successful pair preparation
    /// because checkpoint publication is still an unimplemented scaffold.
    pub async fn initialize(
        stream_id: StreamId,
        storage_provider: &StorageProvider,
    ) -> Result<InitializedStream> {
        let mut state = InitializationState::new(stream_id, storage_provider);
        state.load_checkpoint().await?;
        state.discover_log_files().await?;
        state.validate_files().await?;
        state.remove_later_files().await?;
        state.prepare_active_pair().await?;
        state.advance_checkpoint().await?;
        Ok(state.finish())
    }
}

/// Private working state owned by a single initialization operation.
struct InitializationState<'a> {
    /// Stream selected by the public entry point.
    stream_id: StreamId,
    /// Borrowed path and file operations; never retained in the returned result.
    storage_provider: &'a StorageProvider,
    /// Original checkpoint, loaded and checked before discovering log files.
    checkpoint: Option<StreamCheckpoint>,
    /// Highest existing log found during discovery; also bounds later-file cleanup.
    maximum_log_file: Option<LogFileId>,
    /// Latest accepted endpoint across the stream, even if the active pair is empty.
    recovered_end: Option<RecordEndLocation>,
    /// Retained partial pair, or a newly prepared empty pair; absent until available.
    active_pair: Option<InitializedStream>,
    /// First pair to discard through the discovered maximum, including a gap's index.
    first_file_to_remove: Option<LogFileId>,
}

impl<'a> InitializationState<'a> {
    /// Establishes empty working state without performing I/O.
    fn new(stream_id: StreamId, storage_provider: &'a StorageProvider) -> Self {
        Self {
            stream_id,
            storage_provider,
            checkpoint: None,
            maximum_log_file: None,
            recovered_end: None,
            active_pair: None,
            first_file_to_remove: None,
        }
    }

    /// Loads the optional checkpoint and checks that it belongs to this stream.
    ///
    /// Missing metadata leaves no checkpoint. Read or identity errors preserve
    /// the previous state. This step does not inspect or modify log/index files.
    async fn load_checkpoint(&mut self) -> Result<()> {
        let checkpoint = self
            .storage_provider
            .read_checkpoint(self.stream_id)
            .await?;
        if let Some(checkpoint) = checkpoint {
            let checkpoint_stream_id = checkpoint.end().record_id().stream_id();
            ensure!(
                checkpoint_stream_id == self.stream_id,
                "checkpoint at {} belongs to stream {}, expected {}",
                self.storage_provider
                    .checkpoint_file_path(self.stream_id)
                    .display(),
                checkpoint_stream_id,
                self.stream_id,
            );
        }
        self.checkpoint = checkpoint;
        Ok(())
    }

    /// Finds the maximum log and checks that discovery covers the checkpoint's file.
    ///
    /// Checkpoint loading must succeed first. With no checkpoint, no logs is valid.
    /// Assigns the maximum only after all checks succeed; file contents, indexes
    /// and gaps within the discovered range are checked during validation.
    async fn discover_log_files(&mut self) -> Result<()> {
        let maximum_log_file = self
            .storage_provider
            .maximum_log_file(self.stream_id)
            .await?;
        if let Some(checkpoint) = self.checkpoint {
            let checkpoint_file = checkpoint.end().log_file_id();
            let Some(maximum) = maximum_log_file else {
                bail!(
                    "stream {} has checkpointed file {} but no log files",
                    self.stream_id,
                    checkpoint_file.file_number(),
                );
            };
            // Loading checked the checkpoint's stream; discovery uses that same stream.
            ensure!(
                maximum.file_number() >= checkpoint_file.file_number(),
                "maximum log file {} for stream {} is below checkpointed file {}",
                maximum.file_number(),
                self.stream_id,
                checkpoint_file.file_number(),
            );
        }
        self.maximum_log_file = maximum_log_file;
        Ok(())
    }

    /// Recovers the contiguous prefix, retaining a partial pair and cleanup boundary.
    ///
    /// Loading and discovery must succeed first. Visits files lazily from the
    /// checkpoint's file (or zero), supplying trust only to the checkpoint's file.
    /// A missing uncheckpointed log ends the prefix; other failures propagate.
    /// An empty partial pair preserves the preceding complete file's endpoint.
    ///
    /// Pair repair is not atomic: errors/cancellation can follow completed repairs
    /// and recovered state updates. No later files are removed and no checkpoint
    /// is published here. Call once per initialization, with exclusive file access.
    async fn validate_files(&mut self) -> Result<()> {
        let Some(maximum) = self.maximum_log_file else {
            return Ok(());
        };
        let checkpoint_end = self.checkpoint.map(|checkpoint| checkpoint.end());
        let first =
            checkpoint_end.map_or(LogFileId::first(self.stream_id), |end| end.log_file_id());

        for file_id in first.iter_to(maximum)? {
            let last_trusted = checkpoint_end.filter(|end| end.log_file_id() == file_id);
            match IndexedLogValidator::validate(self.storage_provider, file_id, last_trusted).await
            {
                Ok(ValidatedFilePair::Complete { end }) => self.recovered_end = Some(end),
                Ok(ValidatedFilePair::Partial { end, log, index }) => {
                    self.recovered_end = end.or(self.recovered_end);
                    self.active_pair = Some(InitializedStream {
                        file_id,
                        log,
                        index,
                        end,
                    });
                    self.first_file_to_remove = (file_id != maximum).then(|| file_id.next());
                    break;
                }
                Err(IndexedLogValidationError::Storage(StorageError::Io { path, source }))
                    if last_trusted.is_none()
                        && source.kind() == io::ErrorKind::NotFound
                        && path == self.storage_provider.log_file_path(file_id) =>
                {
                    self.first_file_to_remove = Some(file_id);
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Removes log/index pairs beyond the recovered contiguous prefix.
    ///
    /// Validation must succeed first. No cleanup boundary means no I/O. Otherwise
    /// passes the inclusive range through the discovered maximum to storage,
    /// including a missing log's orphan index. The provider tolerates absent files
    /// and batches asynchronous directory synchronization. Preserves its errors.
    ///
    /// Errors/cancellation can leave partial deletion. Bounds are retained so
    /// cleanup can be repeated after outstanding I/O is quiescent. The retained
    /// pair, recovered endpoint and checkpoint are not changed.
    async fn remove_later_files(&mut self) -> Result<()> {
        let Some(first) = self.first_file_to_remove else {
            return Ok(());
        };
        let maximum = self
            .maximum_log_file
            .expect("a cleanup boundary requires a discovered maximum log file");
        self.storage_provider
            .remove_log_and_index(first..=maximum)
            .await?;
        Ok(())
    }

    /// Retains the partial pair or creates an empty pair after a full or absent stream.
    ///
    /// Validation and cleanup must succeed first. A retained pair is already
    /// synchronized and positioned for appending. Otherwise, creates file zero or
    /// the successor of the recovered complete file. New empty files need no sync:
    /// they contain no records and are not covered by the recovered checkpoint.
    /// Installs the pair after creation succeeds, with no file-local endpoint,
    /// preserving the stream-wide recovered endpoint.
    ///
    /// Errors/cancellation can leave newly created files without updating state.
    /// No rollback or automatic retry is provided. Durability of the recovered
    /// prefix must be established before the following checkpoint publication step.
    async fn prepare_active_pair(&mut self) -> Result<()> {
        if self.active_pair.is_some() {
            return Ok(());
        }
        let file_id = self
            .recovered_end
            .map_or(LogFileId::first(self.stream_id), |end| {
                end.log_file_id().next()
            });
        let (log, index) = self.storage_provider.create_log_and_index(file_id).await?;
        self.active_pair = Some(InitializedStream {
            file_id,
            log,
            index,
            end: None,
        });
        Ok(())
    }

    /// Synchronizes required directories and publishes the recovered stream endpoint.
    ///
    /// Awaits the provider's checkpoint publication after establishing pair durability.
    async fn advance_checkpoint(&mut self) -> Result<()> {
        todo!("publish the recovered checkpoint after cleanup and pair preparation")
    }

    /// Consumes completed state and returns the active pair without additional I/O.
    ///
    /// Called after checkpoint advancement succeeds. A missing prepared pair is
    /// an internal initialization-order violation and panics.
    fn finish(self) -> InitializedStream {
        self.active_pair
            .expect("initialization must prepare an active pair before finishing")
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io, ops::Range};

    use tokio::io::AsyncSeekExt;
    use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

    use super::InitializationState;
    use crate::{
        storage::{
            StorageConfig, StorageError, StorageProvider,
            test_support::{record_fixture, storage_fixture},
        },
        streams::{
            IndexFileValidationError, IndexedLogValidationError, LogFileId, LogFileNumber,
            LogFileValidationError, RecordEndLocation, StreamCheckpoint,
        },
    };

    // Independent protocol fixtures: stream 7, sequences 0/1, payloads empty/abc.
    const FIRST: &[u8] = &[16, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 226, 50, 73];
    const SECOND: &[u8] = &[
        19, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 18, 247, 123, 204,
    ];

    fn file_id(stream_id: StreamId, number: u64) -> LogFileId {
        LogFileId::new(stream_id, LogFileNumber::new(number).unwrap())
    }

    fn store_pair(storage: &StorageProvider, id: LogFileId, log: &[u8], index: Option<&[u8]>) {
        let path = storage.log_file_path(id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, log).unwrap();
        if let Some(index) = index {
            fs::write(storage.index_file_path(id), index).unwrap();
        }
    }

    async fn encoded_records(id: LogFileId, indices: Range<u64>) -> Vec<u8> {
        let mut writer = RecordWriter::for_serialization(Vec::new());
        for index in indices {
            writer
                .write(id.record_id_at(index).unwrap(), |_| Ok::<_, io::Error>(()))
                .unwrap();
        }
        writer.flush_buffer().await.unwrap();
        writer.into_inner()
    }

    async fn validation_state(
        stream_id: StreamId,
        storage: &StorageProvider,
    ) -> InitializationState<'_> {
        let mut state = InitializationState::new(stream_id, storage);
        state.load_checkpoint().await.unwrap();
        state.discover_log_files().await.unwrap();
        state
    }

    fn store_checkpoint(storage: &StorageProvider, end: RecordEndLocation) -> Vec<u8> {
        let path = storage.checkpoint_file_path(end.record_id().stream_id());
        let bytes = serde_json::to_vec(&StreamCheckpoint::new(end)).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, &bytes).unwrap();
        bytes
    }

    #[tokio::test]
    async fn load_checkpoint_accepts_absence_without_creating_files() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let mut state = InitializationState::new(stream_id, provider);

        state.load_checkpoint().await.unwrap();

        assert_eq!(state.checkpoint, None);
        assert_eq!(
            provider
                .root_directory()
                .join(format!("streams/{stream_id:04}"))
                .read_dir()
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn load_checkpoint_loads_literal_metadata_without_requiring_log_files() {
        for (sequence, position) in [(0, 16), (100_001, 35)] {
            let test_stream = storage_fixture().await;
            let provider = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let path = provider.checkpoint_file_path(stream_id);
            let json = format!(
                r#"{{"end":{{"record_id":{{"stream_id":{stream_id},"sequence_number":{sequence}}},"position":{position}}}}}"#
            );
            fs::write(&path, &json).unwrap();
            let mut state = InitializationState::new(stream_id, provider);

            state.load_checkpoint().await.unwrap();

            assert_eq!(
                state.checkpoint,
                Some(StreamCheckpoint::new(RecordEndLocation::new(
                    RecordId::new(stream_id, SequenceNumber::new(sequence)),
                    position,
                ))),
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), json);
            assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
        }
    }

    #[tokio::test]
    async fn load_checkpoint_rejects_another_stream_without_changing_state_or_storage() {
        let test_stream = storage_fixture().await;
        let other_test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        let other_stream_id = other_test_stream.stream_id();
        let json = format!(
            r#"{{"end":{{"record_id":{{"stream_id":{other_stream_id},"sequence_number":0}},"position":16}}}}"#
        );
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &json).unwrap();
        let mut state = InitializationState::new(stream_id, provider);
        let previous = Some(StreamCheckpoint::new(RecordEndLocation::new(
            RecordId::new(stream_id, SequenceNumber::new(0)),
            16,
        )));
        state.checkpoint = previous;

        let error = state.load_checkpoint().await.unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "checkpoint at {} belongs to stream {other_stream_id}, expected {stream_id}",
                path.display(),
            ),
        );
        assert_eq!(state.checkpoint, previous);
        assert_eq!(fs::read_to_string(&path).unwrap(), json);
    }

    #[tokio::test]
    async fn load_checkpoint_preserves_json_errors_and_previous_state() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"{invalid").unwrap();
        let mut state = InitializationState::new(stream_id, provider);
        let previous = Some(StreamCheckpoint::new(RecordEndLocation::new(
            RecordId::new(stream_id, SequenceNumber::new(0)),
            16,
        )));
        state.checkpoint = previous;

        let error = state.load_checkpoint().await.unwrap_err();

        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::Json { path: error_path, .. }) if error_path == &path
        ));
        assert_eq!(state.checkpoint, previous);
        assert_eq!(fs::read(&path).unwrap(), b"{invalid");
    }

    #[tokio::test]
    async fn load_checkpoint_preserves_io_errors_and_previous_state() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let path = provider.checkpoint_file_path(stream_id);
        fs::create_dir_all(&path).unwrap();
        let mut state = InitializationState::new(stream_id, provider);
        let previous = Some(StreamCheckpoint::new(RecordEndLocation::new(
            RecordId::new(stream_id, SequenceNumber::new(0)),
            16,
        )));
        state.checkpoint = previous;

        let error = state.load_checkpoint().await.unwrap_err();

        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::Io { path: error_path, .. }) if error_path == &path
        ));
        assert_eq!(state.checkpoint, previous);
        assert!(path.is_dir());
    }

    #[tokio::test]
    async fn discovery_accepts_absent_logs_without_a_checkpoint() {
        let test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let mut state = InitializationState::new(stream_id, provider);

        state.discover_log_files().await.unwrap();

        assert_eq!(state.maximum_log_file, None);
        assert_eq!(
            provider
                .root_directory()
                .join(format!("streams/{stream_id:04}"))
                .read_dir()
                .unwrap()
                .count(),
            0
        );

        let index = provider.index_file_path(LogFileId::new(stream_id, LogFileNumber::MIN));
        fs::create_dir_all(index.parent().unwrap()).unwrap();
        fs::write(&index, b"orphan index").unwrap();

        state.discover_log_files().await.unwrap();

        assert_eq!(state.maximum_log_file, None);
        assert_eq!(fs::read(&index).unwrap(), b"orphan index");
        assert_eq!(fs::read_dir(index.parent().unwrap()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn discovery_records_the_maximum_for_this_stream_including_an_empty_log() {
        let test_stream = storage_fixture().await;
        let other_test_stream = storage_fixture().await;
        let provider = test_stream.provider();
        let stream_id = test_stream.stream_id();
        for (stream, number, bytes) in [
            (stream_id, 0, &b"unvalidated"[..]),
            (stream_id, 4, &b""[..]),
            (other_test_stream.stream_id(), 10, &b"other stream"[..]),
        ] {
            let file_id = file_id(stream, number);
            let path = provider.log_file_path(file_id);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
        let mut state = InitializationState::new(stream_id, provider);

        state.discover_log_files().await.unwrap();

        let maximum = LogFileId::new(stream_id, LogFileNumber::new(4).unwrap());
        assert_eq!(state.maximum_log_file, Some(maximum));
        assert_eq!(
            fs::metadata(provider.log_file_path(maximum)).unwrap().len(),
            0
        );
        assert!(!provider.index_file_path(maximum).exists());
        let first = LogFileId::new(stream_id, LogFileNumber::MIN);
        assert_eq!(
            fs::read(provider.log_file_path(first)).unwrap(),
            b"unvalidated"
        );
    }

    #[tokio::test]
    async fn discovery_accepts_a_maximum_at_or_after_the_checkpoint_file() {
        for maximum_number in [2, 4] {
            let test_stream = storage_fixture().await;
            let provider = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let maximum = LogFileId::new(stream_id, LogFileNumber::new(maximum_number).unwrap());
            let path = provider.log_file_path(maximum);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"").unwrap();
            let checkpoint = StreamCheckpoint::new(RecordEndLocation::new(
                RecordId::new(stream_id, SequenceNumber::new(200_000)),
                16,
            ));
            let mut state = InitializationState::new(stream_id, provider);
            state.checkpoint = Some(checkpoint);

            state.discover_log_files().await.unwrap();

            // Presence/length of the checkpointed log and index belong to validation.
            assert_eq!(state.maximum_log_file, Some(maximum));
            assert_eq!(state.checkpoint, Some(checkpoint));
            assert_eq!(state.recovered_end, None);
            assert!(!provider.index_file_path(maximum).exists());
            assert_eq!(fs::metadata(path).unwrap().len(), 0);
        }
    }

    #[tokio::test]
    async fn discovery_rejects_absent_or_older_logs_without_replacing_state() {
        for (checkpoint_number, maximum_number) in
            [(0, None), (2, None), (2, Some(0)), (2, Some(1))]
        {
            let test_stream = storage_fixture().await;
            let provider = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let checkpoint_file =
                LogFileId::new(stream_id, LogFileNumber::new(checkpoint_number).unwrap());
            let checkpoint = StreamCheckpoint::new(RecordEndLocation::new(
                checkpoint_file.first_record_id(),
                16,
            ));
            if let Some(number) = maximum_number {
                let path = provider.log_file_path(LogFileId::new(
                    stream_id,
                    LogFileNumber::new(number).unwrap(),
                ));
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, b"preserve").unwrap();
            }
            let mut state = InitializationState::new(stream_id, provider);
            state.checkpoint = Some(checkpoint);
            state.maximum_log_file = Some(checkpoint_file);

            let error = state.discover_log_files().await.unwrap_err();

            let expected_message = match maximum_number {
                None => {
                    format!(
                        "stream {stream_id} has checkpointed file {checkpoint_number} but no log files"
                    )
                }
                Some(number) => format!(
                    "maximum log file {number} for stream {stream_id} is below checkpointed file {checkpoint_number}"
                ),
            };
            assert_eq!(error.to_string(), expected_message);
            assert_eq!(state.maximum_log_file, Some(checkpoint_file));
            assert_eq!(state.checkpoint, Some(checkpoint));
            assert_eq!(
                provider.maximum_log_file(stream_id).await.unwrap(),
                maximum_number.map(|number| {
                    LogFileId::new(stream_id, LogFileNumber::new(number).unwrap())
                }),
            );
        }
    }

    #[tokio::test]
    async fn discovery_preserves_io_errors_and_previous_state() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap())
            .await
            .unwrap();
        let stream_id = StreamId::new(7).unwrap();
        let checkpoint_path = provider.checkpoint_file_path(stream_id);
        let stream_path = checkpoint_path.parent().unwrap();
        fs::remove_dir(stream_path).unwrap();
        fs::write(stream_path, b"not a directory").unwrap();
        let mut state = InitializationState::new(stream_id, &provider);
        let previous = Some(LogFileId::new(stream_id, LogFileNumber::MIN));
        state.maximum_log_file = previous;

        let error = state.discover_log_files().await.unwrap_err();

        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::Io { path, .. }) if path == stream_path
        ));
        assert_eq!(state.maximum_log_file, previous);
        assert_eq!(fs::read(stream_path).unwrap(), b"not a directory");
    }

    #[tokio::test]
    async fn validation_with_no_logs_creates_nothing() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let mut state = validation_state(stream_id, storage).await;

        state.validate_files().await.unwrap();

        assert_eq!(state.recovered_end, None);
        assert!(state.active_pair.is_none());
        assert_eq!(state.first_file_to_remove, None);
        assert_eq!(
            storage
                .root_directory()
                .join(format!("streams/{stream_id:04}"))
                .read_dir()
                .unwrap()
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn validation_stops_at_the_first_partial_pair_and_only_marks_future_cleanup() {
        for (include_record, suffix) in [(false, &[][..]), (true, &[][..]), (true, &[0xff; 3][..])]
        {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let first = record_fixture(stream_id, FIRST);
            let log = if include_record {
                [first.as_slice(), suffix].concat()
            } else {
                Vec::new()
            };
            store_pair(storage, file_id(stream_id, 0), &log, Some(&[0xff; 19]));
            store_pair(
                storage,
                file_id(stream_id, 2),
                b"future log",
                Some(b"future index"),
            );
            let mut state = validation_state(stream_id, storage).await;

            state.validate_files().await.unwrap();

            let end = (!log.is_empty())
                .then(|| RecordEndLocation::new(file_id(stream_id, 0).first_record_id(), 16));
            assert_eq!(state.recovered_end, end);
            assert_eq!(state.first_file_to_remove, Some(file_id(stream_id, 1)));
            let mut pair = state.active_pair.take().unwrap();
            assert_eq!(pair.file_id(), file_id(stream_id, 0));
            assert_eq!(pair.end(), end);
            assert_eq!(
                pair.log.stream_position().await.unwrap(),
                end.map_or(0, |end| end.position())
            );
            assert_eq!(
                pair.index.stream_position().await.unwrap(),
                if end.is_some() { 8 } else { 0 }
            );
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
                if end.is_some() { first.as_slice() } else { &[] }
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
                if end.is_some() {
                    &[16, 0, 0, 0, 0, 0, 0, 0][..]
                } else {
                    &[]
                }
            );
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 2))).unwrap(),
                b"future log"
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 2))).unwrap(),
                b"future index"
            );
            assert!(!storage.checkpoint_file_path(state.stream_id).exists());
        }
    }

    #[tokio::test]
    async fn validation_walks_complete_files_and_preserves_progress_at_an_empty_active_pair() {
        let full_index: Vec<u8> = (1..=100_000_u64)
            .flat_map(|n| (n * 16).to_le_bytes())
            .collect();
        for tail_record_count in [None, Some(0), Some(1)] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let first_log = encoded_records(file_id(stream_id, 0), 0..100_000).await;
            let second_log = encoded_records(file_id(stream_id, 1), 0..100_000).await;
            let tail = match tail_record_count {
                None => None,
                Some(count) => Some(encoded_records(file_id(stream_id, 2), 0..count).await),
            };
            store_pair(storage, file_id(stream_id, 0), &first_log, None);
            store_pair(storage, file_id(stream_id, 1), &second_log, None);
            if let Some(tail) = tail.as_deref() {
                store_pair(storage, file_id(stream_id, 2), tail, None);
            }
            let mut state = validation_state(stream_id, storage).await;

            state.validate_files().await.unwrap();

            let expected_end = if tail.as_ref().is_some_and(|bytes| !bytes.is_empty()) {
                RecordEndLocation::new(file_id(stream_id, 2).first_record_id(), 16)
            } else {
                RecordEndLocation::new(file_id(stream_id, 1).last_record_id(), 1_600_000)
            };
            assert_eq!(state.recovered_end, Some(expected_end));
            assert_eq!(state.first_file_to_remove, None);
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
                full_index
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 1))).unwrap(),
                full_index
            );
            if let Some(tail) = tail.as_deref() {
                let mut pair = state.active_pair.take().unwrap();
                assert_eq!(pair.file_id(), file_id(stream_id, 2));
                assert_eq!(pair.end(), (!tail.is_empty()).then_some(expected_end));
                assert_eq!(pair.log.stream_position().await.unwrap(), tail.len() as u64);
                assert_eq!(
                    pair.index.stream_position().await.unwrap(),
                    if tail.is_empty() { 0 } else { 8 }
                );
            } else {
                assert!(state.active_pair.is_none());
                assert!(!storage.log_file_path(file_id(stream_id, 2)).exists());
                assert!(!storage.index_file_path(file_id(stream_id, 2)).exists());
            }
            assert!(!storage.checkpoint_file_path(state.stream_id).exists());
        }
    }

    #[tokio::test]
    async fn validation_uses_the_original_checkpoint_for_the_index_and_does_not_publish() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let first = record_fixture(stream_id, FIRST);
        let second = record_fixture(stream_id, SECOND);
        store_pair(
            storage,
            file_id(stream_id, 0),
            &[first, second].concat(),
            Some(&16_u64.to_le_bytes()),
        );
        let trusted = RecordEndLocation::new(file_id(stream_id, 0).first_record_id(), 16);
        let checkpoint_bytes = store_checkpoint(storage, trusted);
        let mut state = validation_state(stream_id, storage).await;

        state.validate_files().await.unwrap();

        let expected = RecordEndLocation::new(trusted.record_id().next(), 35);
        assert_eq!(state.recovered_end, Some(expected));
        assert_eq!(state.active_pair.as_ref().unwrap().end(), Some(expected));
        assert_eq!(state.checkpoint.unwrap().end(), trusted);
        assert_eq!(state.first_file_to_remove, None);
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
            [16_u64.to_le_bytes(), 35_u64.to_le_bytes()].concat()
        );
        assert_eq!(
            fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
            checkpoint_bytes
        );
    }

    #[tokio::test]
    async fn validation_starts_in_the_checkpoint_file_and_clears_trust_for_following_files() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        store_pair(
            storage,
            file_id(stream_id, 0),
            b"earlier log",
            Some(b"earlier index"),
        );
        // Deliberately unparseable certified bytes prove that the prefix is not rescanned.
        let trusted_log = vec![0xff; 1_600_000];
        let mut trusted_index = vec![0xff; 800_000];
        trusted_index[799_992..].copy_from_slice(&1_600_000_u64.to_le_bytes());
        store_pair(
            storage,
            file_id(stream_id, 1),
            &trusted_log,
            Some(&trusted_index),
        );
        let trusted = RecordEndLocation::new(file_id(stream_id, 1).last_record_id(), 1_600_000);
        let checkpoint_bytes = store_checkpoint(storage, trusted);
        let next_log = encoded_records(file_id(stream_id, 2), 0..1).await;
        store_pair(storage, file_id(stream_id, 2), &next_log, None);
        let mut state = validation_state(stream_id, storage).await;

        state.validate_files().await.unwrap();

        assert_eq!(
            state.recovered_end,
            Some(RecordEndLocation::new(
                file_id(stream_id, 2).first_record_id(),
                16
            ))
        );
        assert_eq!(
            state.active_pair.as_ref().unwrap().file_id(),
            file_id(stream_id, 2)
        );
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
            b"earlier log"
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
            b"earlier index"
        );
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 1))).unwrap(),
            trusted_log
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 1))).unwrap(),
            trusted_index
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 2))).unwrap(),
            16_u64.to_le_bytes()
        );
        assert_eq!(
            fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
            checkpoint_bytes
        );
    }

    #[tokio::test]
    async fn validation_marks_an_uncheckpointed_gap_including_its_orphan_index() {
        for gap_number in [0, 1] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let gap = file_id(stream_id, gap_number);
            if gap == file_id(stream_id, 1) {
                let full_log = encoded_records(file_id(stream_id, 0), 0..100_000).await;
                store_pair(storage, file_id(stream_id, 0), &full_log, None);
            }
            store_pair(
                storage,
                file_id(stream_id, 2),
                b"future log",
                Some(b"future index"),
            );
            fs::write(storage.index_file_path(gap), b"orphan index").unwrap();
            let mut state = validation_state(stream_id, storage).await;

            state.validate_files().await.unwrap();

            assert_eq!(state.first_file_to_remove, Some(gap));
            assert!(state.active_pair.is_none());
            assert_eq!(
                state.recovered_end,
                (gap == file_id(stream_id, 1)).then(|| RecordEndLocation::new(
                    file_id(stream_id, 0).last_record_id(),
                    1_600_000
                ))
            );
            assert!(!storage.log_file_path(gap).exists());
            assert_eq!(
                fs::read(storage.index_file_path(gap)).unwrap(),
                b"orphan index"
            );
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 2))).unwrap(),
                b"future log"
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 2))).unwrap(),
                b"future index"
            );
        }
    }

    #[tokio::test]
    async fn validation_rejects_a_missing_checkpointed_log_without_marking_cleanup() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        store_pair(storage, file_id(stream_id, 2), b"future log", None);
        fs::write(
            storage.index_file_path(file_id(stream_id, 1)),
            b"orphan index",
        )
        .unwrap();
        let trusted = RecordEndLocation::new(file_id(stream_id, 1).first_record_id(), 16);
        let checkpoint_bytes = store_checkpoint(storage, trusted);
        let mut state = validation_state(stream_id, storage).await;

        let error = state.validate_files().await.unwrap_err();

        assert!(matches!(error.downcast_ref::<IndexedLogValidationError>(),
            Some(IndexedLogValidationError::Storage(StorageError::Io { path, source }))
                if path == &storage.log_file_path(file_id(stream_id, 1)) && source.kind() == io::ErrorKind::NotFound));
        assert_eq!(state.first_file_to_remove, None);
        assert_eq!(state.recovered_end, None);
        assert!(state.active_pair.is_none());
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 1))).unwrap(),
            b"orphan index"
        );
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 2))).unwrap(),
            b"future log"
        );
        assert_eq!(
            fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
            checkpoint_bytes
        );
    }

    #[tokio::test]
    async fn validation_propagates_invalid_trusted_log_and_index_boundaries() {
        for invalid_log in [true, false] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let first = record_fixture(stream_id, FIRST);
            store_pair(storage, file_id(stream_id, 0), &first, None);
            store_pair(storage, file_id(stream_id, 1), b"future log", None);
            let trusted = RecordEndLocation::new(
                file_id(stream_id, 0).first_record_id(),
                if invalid_log { 17 } else { 16 },
            );
            let checkpoint_bytes = store_checkpoint(storage, trusted);
            let mut state = validation_state(stream_id, storage).await;

            let error = state.validate_files().await.unwrap_err();

            match error.downcast_ref::<IndexedLogValidationError>().unwrap() {
                IndexedLogValidationError::Log(LogFileValidationError::InvalidStart(_)) => {
                    assert!(invalid_log)
                }
                IndexedLogValidationError::Index(
                    IndexFileValidationError::TrustedIndexMismatch { .. },
                ) => assert!(!invalid_log),
                error => panic!("unexpected validation error: {error:?}"),
            }
            assert_eq!(state.first_file_to_remove, None);
            assert_eq!(state.recovered_end, None);
            assert!(state.active_pair.is_none());
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
                first
            );
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 1))).unwrap(),
                b"future log"
            );
            assert_eq!(
                fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
                checkpoint_bytes
            );
        }
    }

    #[tokio::test]
    async fn validation_preserves_prior_progress_when_a_later_index_open_fails() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let full_log = encoded_records(file_id(stream_id, 0), 0..100_000).await;
        store_pair(storage, file_id(stream_id, 0), &full_log, None);
        store_pair(storage, file_id(stream_id, 1), b"unvalidated", None);
        let index_path = storage.index_file_path(file_id(stream_id, 1));
        fs::create_dir(&index_path).unwrap();
        let mut state = validation_state(stream_id, storage).await;

        let error = state.validate_files().await.unwrap_err();

        assert!(matches!(error.downcast_ref::<IndexedLogValidationError>(),
            Some(IndexedLogValidationError::Storage(StorageError::Io { path, .. })) if path == &index_path));
        assert_eq!(
            state.recovered_end,
            Some(RecordEndLocation::new(
                file_id(stream_id, 0).last_record_id(),
                1_600_000
            ))
        );
        assert_eq!(state.first_file_to_remove, None);
        assert!(state.active_pair.is_none());
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 1))).unwrap(),
            b"unvalidated"
        );
        assert!(!storage.checkpoint_file_path(state.stream_id).exists());
    }

    #[tokio::test]
    async fn cleanup_without_logs_creates_nothing() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let mut state = validation_state(stream_id, storage).await;
        state.validate_files().await.unwrap();

        state.remove_later_files().await.unwrap();

        assert_eq!(
            storage
                .root_directory()
                .join(format!("streams/{stream_id:04}"))
                .read_dir()
                .unwrap()
                .count(),
            0
        );
        assert_eq!(state.recovered_end, None);
        assert!(state.active_pair.is_none());
    }

    #[tokio::test]
    async fn cleanup_without_a_boundary_keeps_the_final_partial_pair() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let first = record_fixture(stream_id, FIRST);
        store_pair(storage, file_id(stream_id, 0), &first, None);
        let mut state = validation_state(stream_id, storage).await;
        state.validate_files().await.unwrap();

        state.remove_later_files().await.unwrap();

        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
            first
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
            16_u64.to_le_bytes()
        );
        assert_eq!(
            state.active_pair.as_ref().unwrap().file_id(),
            file_id(stream_id, 0)
        );
        assert_eq!(
            state.recovered_end,
            Some(RecordEndLocation::new(
                file_id(stream_id, 0).first_record_id(),
                16
            ))
        );
        assert!(!storage.checkpoint_file_path(state.stream_id).exists());
    }

    #[tokio::test]
    async fn cleanup_after_a_partial_pair_removes_only_later_pairs_through_the_maximum() {
        let test_stream = storage_fixture().await;
        let other_test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        store_pair(
            storage,
            file_id(stream_id, 0),
            b"earlier log",
            Some(b"earlier index"),
        );
        let active = file_id(stream_id, 999);
        let active_log = encoded_records(active, 0..1).await;
        store_pair(storage, active, &active_log, Some(&16_u64.to_le_bytes()));
        let trusted = RecordEndLocation::new(active.first_record_id(), 16);
        let checkpoint_bytes = store_checkpoint(storage, trusted);
        // Cross a range-directory boundary; include log-only, index-only and absent pairs.
        store_pair(storage, file_id(stream_id, 1000), b"log only", None);
        fs::write(
            storage.index_file_path(file_id(stream_id, 1001)),
            b"index only",
        )
        .unwrap();
        store_pair(
            storage,
            file_id(stream_id, 1003),
            b"maximum log",
            Some(b"maximum index"),
        );
        let beyond_maximum_index = storage.index_file_path(file_id(stream_id, 1005));
        fs::write(&beyond_maximum_index, b"outside cleanup range").unwrap();
        let other_stream = file_id(other_test_stream.stream_id(), 1000);
        store_pair(storage, other_stream, b"other log", Some(b"other index"));
        let range_directory = storage
            .log_file_path(file_id(stream_id, 1000))
            .parent()
            .unwrap()
            .to_owned();
        let notes = range_directory.join("notes.txt");
        fs::write(&notes, b"unrelated").unwrap();
        let mut state = validation_state(stream_id, storage).await;
        state.validate_files().await.unwrap();
        assert_eq!(state.first_file_to_remove, Some(file_id(stream_id, 1000)));

        state.remove_later_files().await.unwrap();
        // Repeating successful cleanup tolerates the now-absent pairs.
        state.remove_later_files().await.unwrap();

        for number in 1000..=1003 {
            assert!(!storage.log_file_path(file_id(stream_id, number)).exists());
            assert!(!storage.index_file_path(file_id(stream_id, number)).exists());
        }
        assert!(range_directory.is_dir());
        assert_eq!(fs::read(notes).unwrap(), b"unrelated");
        assert_eq!(
            fs::read(beyond_maximum_index).unwrap(),
            b"outside cleanup range"
        );
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
            b"earlier log"
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
            b"earlier index"
        );
        assert_eq!(
            fs::read(storage.log_file_path(other_stream)).unwrap(),
            b"other log"
        );
        assert_eq!(
            fs::read(storage.index_file_path(other_stream)).unwrap(),
            b"other index"
        );
        assert_eq!(fs::read(storage.log_file_path(active)).unwrap(), active_log);
        assert_eq!(
            fs::read(storage.index_file_path(active)).unwrap(),
            16_u64.to_le_bytes()
        );
        assert_eq!(
            fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
            checkpoint_bytes
        );
        assert_eq!(state.checkpoint.unwrap().end(), trusted);
        assert_eq!(state.recovered_end, Some(trusted));
        let pair = state.active_pair.as_mut().unwrap();
        assert_eq!(pair.file_id(), active);
        assert_eq!(pair.end(), Some(trusted));
        assert_eq!(pair.log.stream_position().await.unwrap(), 16);
        assert_eq!(pair.index.stream_position().await.unwrap(), 8);
    }

    #[tokio::test]
    async fn cleanup_after_a_gap_removes_its_orphan_index_and_later_pairs() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let full_log = encoded_records(file_id(stream_id, 0), 0..100_000).await;
        store_pair(storage, file_id(stream_id, 0), &full_log, None);
        fs::write(
            storage.index_file_path(file_id(stream_id, 1)),
            b"orphan index",
        )
        .unwrap();
        store_pair(
            storage,
            file_id(stream_id, 3),
            b"future log",
            Some(b"future index"),
        );
        let mut state = validation_state(stream_id, storage).await;
        state.validate_files().await.unwrap();
        assert_eq!(state.first_file_to_remove, Some(file_id(stream_id, 1)));
        let full_index = fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap();

        state.remove_later_files().await.unwrap();

        for number in 1..=3 {
            assert!(!storage.log_file_path(file_id(stream_id, number)).exists());
            assert!(!storage.index_file_path(file_id(stream_id, number)).exists());
        }
        assert_eq!(
            fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
            full_log
        );
        assert_eq!(
            fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
            full_index
        );
        assert_eq!(
            state.recovered_end,
            Some(RecordEndLocation::new(
                file_id(stream_id, 0).last_record_id(),
                1_600_000
            ))
        );
        assert!(state.active_pair.is_none());
        assert!(!storage.checkpoint_file_path(state.stream_id).exists());
    }

    #[tokio::test]
    async fn cleanup_stops_on_deletion_errors_and_can_repeat_after_partial_progress() {
        for block_log in [true, false] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let first = record_fixture(stream_id, FIRST);
            store_pair(
                storage,
                file_id(stream_id, 0),
                &first,
                Some(&16_u64.to_le_bytes()),
            );
            let trusted = RecordEndLocation::new(file_id(stream_id, 0).first_record_id(), 16);
            let checkpoint_bytes = store_checkpoint(storage, trusted);
            for number in 1..=3 {
                store_pair(
                    storage,
                    file_id(stream_id, number),
                    b"discard log",
                    Some(b"discard index"),
                );
            }
            let blocked_path = if block_log {
                storage.log_file_path(file_id(stream_id, 2))
            } else {
                storage.index_file_path(file_id(stream_id, 2))
            };
            fs::remove_file(&blocked_path).unwrap();
            fs::create_dir(&blocked_path).unwrap();
            let mut state = validation_state(stream_id, storage).await;
            state.validate_files().await.unwrap();

            let error = state.remove_later_files().await.unwrap_err();

            assert!(matches!(error.downcast_ref::<StorageError>(),
                Some(StorageError::Io { path, .. }) if path == &blocked_path));
            assert!(!storage.log_file_path(file_id(stream_id, 1)).exists());
            assert!(!storage.index_file_path(file_id(stream_id, 1)).exists());
            assert!(blocked_path.is_dir());
            if block_log {
                assert_eq!(
                    fs::read(storage.index_file_path(file_id(stream_id, 2))).unwrap(),
                    b"discard index"
                );
            } else {
                assert!(!storage.log_file_path(file_id(stream_id, 2)).exists());
            }
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 3))).unwrap(),
                b"discard log"
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 3))).unwrap(),
                b"discard index"
            );
            assert_eq!(state.first_file_to_remove, Some(file_id(stream_id, 1)));
            assert_eq!(state.recovered_end, Some(trusted));
            assert_eq!(
                state.active_pair.as_ref().unwrap().file_id(),
                file_id(stream_id, 0)
            );
            assert_eq!(
                fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
                checkpoint_bytes
            );

            // The failed operation is awaited; remove the empty test obstruction and retry.
            fs::remove_dir(&blocked_path).unwrap();
            state.remove_later_files().await.unwrap();

            for number in 1..=3 {
                assert!(!storage.log_file_path(file_id(stream_id, number)).exists());
                assert!(!storage.index_file_path(file_id(stream_id, number)).exists());
            }
            assert_eq!(
                fs::read(storage.log_file_path(file_id(stream_id, 0))).unwrap(),
                first
            );
            assert_eq!(
                fs::read(storage.index_file_path(file_id(stream_id, 0))).unwrap(),
                16_u64.to_le_bytes()
            );
            assert_eq!(
                fs::read(storage.checkpoint_file_path(state.stream_id)).unwrap(),
                checkpoint_bytes
            );
        }
    }

    #[tokio::test]
    async fn preparation_creates_file_zero_for_empty_storage_or_a_gap_before_any_records() {
        for has_later_log in [false, true] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            if has_later_log {
                store_pair(storage, file_id(stream_id, 2), b"discard", Some(b"discard"));
            }
            let mut state = validation_state(stream_id, storage).await;
            state.validate_files().await.unwrap();
            state.remove_later_files().await.unwrap();
            let active_id = file_id(stream_id, 0);

            // Merely creating and dropping the future performs no file operations.
            drop(state.prepare_active_pair());
            assert!(state.active_pair.is_none());
            assert!(!storage.log_file_path(active_id).exists());
            assert!(!storage.index_file_path(active_id).exists());

            state.prepare_active_pair().await.unwrap();

            let pair = state.active_pair.as_mut().unwrap();
            assert_eq!(pair.file_id(), active_id);
            assert_eq!(pair.end(), None);
            assert_eq!(pair.log.metadata().await.unwrap().len(), 0);
            assert_eq!(pair.index.metadata().await.unwrap().len(), 0);
            assert_eq!(pair.log.stream_position().await.unwrap(), 0);
            assert_eq!(pair.index.stream_position().await.unwrap(), 0);
            assert_eq!(state.recovered_end, None);
            assert_eq!(state.checkpoint, None);
            assert!(!storage.checkpoint_file_path(stream_id).exists());
            assert!(!storage.log_file_path(file_id(stream_id, 1)).exists());
            assert!(!storage.log_file_path(file_id(stream_id, 2)).exists());

            // An installed pair is retained, even when preparation is called again.
            state.prepare_active_pair().await.unwrap();
        }
    }

    #[tokio::test]
    async fn preparation_and_handover_retain_validated_empty_and_nonempty_partial_pairs() {
        for has_records in [false, true] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let active_id = file_id(stream_id, 0);
            let log_bytes = if has_records {
                record_fixture(stream_id, FIRST)
            } else {
                Vec::new()
            };
            store_pair(storage, active_id, &log_bytes, None);
            let mut state = validation_state(stream_id, storage).await;
            state.validate_files().await.unwrap();
            state.remove_later_files().await.unwrap();
            let expected_end =
                has_records.then(|| RecordEndLocation::new(active_id.first_record_id(), 16));
            let index_bytes = if has_records {
                16_u64.to_le_bytes().to_vec()
            } else {
                Vec::new()
            };

            state.prepare_active_pair().await.unwrap();

            assert_eq!(state.recovered_end, expected_end);
            let mut pair = state.finish();
            assert_eq!(pair.file_id(), active_id);
            assert_eq!(pair.end(), expected_end);
            assert_eq!(
                pair.log.stream_position().await.unwrap(),
                log_bytes.len() as u64
            );
            assert_eq!(
                pair.index.stream_position().await.unwrap(),
                index_bytes.len() as u64
            );
            assert_eq!(
                fs::read(storage.log_file_path(active_id)).unwrap(),
                log_bytes
            );
            assert_eq!(
                fs::read(storage.index_file_path(active_id)).unwrap(),
                index_bytes
            );
            assert!(!storage.log_file_path(active_id.next()).exists());
            assert!(!storage.index_file_path(active_id.next()).exists());
            assert!(!storage.checkpoint_file_path(stream_id).exists());
        }
    }

    #[tokio::test]
    async fn preparation_and_handover_return_the_recovered_complete_files_successor() {
        // Exercise file zero, a range-directory carry, and a discarded later maximum.
        for (number, has_gap) in [(0, false), (999, false), (999, true)] {
            let test_stream = storage_fixture().await;
            let storage = test_stream.provider();
            let stream_id = test_stream.stream_id();
            let complete_id = file_id(stream_id, number);
            let log_bytes = encoded_records(complete_id, 0..100_000).await;
            let index_bytes: Vec<u8> = (1..=100_000_u64)
                .flat_map(|count| (count * 16).to_le_bytes())
                .collect();
            store_pair(storage, complete_id, &log_bytes, Some(&index_bytes));
            let trusted = RecordEndLocation::new(complete_id.last_record_id(), 1_600_000);
            let checkpoint_bytes = store_checkpoint(storage, trusted);
            let active_id = complete_id.next();
            let maximum = if has_gap {
                let later_id = active_id.next();
                store_pair(storage, later_id, b"discard", Some(b"discard"));
                fs::write(storage.index_file_path(active_id), b"orphan index").unwrap();
                later_id
            } else {
                complete_id
            };
            let mut state = validation_state(stream_id, storage).await;
            state.validate_files().await.unwrap();
            state.remove_later_files().await.unwrap();
            assert!(state.active_pair.is_none());

            state.prepare_active_pair().await.unwrap();

            assert_eq!(state.recovered_end, Some(trusted));
            assert_eq!(state.checkpoint.unwrap().end(), trusted);
            assert_eq!(state.maximum_log_file, Some(maximum));
            assert_eq!(state.first_file_to_remove, has_gap.then_some(active_id));
            let mut pair = state.finish();
            assert_eq!(pair.file_id(), active_id);
            assert_eq!(pair.end(), None);
            assert_eq!(pair.log.metadata().await.unwrap().len(), 0);
            assert_eq!(pair.index.metadata().await.unwrap().len(), 0);
            assert_eq!(pair.log.stream_position().await.unwrap(), 0);
            assert_eq!(pair.index.stream_position().await.unwrap(), 0);
            assert_eq!(
                fs::read(storage.log_file_path(complete_id)).unwrap(),
                log_bytes
            );
            assert_eq!(
                fs::read(storage.index_file_path(complete_id)).unwrap(),
                index_bytes
            );
            assert_eq!(
                fs::read(storage.checkpoint_file_path(stream_id)).unwrap(),
                checkpoint_bytes
            );
            assert!(!storage.log_file_path(active_id.next()).exists());
            assert!(!storage.index_file_path(active_id.next()).exists());
        }
    }

    #[tokio::test]
    async fn preparation_preserves_orphan_indexes_and_reports_partial_creation() {
        let test_stream = storage_fixture().await;
        let storage = test_stream.provider();
        let stream_id = test_stream.stream_id();
        let active_id = file_id(stream_id, 0);
        let index_path = storage.index_file_path(active_id);
        fs::create_dir_all(index_path.parent().unwrap()).unwrap();
        fs::write(&index_path, b"orphan index").unwrap();
        let mut state = validation_state(stream_id, storage).await;
        state.validate_files().await.unwrap();
        state.remove_later_files().await.unwrap();

        let error = state.prepare_active_pair().await.unwrap_err();

        assert!(matches!(error.downcast_ref::<StorageError>(),
            Some(StorageError::Io { path, source })
                if path == &index_path && source.kind() == io::ErrorKind::AlreadyExists));
        assert_eq!(fs::read(&index_path).unwrap(), b"orphan index");
        assert_eq!(
            fs::metadata(storage.log_file_path(active_id))
                .unwrap()
                .len(),
            0
        );
        assert!(state.active_pair.is_none());
        assert_eq!(state.recovered_end, None);
        assert_eq!(state.checkpoint, None);
        assert_eq!(state.maximum_log_file, None);
        assert_eq!(state.first_file_to_remove, None);
        assert!(!storage.checkpoint_file_path(stream_id).exists());
    }
}

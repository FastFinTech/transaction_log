// use anyhow::{Result, ensure};
// use tokio::fs::File;
// use transaction_log_exports::StreamId;

// use crate::storage::{StorageError, StorageProvider};
// use crate::streams::{
//     IndexedLogValidator, IndexedLogWriter, LogFileId, LogFileNumber, LogValidationError,
//     RecordEndLocation, StreamCheckpoint,
// };

// /// Scaffold for startup recovery of one stream.
// ///
// /// Checkpoint loading, file enumeration and single-file repair are implemented.
// /// Recovery, later-file cleanup and checkpoint publication are implemented.
// /// Fresh-pair creation and returning the active writer remain deferred.
// pub struct StreamInitializer<'a> {
//     provider: &'a StorageProvider,
//     stream_id: StreamId,
//     // Set only after a successful load and stream-identity check.
//     checkpoint: Option<StreamCheckpoint>,
//     // Inclusive bounds to inspect; IDs between them need not exist on disk.
//     first_log_file: Option<LogFileId>,
//     maximum_log_file: Option<LogFileId>,
//     // Accepted contiguous prefix; a partial file is not synchronized yet.
//     validated_end: Option<RecordEndLocation>,
//     // Repaired final partial pair retained for synchronization and handover.
//     partial_validator: Option<IndexedLogValidator>,
//     // Synchronized partial pair retained after checkpoint advancement.
//     active_writer: Option<IndexedLogWriter<File>>,
//     // First discarded file (including a gap's orphan index), through discovered maximum.
//     first_file_to_remove: Option<LogFileId>,
// }

// impl<'a> StreamInitializer<'a> {
//     /// Selects one stream and borrows its storage provider without performing I/O.
//     pub fn new(provider: &'a StorageProvider, stream_id: StreamId) -> Self {
//         Self {
//             provider,
//             stream_id,
//             checkpoint: None,
//             first_log_file: None,
//             maximum_log_file: None,
//             validated_end: None,
//             partial_validator: None,
//             active_writer: None,
//             first_file_to_remove: None,
//         }
//     }

//     /// Runs recovery and cleanup for one stream, then prepares its active writer.
//     ///
//     /// Loads the checkpoint, discovers bounds and recovers the contiguous prefix.
//     /// Cleans up later files and publishes the synchronized checkpoint.
//     /// Fresh-pair creation and returning the active writer are deferred.
//     ///
//     /// # Panics
//     ///
//     /// Currently panics at active-writer preparation after successful recovery.
//     pub async fn initialize(&mut self) -> Result<IndexedLogWriter<File>> {
//         self.load_checkpoint().await?;
//         self.discover_log_files().await?;
//         self.validate_and_repair().await?;
//         self.remove_later_files().await?;
//         self.advance_checkpoint().await?;
//         self.prepare_active_writer().await
//     }

//     /// Load the checkpoint belonging to this stream, or establish its absence.
//     async fn load_checkpoint(&mut self) -> Result<()> {
//         let checkpoint = self.provider.read_checkpoint(self.stream_id).await?;
//         if let Some(checkpoint) = checkpoint {
//             ensure!(
//                 checkpoint.end().record_id().stream_id() == self.stream_id,
//                 "checkpoint at {} belongs to stream {}, expected {}",
//                 self.provider.checkpoint_file_path(self.stream_id).display(),
//                 checkpoint.end().record_id().stream_id(),
//                 self.stream_id,
//             );
//         }
//         self.checkpoint = checkpoint;
//         Ok(())
//     }

//     /// Establish inclusive file enumeration from the checkpoint's file or file zero.
//     async fn discover_log_files(&mut self) -> Result<()> {
//         let maximum = self.provider.maximum_log_file(self.stream_id).await?;
//         let first = self.checkpoint.map_or(
//             LogFileId::new(self.stream_id, LogFileNumber::MIN),
//             |checkpoint| checkpoint.end().log_file_id(),
//         );
//         if self.checkpoint.is_some() {
//             // Loading checked the checkpoint's stream; the provider searched it.
//             ensure!(
//                 maximum.is_some_and(|maximum| maximum.file_number() >= first.file_number()),
//                 "maximum log file {:?} for stream {} is absent or below checkpointed file {}",
//                 maximum.map(|id| id.file_number()),
//                 self.stream_id,
//                 first.file_number(),
//             );
//         }
//         self.first_log_file = maximum.map(|_| first);
//         self.maximum_log_file = maximum;
//         Ok(())
//     }

//     /// Recover the contiguous file prefix, recording later files for future cleanup.
//     async fn validate_and_repair(&mut self) -> Result<()> {
//         let mut validated_end = self.checkpoint.map(|checkpoint| checkpoint.end());
//         let mut partial_validator = None;
//         let mut first_file_to_remove = None;
//         if let (Some(first), Some(maximum)) = (self.first_log_file, self.maximum_log_file) {
//             for file in first.iter_to(maximum)? {
//                 let validator = match self.validate_and_repair_file(file).await {
//                     Ok(validator) => validator,
//                     Err(error)
//                         if matches!(&error,
//                             LogValidationError::Open(StorageError::Io { path, source })
//                             if *path == self.provider.log_file_path(file)
//                                 && source.kind() == std::io::ErrorKind::NotFound)
//                             && !self.checkpoint.is_some_and(|checkpoint| {
//                                 checkpoint.end().log_file_id() == file
//                             }) =>
//                     {
//                         first_file_to_remove = Some(file);
//                         break;
//                     }
//                     Err(error) => return Err(error.into()),
//                 };
//                 if validator.report().unwrap().is_full() {
//                     // Finish and close full pairs before visiting the next file.
//                     validated_end = Some(validator.into_completed().await?.end());
//                     self.provider.sync_log_file_directory(file)?;
//                 } else {
//                     // An empty final pair must not erase preceding accepted records.
//                     validated_end = validator
//                         .report()
//                         .unwrap()
//                         .validated_end()
//                         .or(validated_end);
//                     partial_validator = Some(validator);
//                     if file != maximum {
//                         first_file_to_remove = Some(file.next());
//                     }
//                     break;
//                 }
//             }
//         }
//         self.validated_end = validated_end;
//         self.partial_validator = partial_validator;
//         self.first_file_to_remove = first_file_to_remove;
//         Ok(())
//     }

//     /// Remove marked log/index pairs before allowing checkpoint advancement.
//     async fn remove_later_files(&mut self) -> Result<()> {
//         if let Some(first) = self.first_file_to_remove {
//             let maximum = self.maximum_log_file.unwrap();
//             for file in first.iter_to(maximum)? {
//                 self.provider.remove_log_and_index(file).await?;
//             }
//             self.first_file_to_remove = None;
//         }
//         Ok(())
//     }

//     /// Recovers one file from its checkpoint boundary, or from the beginning.
//     ///
//     /// Truncates only a tail reported corrupt by successful validation and repairs
//     /// the index through the accepted prefix. Returns the file-owning validator
//     /// without synchronization, checkpoint publication or writer handover.
//     /// The range-loop caller supplies a file ID from this initializer's stream.
//     async fn validate_and_repair_file(
//         &self,
//         file_id: LogFileId,
//     ) -> std::result::Result<IndexedLogValidator, LogValidationError> {
//         let start = self
//             .checkpoint
//             .filter(|checkpoint| checkpoint.end().log_file_id() == file_id)
//             .map(|checkpoint| checkpoint.end());
//         let mut validator = IndexedLogValidator::open(self.provider, file_id, start).await?;
//         validator.validate().await?;
//         validator.repair().await?;
//         Ok(validator)
//     }

//     /// Synchronize recovered pairs before publishing an advanced checkpoint.
//     async fn advance_checkpoint(&mut self) -> Result<()> {
//         ensure!(
//             self.first_file_to_remove.is_none(),
//             "complete later-file cleanup before checkpoint advancement"
//         );
//         if let Some(validator) = self.partial_validator.take() {
//             let file = validator.report().unwrap().file_id();
//             // Existing handover positions and synchronizes log then index.
//             let writer = validator.into_writer().await?;
//             self.provider.sync_log_file_directory(file)?;
//             self.active_writer = Some(writer);
//         }
//         if let Some(end) = self.validated_end {
//             let checkpoint = StreamCheckpoint::new(end);
//             self.provider.write_checkpoint(&checkpoint)?;
//             self.checkpoint = Some(checkpoint);
//         }
//         Ok(())
//     }

//     /// Hand over a partial pair or create a fresh pair after a full or empty stream.
//     async fn prepare_active_writer(&mut self) -> Result<IndexedLogWriter<File>> {
//         todo!("return this stream's active writer")
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::storage::{StorageConfig, StorageError};
//     use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber};

//     async fn encoded_records(file_id: LogFileId, count: u64) -> Vec<u8> {
//         let mut writer = RecordWriter::for_serialization(Vec::new());
//         for offset in 0..count {
//             let id = RecordId::new(
//                 file_id.stream_id(),
//                 SequenceNumber::new(file_id.first_record_id().sequence_number().get() + offset),
//             );
//             writer.write(id, |_| Ok::<(), std::io::Error>(())).unwrap();
//         }
//         writer.flush_buffer().await.unwrap();
//         writer.into_inner()
//     }

//     #[tokio::test]
//     async fn cleanup_precedes_checkpoint_publication_and_preserves_the_recovered_pair() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let first = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let orphan = first.next();
//         let future = orphan.next();
//         let path = provider.log_file_path(first);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let valid = encoded_records(first, 2).await;
//         let mut log = valid.clone();
//         log.push(1);
//         std::fs::write(&path, log).unwrap();
//         std::fs::write(provider.index_file_path(orphan), b"orphan").unwrap();
//         std::fs::write(provider.log_file_path(future), b"future").unwrap();
//         std::fs::write(provider.index_file_path(future), b"future").unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert!(initializer.advance_checkpoint().await.is_err());
//         assert!(!provider.checkpoint_file_path(StreamId::MIN).exists());
//         initializer.remove_later_files().await.unwrap();
//         assert!(!provider.index_file_path(orphan).exists());
//         assert!(!provider.log_file_path(future).exists());
//         assert!(!provider.index_file_path(future).exists());
//         initializer.advance_checkpoint().await.unwrap();
//         assert_eq!(std::fs::read(path).unwrap(), valid);
//         let checkpoint = provider
//             .read_checkpoint(StreamId::MIN)
//             .await
//             .unwrap()
//             .unwrap();
//         assert_eq!(checkpoint.end().record_id().sequence_number().get(), 1);
//         assert_eq!(checkpoint.end().position(), 32);
//         assert_eq!(initializer.checkpoint, Some(checkpoint));
//         assert!(initializer.partial_validator.is_none());
//         assert_eq!(
//             initializer.active_writer.as_ref().unwrap().synced_end(),
//             Some(checkpoint.end())
//         );
//         let mut restarted = StreamInitializer::new(&provider, StreamId::MIN);
//         restarted.load_checkpoint().await.unwrap();
//         restarted.discover_log_files().await.unwrap();
//         assert_eq!(restarted.maximum_log_file, Some(first));
//     }

//     #[tokio::test]
//     async fn cleanup_failures_prevent_checkpoint_publication() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let future = file.next();
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(path, encoded_records(file, 1).await).unwrap();
//         std::fs::write(provider.log_file_path(future), b"future").unwrap();
//         std::fs::create_dir(provider.index_file_path(future)).unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert!(initializer.remove_later_files().await.is_err());
//         assert_eq!(initializer.first_file_to_remove, Some(future));
//         assert!(initializer.advance_checkpoint().await.is_err());
//         assert!(!provider.checkpoint_file_path(StreamId::MIN).exists());
//     }

//     #[tokio::test]
//     async fn empty_streams_have_no_checkpoint_but_sequence_zero_is_checkpointed() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         initializer.remove_later_files().await.unwrap();
//         initializer.advance_checkpoint().await.unwrap();
//         assert!(!directory.path().join("streams").exists());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         for count in [0, 1] {
//             std::fs::write(&path, encoded_records(file, count).await).unwrap();
//             let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//             initializer.discover_log_files().await.unwrap();
//             initializer.validate_and_repair().await.unwrap();
//             initializer.remove_later_files().await.unwrap();
//             initializer.advance_checkpoint().await.unwrap();
//             let checkpoint = provider.read_checkpoint(StreamId::MIN).await.unwrap();
//             assert_eq!(checkpoint.is_some(), count == 1);
//             assert_eq!(
//                 initializer.active_writer.as_ref().unwrap().synced_end(),
//                 checkpoint.map(|checkpoint| checkpoint.end())
//             );
//         }
//     }

//     #[tokio::test]
//     async fn checkpoint_publication_failure_preserves_the_previous_checkpoint() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(path, encoded_records(file, 2).await).unwrap();
//         std::fs::write(provider.index_file_path(file), [16, 0, 0, 0, 0, 0, 0, 0]).unwrap();
//         let checkpoint_path = provider.checkpoint_file_path(StreamId::MIN);
//         let previous = r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#;
//         std::fs::write(&checkpoint_path, previous).unwrap();
//         std::fs::create_dir(
//             checkpoint_path
//                 .parent()
//                 .unwrap()
//                 .join(".checkpoint.pending"),
//         )
//         .unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.load_checkpoint().await.unwrap();
//         let loaded = initializer.checkpoint;
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         initializer.remove_later_files().await.unwrap();
//         assert!(initializer.advance_checkpoint().await.is_err());
//         assert_eq!(initializer.checkpoint, loaded);
//         assert_eq!(std::fs::read_to_string(checkpoint_path).unwrap(), previous);
//         assert_eq!(
//             initializer
//                 .active_writer
//                 .as_ref()
//                 .unwrap()
//                 .synced_end()
//                 .unwrap()
//                 .record_id()
//                 .sequence_number()
//                 .get(),
//             1
//         );
//     }

//     #[tokio::test]
//     async fn checkpoint_covers_the_full_file_when_the_following_pair_is_empty() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(path, encoded_records(file, 100_000).await).unwrap();
//         std::fs::write(provider.log_file_path(file.next()), b"").unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         initializer.remove_later_files().await.unwrap();
//         initializer.advance_checkpoint().await.unwrap();
//         let checkpoint = provider
//             .read_checkpoint(StreamId::MIN)
//             .await
//             .unwrap()
//             .unwrap();
//         assert_eq!(checkpoint.end().record_id().sequence_number().get(), 99_999);
//         assert_eq!(checkpoint.end().position(), 1_600_000);
//         let writer = initializer.active_writer.as_ref().unwrap();
//         assert_eq!(writer.file_id(), file.next());
//         assert_eq!(writer.synced_end(), None);
//     }

//     #[tokio::test]
//     async fn recovery_loop_completes_full_files_and_stops_at_the_partial_pair() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let first = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let partial = first.next();
//         let future = partial.next();
//         let path = provider.log_file_path(first);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let full_log = encoded_records(first, 100_000).await;
//         std::fs::write(&path, &full_log).unwrap();
//         std::fs::write(
//             provider.log_file_path(future),
//             b"do not inspect or delete yet",
//         )
//         .unwrap();
//         for count in [0, 2] {
//             let mut partial_log = encoded_records(partial, count).await;
//             if count != 0 {
//                 partial_log.push(1);
//             }
//             std::fs::write(provider.log_file_path(partial), partial_log).unwrap();
//             let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//             initializer.discover_log_files().await.unwrap();
//             initializer.validate_and_repair().await.unwrap();
//             assert_eq!(
//                 initializer
//                     .validated_end
//                     .unwrap()
//                     .record_id()
//                     .sequence_number()
//                     .get(),
//                 if count == 0 { 99_999 } else { 100_001 }
//             );
//             assert_eq!(
//                 initializer
//                     .partial_validator
//                     .as_ref()
//                     .unwrap()
//                     .report()
//                     .unwrap()
//                     .file_id(),
//                 partial
//             );
//             assert_eq!(initializer.first_file_to_remove, Some(future));
//             assert_eq!(
//                 std::fs::read(provider.log_file_path(future)).unwrap(),
//                 b"do not inspect or delete yet"
//             );
//             assert_eq!(std::fs::read(&path).unwrap(), full_log);
//             let index = std::fs::read(provider.index_file_path(first)).unwrap();
//             assert_eq!(index.len(), 800_000);
//             assert_eq!(&index[index.len() - 8..], &1_600_000u64.to_le_bytes());
//             assert_eq!(
//                 std::fs::metadata(provider.log_file_path(partial))
//                     .unwrap()
//                     .len(),
//                 count * 16
//             );
//             assert!(!provider.checkpoint_file_path(StreamId::MIN).exists());
//         }
//     }

//     #[tokio::test]
//     async fn recovery_loop_handles_a_gap_after_a_full_file_and_a_full_final_file() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let first = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let future = first.next().next();
//         let path = provider.log_file_path(first);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, encoded_records(first, 100_000).await).unwrap();
//         std::fs::write(provider.log_file_path(future), b"preserve for cleanup step").unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert_eq!(initializer.first_file_to_remove, Some(first.next()));
//         assert!(initializer.partial_validator.is_none());
//         assert_eq!(
//             initializer
//                 .validated_end
//                 .unwrap()
//                 .record_id()
//                 .sequence_number()
//                 .get(),
//             99_999
//         );
//         assert!(provider.log_file_path(future).exists());

//         std::fs::remove_file(provider.log_file_path(future)).unwrap();
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert_eq!(initializer.first_file_to_remove, None);
//         assert!(initializer.partial_validator.is_none());
//     }

//     #[tokio::test]
//     async fn recovery_loop_handles_an_empty_stream_and_a_gap_at_file_zero() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert_eq!(initializer.validated_end, None);
//         assert!(initializer.partial_validator.is_none());
//         assert_eq!(initializer.first_file_to_remove, None);
//         assert!(!directory.path().join("streams").exists());
//         let future = LogFileId::new(StreamId::MIN, LogFileNumber::new(2).unwrap());
//         let path = provider.log_file_path(future);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, b"untouched").unwrap();
//         initializer.discover_log_files().await.unwrap();
//         initializer.validate_and_repair().await.unwrap();
//         assert_eq!(
//             initializer.first_file_to_remove,
//             Some(LogFileId::new(StreamId::MIN, LogFileNumber::MIN))
//         );
//         assert_eq!(std::fs::read(path).unwrap(), b"untouched");
//     }

//     #[tokio::test]
//     async fn recovery_loop_rejects_a_missing_checkpoint_file_and_operational_errors() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let future = LogFileId::new(StreamId::MIN, LogFileNumber::new(2).unwrap());
//         let path = provider.log_file_path(future);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, b"untouched").unwrap();
//         let checkpoint_path = provider.checkpoint_file_path(StreamId::MIN);
//         std::fs::write(
//             &checkpoint_path,
//             r#"{"end":{"record_id":{"stream_id":0,"sequence_number":100000},"position":16}}"#,
//         )
//         .unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.load_checkpoint().await.unwrap();
//         initializer.discover_log_files().await.unwrap();
//         let error = initializer.validate_and_repair().await.unwrap_err();
//         assert!(
//             matches!(error.downcast_ref::<LogValidationError>(), Some(LogValidationError::Open(StorageError::Io { source, .. })) if source.kind() == std::io::ErrorKind::NotFound)
//         );
//         assert!(initializer.first_file_to_remove.is_none());
//         assert_eq!(std::fs::read(&path).unwrap(), b"untouched");

//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         std::fs::write(provider.log_file_path(file), encoded_records(file, 1).await).unwrap();
//         std::fs::create_dir(provider.index_file_path(file)).unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         assert!(initializer.validate_and_repair().await.is_err());
//         assert!(initializer.first_file_to_remove.is_none());
//         assert!(initializer.validated_end.is_none());
//     }

//     #[tokio::test]
//     async fn single_file_recovery_repairs_indexes_without_changing_clean_logs() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let log = encoded_records(file, 2).await;
//         assert_eq!(log.len(), 32);
//         std::fs::write(&path, &log).unwrap();
//         let index = provider.index_file_path(file);
//         let initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         // First a missing index; then a matching first entry and a bad/extra suffix.
//         for attempt in 0..2 {
//             if attempt == 1 {
//                 std::fs::write(&index, [16, 0, 0, 0, 0, 0, 0, 0, 99, 0, 1]).unwrap();
//             }
//             let validator = initializer.validate_and_repair_file(file).await.unwrap();
//             assert_eq!(validator.report().unwrap().validated_record_count(), 2);
//             assert!(validator.report().unwrap().tail_error().is_none());
//             assert_eq!(std::fs::read(&path).unwrap(), log);
//             assert_eq!(
//                 std::fs::read(&index).unwrap(),
//                 [16, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0]
//             );
//             drop(validator);
//         }
//         assert!(!provider.checkpoint_file_path(StreamId::MIN).exists());
//     }

//     #[tokio::test]
//     async fn single_file_recovery_truncates_corrupt_tails_and_preserves_the_valid_prefix() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let valid = encoded_records(file, 2).await;
//         let initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         for keep in [0, 16, 32] {
//             let mut log = valid[..keep].to_vec();
//             log.push(1); // Incomplete next record, including corruption before any record.
//             std::fs::write(&path, log).unwrap();
//             std::fs::write(provider.index_file_path(file), [0xff; 24]).unwrap();
//             let validator = initializer.validate_and_repair_file(file).await.unwrap();
//             assert!(validator.report().unwrap().tail_error().is_some());
//             assert_eq!(std::fs::read(&path).unwrap(), valid[..keep]);
//             let expected = [16, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0];
//             assert_eq!(
//                 std::fs::read(provider.index_file_path(file)).unwrap(),
//                 expected[..keep / 2]
//             );
//             drop(validator);
//         }
//     }

//     #[tokio::test]
//     async fn single_file_recovery_uses_the_checkpoint_only_in_its_own_file() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let mut log = encoded_records(file, 2).await;
//         log[12] ^= 1; // Certified first record must not be rescanned.
//         std::fs::write(&path, &log).unwrap();
//         std::fs::write(provider.index_file_path(file), [16, 0, 0, 0, 0, 0, 0, 0]).unwrap();
//         std::fs::write(
//             provider.checkpoint_file_path(StreamId::MIN),
//             r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#,
//         )
//         .unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.load_checkpoint().await.unwrap();
//         let validator = initializer.validate_and_repair_file(file).await.unwrap();
//         assert_eq!(validator.report().unwrap().validated_record_count(), 2);
//         assert_eq!(std::fs::read(&path).unwrap(), log);
//         drop(validator);

//         let later = file.next();
//         let path = provider.log_file_path(later);
//         std::fs::write(&path, encoded_records(later, 2).await).unwrap();
//         let validator = initializer.validate_and_repair_file(later).await.unwrap();
//         assert_eq!(validator.report().unwrap().validated_record_count(), 2);
//         assert_eq!(
//             validator
//                 .report()
//                 .unwrap()
//                 .validated_end()
//                 .unwrap()
//                 .record_id()
//                 .sequence_number()
//                 .get(),
//             100_001
//         );
//     }

//     #[tokio::test]
//     async fn single_file_recovery_errors_do_not_authorize_truncation() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let file = LogFileId::new(StreamId::MIN, LogFileNumber::MIN);
//         let initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         assert!(matches!(initializer.validate_and_repair_file(file).await,
//             Err(LogValidationError::Open(StorageError::Io { source, .. })) if source.kind() == std::io::ErrorKind::NotFound));
//         assert!(!provider.index_file_path(file).exists());

//         let path = provider.log_file_path(file);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let log = encoded_records(file, 1).await;
//         std::fs::write(&path, &log).unwrap();
//         std::fs::write(
//             provider.checkpoint_file_path(StreamId::MIN),
//             r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#,
//         )
//         .unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.load_checkpoint().await.unwrap();
//         assert!(matches!(
//             initializer.validate_and_repair_file(file).await,
//             Err(LogValidationError::TrustedIndexUnavailable)
//         ));
//         assert_eq!(std::fs::read(path).unwrap(), log);
//     }

//     #[tokio::test]
//     async fn discovery_is_lazy_inclusive_and_enumerates_missing_intermediate_files() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.discover_log_files().await.unwrap();
//         assert!(initializer.first_log_file.is_none());
//         assert!(initializer.maximum_log_file.is_none());
//         assert!(!directory.path().join("streams").exists());

//         let last = LogFileId::new(StreamId::MIN, LogFileNumber::new(2).unwrap());
//         let path = provider.log_file_path(last);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, b"").unwrap();
//         initializer.discover_log_files().await.unwrap();
//         let ids: Vec<_> = initializer
//             .first_log_file
//             .unwrap()
//             .iter_to(initializer.maximum_log_file.unwrap())
//             .unwrap()
//             .collect();
//         assert_eq!(
//             ids.iter()
//                 .map(|id| id.file_number().get())
//                 .collect::<Vec<_>>(),
//             [0, 1, 2]
//         );
//         assert_eq!(ids.last(), Some(&last));
//         assert!(!provider.log_file_path(ids[0]).exists());
//         assert!(!provider.log_file_path(ids[1]).exists());
//         assert_eq!(std::fs::read(path).unwrap(), b"");
//     }

//     #[tokio::test]
//     async fn discovery_starts_in_the_checkpoint_file_and_rejects_contradicting_maxima() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let stream = StreamId::MAX;
//         let checkpoint_path = provider.checkpoint_file_path(stream);
//         std::fs::create_dir_all(checkpoint_path.parent().unwrap()).unwrap();
//         let json =
//             r#"{"end":{"record_id":{"stream_id":4095,"sequence_number":100000},"position":16}}"#;
//         std::fs::write(&checkpoint_path, json).unwrap();
//         let mut initializer = StreamInitializer::new(&provider, stream);
//         initializer.load_checkpoint().await.unwrap();
//         assert!(initializer.discover_log_files().await.is_err());
//         assert!(initializer.first_log_file.is_none());
//         assert!(initializer.maximum_log_file.is_none());

//         let zero = LogFileId::new(stream, LogFileNumber::MIN);
//         let path = provider.log_file_path(zero);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, b"").unwrap();
//         assert!(initializer.discover_log_files().await.is_err());
//         assert!(initializer.first_log_file.is_none());
//         assert!(initializer.maximum_log_file.is_none());

//         for maximum in [1, 3, LogFileNumber::MAX.get()] {
//             let last = LogFileId::new(stream, LogFileNumber::new(maximum).unwrap());
//             let path = provider.log_file_path(last);
//             std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//             std::fs::write(path, b"").unwrap();
//             initializer.discover_log_files().await.unwrap();
//             // Taking only two items from a huge range must not allocate its history.
//             let ids: Vec<_> = initializer
//                 .first_log_file
//                 .unwrap()
//                 .iter_to(initializer.maximum_log_file.unwrap())
//                 .unwrap()
//                 .take(2)
//                 .collect();
//             assert_eq!(ids[0].file_number().get(), 1);
//             assert_eq!(ids.len(), if maximum == 1 { 1 } else { 2 });
//         }
//         assert_eq!(std::fs::read_to_string(checkpoint_path).unwrap(), json);
//     }

//     #[tokio::test]
//     async fn discovery_preserves_provider_errors() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let path = directory.path().join("streams/0000");
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         std::fs::write(&path, b"obstruction").unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         let error = initializer.discover_log_files().await.unwrap_err();
//         assert!(
//             matches!(error.downcast_ref::<StorageError>(), Some(StorageError::Io { path: failed, .. }) if failed == &path)
//         );
//         assert!(initializer.first_log_file.is_none());
//         assert!(initializer.maximum_log_file.is_none());
//     }

//     #[tokio::test]
//     async fn loads_only_the_requested_stream_and_preserves_the_boundary() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let stream = StreamId::new(42).unwrap();
//         let path = provider.checkpoint_file_path(stream);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let json = r#"{"end":{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553500000}}"#;
//         std::fs::write(&path, json).unwrap();
//         let mut initializer = StreamInitializer::new(&provider, stream);
//         initializer.load_checkpoint().await.unwrap();
//         let checkpoint = initializer.checkpoint.unwrap();
//         assert_eq!(checkpoint.end().record_id().sequence_number().get(), 99_999);
//         assert_eq!(checkpoint.end().position(), 6_553_500_000);
//         assert_eq!(std::fs::read_to_string(path).unwrap(), json);

//         let mut other = StreamInitializer::new(&provider, StreamId::MIN);
//         other.load_checkpoint().await.unwrap();
//         assert_eq!(other.checkpoint, None);
//     }

//     #[tokio::test]
//     async fn absence_is_not_a_sequence_zero_sentinel_and_creates_nothing() {
//         let directory = tempfile::tempdir().unwrap();
//         let root = directory.path().join("absent");
//         let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         initializer.load_checkpoint().await.unwrap();
//         assert_eq!(initializer.checkpoint, None);
//         assert!(!root.exists());
//     }

//     #[tokio::test]
//     async fn rejects_wrong_stream_and_malformed_metadata_without_accepting_or_changing_it() {
//         let directory = tempfile::tempdir().unwrap();
//         let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
//         let path = provider.checkpoint_file_path(StreamId::MIN);
//         std::fs::create_dir_all(path.parent().unwrap()).unwrap();
//         let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
//         let valid = r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#;
//         std::fs::write(&path, valid).unwrap();
//         initializer.load_checkpoint().await.unwrap();
//         let previous = initializer.checkpoint;
//         assert_eq!(
//             previous.unwrap().end().record_id().sequence_number().get(),
//             0
//         );
//         let json = r#"{"end":{"record_id":{"stream_id":1,"sequence_number":0},"position":16}}"#;
//         std::fs::write(&path, json).unwrap();
//         let error = initializer.load_checkpoint().await.unwrap_err();
//         assert!(
//             error
//                 .to_string()
//                 .contains("belongs to stream 1, expected 0")
//         );
//         assert_eq!(initializer.checkpoint, previous);
//         assert_eq!(std::fs::read_to_string(&path).unwrap(), json);

//         std::fs::write(&path, "{}").unwrap();
//         let error = initializer.load_checkpoint().await.unwrap_err();
//         assert!(
//             matches!(error.downcast_ref::<StorageError>(), Some(StorageError::Json { path: failed, .. }) if failed == &path)
//         );
//         assert_eq!(initializer.checkpoint, previous);
//         assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
//     }
// }

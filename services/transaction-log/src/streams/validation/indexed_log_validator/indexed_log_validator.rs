use std::io::SeekFrom;

use tokio::io::AsyncSeekExt;

use super::{IndexedLogValidationError as Error, ValidatedFilePair};
use crate::{
    storage::StorageProvider,
    streams::{
        IndexFileValidationError, IndexFileValidator, LogFileId, LogFileValidationError,
        LogFileValidator, RECORDS_PER_FILE, RecordEndLocation, index_writer::INDEX_ENTRY_LEN,
    },
};

/// Recovers one existing log and its paired index through the focused validators.
///
/// This stateless type groups one operation. It does not enumerate files, publish
/// checkpoints, remove later files or construct a writer.
pub struct IndexedLogValidator;

impl IndexedLogValidator {
    /// Opens, validates and synchronizes one pair, returning its accepted endpoint.
    ///
    /// `last_trusted` is a checkpoint-certified endpoint within `file_id`, or `None`
    /// to validate from this file's beginning. Earlier certified bytes and entries
    /// are trusted. A checkpoint from a preceding file must not be passed here.
    /// The caller excludes all other access and finishes earlier I/O before calling.
    ///
    /// The existing log is opened first; a missing log remains a path-bearing
    /// storage error and does not create an index. The index is opened or created
    /// before validation. A newly created index can be rebuilt only when no trusted
    /// entry is required; a missing or mismatched trusted entry is an error.
    ///
    /// Log recovery removes corruption and synchronizes the accepted bytes. Index
    /// recovery receives the original trusted endpoint and just the newly accepted
    /// offsets, replaces that suffix, then flushes and synchronizes the index.
    /// Full files return [`ValidatedFilePair::Complete`] without handles. Partial
    /// files return both handles explicitly positioned at their accepted ends.
    ///
    /// Errors return no pair, but may follow index creation or completed repairs.
    /// Cancellation can also leave outstanding OS I/O; the caller must quiesce it
    /// before retrying. Recovery is not atomic across the two files. File data is
    /// synchronized before success; directory-entry durability, checkpoint publication
    /// and readiness of the whole stream remain the caller's responsibility.
    pub async fn validate(
        storage: &StorageProvider,
        file_id: LogFileId,
        last_trusted: Option<RecordEndLocation>,
    ) -> Result<ValidatedFilePair, Error> {
        let log = storage.open_log_for_validation(file_id).await?;
        let index = storage.open_index_for_repair(file_id).await?;

        let validated_log = LogFileValidator::validate(log, file_id, last_trusted).await?;
        let mut index =
            IndexFileValidator::validate(index, last_trusted, validated_log.suffix_ends()).await?;
        let end = validated_log.validated_end();
        let record_count = end.map_or(0, |end| LogFileId::record_count_through(end.record_id()));

        match end {
            Some(end) if record_count == RECORDS_PER_FILE => {
                Ok(ValidatedFilePair::Complete { end })
            }
            end => {
                let mut log = validated_log.into_inner();
                log.seek(SeekFrom::Start(end.map_or(0, |end| end.position())))
                    .await
                    .map_err(LogFileValidationError::from)?;
                index
                    .seek(SeekFrom::Start(record_count * INDEX_ENTRY_LEN as u64))
                    .await
                    .map_err(IndexFileValidationError::from)?;
                Ok(ValidatedFilePair::Partial { end, log, index })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tempfile::TempDir;
    use tokio::{fs::File, io::AsyncWriteExt};
    use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

    use super::*;
    use crate::storage::{StorageConfig, StorageError};

    // Independent protocol fixtures: stream 7, sequences 0/1/2, payloads empty/abc/x.
    const FIRST: &[u8] = &[16, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 226, 50, 73];
    const SECOND: &[u8] = &[
        19, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 18, 247, 123, 204,
    ];
    const THIRD: &[u8] = &[17, 0, 7, 0, 2, 0, 0, 0, 0, 0, 0, 0, 120, 53, 10, 47, 115];

    fn end(sequence: u64, position: u64) -> RecordEndLocation {
        RecordEndLocation::new(
            RecordId::new(StreamId::new(7).unwrap(), SequenceNumber::new(sequence)),
            position,
        )
    }

    fn file_id() -> LogFileId {
        end(0, 0).log_file_id()
    }

    fn encoded_index(ends: &[u64]) -> Vec<u8> {
        ends.iter().flat_map(|end| end.to_le_bytes()).collect()
    }

    async fn stored_pair(
        file_id: LogFileId,
        log: &[u8],
        index: Option<&[u8]>,
    ) -> (TempDir, StorageProvider) {
        let directory = tempfile::tempdir().unwrap();
        let storage = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let log_path = storage.log_file_path(file_id);
        tokio::fs::create_dir_all(log_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(log_path, log).await.unwrap();
        if let Some(index) = index {
            tokio::fs::write(storage.index_file_path(file_id), index)
                .await
                .unwrap();
        }
        (directory, storage)
    }

    async fn assert_bytes(storage: &StorageProvider, file_id: LogFileId, log: &[u8], index: &[u8]) {
        assert_eq!(
            tokio::fs::read(storage.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );
        assert_eq!(
            tokio::fs::read(storage.index_file_path(file_id))
                .await
                .unwrap(),
            index
        );
    }

    async fn assert_partial(
        result: ValidatedFilePair,
        expected_end: Option<RecordEndLocation>,
        log_length: u64,
        index_length: u64,
    ) -> (File, File) {
        let ValidatedFilePair::Partial {
            end,
            mut log,
            mut index,
        } = result
        else {
            panic!("expected a partial pair");
        };
        assert_eq!(end, expected_end);
        assert_eq!(log.metadata().await.unwrap().len(), log_length);
        assert_eq!(index.metadata().await.unwrap().len(), index_length);
        assert_eq!(log.stream_position().await.unwrap(), log_length);
        assert_eq!(index.stream_position().await.unwrap(), index_length);
        (log, index)
    }

    #[tokio::test]
    async fn empty_and_entirely_corrupt_logs_return_empty_appendable_pairs() {
        for log_bytes in [&[][..], &[1, 0, 3][..]] {
            for index_bytes in [None, Some(&[0xff; 19][..])] {
                let (_directory, storage) = stored_pair(file_id(), log_bytes, index_bytes).await;
                let result = IndexedLogValidator::validate(&storage, file_id(), None)
                    .await
                    .unwrap();
                assert_partial(result, None, 0, 0).await;
                assert_bytes(&storage, file_id(), &[], &[]).await;
                assert!(!storage.checkpoint_file_path(file_id().stream_id()).exists());
            }
        }
    }

    #[tokio::test]
    async fn hands_off_only_new_offsets_with_the_original_trusted_endpoint() {
        let log_bytes = [FIRST, SECOND, THIRD].concat();
        let index_bytes = encoded_index(&[16, 35, 52]);
        for (trusted, prefix_length) in [
            (None, 0),
            (Some(end(0, 16)), 8),
            (Some(end(1, 35)), 16),
            (Some(end(2, 52)), 24),
        ] {
            let stale_index = [&index_bytes[..prefix_length], &[0xff; 11]].concat();
            let (_directory, storage) =
                stored_pair(file_id(), &log_bytes, Some(&stale_index)).await;
            let result = IndexedLogValidator::validate(&storage, file_id(), trusted)
                .await
                .unwrap();
            assert_partial(result, Some(end(2, 52)), 52, 24).await;
            assert_bytes(&storage, file_id(), &log_bytes, &index_bytes).await;
        }
    }

    #[tokio::test]
    async fn missing_index_is_rebuilt_and_returned_handles_can_append_without_seeking() {
        let (_directory, storage) = stored_pair(file_id(), &[FIRST, SECOND].concat(), None).await;
        let result = IndexedLogValidator::validate(&storage, file_id(), None)
            .await
            .unwrap();
        let (mut log, mut index) = assert_partial(result, Some(end(1, 35)), 35, 16).await;
        log.write_all(THIRD).await.unwrap();
        log.flush().await.unwrap();
        log.sync_data().await.unwrap();
        index.write_all(&52u64.to_le_bytes()).await.unwrap();
        index.flush().await.unwrap();
        index.sync_data().await.unwrap();
        drop((log, index));

        assert_bytes(
            &storage,
            file_id(),
            &[FIRST, SECOND, THIRD].concat(),
            &encoded_index(&[16, 35, 52]),
        )
        .await;
        // Reopen using the original boundary; repeated recovery must preserve the pair.
        for _ in 0..2 {
            let result = IndexedLogValidator::validate(&storage, file_id(), Some(end(1, 35)))
                .await
                .unwrap();
            assert_partial(result, Some(end(2, 52)), 52, 24).await;
            assert_bytes(
                &storage,
                file_id(),
                &[FIRST, SECOND, THIRD].concat(),
                &encoded_index(&[16, 35, 52]),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn corruption_removes_its_index_and_all_following_records_and_entries() {
        let mut corrupt = SECOND.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        let log_bytes = [FIRST, &corrupt, THIRD].concat();
        let index_bytes = [encoded_index(&[16, 35, 52]), vec![0xff; 3]].concat();
        for trusted in [None, Some(end(0, 16))] {
            let (_directory, storage) =
                stored_pair(file_id(), &log_bytes, Some(&index_bytes)).await;
            let result = IndexedLogValidator::validate(&storage, file_id(), trusted)
                .await
                .unwrap();
            assert_partial(result, Some(end(0, 16)), 16, 8).await;
            assert_bytes(&storage, file_id(), FIRST, &encoded_index(&[16])).await;
        }
    }

    #[tokio::test]
    async fn full_files_return_complete_after_index_rebuilding_and_extra_log_removal() {
        for (file, trusted_count, extra) in [
            (file_id(), 0, false),
            (file_id().next(), 0, true),
            (file_id().next(), RECORDS_PER_FILE - 1, false),
            (file_id().next(), RECORDS_PER_FILE, true),
        ] {
            let mut writer = RecordWriter::for_serialization(Vec::new());
            for position in 0..RECORDS_PER_FILE {
                writer
                    .write(file.record_id_at(position).unwrap(), |_| {
                        Ok::<_, io::Error>(())
                    })
                    .unwrap();
            }
            writer.flush_buffer().await.unwrap();
            let log_bytes = writer.into_inner();
            // Empty payload records are independently known to occupy 16 bytes.
            let index_bytes: Vec<u8> = (1..=RECORDS_PER_FILE)
                .flat_map(|n| (n * 16).to_le_bytes())
                .collect();
            let on_disk_log = [&log_bytes[..], if extra { &[0xff; 37] } else { &[] }].concat();
            let stale_index = [&index_bytes[..trusted_count as usize * 8], &[0xff; 11]].concat();
            let trusted = (trusted_count != 0).then(|| {
                RecordEndLocation::new(
                    file.record_id_at(trusted_count - 1).unwrap(),
                    trusted_count * 16,
                )
            });
            let (_directory, storage) = stored_pair(file, &on_disk_log, Some(&stale_index)).await;
            let result = IndexedLogValidator::validate(&storage, file, trusted)
                .await
                .unwrap();
            let ValidatedFilePair::Complete { end } = result else {
                panic!("expected a complete pair");
            };
            assert_eq!(
                end,
                RecordEndLocation::new(file.last_record_id(), RECORDS_PER_FILE * 16)
            );
            assert_bytes(&storage, file, &log_bytes, &index_bytes).await;

            // The next file has no file-local endpoint. The preceding result stays usable.
            tokio::fs::write(storage.log_file_path(file.next()), [])
                .await
                .unwrap();
            let result = IndexedLogValidator::validate(&storage, file.next(), None)
                .await
                .unwrap();
            assert_partial(result, None, 0, 0).await;
            assert_eq!(end.log_file_id(), file);
        }
    }

    #[tokio::test]
    async fn nearly_full_file_stays_partial_and_uses_file_local_index_positions() {
        let file = file_id().next();
        let count = RECORDS_PER_FILE - 1;
        // This prefix is caller-certified; recovery must not rescan it.
        let log_bytes = vec![0xff; count as usize * 16];
        let mut index_bytes = vec![0xff; count as usize * 8];
        index_bytes[count as usize * 8 - 8..].copy_from_slice(&(count * 16).to_le_bytes());
        let trusted = RecordEndLocation::new(file.record_id_at(count - 1).unwrap(), count * 16);
        let (_directory, storage) = stored_pair(file, &log_bytes, Some(&index_bytes)).await;
        let result = IndexedLogValidator::validate(&storage, file, Some(trusted))
            .await
            .unwrap();
        assert_partial(result, Some(trusted), count * 16, count * 8).await;
        assert_bytes(&storage, file, &log_bytes, &index_bytes).await;
    }

    #[tokio::test]
    async fn missing_log_preserves_not_found_and_does_not_create_or_change_an_index() {
        for index_bytes in [None, Some(&[0xff; 11][..])] {
            let (_directory, storage) = stored_pair(file_id(), FIRST, index_bytes).await;
            tokio::fs::remove_file(storage.log_file_path(file_id()))
                .await
                .unwrap();
            let error = IndexedLogValidator::validate(&storage, file_id(), None)
                .await
                .unwrap_err();
            let Error::Storage(StorageError::Io { path, source }) = error else {
                panic!("expected a storage I/O error");
            };
            assert_eq!(path, storage.log_file_path(file_id()));
            assert_eq!(source.kind(), io::ErrorKind::NotFound);
            let index_path = storage.index_file_path(file_id());
            if let Some(bytes) = index_bytes {
                assert_eq!(tokio::fs::read(index_path).await.unwrap(), bytes);
            } else {
                assert!(!index_path.exists());
            }
        }
    }

    #[tokio::test]
    async fn index_open_failure_keeps_its_path_and_precedes_log_repair() {
        let log_bytes = [FIRST, &[0xff; 13]].concat();
        let (_directory, storage) = stored_pair(file_id(), &log_bytes, None).await;
        let index_path = storage.index_file_path(file_id());
        tokio::fs::create_dir(&index_path).await.unwrap();
        let error = IndexedLogValidator::validate(&storage, file_id(), None)
            .await
            .unwrap_err();
        let Error::Storage(StorageError::Io { path, source }) = error else {
            panic!("expected a storage I/O error");
        };
        assert_eq!(path, index_path);
        assert!(source.raw_os_error().is_some());
        assert_eq!(
            tokio::fs::read(storage.log_file_path(file_id()))
                .await
                .unwrap(),
            log_bytes
        );
    }

    #[tokio::test]
    async fn invalid_log_boundary_returns_the_log_error_without_rewriting_the_index() {
        let index_bytes = encoded_index(&[16, 35, 52]);
        let other_stream = RecordEndLocation::new(
            RecordId::new(StreamId::new(8).unwrap(), SequenceNumber::new(0)),
            16,
        );
        for trusted in [
            other_stream,
            end(RECORDS_PER_FILE, 16),
            end(0, 17),
            end(0, 0),
        ] {
            let (_directory, storage) = stored_pair(file_id(), FIRST, Some(&index_bytes)).await;
            let error = IndexedLogValidator::validate(&storage, file_id(), Some(trusted))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                Error::Log(LogFileValidationError::InvalidStart(_))
            ));
            assert_bytes(&storage, file_id(), FIRST, &index_bytes).await;
        }
    }

    #[tokio::test]
    async fn missing_or_wrong_trusted_index_is_an_error_even_after_successful_log_repair() {
        let log_bytes = [FIRST, SECOND, &[0xff; 3]].concat();
        for index_bytes in [None, Some(&[16, 0, 0][..]), Some(&[0xff; 24][..])] {
            let (_directory, storage) = stored_pair(file_id(), &log_bytes, index_bytes).await;
            let error = IndexedLogValidator::validate(&storage, file_id(), Some(end(0, 16)))
                .await
                .unwrap_err();
            let Error::Index(IndexFileValidationError::TrustedIndexMismatch {
                entry_index,
                expected_position,
                actual_position,
            }) = error
            else {
                panic!("expected the original trusted-index error");
            };
            assert_eq!(entry_index, 0);
            assert_eq!(expected_position, 16);
            assert_eq!(
                actual_position,
                index_bytes
                    .filter(|bytes| bytes.len() >= 8)
                    .map(|_| u64::MAX)
            );
            assert_bytes(
                &storage,
                file_id(),
                &[FIRST, SECOND].concat(),
                index_bytes.unwrap_or_default(),
            )
            .await;
            assert!(!storage.checkpoint_file_path(file_id().stream_id()).exists());
        }
    }

    #[tokio::test]
    async fn recovery_leaves_checkpoints_and_other_file_pairs_untouched() {
        let (_directory, storage) =
            stored_pair(file_id(), &[FIRST, &[0xff; 3]].concat(), None).await;
        let checkpoint_path = storage.checkpoint_file_path(file_id().stream_id());
        tokio::fs::write(&checkpoint_path, b"checkpoint remains caller-owned")
            .await
            .unwrap();
        let later = file_id().next();
        tokio::fs::write(storage.log_file_path(later), b"future log")
            .await
            .unwrap();
        tokio::fs::write(storage.index_file_path(later), b"future index")
            .await
            .unwrap();

        let result = IndexedLogValidator::validate(&storage, file_id(), None)
            .await
            .unwrap();
        assert_partial(result, Some(end(0, 16)), 16, 8).await;
        assert_eq!(
            tokio::fs::read(checkpoint_path).await.unwrap(),
            b"checkpoint remains caller-owned"
        );
        assert_bytes(&storage, later, b"future log", b"future index").await;
        assert_bytes(&storage, file_id(), FIRST, &encoded_index(&[16])).await;
    }
}

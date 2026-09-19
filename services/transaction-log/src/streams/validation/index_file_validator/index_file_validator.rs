use std::io::SeekFrom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite};

use super::IndexFileValidationError as Error;
use crate::streams::{
    IndexWriter, RECORDS_PER_FILE, RecordEndLocation, ValidationFile,
    index_writer::INDEX_ENTRY_LEN, location::LogFileId,
};

/// Checks a trusted index entry and replaces the suffix of an owned dense index file.
///
/// This stateless type groups the [`validate`](Self::validate) operation. The caller
/// supplies a checkpoint-certified endpoint and the following offsets from log
/// validation. The suffix is always rewritten and synchronized, including entries
/// that already contain the supplied values.
///
/// This component owns only the index file. It does not read a log, establish
/// record validity, choose a trusted checkpoint, open paths or publish recovery.
/// Validation runs during startup with exclusive file access. The caller ensures
/// no other code accesses the file; this component does not check for external changes.
pub struct IndexFileValidator;

impl IndexFileValidator {
    /// Checks the trusted entry, replaces its suffix, and returns the synchronized file.
    ///
    /// The file must permit seeking, writing, truncation and data synchronization,
    /// plus reading when a trusted entry is supplied. The caller must finish flushing
    /// earlier writes and exclude all other file access throughout recovery.
    /// [`ValidationFile`] supplies shared recovery operations; [`AsyncWrite`] adds
    /// suffix output. Tokio files and explicitly supplied file wrappers are supported,
    /// and the same concrete file type is returned on success.
    ///
    /// `last_trusted` comes from the stream checkpoint that certified the log and
    /// index during earlier recovery. Its record ID locates the dense entry, whose
    /// value must equal its byte position. Earlier entries remain caller-certified.
    /// `None` means no trusted prefix, so the entire index is replaced.
    ///
    /// `suffix_ends` contains the absolute exclusive ends of every consecutive record
    /// accepted by log validation after that same checkpoint. The caller establishes
    /// their validity; this operation checks only the combined entry count. Existing
    /// suffix bytes are never read or compared. An empty slice removes the suffix.
    /// All writes are flushed and the file is synchronized, even for an empty index.
    ///
    /// Excessive counts are rejected before I/O. A missing or mismatched trusted
    /// entry is rejected before mutation. Any error returns no file; failure or
    /// cancellation during replacement may leave partial changes and outstanding OS
    /// I/O. The caller must quiesce it before retrying. Dropping an unpolled future
    /// drops the file without starting I/O. No checkpoint is published here.
    pub async fn validate<F: ValidationFile + AsyncWrite>(
        mut file: F,
        last_trusted: Option<RecordEndLocation>,
        suffix_ends: &[u64],
    ) -> Result<F, Error> {
        let trusted_entry_count =
            last_trusted.map_or(0, |end| LogFileId::record_count_through(end.record_id()));
        let entry_count = trusted_entry_count + suffix_ends.len() as u64;
        if entry_count > RECORDS_PER_FILE {
            return Err(Error::TooManyEntries {
                count: entry_count,
                maximum: RECORDS_PER_FILE,
            });
        }

        if let Some(last_trusted) = last_trusted {
            let entry_index = trusted_entry_count - 1;
            let actual_position = Self::read_index_at(&mut file, entry_index).await?;
            if actual_position != Some(last_trusted.position()) {
                return Err(Error::TrustedIndexMismatch {
                    entry_index,
                    expected_position: last_trusted.position(),
                    actual_position,
                });
            }
        }

        let suffix_start = trusted_entry_count * INDEX_ENTRY_LEN as u64;
        file.set_len(suffix_start).await?;
        file.seek(SeekFrom::Start(suffix_start)).await?;
        {
            let mut writer = IndexWriter::with_file(&mut file);
            for &end in suffix_ends {
                writer.write(end)?;
            }
            writer.flush_buffer().await?;
            writer.flush().await?;
        }
        file.sync_data().await?;
        Ok(file)
    }

    /// Reads one zero-based entry selected from a record's position within its file.
    ///
    /// Returns `None` if the file length does not cover the complete entry.
    /// Seeking or reading errors, including a later short read, remain I/O errors.
    async fn read_index_at<F: ValidationFile>(
        file: &mut F,
        entry_index: u64,
    ) -> Result<Option<u64>, Error> {
        let entry_start = entry_index * INDEX_ENTRY_LEN as u64;
        if file.length().await? < entry_start + INDEX_ENTRY_LEN as u64 {
            return Ok(None);
        }
        file.seek(SeekFrom::Start(entry_start)).await?;
        let mut bytes = [0; INDEX_ENTRY_LEN];
        file.read_exact(&mut bytes).await?;
        Ok(Some(u64::from_le_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        path::{Path, PathBuf},
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use tempfile::TempDir;
    use tokio::fs::{File, OpenOptions};
    use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

    use super::*;
    use crate::streams::validation::test_file::{
        FileOperation as Operation, FileProbe, TestFile, wait_for_pause,
    };

    fn encoded(entries: &[u64]) -> Vec<u8> {
        entries
            .iter()
            .flat_map(|entry| entry.to_le_bytes())
            .collect()
    }

    fn end(sequence: u64, position: u64) -> RecordEndLocation {
        RecordEndLocation::new(
            RecordId::new(StreamId::new(7).unwrap(), SequenceNumber::new(sequence)),
            position,
        )
    }

    async fn open_read_write(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await
            .unwrap()
    }

    async fn opened_file(bytes: &[u8]) -> (TempDir, PathBuf, File) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("index.idx");
        tokio::fs::write(&path, bytes).await.unwrap();
        let file = open_read_write(&path).await;
        (directory, path, file)
    }

    #[tokio::test]
    async fn preserves_the_checkpoint_prefix_and_returns_the_repaired_file() {
        let mut bytes = encoded(&[16, 35, 52, 999]);
        bytes.push(0xaa);
        let (_directory, path, file) = opened_file(&bytes).await;
        let mut file = IndexFileValidator::validate(file, Some(end(100_002, 52)), &[70])
            .await
            .unwrap();
        let expected = encoded(&[16, 35, 52, 70]);
        assert_eq!(tokio::fs::read(path).await.unwrap(), expected);
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut returned_bytes = Vec::new();
        file.read_to_end(&mut returned_bytes).await.unwrap();
        assert_eq!(returned_bytes, expected);
    }

    #[tokio::test]
    async fn missing_partial_and_wrong_trusted_entries_fail_without_modifying_the_file() {
        let complete = encoded(&[16, 35, 52]);
        let cases = (0..complete.len())
            .map(|length| (complete[..length].to_vec(), None))
            .chain([(encoded(&[16, 35, 51]), Some(51))]);
        for (bytes, actual_position) in cases {
            let (_directory, path, file) = opened_file(&bytes).await;
            assert!(matches!(
                IndexFileValidator::validate(file, Some(end(2, 52)), &[70]).await,
                Err(Error::TrustedIndexMismatch {
                    entry_index: 2,
                    expected_position: 52,
                    actual_position: actual,
                }) if actual == actual_position
            ));
            assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn trusted_entry_read_failure_preserves_the_io_error_and_file_bytes() {
        let bytes = encoded(&[16]);
        let (_directory, path, file) = opened_file(&bytes).await;
        drop(file);
        let write_only = OpenOptions::new().write(true).open(&path).await.unwrap();
        match IndexFileValidator::validate(write_only, Some(end(0, 16)), &[]).await {
            Err(Error::Io(error)) => assert!(error.raw_os_error().is_some()),
            _ => panic!("reading the trusted entry through a write-only handle must fail"),
        }
        assert_eq!(tokio::fs::read(&path).await.unwrap(), bytes);
        let _file =
            IndexFileValidator::validate(open_read_write(&path).await, Some(end(0, 16)), &[])
                .await
                .unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn replaces_missing_partial_matching_wrong_and_excessive_suffixes() {
        // Independent little-endian fixture for the authoritative ends 35 and 52.
        let expected_suffix = [35, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0];
        let cases = (0..=expected_suffix.len())
            .map(|length| expected_suffix[..length].to_vec())
            .chain([
                encoded(&[99, 52]),
                encoded(&[35, 99]),
                encoded(&[35, 52, 70]),
                [expected_suffix.as_slice(), &[0xaa]].concat(),
            ])
            .collect::<Vec<_>>();
        for last_trusted in [None, Some(end(100_000, 16))] {
            let prefix = if last_trusted.is_some() {
                encoded(&[16])
            } else {
                Vec::new()
            };
            let expected = [prefix.as_slice(), &expected_suffix].concat();
            for suffix in &cases {
                let bytes = [prefix.as_slice(), suffix].concat();
                let (_directory, path, file) = opened_file(&bytes).await;
                let file = IndexFileValidator::validate(file, last_trusted, &[35, 52])
                    .await
                    .unwrap();
                assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
                // Repeating replacement must never append a second copy.
                let _file = IndexFileValidator::validate(file, last_trusted, &[35, 52])
                    .await
                    .unwrap();
                assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
            }
        }
    }

    #[tokio::test]
    async fn repeated_validation_uses_the_supplied_checkpoint_for_each_suffix() {
        let bytes = encoded(&[16, 35, 99, 101]);
        let (_directory, path, mut file) = opened_file(&bytes).await;
        for ends in [&[52, 70][..], &[88], &[], &[], &[55, 89]] {
            file = IndexFileValidator::validate(file, Some(end(1, 35)), ends)
                .await
                .unwrap();
            let expected = [encoded(&[16, 35]), encoded(ends)].concat();
            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn empty_suffix_without_a_trusted_entry_empties_the_file() {
        for bytes in [Vec::new(), vec![0xaa], encoded(&[16, 35])] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let _file = IndexFileValidator::validate(file, None, &[]).await.unwrap();
            assert!(tokio::fs::read(path).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn replacement_preserves_full_width_offsets_in_little_endian_order() {
        let (_directory, path, file) = opened_file(&[0xaa; 25]).await;
        let _file = IndexFileValidator::validate(file, None, &[0x0102_0304_0506_0708, u64::MAX])
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(path).await.unwrap(),
            [
                8, 7, 6, 5, 4, 3, 2, 1, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );
    }

    #[tokio::test]
    async fn count_limit_includes_the_trusted_prefix_and_rejects_before_modifying_bytes() {
        for trusted_count in [0, RECORDS_PER_FILE - 1, RECORDS_PER_FILE] {
            let mut bytes = encoded(&vec![16; trusted_count as usize]);
            let prefix = bytes.clone();
            bytes.push(0xaa);
            let last_trusted = trusted_count
                .checked_sub(1)
                .map(|sequence| end(sequence, 16));
            let (_directory, path, file) = opened_file(&bytes).await;
            let available = (RECORDS_PER_FILE - trusted_count) as usize;
            let ends = vec![32; available + 1];
            assert!(matches!(
                IndexFileValidator::validate(file, last_trusted, &ends).await,
                Err(Error::TooManyEntries {
                    count,
                    maximum: RECORDS_PER_FILE,
                }) if count == RECORDS_PER_FILE + 1
            ));
            assert_eq!(tokio::fs::read(&path).await.unwrap(), bytes);
            let _file = IndexFileValidator::validate(
                open_read_write(&path).await,
                last_trusted,
                &ends[..available],
            )
            .await
            .unwrap();
            assert_eq!(
                tokio::fs::read(path).await.unwrap(),
                [prefix, encoded(&ends[..available])].concat()
            );
        }
    }

    #[tokio::test]
    async fn replacement_does_not_read_the_existing_suffix() {
        let (_directory, path, file) = opened_file(&encoded(&[16, 99])).await;
        drop(file);
        let file = OpenOptions::new().write(true).open(&path).await.unwrap();
        let _file = IndexFileValidator::validate(file, None, &[16, 35])
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), encoded(&[16, 35]));
    }

    #[tokio::test]
    async fn read_only_replacement_fails_even_for_matching_contents_without_returning_a_file() {
        let bytes = encoded(&[16]);
        for ends in [&[16][..], &[]] {
            let (_directory, path, file) = opened_file(&bytes).await;
            drop(file);
            match IndexFileValidator::validate(File::open(&path).await.unwrap(), None, ends).await {
                Err(Error::Io(error)) => assert!(error.raw_os_error().is_some()),
                _ => panic!("replacement must require a writable handle even for matching entries"),
            }
            assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
        }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn dropping_unpolled_or_pending_trusted_entry_validation_preserves_the_file() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let bytes = encoded(&[16]);
            for poll in [false, true] {
                let (_directory, path, file) = opened_file(&bytes).await;
                // Occupy the only blocking worker so validation cannot finish
                // its initial metadata request during the first poll.
                let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
                let (release_sender, release_receiver) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    ready_sender.send(()).unwrap();
                    let _ = release_receiver.recv_timeout(std::time::Duration::from_secs(5));
                });
                ready_receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                let mut future =
                    Box::pin(IndexFileValidator::validate(file, Some(end(0, 16)), &[]));
                let pending = !poll || poll_once(future.as_mut()).is_pending();
                drop(future);
                release_sender.send(()).unwrap();
                blocker.await.unwrap();
                assert!(pending);
                assert_eq!(tokio::fs::read(&path).await.unwrap(), bytes);
            }
        });
    }

    #[tokio::test]
    async fn empty_matching_trusted_and_rebuilt_indexes_wait_for_sync_before_returning() {
        for (bytes, last_trusted, suffix_ends, expected) in [
            (vec![], None, vec![], vec![]),
            (encoded(&[16]), None, vec![16], encoded(&[16])),
            (encoded(&[16]), Some(end(0, 16)), vec![], encoded(&[16])),
            (
                [encoded(&[16, 99]), vec![0xaa]].concat(),
                Some(end(0, 16)),
                vec![35, 52],
                encoded(&[16, 35, 52]),
            ),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe {
                pause_at: Some(Operation::Sync),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let mut validation = Box::pin(IndexFileValidator::validate(
                file,
                last_trusted,
                &suffix_ends,
            ));
            wait_for_pause(&probe, validation.as_mut()).await;
            assert_eq!(probe.completed.borrow().last(), Some(&Operation::Flush));
            assert!(!probe.completed.borrow().contains(&Operation::Sync));
            assert_eq!(probe.bytes_written.get(), suffix_ends.len() * 8);
            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected);
            assert!(poll_once(validation.as_mut()).is_pending());

            probe.resume.set(true);
            let _file = validation.await.unwrap();
            assert_eq!(probe.completed.borrow().last(), Some(&Operation::Sync));
            assert_eq!(
                probe
                    .completed
                    .borrow()
                    .iter()
                    .filter(|op| **op == Operation::Sync)
                    .count(),
                1
            );
            assert_eq!(tokio::fs::read(path).await.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn sync_failure_returns_no_file_for_empty_matching_or_trusted_indexes() {
        for (bytes, last_trusted, suffix_ends) in [
            (vec![], None, vec![]),
            (encoded(&[16]), None, vec![16]),
            (encoded(&[16]), Some(end(0, 16)), vec![]),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe {
                fail_at: Some(Operation::Sync),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let result = IndexFileValidator::validate(file, last_trusted, &suffix_ends).await;
            assert!(matches!(result, Err(Error::Io(error)) if error.raw_os_error() == Some(123)));
            assert_eq!(probe.completed.borrow().last(), Some(&Operation::Flush));
            probe.quiesce().await;
            assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn short_writes_across_entry_boundaries_produce_the_complete_synchronized_suffix() {
        for max_write in [1, 3, 7, 8, 17] {
            let (_directory, path, file) = opened_file(&encoded(&[16, 99])).await;
            let probe = Rc::new(FileProbe {
                max_write: Some(max_write),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let _file = IndexFileValidator::validate(file, Some(end(0, 16)), &[35, 52, 70])
                .await
                .unwrap();
            assert_eq!(probe.bytes_written.get(), 24);
            assert!(
                probe
                    .completed
                    .borrow()
                    .ends_with(&[Operation::Flush, Operation::Sync])
            );
            assert_eq!(
                tokio::fs::read(path).await.unwrap(),
                encoded(&[16, 35, 52, 70])
            );
        }
    }

    // Expected bytes after stopping replacement of [16, 99, 100] with the trusted
    // prefix [16] and authoritative suffix [35, 52]. Includes every incomplete
    // byte prefix of those two entries, and stops before truncation/flush/sync.
    fn interrupted_repair_cases() -> Vec<(Operation, Vec<u8>)> {
        let prefix = encoded(&[16]);
        let suffix = encoded(&[35, 52]);
        let mut cases = vec![(Operation::Truncate(8), encoded(&[16, 99, 100]))];
        for written in 0..suffix.len() {
            cases.push((
                Operation::Write(written),
                [prefix.as_slice(), &suffix[..written]].concat(),
            ));
        }
        cases.push((Operation::Flush, encoded(&[16, 35, 52])));
        cases.push((Operation::Sync, encoded(&[16, 35, 52])));
        cases
    }

    #[tokio::test]
    async fn repair_failures_preserve_concrete_errors_and_never_return_a_file() {
        for (fail_at, expected) in interrupted_repair_cases() {
            let (_directory, path, file) = opened_file(&encoded(&[16, 99, 100])).await;
            let probe = Rc::new(FileProbe {
                fail_at: Some(fail_at),
                max_write: Some(1),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let result = IndexFileValidator::validate(file, Some(end(0, 16)), &[35, 52]).await;
            let error = match (fail_at, result) {
                (Operation::Truncate(_) | Operation::Sync, Err(Error::Io(error))) => error,
                (
                    Operation::Write(_) | Operation::Flush,
                    Err(Error::Write(crate::streams::IndexWriteError::Io(error))),
                ) => error,
                (operation, result) => panic!("unexpected result at {operation:?}: {result:?}"),
            };
            assert_eq!(error.raw_os_error(), Some(123));
            assert!(!probe.completed.borrow().contains(&Operation::Sync));
            assert!(probe.abandoned_file.borrow().is_some());
            probe.quiesce().await;
            assert_eq!(
                tokio::fs::read(path).await.unwrap(),
                expected,
                "{fail_at:?}"
            );
        }
    }

    #[tokio::test]
    async fn cancellation_preserves_completed_progress_at_each_repair_boundary() {
        for (pause_at, expected) in interrupted_repair_cases() {
            let (_directory, path, file) = opened_file(&encoded(&[16, 99, 100])).await;
            let probe = Rc::new(FileProbe {
                pause_at: Some(pause_at),
                max_write: Some(1),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let mut validation = Box::pin(IndexFileValidator::validate(
                file,
                Some(end(0, 16)),
                &[35, 52],
            ));
            wait_for_pause(&probe, validation.as_mut()).await;
            drop(validation);
            assert!(!probe.completed.borrow().contains(&Operation::Sync));
            assert!(probe.abandoned_file.borrow().is_some());
            probe.quiesce().await;
            assert_eq!(
                tokio::fs::read(path).await.unwrap(),
                expected,
                "{pause_at:?}"
            );
        }
    }
}

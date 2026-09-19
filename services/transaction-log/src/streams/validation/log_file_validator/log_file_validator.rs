use std::io::SeekFrom;

use tokio::io::{AsyncRead, AsyncSeekExt};
use transaction_log_exports::{
    RecordReadError, RecordReader,
    record::record_protocol::{MAX_RECORD_LEN, MIN_RECORD_LEN},
};

use super::{LogFileValidationError as Error, ValidatedLogFile};
use crate::streams::{
    LogFileId, LogTailError, RECORDS_PER_FILE, RecordEndLocation, ValidationFile,
};

/// Startup validation and tail recovery for an exclusively owned log file.
///
/// This stateless type groups the [`validate`](Self::validate) operation. The caller
/// passes ownership of the file, its identity and an optional checkpoint-certified
/// end directly. Validation preserves the trusted prefix and each valid following
/// record, then removes a corrupt tail and synchronizes the file.
/// It performs no index I/O, checkpoint publication or external-change checks.
pub struct LogFileValidator;

impl LogFileValidator {
    /// Scans, removes an invalid tail, and synchronizes the log before returning it.
    ///
    /// Takes ownership of an already-open file. It must support reading, seeking,
    /// truncation and data synchronization. The caller must flush earlier writes and
    /// supply exclusive access. [`ValidationFile`] supports Tokio files and explicit
    /// file wrappers; the result retains the same concrete file type. Validation
    /// does not require [`tokio::io::AsyncWrite`] or write any record bytes.
    ///
    /// `file_id` supplies the expected stream and sequence range;
    /// `last_trusted` supplies the caller-certified prefix.
    /// Its file identity, plausible byte position and presence are checked before scanning.
    ///
    /// Trusted bytes are not rescanned. New records must match the file's stream,
    /// exact next sequence, framing and CRC. Content corruption stops acceptance at
    /// the preceding valid end, and all subsequent bytes are truncated. Bytes after
    /// the file's assigned record range are likewise removed. A clean log is also
    /// synchronized, including when no new records were scanned.
    ///
    /// Invalid inputs and operational errors return no successful findings. Read
    /// errors never authorize truncation. Errors during repair/synchronization may
    /// follow partial progress. Cancellation drops ownership of the file and can leave
    /// outstanding OS I/O; the caller must quiesce it before another recovery attempt.
    /// Dropping an unpolled future drops the owned file without starting I/O.
    pub async fn validate<F: ValidationFile>(
        mut file: F,
        file_id: LogFileId,
        last_trusted: Option<RecordEndLocation>,
    ) -> Result<ValidatedLogFile<F>, Error> {
        if last_trusted.is_some_and(|end| end.log_file_id() != file_id) {
            return Err(Error::InvalidStart(
                "trusted endpoint belongs to another file",
            ));
        }
        let file_length = file.length().await?;
        let start_position = last_trusted.map_or(0, |end| end.position());
        let trusted_count =
            last_trusted.map_or(0, |end| LogFileId::record_count_through(end.record_id()));
        if start_position > file_length
            || start_position < trusted_count * MIN_RECORD_LEN as u64
            || start_position > trusted_count * MAX_RECORD_LEN as u64
        {
            return Err(Error::InvalidStart(
                "position is outside the possible trusted record extent",
            ));
        }

        file.seek(SeekFrom::Start(start_position)).await?;
        let scan_results = scan_records(&mut file, file_id, last_trusted, file_length).await?;
        if scan_results.tail_error.is_some() {
            let valid_length = scan_results.validated_end.map_or(0, |end| end.position());
            file.set_len(valid_length).await?;
        }
        file.sync_data().await?;
        Ok(ValidatedLogFile {
            file,
            validated_end: scan_results.validated_end,
            suffix_ends: scan_results.suffix_ends,
            removed_tail: scan_results.tail_error,
        })
    }
}

/// Complete content findings before truncation or synchronization has occurred.
struct LogScanResult {
    /// Last accepted record, retaining the trusted endpoint if no new record passed.
    validated_end: Option<RecordEndLocation>,
    /// Absolute exclusive ends of newly accepted records only, in file order.
    suffix_ends: Vec<u64>,
    /// First content violation, or `None` when the file ends at the accepted boundary.
    tail_error: Option<LogTailError>,
}

/// Determines the accepted endpoint, suffix offsets and complete tail diagnosis.
///
/// The caller establishes a valid trusted boundary and positions the source there.
/// `file_length` is the entire file's byte length, not the source's remaining length.
/// It lets the scanner diagnose bytes beyond the record cap without an EOF probe.
/// An operational error drops partial findings instead of returning a scan result.
async fn scan_records<R: AsyncRead + Unpin>(
    source: R,
    file_id: LogFileId,
    last_trusted: Option<RecordEndLocation>,
    file_length: u64,
) -> Result<LogScanResult, Error> {
    if let Some(end) = last_trusted {
        assert_eq!(
            end.log_file_id(),
            file_id,
            "trusted endpoint belongs to another file"
        );
    }
    let mut reader = RecordReader::new(source);
    let mut count = last_trusted.map_or(0, |end| LogFileId::record_count_through(end.record_id()));
    let mut position = last_trusted.map_or(0, |end| end.position());
    let mut validated_end = last_trusted;
    let mut suffix_ends = Vec::new();
    let mut expected_record_id =
        last_trusted.map_or_else(|| file_id.first_record_id(), |end| end.record_id().next());
    let tail_error = 'scan: loop {
        if count == RECORDS_PER_FILE {
            break (position < file_length).then_some(LogTailError::ExtraData);
        }
        // Keep this exhaustive: a new reader error must get an explicit recovery
        // policy rather than accidentally authorizing deletion after an I/O failure.
        match reader.wait_to_read().await {
            Ok(false) => break None,
            Ok(true) => {}
            Err(error @ (RecordReadError::Io(_) | RecordReadError::ReaderFailed)) => {
                return Err(Error::Read(error));
            }
            Err(
                error @ (RecordReadError::TruncatedHeader { .. }
                | RecordReadError::InvalidLength { .. }
                | RecordReadError::InvalidStreamId(_)
                | RecordReadError::TruncatedRecord { .. }
                | RecordReadError::CrcMismatch { .. }),
            ) => break Some(LogTailError::Record(error)),
        }
        while count < RECORDS_PER_FILE {
            let Some(record) = reader.try_read_next().map_err(Error::Read)? else {
                break;
            };
            if record.id() != expected_record_id {
                break 'scan Some(LogTailError::UnexpectedRecordId {
                    expected: expected_record_id,
                    actual: record.id(),
                });
            }
            position += u64::from(record.length());
            validated_end = Some(RecordEndLocation::new(record.id(), position));
            suffix_ends.push(position);
            count += 1;
            expected_record_id = expected_record_id.next();
        }
    };
    Ok(LogScanResult {
        validated_end,
        suffix_ends,
        tail_error,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        io,
        path::PathBuf,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
    };

    use tempfile::TempDir;
    use tokio::{
        fs::{File, OpenOptions},
        io::{AsyncReadExt, ReadBuf},
    };
    use transaction_log_exports::{
        RecordId, RecordWriter, SequenceNumber, StreamId, record::record_protocol::MAX_PAYLOAD_LEN,
    };

    use super::*;
    use crate::streams::validation::test_file::{
        FileOperation, FileProbe, TestFile, wait_for_pause,
    };

    // Independent wire fixtures: stream 7, sequences 0/1/2, payloads empty/abc/x.
    const FIRST: &[u8] = &[16, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 226, 50, 73];
    const SECOND: &[u8] = &[
        19, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 18, 247, 123, 204,
    ];
    const THIRD: &[u8] = &[17, 0, 7, 0, 2, 0, 0, 0, 0, 0, 0, 0, 120, 53, 10, 47, 115];

    fn id(stream: u16, sequence: u64) -> RecordId {
        RecordId::new(
            StreamId::new(stream).unwrap(),
            SequenceNumber::new(sequence),
        )
    }

    fn end(sequence: u64, position: u64) -> RecordEndLocation {
        RecordEndLocation::new(id(7, sequence), position)
    }

    fn file_id() -> LogFileId {
        LogFileId::from_record_id(id(7, 0))
    }

    async fn opened_file(bytes: &[u8]) -> (TempDir, PathBuf, File) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log.log");
        tokio::fs::write(&path, bytes).await.unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        (directory, path, file)
    }

    async fn encoded(stream: u16, first: u64, count: u64) -> Vec<u8> {
        let mut writer = RecordWriter::for_serialization(Vec::new());
        for sequence in first..first + count {
            writer
                .write(id(stream, sequence), |_| Ok::<_, io::Error>(()))
                .unwrap();
        }
        writer.flush_buffer().await.unwrap();
        writer.into_inner()
    }

    #[tokio::test]
    async fn scan_returns_complete_findings_for_empty_clean_and_trusted_inputs() {
        let bytes = [FIRST, SECOND, THIRD].concat();
        for (last_trusted, start, expected_ends) in [
            (None, 0, vec![16, 35, 52]),
            (Some(end(0, 16)), 16, vec![35, 52]),
            (Some(end(1, 35)), 35, vec![52]),
            (Some(end(2, 52)), 52, vec![]),
        ] {
            let scan = scan_records(&bytes[start..], file_id(), last_trusted, 52)
                .await
                .unwrap();
            assert_eq!(scan.validated_end, Some(end(2, 52)));
            assert_eq!(scan.suffix_ends, expected_ends);
            assert!(scan.tail_error.is_none());
        }
        let scan = scan_records(&[][..], file_id(), None, 0).await.unwrap();
        assert_eq!(scan.validated_end, None);
        assert!(scan.suffix_ends.is_empty());
        assert!(scan.tail_error.is_none());
    }

    #[tokio::test]
    async fn scan_preserves_the_boundary_before_every_partial_record_tail() {
        for last_trusted in [None, Some(end(0, 16))] {
            for length in 1..SECOND.len() {
                let bytes = [FIRST, &SECOND[..length]].concat();
                let start = last_trusted.map_or(0, |end| end.position()) as usize;
                let scan =
                    scan_records(&bytes[start..], file_id(), last_trusted, bytes.len() as u64)
                        .await
                        .unwrap();
                assert_eq!(scan.validated_end, Some(end(0, 16)));
                assert_eq!(
                    scan.suffix_ends,
                    if last_trusted.is_some() {
                        vec![]
                    } else {
                        vec![16]
                    }
                );
                if length < 12 {
                    assert!(matches!(
                        scan.tail_error,
                        Some(LogTailError::Record(
                            RecordReadError::TruncatedHeader { .. }
                        ))
                    ));
                } else {
                    assert!(matches!(
                        scan.tail_error,
                        Some(LogTailError::Record(
                            RecordReadError::TruncatedRecord { .. }
                        ))
                    ));
                }
            }
        }
    }

    #[tokio::test]
    async fn scan_keeps_the_first_content_diagnosis_and_never_accepts_past_it() {
        let mut corrupt_second = SECOND.to_vec();
        corrupt_second[12] ^= 1;
        let bytes = [FIRST, &corrupt_second, THIRD].concat();
        let scan = scan_records(bytes.as_slice(), file_id(), None, bytes.len() as u64)
            .await
            .unwrap();
        assert_eq!(scan.validated_end, Some(end(0, 16)));
        assert_eq!(scan.suffix_ends, [16]);
        assert!(matches!(
            scan.tail_error,
            Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
        ));

        // An earlier sequence violation wins over the later CRC error in the same batch.
        for (offending, actual) in [
            (FIRST.to_vec(), id(7, 0)),
            (THIRD.to_vec(), id(7, 2)),
            (encoded(8, 1, 1).await, id(8, 1)),
        ] {
            let bytes = [FIRST, &offending, &corrupt_second].concat();
            let scan = scan_records(bytes.as_slice(), file_id(), None, bytes.len() as u64)
                .await
                .unwrap();
            assert_eq!(scan.validated_end, Some(end(0, 16)));
            assert_eq!(scan.suffix_ends, [16]);
            assert!(matches!(scan.tail_error,
                Some(LogTailError::UnexpectedRecordId { expected, actual: found })
                if expected == id(7, 1) && found == actual));
        }
    }

    #[tokio::test]
    async fn scan_classifies_invalid_lengths_and_stream_domains_as_content_failures() {
        let mut invalid_length = FIRST.to_vec();
        invalid_length[..2].copy_from_slice(&15u16.to_le_bytes());
        let mut invalid_stream = FIRST.to_vec();
        invalid_stream[2..4].copy_from_slice(&4096u16.to_le_bytes());
        for (bytes, length_error) in [(invalid_length, true), (invalid_stream, false)] {
            let scan = scan_records(bytes.as_slice(), file_id(), None, bytes.len() as u64)
                .await
                .unwrap();
            assert_eq!(scan.validated_end, None);
            assert!(scan.suffix_ends.is_empty());
            if length_error {
                assert!(matches!(
                    scan.tail_error,
                    Some(LogTailError::Record(RecordReadError::InvalidLength {
                        declared: 15
                    }))
                ));
            } else {
                assert!(matches!(
                    scan.tail_error,
                    Some(LogTailError::Record(RecordReadError::InvalidStreamId(_)))
                ));
            }
        }
    }

    struct ReadFailure;

    impl AsyncRead for ReadFailure {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from_raw_os_error(123)))
        }
    }

    #[tokio::test]
    async fn operational_read_errors_return_no_scan_result_even_after_valid_records() {
        for prefix in [vec![], [FIRST, SECOND].concat()] {
            let source = prefix.as_slice().chain(ReadFailure);
            let result = scan_records(source, file_id(), None, prefix.len() as u64 + 1).await;
            assert!(matches!(result,
                Err(Error::Read(RecordReadError::Io(error))) if error.raw_os_error() == Some(123)));
        }
    }

    #[tokio::test]
    async fn full_trusted_prefix_is_classified_without_reading_past_the_cap() {
        let trusted = end(RECORDS_PER_FILE - 1, RECORDS_PER_FILE * 16);
        for extra in [0, 1, 16] {
            // Any read would fail: the scanner must use the supplied whole-file length.
            let scan = scan_records(
                ReadFailure,
                file_id(),
                Some(trusted),
                trusted.position() + extra,
            )
            .await
            .unwrap();
            assert_eq!(scan.validated_end, Some(trusted));
            assert!(scan.suffix_ends.is_empty());
            if extra == 0 {
                assert!(scan.tail_error.is_none());
            } else {
                assert!(matches!(scan.tail_error, Some(LogTailError::ExtraData)));
            }
        }
    }

    #[tokio::test]
    async fn scan_tracks_the_last_record_directly_and_diagnoses_extra_bytes_at_the_cap() {
        let records = encoded(7, 100_000, RECORDS_PER_FILE).await;
        let file_id = LogFileId::from_record_id(id(7, 100_000));
        let expected_end = end(199_999, 1_600_000);
        for extra in [vec![], vec![0xaa], encoded(7, 200_000, 1).await] {
            let bytes = [records.as_slice(), &extra].concat();
            let scan = scan_records(bytes.as_slice(), file_id, None, bytes.len() as u64)
                .await
                .unwrap();
            assert_eq!(scan.validated_end, Some(expected_end));
            assert_eq!(
                scan.suffix_ends,
                (1..=RECORDS_PER_FILE).map(|n| n * 16).collect::<Vec<_>>()
            );
            if extra.is_empty() {
                assert!(scan.tail_error.is_none());
            } else {
                assert!(matches!(scan.tail_error, Some(LogTailError::ExtraData)));
            }
            let trusted = end(199_998, 1_599_984);
            let scan = scan_records(
                &bytes[1_599_984..],
                file_id,
                Some(trusted),
                bytes.len() as u64,
            )
            .await
            .unwrap();
            assert_eq!(scan.validated_end, Some(expected_end));
            assert_eq!(scan.suffix_ends, [1_600_000]);
            assert_eq!(scan.tail_error.is_some(), !extra.is_empty());
        }
    }

    struct Fragmented<'a> {
        bytes: &'a [u8],
        chunk_size: usize,
    }

    impl AsyncRead for Fragmented<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let length = self
                .bytes
                .len()
                .min(self.chunk_size)
                .min(output.remaining());
            output.put_slice(&self.bytes[..length]);
            self.bytes = &self.bytes[length..];
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn scan_findings_do_not_depend_on_read_fragmentation() {
        let mut corrupt_third = THIRD.to_vec();
        corrupt_third[12] ^= 1;
        let bytes = [FIRST, SECOND, &corrupt_third].concat();
        for chunk_size in [1, 2, 7, 12, 16, 35, 65_536] {
            let source = Fragmented {
                bytes: &bytes,
                chunk_size,
            };
            let scan = scan_records(source, file_id(), None, bytes.len() as u64)
                .await
                .unwrap();
            assert_eq!(scan.validated_end, Some(end(1, 35)));
            assert_eq!(scan.suffix_ends, [16, 35]);
            assert!(matches!(
                scan.tail_error,
                Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
            ));
        }
    }

    #[tokio::test]
    async fn validate_applies_scan_findings_and_returns_the_same_endpoint_and_offsets() {
        for (bytes, last_trusted, expected_bytes, expected_end, expected_ends, repaired) in [
            (vec![], None, vec![], None, vec![], false),
            (
                [FIRST, SECOND, THIRD].concat(),
                None,
                [FIRST, SECOND, THIRD].concat(),
                Some(end(2, 52)),
                vec![16, 35, 52],
                false,
            ),
            (
                [FIRST, SECOND, &[0xaa]].concat(),
                Some(end(0, 16)),
                [FIRST, SECOND].concat(),
                Some(end(1, 35)),
                vec![35],
                true,
            ),
            (
                [FIRST, &[0xaa]].concat(),
                Some(end(0, 16)),
                FIRST.to_vec(),
                Some(end(0, 16)),
                vec![],
                true,
            ),
            (vec![0xaa], None, vec![], None, vec![], true),
            // An earlier certified prefix is deliberately not rescanned.
            (
                [&[0; 16][..], SECOND].concat(),
                Some(end(0, 16)),
                [&[0; 16][..], SECOND].concat(),
                Some(end(1, 35)),
                vec![35],
                false,
            ),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let result = LogFileValidator::validate(file, file_id(), last_trusted)
                .await
                .unwrap();
            assert_eq!(result.validated_end(), expected_end);
            assert_eq!(result.suffix_ends(), expected_ends);
            assert_eq!(result.removed_tail().is_some(), repaired);
            assert_eq!(tokio::fs::read(&path).await.unwrap(), expected_bytes);
            assert_eq!(
                result.into_inner().metadata().await.unwrap().len(),
                expected_bytes.len() as u64
            );
        }
    }

    #[tokio::test]
    async fn invalid_trusted_endpoints_leave_every_byte_unchanged() {
        for (description, bytes, trusted) in [
            (
                "wrong stream",
                FIRST.to_vec(),
                RecordEndLocation::new(id(8, 0), 16),
            ),
            ("wrong file", FIRST.to_vec(), end(100_000, 16)),
            ("empty file", vec![], end(0, 16)),
            ("past EOF", FIRST.to_vec(), end(0, 17)),
            ("zero position", FIRST.to_vec(), end(0, 0)),
            ("below minimum for one record", FIRST.to_vec(), end(0, 15)),
            (
                "below minimum for two records",
                [FIRST, SECOND].concat(),
                end(1, 31),
            ),
            (
                "above maximum for one record",
                vec![0; 65_536],
                end(0, 65_536),
            ),
            (
                "above maximum for two records",
                vec![0; 131_071],
                end(1, 131_071),
            ),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe::default());
            let file = TestFile::new(file, Rc::clone(&probe));
            let result = LogFileValidator::validate(file, file_id(), Some(trusted)).await;
            assert!(
                matches!(result, Err(Error::InvalidStart(_))),
                "{description}"
            );
            assert_eq!(tokio::fs::read(path).await.unwrap(), bytes, "{description}");
            assert!(
                probe
                    .started
                    .borrow()
                    .iter()
                    .all(|op| *op == FileOperation::Length),
                "{description}: invalid input must not reach repair or synchronization"
            );
        }
    }

    #[tokio::test]
    async fn clean_empty_trusted_and_repaired_files_wait_for_sync_before_success() {
        for (bytes, trusted, retained, repaired) in [
            (vec![], None, vec![], false),
            (FIRST.to_vec(), None, FIRST.to_vec(), false),
            (FIRST.to_vec(), Some(end(0, 16)), FIRST.to_vec(), false),
            ([FIRST, &[0xaa]].concat(), None, FIRST.to_vec(), true),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe {
                pause_at: Some(FileOperation::Sync),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let mut validation = Box::pin(LogFileValidator::validate(file, file_id(), trusted));
            wait_for_pause(&probe, validation.as_mut()).await;
            let mut expected_operations = vec![FileOperation::Length];
            if repaired {
                expected_operations.push(FileOperation::Truncate(16));
            }
            assert_eq!(*probe.completed.borrow(), expected_operations);
            expected_operations.push(FileOperation::Sync);
            assert_eq!(*probe.started.borrow(), expected_operations);
            assert_eq!(tokio::fs::read(&path).await.unwrap(), retained);
            assert!(
                validation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "the file must not be returned while synchronization is pending"
            );

            probe.resume.set(true);
            let result = validation.await.unwrap();
            assert_eq!(*probe.completed.borrow(), expected_operations);
            assert_eq!(result.removed_tail().is_some(), repaired);
            assert_eq!(tokio::fs::read(path).await.unwrap(), retained);
            let mut returned: TestFile = result.into_inner();
            assert_eq!(returned.length().await.unwrap(), retained.len() as u64);
        }
    }

    #[tokio::test]
    async fn sync_failure_never_returns_a_validated_file_even_after_successful_repair() {
        for (bytes, trusted, retained, repaired) in [
            (vec![], None, vec![], false),
            (FIRST.to_vec(), None, FIRST.to_vec(), false),
            (FIRST.to_vec(), Some(end(0, 16)), FIRST.to_vec(), false),
            ([FIRST, &[0xaa]].concat(), None, FIRST.to_vec(), true),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe {
                fail_at: Some(FileOperation::Sync),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let result = LogFileValidator::validate(file, file_id(), trusted).await;
            assert!(matches!(result,
                Err(Error::Io(error)) if error.raw_os_error() == Some(123)));
            let mut expected_operations = vec![FileOperation::Length];
            if repaired {
                expected_operations.push(FileOperation::Truncate(16));
            }
            assert_eq!(*probe.completed.borrow(), expected_operations);
            expected_operations.push(FileOperation::Sync);
            assert_eq!(*probe.started.borrow(), expected_operations);
            // Failure does not roll back a truncation that has already completed.
            assert_eq!(tokio::fs::read(path).await.unwrap(), retained);
        }
    }

    #[tokio::test]
    async fn dropping_an_unpolled_validation_future_starts_no_file_operations() {
        let bytes = [FIRST, &[0xaa]].concat();
        let (_directory, path, file) = opened_file(&bytes).await;
        let probe = Rc::new(FileProbe::default());
        let file = TestFile::new(file, Rc::clone(&probe));
        let validation = LogFileValidator::validate(file, file_id(), None);
        drop(validation);
        assert!(probe.started.borrow().is_empty());
        assert!(probe.completed.borrow().is_empty());
        assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn cancelling_validation_at_file_operation_boundaries_preserves_completed_progress() {
        let bytes = [FIRST, &[0xaa]].concat();
        for (pause_at, completed, retained) in [
            (FileOperation::Length, vec![], bytes.as_slice()),
            (
                FileOperation::Truncate(16),
                vec![FileOperation::Length],
                bytes.as_slice(),
            ),
            (
                FileOperation::Sync,
                vec![FileOperation::Length, FileOperation::Truncate(16)],
                FIRST,
            ),
        ] {
            let (_directory, path, file) = opened_file(&bytes).await;
            let probe = Rc::new(FileProbe {
                pause_at: Some(pause_at),
                ..FileProbe::default()
            });
            let file = TestFile::new(file, Rc::clone(&probe));
            let mut validation = Box::pin(LogFileValidator::validate(file, file_id(), None));
            wait_for_pause(&probe, validation.as_mut()).await;
            drop(validation);

            let mut started = completed.clone();
            started.push(pause_at);
            assert_eq!(*probe.started.borrow(), started);
            assert_eq!(*probe.completed.borrow(), completed);
            assert_eq!(tokio::fs::read(path).await.unwrap(), retained);
        }
    }

    #[tokio::test]
    async fn public_validation_removes_extra_bytes_after_full_scanned_or_trusted_files() {
        let records = encoded(7, 0, RECORDS_PER_FILE).await;
        let final_end = end(99_999, 1_600_000);
        for trusted in [None, Some(end(99_998, 1_599_984)), Some(final_end)] {
            for extra in [vec![], vec![0xaa], encoded(7, 100_000, 1).await] {
                let bytes = [records.as_slice(), &extra].concat();
                let (_directory, path, file) = opened_file(&bytes).await;
                let result = LogFileValidator::validate(file, file_id(), trusted)
                    .await
                    .unwrap();
                assert_eq!(result.validated_end(), Some(final_end));
                let first_new_count =
                    trusted.map_or(1, |end| end.record_id().sequence_number().get() + 2);
                let expected_ends: Vec<_> = (first_new_count..=100_000).map(|n| n * 16).collect();
                assert_eq!(result.suffix_ends(), expected_ends);
                if extra.is_empty() {
                    assert!(result.removed_tail().is_none());
                } else {
                    assert!(matches!(
                        result.removed_tail(),
                        Some(LogTailError::ExtraData)
                    ));
                }
                assert_eq!(tokio::fs::read(path).await.unwrap(), records);
            }
        }
    }

    #[tokio::test]
    async fn maximum_size_records_across_refills_keep_exact_offsets_and_repair_boundaries() {
        let payload = vec![0xa5; MAX_PAYLOAD_LEN];
        let mut writer = RecordWriter::for_serialization(Vec::new());
        for sequence in 0..3 {
            writer
                .write(id(7, sequence), |body| {
                    std::io::Write::write_all(body, &payload)
                })
                .unwrap();
        }
        writer.flush_buffer().await.unwrap();
        let records = writer.into_inner();
        assert_eq!(records.len(), 196_605);

        // The 65,535-byte records cross the reader's 65,536-byte refills.
        // Include a fully trusted maximum-size first record to exercise the
        // inclusive upper bound on trusted byte positions.
        for trusted in [None, Some(end(0, 65_535))] {
            for removed_bytes in [0, 1, 4, 32_768] {
                let bytes = &records[..records.len() - removed_bytes];
                let (_directory, path, file) = opened_file(bytes).await;
                let result = LogFileValidator::validate(file, file_id(), trusted)
                    .await
                    .unwrap();
                let expected_ends: &[u64] = match (trusted.is_some(), removed_bytes == 0) {
                    (false, true) => &[65_535, 131_070, 196_605],
                    (false, false) => &[65_535, 131_070],
                    (true, true) => &[131_070, 196_605],
                    (true, false) => &[131_070],
                };
                let retained_length = *expected_ends.last().unwrap();
                let final_sequence = if removed_bytes == 0 { 2 } else { 1 };
                assert_eq!(
                    result.validated_end(),
                    Some(end(final_sequence, retained_length))
                );
                assert_eq!(result.suffix_ends(), expected_ends);
                if removed_bytes == 0 {
                    assert!(result.removed_tail().is_none());
                } else {
                    assert!(matches!(
                        result.removed_tail(),
                        Some(LogTailError::Record(
                            RecordReadError::TruncatedRecord { .. }
                        ))
                    ));
                }
                assert_eq!(
                    tokio::fs::read(path).await.unwrap(),
                    records[..retained_length as usize]
                );
            }
        }
    }

    #[tokio::test]
    async fn failed_truncation_does_not_publish_scan_findings_as_a_validated_file() {
        let bytes = [FIRST, &[0xaa]].concat();
        let (_directory, path, file) = opened_file(&bytes).await;
        drop(file);
        let readonly = File::open(&path).await.unwrap();
        assert!(matches!(
            LogFileValidator::validate(readonly, file_id(), None).await,
            Err(Error::Io(_))
        ));
        assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
    }

    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[test]
    fn cancelled_scan_cannot_return_its_partial_findings() {
        let source = FIRST.chain(PendingReader);
        let mut future = Box::pin(scan_records(source, file_id(), None, 17));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(future);
    }
}

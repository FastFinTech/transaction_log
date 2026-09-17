use std::io::SeekFrom;

use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use transaction_log_exports::{
    RecordId, RecordReadError, RecordReader, SequenceNumber,
    record::record_protocol::{MAX_RECORD_LEN, MIN_RECORD_LEN},
};

use super::{CompletedIndexedLog, LogTailError, LogValidationError as Error, LogValidationReport};
use crate::storage::{LogFileId, RECORDS_PER_FILE, RecordEndLocation, StorageProvider};
use crate::streams::{IndexWriter, IndexedLogWriter, index_writer::INDEX_ENTRY_LEN};

/// Validates one exclusively owned log/index pair from a caller-supplied boundary.
///
/// Open through [`open`](Self::open), then call [`validate`](Self::validate) to
/// inspect content without modifying existing bytes. The supplied endpoint
/// certifies **both** prefixes; this component never loads a checkpoint or scans
/// behind that endpoint. `None` requests validation from the beginning.
///
/// Index repair is explicit. Removing a corrupt log suffix additionally requires
/// [`truncate_invalid_tail`](Self::truncate_invalid_tail); an I/O error never
/// authorizes it. Handover synchronizes both files: partial pairs become writers,
/// full pairs become completion metadata with their handles closed.
///
/// The caller must exclude concurrent writers and withhold this recovering pair
/// from historical requests through validation, repairs and handover. No locks,
/// timers, checkpoint publication or directory synchronization are supplied here.
/// Failed or cancelled operations make this instance unusable; reopen to recover.
pub struct IndexedLogValidator {
    file_id: LogFileId,
    log: File,
    index: File,
    start: Option<RecordEndLocation>,
    trusted_records: u64,
    // Only offsets for the newly scanned suffix, at most 800 KB per validator.
    // We never retain record bodies or a file-sized payload buffer.
    ends: Vec<u64>,
    report: Option<LogValidationReport>,
    // Current lengths change after repairs; the report preserves original findings.
    log_length: u64,
    index_length: u64,
    usable: bool,
    index_ready: bool,
    log_ready: bool,
}

impl IndexedLogValidator {
    /// Opens the provider's existing log and paired index for validation/repair.
    ///
    /// `start` is the last record already trusted in this file, not the next one
    /// to inspect. `None` means offset zero and the file's first assigned ID. A
    /// full-file endpoint is valid. The caller certifies correct record and index
    /// prefixes; numeric checks and index-prefix checks do not establish that trust.
    ///
    /// A missing log is an error. A missing index is created empty, then can be
    /// rebuilt from the beginning; with a nonempty trusted prefix, validation
    /// reports `TrustedIndexUnavailable` instead of silently scanning earlier data.
    pub async fn open(
        provider: &StorageProvider,
        file_id: LogFileId,
        start: Option<RecordEndLocation>,
    ) -> Result<Self, Error> {
        if start.is_some_and(|end| end.log_file_id() != file_id) {
            return Err(Error::InvalidStart("endpoint belongs to another file"));
        }
        let trusted_records = start.map_or(0, |end| {
            end.record_id().sequence_number().get()
                - file_id.first_record_id().sequence_number().get()
                + 1
        });
        let log = provider.open_log_for_validation(file_id).await?;
        let index = provider.open_index_for_repair(file_id).await?;
        Ok(Self {
            file_id,
            log,
            index,
            start,
            trusted_records,
            ends: Vec::new(),
            report: None,
            log_length: 0,
            index_length: 0,
            usable: true,
            index_ready: false,
            log_ready: false,
        })
    }

    /// Borrows the original findings, including after a later repair failure.
    pub fn report(&self) -> Option<&LogValidationReport> {
        self.report.as_ref()
    }

    /// Extracts raw handles without validating, repairing or synchronizing them.
    ///
    /// Available after failure so the owner can finish outstanding file operations
    /// before reopening for recovery. Extraction does not grant a validated writer
    /// or completed-file result. Normal handover uses `into_writer`/`into_completed`.
    pub fn into_inner(self) -> (File, File) {
        (self.log, self.index)
    }

    /// Validates the suffix and compares dense index entries without changing bytes.
    ///
    /// Content failures produce a report with the exact valid prefix and first
    /// tail error. Operational failures return `Err`, never a truncation report.
    /// Repeating a successful call returns its original report without rescanning.
    ///
    /// Scanning uses RecordReader's batching in a single forward pass. The reader
    /// yields the valid prefix before reporting corrupt input, so every preceding
    /// record receives its sequence check and end offset without rereading the file.
    pub async fn validate(&mut self) -> Result<&LogValidationReport, Error> {
        if self.report.is_some() {
            return self.validated_report();
        }
        self.check_usable()?;
        // Includes all await points. A cancelled scan cannot become repair authority.
        self.usable = false;
        self.log_length = self.log.metadata().await.map_err(Error::LogIo)?.len();
        self.index_length = self.index.metadata().await.map_err(Error::IndexIo)?.len();
        let start_position = self.start.map_or(0, |end| end.position());
        if start_position > self.log_length
            || start_position < self.trusted_records * MIN_RECORD_LEN as u64
            || start_position > self.trusted_records * MAX_RECORD_LEN as u64
        {
            return Err(Error::InvalidStart(
                "position is outside the possible trusted record extent",
            ));
        }

        // Bound allocation even if a corrupt index advertises an enormous length.
        let index_limit = self
            .index_length
            .min(RECORDS_PER_FILE * INDEX_ENTRY_LEN as u64);
        self.index
            .seek(SeekFrom::Start(0))
            .await
            .map_err(Error::IndexIo)?;
        let mut index_bytes = Vec::with_capacity(index_limit as usize);
        (&mut self.index)
            .take(index_limit)
            .read_to_end(&mut index_bytes)
            .await
            .map_err(Error::IndexIo)?;
        if index_bytes.len() as u64 != index_limit {
            return Err(Error::FilesChanged);
        }
        let trusted_bytes = self.trusted_records as usize * INDEX_ENTRY_LEN;
        let Some(prefix) = index_bytes.get(..trusted_bytes) else {
            return Err(Error::TrustedIndexUnavailable);
        };
        let mut previous = 0;
        for entry in prefix.as_chunks::<INDEX_ENTRY_LEN>().0 {
            let end = u64::from_le_bytes(*entry);
            if !(MIN_RECORD_LEN as u64..=MAX_RECORD_LEN as u64)
                .contains(&end.saturating_sub(previous))
            {
                return Err(Error::TrustedIndexUnavailable);
            }
            previous = end;
        }
        if previous != start_position {
            return Err(Error::TrustedIndexUnavailable);
        }

        self.log
            .seek(SeekFrom::Start(start_position))
            .await
            .map_err(Error::LogIo)?;
        let source = (&mut self.log).take(self.log_length - start_position);
        let mut tail_error = scan_records(
            source,
            self.file_id,
            self.trusted_records,
            start_position,
            &mut self.ends,
        )
        .await?;
        let record_count = self.trusted_records + self.ends.len() as u64;
        let valid_length = self.ends.last().copied().unwrap_or(start_position);
        if tail_error.is_none() && valid_length < self.log_length {
            if record_count == RECORDS_PER_FILE {
                tail_error = Some(LogTailError::ExtraData);
            } else {
                return Err(Error::FilesChanged);
            }
        }
        let matching_suffix = self
            .ends
            .iter()
            .zip(
                index_bytes[trusted_bytes..]
                    .as_chunks::<INDEX_ENTRY_LEN>()
                    .0,
            )
            .take_while(|(expected, entry)| **expected == u64::from_le_bytes(**entry))
            .count();
        let end = if let Some(&position) = self.ends.last() {
            let sequence =
                self.file_id.first_record_id().sequence_number().get() + (record_count - 1);
            Some(RecordEndLocation::new(
                RecordId::new(self.file_id.stream_id(), SequenceNumber::new(sequence)),
                position,
            ))
        } else {
            self.start
        };
        self.check_lengths().await?;
        let report = LogValidationReport {
            file_id: self.file_id,
            end,
            record_count,
            log_length: self.log_length,
            index_length: self.index_length,
            matching_index_entries: self.trusted_records + matching_suffix as u64,
            tail_error,
        };
        self.index_ready = !report.needs_index_repair();
        self.log_ready = report.tail_error().is_none();
        self.report = Some(report);
        self.usable = true;
        Ok(self.report.as_ref().unwrap())
    }

    /// Replaces only the incorrect/missing index suffix, preserving matching entries.
    ///
    /// Uses IndexWriter for encoding and buffered output. Extra entries and partial
    /// trailing entries are removed. No log bytes change, even when its tail is
    /// corrupt. Repaired entries describe only the accepted prefix. Success means
    /// flushed output; handover separately establishes durability.
    pub async fn repair_index(&mut self) -> Result<(), Error> {
        let report = self.validated_report()?;
        if self.index_ready {
            return Ok(());
        }
        let matching = report.matching_index_entries();
        let index_length = report.record_count() * INDEX_ENTRY_LEN as u64;
        self.usable = false;
        self.check_lengths().await?;
        // The log must be readable before repaired entries become visible.
        self.log.flush().await.map_err(Error::LogIo)?;
        let position = matching * INDEX_ENTRY_LEN as u64;
        self.index.set_len(position).await.map_err(Error::IndexIo)?;
        self.index
            .seek(SeekFrom::Start(position))
            .await
            .map_err(Error::IndexIo)?;
        {
            let mut writer = IndexWriter::with_file(&mut self.index);
            for &end in &self.ends[(matching - self.trusted_records) as usize..] {
                writer.write(end)?;
            }
            writer.flush_buffer().await?;
            writer.flush().await?;
        }
        self.index_length = index_length;
        self.index_ready = true;
        self.usable = true;
        Ok(())
    }

    /// Explicitly removes the entire invalid log suffix reported by validation.
    ///
    /// This is the caller's destructive recovery decision. Repairs/truncates the
    /// index first, then truncates the log at the exact last accepted record end.
    /// Preserves all trusted bytes. A clean log is a no-op. Report diagnostics
    /// remain the original findings. No operation-error path can authorize this.
    pub async fn truncate_invalid_tail(&mut self) -> Result<(), Error> {
        let length = self.validated_report()?.valid_log_length();
        if self.log_ready {
            return Ok(());
        }
        self.repair_index().await?;
        self.usable = false;
        self.check_lengths().await?;
        self.log.set_len(length).await.map_err(Error::LogIo)?;
        self.log.flush().await.map_err(Error::LogIo)?;
        self.log_length = length;
        self.log_ready = true;
        self.usable = true;
        Ok(())
    }

    /// Positions and synchronizes a clean partial pair, transferring the same handles.
    ///
    /// Validation and required repairs must have completed. Rejects full files;
    /// use `into_completed` for those. No reader buffer or physical read-ahead
    /// cursor is transferred: both file positions are set to verified append ends.
    /// Failure/cancellation consumes this owner without granting a writable result.
    pub async fn into_writer(mut self) -> Result<IndexedLogWriter<File>, Error> {
        self.check_handover()?;
        let report = self.report.as_ref().unwrap();
        if report.is_full() {
            return Err(Error::Full);
        }
        let end = report.end();
        self.log
            .seek(SeekFrom::Start(self.log_length))
            .await
            .map_err(Error::LogIo)?;
        self.index
            .seek(SeekFrom::Start(self.index_length))
            .await
            .map_err(Error::IndexIo)?;
        self.check_lengths().await?;
        let mut writer = IndexedLogWriter::from_validated(self.file_id, self.log, self.index, end);
        writer.sync_data().await?;
        Ok(writer)
    }

    /// Synchronizes a clean full pair, closes its handles, and returns completion metadata.
    ///
    /// Exactly 100,000 accepted records and a repaired index are required. This
    /// provides the full-file handover without constructing an unusable append
    /// writer, incrementing its last record ID or allocating another record buffer.
    /// The owner still publishes stream/checkpoint state and acquires the next file.
    pub async fn into_completed(mut self) -> Result<CompletedIndexedLog, Error> {
        self.check_handover()?;
        let report = self.report.as_ref().unwrap();
        if !report.is_full() {
            return Err(Error::NotFull);
        }
        let end = report.end().expect("a full file has a last record");
        self.check_lengths().await?;
        self.log.flush().await.map_err(Error::LogIo)?;
        self.log.sync_data().await.map_err(Error::LogIo)?;
        self.index.flush().await.map_err(Error::IndexIo)?;
        self.index.sync_data().await.map_err(Error::IndexIo)?;
        drop(self);
        Ok(CompletedIndexedLog::new(end))
    }

    fn check_usable(&self) -> Result<(), Error> {
        if self.usable {
            Ok(())
        } else {
            Err(Error::Unusable)
        }
    }

    fn validated_report(&self) -> Result<&LogValidationReport, Error> {
        self.check_usable()?;
        self.report.as_ref().ok_or(Error::NotValidated)
    }

    fn check_handover(&self) -> Result<(), Error> {
        self.validated_report()?;
        if !self.log_ready {
            return Err(Error::LogRepairRequired);
        }
        if !self.index_ready {
            return Err(Error::IndexRepairRequired);
        }
        Ok(())
    }

    async fn check_lengths(&self) -> Result<(), Error> {
        if self.log.metadata().await.map_err(Error::LogIo)?.len() != self.log_length
            || self.index.metadata().await.map_err(Error::IndexIo)?.len() != self.index_length
        {
            return Err(Error::FilesChanged);
        }
        Ok(())
    }
}

// RecordReader yields the valid prefix before reporting corrupt input. Apply
// sequence/file policy to each record before the next wait can report a later
// encoding error, preserving the earliest invalid boundary in one forward scan.
async fn scan_records<R: AsyncRead + Unpin>(
    source: R,
    file_id: LogFileId,
    trusted_records: u64,
    start_position: u64,
    ends: &mut Vec<u64>,
) -> Result<Option<LogTailError>, Error> {
    let mut reader = RecordReader::new(source);
    loop {
        if trusted_records + ends.len() as u64 == RECORDS_PER_FILE {
            return Ok(None);
        }
        match reader.wait_to_read().await {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error @ (RecordReadError::Io(_) | RecordReadError::ReaderFailed)) => {
                return Err(Error::Read(error));
            }
            Err(error) => return Ok(Some(LogTailError::Record(error))),
        }
        while let Some(record) = reader.try_read_next().map_err(Error::Read)? {
            let count = trusted_records + ends.len() as u64;
            if count == RECORDS_PER_FILE {
                return Ok(None);
            }
            let Some(sequence) = file_id
                .first_record_id()
                .sequence_number()
                .get()
                .checked_add(count)
            else {
                return Ok(Some(LogTailError::ExtraData));
            };
            let expected = RecordId::new(file_id.stream_id(), SequenceNumber::new(sequence));
            if record.id() != expected {
                return Ok(Some(LogTailError::UnexpectedRecordId {
                    expected,
                    actual: record.id(),
                }));
            }
            ends.push(ends.last().copied().unwrap_or(start_position) + u64::from(record.length()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use std::{
        future::Future,
        io,
        pin::Pin,
        task::{Context, Poll, Waker},
    };
    use tokio::io::ReadBuf;
    use transaction_log_exports::{Record, RecordWriter, StreamId};

    const INDEX: [u8; 24] = [
        16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0, 52, 0, 0, 0, 0, 0, 0, 0,
    ];

    fn id(stream: u16, sequence: u64) -> RecordId {
        RecordId::new(
            StreamId::new(stream).unwrap(),
            SequenceNumber::new(sequence),
        )
    }

    async fn encoded(stream: u16, first: u64, payload_lengths: &[usize]) -> Vec<u8> {
        let mut writer = RecordWriter::for_serialization(Vec::new());
        for (offset, &length) in payload_lengths.iter().enumerate() {
            writer
                .write(id(stream, first + offset as u64), |body| {
                    std::io::Write::write_all(body, &vec![0xa5; length])
                })
                .unwrap();
        }
        writer.flush_buffer().await.unwrap();
        writer.into_inner()
    }

    async fn record(stream: u16, sequence: u64) -> Record {
        let bytes = encoded(stream, sequence, &[0]).await;
        let mut reader = RecordReader::new(bytes.as_slice());
        assert!(reader.wait_to_read().await.unwrap());
        reader.try_read_next().unwrap().unwrap()
    }

    async fn pair(
        file_id: LogFileId,
        log: &[u8],
        index: Option<&[u8]>,
    ) -> (tempfile::TempDir, StorageProvider) {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            StorageProvider::new(StorageConfig::new(directory.path().to_owned()).unwrap());
        let log_path = provider.log_file_path(file_id);
        tokio::fs::create_dir_all(log_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(log_path, log).await.unwrap();
        if let Some(index) = index {
            tokio::fs::write(provider.index_file_path(file_id), index)
                .await
                .unwrap();
        }
        (directory, provider)
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[tokio::test]
    async fn missing_log_does_not_create_index_and_empty_pair_becomes_a_writer() {
        let directory = tempfile::tempdir().unwrap();
        let provider =
            StorageProvider::new(StorageConfig::new(directory.path().to_owned()).unwrap());
        let file_id = LogFileId::from_record_id(id(42, 100_000));
        let Err(Error::Open(error)) = IndexedLogValidator::open(&provider, file_id, None).await
        else {
            panic!("missing log must fail")
        };
        assert_eq!(error.path(), provider.log_file_path(file_id));
        assert!(std::error::Error::source(&error).is_some());
        assert!(!provider.index_file_path(file_id).exists());
        let (_directory, provider) = pair(file_id, &[], None).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        assert!(matches!(
            validator.repair_index().await,
            Err(Error::NotValidated)
        ));
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::NotValidated)
        ));
        let report = validator.validate().await.unwrap();
        assert_eq!(report.file_id(), file_id);
        assert_eq!(report.end(), None);
        assert_eq!(report.record_count(), 0);
        assert!(!report.needs_index_repair());
        assert!(report.tail_error().is_none());
        validator.repair_index().await.unwrap();
        let mut writer = validator.into_writer().await.unwrap();
        writer.write_record(&record(42, 100_000).await).unwrap();
        writer.sync_data().await.unwrap();
        assert_eq!(
            writer.synced_end(),
            Some(RecordEndLocation::new(id(42, 100_000), 16))
        );
        assert_eq!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap(),
            INDEX[..8]
        );
    }

    #[tokio::test]
    async fn repairs_missing_partial_incorrect_and_excess_indexes_without_changing_log_bytes() {
        let file_id = LogFileId::from_record_id(id(42, 100_000));
        let log = encoded(42, 100_000, &[0, 3, 1]).await;
        let mut cases: Vec<(Option<Vec<u8>>, u64)> = (0..=24)
            .map(|length| (Some(INDEX[..length].to_vec()), (length / 8) as u64))
            .collect();
        cases.push((None, 0));
        let mut extra = INDEX.to_vec();
        extra.extend_from_slice(&[255; 11]);
        cases.push((Some(extra), 3));
        for entry in 0..3 {
            let mut wrong = INDEX.to_vec();
            wrong[entry * 8] ^= 1;
            cases.push((Some(wrong), entry as u64));
        }
        for (original, matching) in cases {
            let (_directory, provider) = pair(file_id, &log, original.as_deref()).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, None)
                .await
                .unwrap();
            let report = validator.validate().await.unwrap();
            assert_eq!(
                report.end(),
                Some(RecordEndLocation::new(id(42, 100_002), 52))
            );
            assert_eq!(report.record_count(), 3);
            assert_eq!(report.matching_index_entries(), matching);
            assert!(report.tail_error().is_none());
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                original.clone().unwrap_or_default()
            );
            validator.repair_index().await.unwrap();
            validator.repair_index().await.unwrap(); // Completed repair is idempotent.
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                INDEX
            );
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                log
            );
            assert_eq!(
                validator.report().unwrap().index_length(),
                original.map_or(0, |bytes| bytes.len() as u64)
            );
            let mut writer = validator.into_writer().await.unwrap();
            assert_eq!(writer.synced_end(), writer.buffered_end());
            writer.write_record(&record(42, 100_003).await).unwrap();
            writer.sync_data().await.unwrap();
            let output = tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap();
            assert_eq!(&output[..52], log);
            assert_eq!(output.len(), 68);
            assert_eq!(
                &tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap()[24..],
                &[68, 0, 0, 0, 0, 0, 0, 0]
            );
        }
    }

    #[tokio::test]
    async fn supplied_boundary_skips_trusted_records_and_repairs_only_its_index_suffix() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let mut log = encoded(7, 0, &[0, 3, 1]).await;
        // Deliberately bad CRC in the supplied trusted prefix: this proves the
        // caller's boundary is honored, not recomputed by rescanning old records.
        log[12] ^= 1;
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX[..16])).await;
        let start = RecordEndLocation::new(id(7, 1), 35);
        let mut validator = IndexedLogValidator::open(&provider, file_id, Some(start))
            .await
            .unwrap();
        let report = validator.validate().await.unwrap();
        assert_eq!(report.record_count(), 3);
        assert_eq!(report.matching_index_entries(), 2);
        assert_eq!(report.end(), Some(RecordEndLocation::new(id(7, 2), 52)));
        assert!(report.tail_error().is_none());
        assert_eq!(validator.ends, [52]);
        validator.repair_index().await.unwrap();
        assert_eq!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap(),
            INDEX
        );
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );
    }

    #[tokio::test]
    async fn invalid_start_and_unusable_trusted_index_never_authorize_repairs() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let log = encoded(7, 0, &[0, 3, 1]).await;
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX)).await;
        for wrong_id in [id(8, 0), id(7, 100_000)] {
            assert!(matches!(
                IndexedLogValidator::open(
                    &provider,
                    file_id,
                    Some(RecordEndLocation::new(wrong_id, 16))
                )
                .await,
                Err(Error::InvalidStart(_))
            ));
        }
        for position in [0, 15, 53, 65_536] {
            let mut validator = IndexedLogValidator::open(
                &provider,
                file_id,
                Some(RecordEndLocation::new(id(7, 0), position)),
            )
            .await
            .unwrap();
            assert!(matches!(
                validator.validate().await,
                Err(Error::InvalidStart(_))
            ));
            assert!(matches!(
                validator.repair_index().await,
                Err(Error::Unusable)
            ));
            assert!(matches!(
                validator.truncate_invalid_tail().await,
                Err(Error::Unusable)
            ));
        }
        for index in [
            None,
            Some(INDEX[..15].to_vec()),
            Some([16, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 0, 0, 0, 0].to_vec()),
            Some(vec![255; 16]),
        ] {
            let (_directory, provider) = pair(file_id, &log, index.as_deref()).await;
            let mut validator = IndexedLogValidator::open(
                &provider,
                file_id,
                Some(RecordEndLocation::new(id(7, 1), 35)),
            )
            .await
            .unwrap();
            assert!(matches!(
                validator.validate().await,
                Err(Error::TrustedIndexUnavailable)
            ));
            assert!(validator.report().is_none());
            assert!(matches!(
                validator.truncate_invalid_tail().await,
                Err(Error::Unusable)
            ));
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                log
            );
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                index.unwrap_or_default()
            );
        }
    }

    #[tokio::test]
    async fn corrupt_or_truncated_batch_tail_preserves_every_preceding_complete_record() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let log = encoded(7, 0, &[0, 3, 1]).await;
        let mut cases: Vec<Vec<u8>> = (36..52).map(|length| log[..length].to_vec()).collect();
        for change in [0, 2, 16] {
            let mut corrupt = log.clone();
            match change {
                0 => corrupt[35..37].copy_from_slice(&0u16.to_le_bytes()),
                2 => corrupt[37..39].copy_from_slice(&4096u16.to_le_bytes()),
                _ => corrupt[51] ^= 1,
            }
            // Even valid later records cannot be recovered across a corrupt gap.
            corrupt.extend_from_slice(&encoded(7, 3, &[0]).await);
            cases.push(corrupt);
        }
        for corrupt in cases {
            let (_directory, provider) = pair(file_id, &corrupt, Some(&INDEX)).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, None)
                .await
                .unwrap();
            let report = validator.validate().await.unwrap();
            assert_eq!(report.end(), Some(RecordEndLocation::new(id(7, 1), 35)));
            assert_eq!(report.record_count(), 2);
            assert!(matches!(report.tail_error(), Some(LogTailError::Record(_))));
            assert!(matches!(
                validator.check_handover(),
                Err(Error::LogRepairRequired)
            ));
            validator.repair_index().await.unwrap();
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                corrupt
            );
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                INDEX[..16]
            );
            validator.truncate_invalid_tail().await.unwrap();
            validator.truncate_invalid_tail().await.unwrap();
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                log[..35]
            );
            assert_eq!(
                validator.report().unwrap().log_length(),
                corrupt.len() as u64
            );
            assert!(validator.report().unwrap().tail_error().is_some());
            let mut writer = validator.into_writer().await.unwrap();
            writer.write_record(&record(7, 2).await).unwrap();
            writer.sync_data().await.unwrap();
            assert_eq!(
                writer.synced_end(),
                Some(RecordEndLocation::new(id(7, 2), 51))
            );
        }
    }

    #[tokio::test]
    async fn sequence_or_stream_errors_win_over_later_encoding_corruption() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        for actual in [id(8, 1), id(7, 0), id(7, 2)] {
            let mut log = encoded(7, 0, &[0]).await;
            log.extend_from_slice(
                &encoded(
                    actual.stream_id().get(),
                    actual.sequence_number().get(),
                    &[0],
                )
                .await,
            );
            log.extend_from_slice(&encoded(7, 2, &[0]).await);
            *log.last_mut().unwrap() ^= 1;
            let (_directory, provider) = pair(file_id, &log, None).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, None)
                .await
                .unwrap();
            let report = validator.validate().await.unwrap();
            assert_eq!(report.record_count(), 1);
            assert_eq!(report.valid_log_length(), 16);
            assert!(
                matches!(report.tail_error(), Some(LogTailError::UnexpectedRecordId { expected, actual: found })
                if *expected == id(7, 1) && *found == actual)
            );
            // Explicit log repair includes any necessary index repair first.
            validator.truncate_invalid_tail().await.unwrap();
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                INDEX[..8]
            );
        }
    }

    #[tokio::test]
    async fn full_files_repair_and_handover_as_completion_without_an_append_writer() {
        let file_id = LogFileId::from_record_id(id(9, 0));
        let mut log = encoded(9, 0, &vec![0; 100_000]).await;
        log.extend_from_slice(&[0, 1, 2]);
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX[..8])).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        let report = validator.validate().await.unwrap();
        assert!(report.is_full());
        assert!(matches!(report.tail_error(), Some(LogTailError::ExtraData)));
        assert_eq!(
            report.end(),
            Some(RecordEndLocation::new(id(9, 99_999), 1_600_000))
        );
        validator.truncate_invalid_tail().await.unwrap();
        let completed = validator.into_completed().await.unwrap();
        assert_eq!(completed.file_id(), file_id);
        assert_eq!(
            completed.end(),
            RecordEndLocation::new(id(9, 99_999), 1_600_000)
        );
        let index = tokio::fs::read(provider.index_file_path(file_id))
            .await
            .unwrap();
        assert_eq!(index.len(), 800_000);
        assert_eq!(&index[799_992..], &[0, 106, 24, 0, 0, 0, 0, 0]);
        for (ordinal, entry) in index.as_chunks::<8>().0.iter().enumerate() {
            assert_eq!(u64::from_le_bytes(*entry), (ordinal as u64 + 1) * 16);
        }
        assert_eq!(
            tokio::fs::metadata(provider.log_file_path(file_id))
                .await
                .unwrap()
                .len(),
            1_600_000
        );

        // A supplied full-file boundary needs no successor or log scan.
        let mut validator = IndexedLogValidator::open(&provider, file_id, Some(completed.end()))
            .await
            .unwrap();
        assert!(validator.validate().await.unwrap().is_full());
        assert!(validator.ends.is_empty());
        assert!(matches!(validator.into_writer().await, Err(Error::Full)));
        let mut validator = IndexedLogValidator::open(&provider, file_id, Some(completed.end()))
            .await
            .unwrap();
        validator.validate().await.unwrap();
        assert_eq!(validator.into_completed().await.unwrap(), completed);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // A sharing-denied open succeeds only after recovery handles close.
            let _log = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(provider.log_file_path(file_id))
                .unwrap();
            let _index = std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(provider.index_file_path(file_id))
                .unwrap();
        }
    }

    #[tokio::test]
    async fn handover_requires_validation_index_repair_and_correct_fullness() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let log = encoded(7, 0, &[0]).await;
        let (_directory, provider) = pair(file_id, &log, None).await;
        let validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        assert!(matches!(
            validator.into_writer().await,
            Err(Error::NotValidated)
        ));
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        assert!(matches!(
            validator.into_writer().await,
            Err(Error::IndexRepairRequired)
        ));
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        validator.repair_index().await.unwrap();
        assert!(matches!(
            validator.into_completed().await,
            Err(Error::NotFull)
        ));
    }

    #[tokio::test]
    async fn io_failures_and_external_length_changes_never_authorize_log_truncation() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let log = encoded(7, 0, &[0]).await;
        let (_directory, provider) = pair(file_id, &log, None).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.log = tokio::fs::OpenOptions::new()
            .write(true)
            .open(provider.log_file_path(file_id))
            .await
            .unwrap();
        assert!(matches!(
            validator.validate().await,
            Err(Error::Read(RecordReadError::Io(_)))
        ));
        assert!(validator.report().is_none());
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::Unusable)
        ));

        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        validator.index = File::open(provider.index_file_path(file_id)).await.unwrap();
        assert!(matches!(
            validator.repair_index().await,
            Err(Error::IndexIo(_)) | Err(Error::IndexWrite(_))
        ));
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::Unusable)
        ));
        assert!(validator.report().unwrap().tail_error().is_none());
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );

        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        tokio::fs::write(provider.index_file_path(file_id), [1, 2, 3])
            .await
            .unwrap();
        assert!(matches!(
            validator.repair_index().await,
            Err(Error::FilesChanged)
        ));
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::Unusable)
        ));
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );
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
    async fn read_error_after_good_records_is_operational_not_a_corrupt_tail() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let bytes = encoded(7, 0, &[0, 3]).await;
        let mut ends = Vec::new();
        let source = std::io::Cursor::new(bytes).chain(ReadFailure);
        let result = scan_records(source, file_id, 0, 0, &mut ends).await;
        assert!(
            matches!(result, Err(Error::Read(RecordReadError::Io(error))) if error.raw_os_error() == Some(123))
        );
        assert_eq!(ends, [16, 35]);
    }

    #[tokio::test]
    async fn maximum_records_cross_batches_and_preserve_the_exact_corruption_boundary() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let mut bytes = encoded(7, 0, &[65_519, 65_519, 65_519]).await;
        *bytes.last_mut().unwrap() ^= 1;
        let (_directory, provider) = pair(file_id, &bytes, None).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        let report = validator.validate().await.unwrap();
        assert_eq!(
            report.end(),
            Some(RecordEndLocation::new(id(7, 1), 131_070))
        );
        assert_eq!(report.record_count(), 2);
        assert!(matches!(
            report.tail_error(),
            Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
        ));
        validator.truncate_invalid_tail().await.unwrap();
        assert_eq!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap(),
            [255, 255, 0, 0, 0, 0, 0, 0, 254, 255, 1, 0, 0, 0, 0, 0]
        );
        let writer = validator.into_writer().await.unwrap();
        assert_eq!(
            writer.synced_end(),
            Some(RecordEndLocation::new(id(7, 1), 131_070))
        );
    }

    #[tokio::test]
    async fn an_empty_log_removes_stale_index_entries_without_creating_a_sentinel() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let (_directory, provider) = pair(file_id, &[], Some(&INDEX)).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        let report = validator.validate().await.unwrap();
        assert_eq!(report.end(), None);
        assert!(report.needs_index_repair());
        validator.repair_index().await.unwrap();
        assert!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap()
                .is_empty()
        );
        let writer = validator.into_writer().await.unwrap();
        assert_eq!(writer.synced_end(), None);
        assert_eq!(writer.record_count(), 0);
    }

    #[tokio::test]
    async fn corruption_before_the_first_record_requires_explicit_repair_to_an_empty_pair() {
        let file_id = LogFileId::from_record_id(id(7, 100_000));
        let mut log = vec![0; 12];
        log.extend_from_slice(&encoded(7, 100_000, &[0]).await);
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX)).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        let report = validator.validate().await.unwrap();
        assert_eq!(report.end(), None);
        assert_eq!(report.record_count(), 0);
        assert!(matches!(
            report.tail_error(),
            Some(LogTailError::Record(RecordReadError::InvalidLength {
                declared: 0
            }))
        ));
        assert!(matches!(
            validator.check_handover(),
            Err(Error::LogRepairRequired)
        ));
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );
        validator.truncate_invalid_tail().await.unwrap();
        assert!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap()
                .is_empty()
        );
        let mut writer = validator.into_writer().await.unwrap();
        writer.write_record(&record(7, 100_000).await).unwrap();
        writer.sync_data().await.unwrap();
        assert_eq!(
            writer.synced_end(),
            Some(RecordEndLocation::new(id(7, 100_000), 16))
        );
    }

    #[test]
    fn cancelled_validation_and_repairs_are_terminal_but_unpolled_futures_are_inert() {
        // Occupy the sole blocking worker so the first metadata operation is
        // deterministically Pending. No timing/sleep assumption about disk speed.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let file_id = LogFileId::from_record_id(id(7, 0));
            let mut bytes = encoded(7, 0, &[0, 3]).await;
            *bytes.last_mut().unwrap() ^= 1;
            for operation in 0..3 {
                let index = if operation == 1 { &[][..] } else { &INDEX[..8] };
                let (_directory, provider) = pair(file_id, &bytes, Some(index)).await;
                let mut validator = IndexedLogValidator::open(&provider, file_id, None)
                    .await
                    .unwrap();
                drop(validator.validate());
                assert!(validator.usable);
                assert!(validator.report().is_none());
                if operation != 0 {
                    validator.validate().await.unwrap();
                }
                drop(validator.repair_index());
                drop(validator.truncate_invalid_tail());
                assert!(validator.usable);
                let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
                let (release_sender, release_receiver) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    ready_sender.send(()).unwrap();
                    // A timeout prevents a failed test from hanging runtime teardown.
                    let _ = release_receiver.recv_timeout(std::time::Duration::from_secs(5));
                });
                ready_receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                let mut future = Box::pin(async {
                    match operation {
                        0 => validator.validate().await.map(|_| ()),
                        1 => validator.repair_index().await,
                        _ => validator.truncate_invalid_tail().await,
                    }
                });
                let result = poll_once(future.as_mut());
                drop(future);
                release_sender.send(()).unwrap();
                blocker.await.unwrap();
                assert!(result.is_pending());
                assert!(!validator.usable);
                assert!(matches!(validator.validate().await, Err(Error::Unusable)));
                assert!(matches!(
                    validator.repair_index().await,
                    Err(Error::Unusable)
                ));
                assert!(matches!(
                    validator.truncate_invalid_tail().await,
                    Err(Error::Unusable)
                ));
                let (mut log, mut index) = validator.into_inner();
                log.flush().await.unwrap();
                index.flush().await.unwrap();
                assert_eq!(
                    tokio::fs::read(provider.log_file_path(file_id))
                        .await
                        .unwrap(),
                    bytes
                );
            }
        });
    }
}

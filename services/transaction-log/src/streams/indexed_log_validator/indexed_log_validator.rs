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
    /// Identity of this owned log/index pair, fixed for the validator's lifetime.
    ///
    /// Selects both provider paths and supplies the stream and assigned sequence
    /// range checked during scanning. `open` requires any supplied `start` to
    /// belong to this file. Neither repair nor handover advances to another file.
    file_id: LogFileId,

    /// Owned read/write handle to the existing record-data file.
    ///
    /// Opened without creation, append mode or truncation. The caller excludes
    /// other writers; owning this handle does not acquire an OS-level lock.
    /// Validation may read ahead beyond the last accepted record, so the physical
    /// cursor is not a recovery boundary. `into_writer` seeks to the verified
    /// append end; `into_completed` closes the handle after synchronization.
    /// Only explicit `truncate_invalid_tail` removes bytes from this file.
    log: File,

    /// Owned read/write handle to the paired dense index file.
    ///
    /// A missing index is created empty; existing bytes are preserved on open.
    /// Each eight-byte entry stores one record's absolute exclusive log end.
    /// Validation reads the index independently of the log cursor. Repair seeks
    /// to the first mismatching entry and rewrites/truncates that suffix through
    /// `IndexWriter`; writer handover seeks to the verified index append end.
    /// The caller must exclude other writers and withhold recovery from queries.
    index: File,

    /// Caller-certified last record and exclusive byte end of the trusted prefix.
    ///
    /// `None` means no trusted records: scan from byte zero and the file's first
    /// assigned ID. `Some` certifies both the log and its matching index prefix.
    /// Validation checks presence and the final index entry, without revalidating
    /// earlier log records or index entries. A checkpoint owner must establish
    /// both prefixes' correctness and durability before publishing that checkpoint.
    /// This boundary never advances during scanning or repair. New findings live
    /// in `ends` and `report`; loading or publishing checkpoints belongs upstream.
    start: Option<RecordEndLocation>,

    /// Number of records covered by `start`, including its last record.
    ///
    /// Computed once in `open` as the start sequence minus this file's first
    /// sequence plus one, or zero when `start` is absent. It counts records in
    /// this file, not the stream's lifetime sequence number. It determines the
    /// trusted index-prefix size and the next expected sequence. Scanning leaves
    /// it unchanged; total accepted records are `trusted_records + ends.len()`.
    trusted_records: u64,

    /// Absolute exclusive log ends of newly accepted records after `start`.
    ///
    /// Entry `i` belongs to file-local record index `trusted_records + i`; these
    /// are native `u64` byte positions, not encoded entries or relative lengths.
    /// An end is appended only after framing, stream, sequence and CRC checks
    /// pass. The trusted prefix has no entries here, and record bodies are not
    /// retained. Repair uses these offsets to rebuild the untrusted index suffix.
    /// At most `RECORDS_PER_FILE - trusted_records` entries are used (800,000
    /// bytes when scanning a whole file); Vec growth may reserve extra capacity.
    /// An interrupted scan can leave partial offsets, but without a usable,
    /// completed report they grant no repair or handover authority.
    ends: Vec<u64>,

    /// Immutable findings from the first successfully completed validation pass.
    ///
    /// `None` until the scan, index comparison and final length checks succeed.
    /// A reported content error still counts as completed validation: the report
    /// identifies the valid prefix and invalid tail for an explicit repair choice.
    /// It preserves original file lengths and diagnostics after repairs and even
    /// after a later operation fails. `report()` permits inspection in that case;
    /// repair and handover additionally require `usable`. Repeated successful
    /// `validate` calls return this snapshot rather than inspecting files again.
    report: Option<LogValidationReport>,

    /// Expected current physical log length in bytes, including any invalid tail.
    ///
    /// Starts at zero as a placeholder; `validate` loads the actual metadata.
    /// Bounds the source scan and detects length changes between recovery phases.
    /// Successful explicit truncation updates it to the report's valid log end;
    /// the report retains the original length. It is neither a cursor nor a
    /// durable/validated boundary by itself. Failed I/O may change the real file
    /// without updating this field, which is why `usable` must gate further work.
    log_length: u64,

    /// Expected current physical index length in bytes, including partial/excess entries.
    ///
    /// Starts at zero as a placeholder and is populated from metadata during
    /// validation. It bounds index input and is checked for external length
    /// changes. After successful index repair it equals the report's accepted
    /// record count times `INDEX_ENTRY_LEN`; the original length stays in `report`.
    /// It is not an entry count or independent proof of index validity. As with
    /// `log_length`, interrupted I/O can make it stale on an unusable instance.
    index_length: u64,

    /// Permission to continue this validator's in-place recovery operations.
    ///
    /// Starts true, allowing validation but not yet repair or handover. Validation
    /// and repairs set it false before their I/O and restore it only on complete
    /// success. Errors, unwinding or cancellation therefore leave the instance
    /// terminal, even if `report` or readiness flags still describe earlier success.
    /// Content corruption returned inside a successful report is not an operation
    /// failure. Raw-handle extraction and report inspection remain available when
    /// false; consuming handovers cannot return the validator after failed I/O.
    usable: bool,

    /// Whether the index exactly describes the report's entire accepted prefix.
    ///
    /// Initially false because the index has not been inspected. Successful
    /// validation sets it when the suffix matches the scanned records, the trusted
    /// endpoint agrees and no missing, partial or extra bytes remain. Earlier
    /// entries retain the checkpoint's certification. Successful `repair_index`
    /// also sets it after flushing output.
    /// It may be true while the log still has an invalid tail (`log_ready` false).
    /// This is repair readiness, not durable sync or permission to serve queries.
    /// Handover requires `usable`, a report and both readiness flags together.
    index_ready: bool,

    /// Whether the log ends exactly at the report's accepted record boundary.
    ///
    /// Initially false until validation establishes that no invalid/extra tail
    /// remains. If validation reports a tail, only successful explicit truncation
    /// sets this flag. A clean log can be ready while its index still needs repair.
    /// Truncation leaves the original report's tail error intact, so this flag
    /// tracks current readiness separately. It does not establish durability;
    /// handover synchronizes the pair after checking both flags and `usable`.
    log_ready: bool,
}

impl IndexedLogValidator {
    /// Opens the provider's existing log and paired index for validation/repair.
    ///
    /// `start` is the last record already trusted in this file, not the next one
    /// to inspect. `None` means offset zero and the file's first assigned ID. A
    /// full-file endpoint is valid. The caller certifies correct record and index
    /// prefixes; presence and endpoint checks do not establish that trust.
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
        // Count the supplied endpoint inclusively within this file. None means
        // zero trusted records; it never enters the closure or adds one.
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
    /// The report remains owned by this validator. Repeating a successful call
    /// returns it without rescanning, provided no later operation made the
    /// validator unusable. Repair does not rewrite these original findings.
    ///
    /// An invalid supplied boundary also returns `Err`. Failure or cancellation
    /// after validation begins leaves the instance unusable for repair or handover.
    /// A completed report may describe corruption and still permit explicit repair;
    /// validation itself neither repairs files nor durably synchronizes them.
    ///
    /// Scanning uses RecordReader's batching in a single forward pass. The reader
    /// yields the valid prefix before reporting corrupt input, so every preceding
    /// record receives its sequence check and end offset without rereading the file.
    pub async fn validate(&mut self) -> Result<&LogValidationReport, Error> {
        if self.report.is_some() {
            // A cached report cannot bypass an intervening repair failure:
            // validated_report also checks that the instance is still usable.
            return self.validated_report();
        }
        self.check_usable()?;
        // Set this before the first await. Early errors, panic or cancellation
        // must leave partial findings unable to authorize repair or handover.
        // Only a completed validation restores usability, even if it found corruption.
        self.usable = false;
        self.log_length = self.log.metadata().await.map_err(Error::LogIo)?.len();
        self.index_length = self.index.metadata().await.map_err(Error::IndexIo)?.len();
        let start_position = self.start.map_or(0, |end| end.position());
        // Check only plausibility, not the caller-certified records themselves.
        // No trusted prefix means count and position are both zero, even at EOF.
        if start_position > self.log_length
            || start_position < self.trusted_records * MIN_RECORD_LEN as u64
            || start_position > self.trusted_records * MAX_RECORD_LEN as u64
        {
            return Err(Error::InvalidStart(
                "position is outside the possible trusted record extent",
            ));
        }

        // Load the bounded index once for its checkpoint endpoint and suffix.
        // Cap input at a full file's index size; retain the actual index_length
        // so excess entries or bytes still require repair without being loaded.
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
            // EOF arrived before the extent observed in metadata. This is not
            // a stable input from which to authorize index repair.
            return Err(Error::FilesChanged);
        }
        let mut index_ends = index_bytes
            .as_chunks::<INDEX_ENTRY_LEN>()
            .0
            .iter()
            .map(|entry| u64::from_le_bytes(*entry));
        // The checkpoint certifies earlier entries; do not revalidate them.
        // nth checks that its final complete entry exists and agrees with the
        // endpoint, leaving the iterator at the suffix. With no checkpoint,
        // consume nothing: the first entry belongs to the first scanned record.
        if self.trusted_records != 0
            && index_ends.nth(self.trusted_records as usize - 1) != Some(start_position)
        {
            return Err(Error::TrustedIndexUnavailable);
        }

        self.log
            .seek(SeekFrom::Start(start_position))
            .await
            .map_err(Error::LogIo)?;
        // Scan only the suffix within the observed file extent. This bounds input,
        // not concurrent modification; the owner must still exclude other writers.
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
        // Build one accepted endpoint for both the extent check and the report.
        // Read-ahead can advance the handle beyond this logical record boundary.
        let end = if let Some(&position) = self.ends.last() {
            // A new accepted record proves count > 0 and a representable sequence.
            let sequence =
                self.file_id.first_record_id().sequence_number().get() + (record_count - 1);
            Some(RecordEndLocation::new(
                RecordId::new(self.file_id.stream_id(), SequenceNumber::new(sequence)),
                position,
            ))
        } else {
            // No new accepted records: preserve the checkpoint, including None.
            self.start
        };
        let valid_length = end.map_or(0, |end| end.position());
        // The scanner also returns no tail error when it reaches the record cap,
        // without requiring EOF. Compare the logical accepted end with the known
        // file extent: even one extra byte after a full file is an invalid tail.
        if tail_error.is_none() && valid_length < self.log_length {
            if record_count == RECORDS_PER_FILE {
                tail_error = Some(LogTailError::ExtraData);
            } else {
                // Below the record cap, a clean scan must reach the observed EOF.
                // An earlier clean EOF is a file change, not truncation authority.
                return Err(Error::FilesChanged);
            }
        }
        // Preserve only the consecutive matching suffix entries after the trusted
        // prefix. Stop at the first mismatch or missing complete entry, even if
        // later entries match. The report also checks total byte length, catching
        // extra entries and partial trailing entries omitted by this iterator.
        let matching_suffix = self
            .ends
            .iter()
            .copied()
            .zip(index_ends)
            .take_while(|(expected, stored)| expected == stored)
            .count();
        // Recheck lengths before publishing findings. This detects size changes,
        // not same-length overwrites, and does not replace exclusive write ownership.
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
        // These independent flags describe repair needs, not durability. The index
        // must match every accepted record and have exactly the corresponding byte
        // length; the log must have no invalid tail. Handover supplies synchronization.
        self.index_ready = !report.needs_index_repair();
        self.log_ready = report.tail_error().is_none();
        // Publish complete original findings before enabling follow-up operations.
        // No await separates publication, restoring usability and returning the borrow.
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

/// Scans the untrusted suffix, collecting ends of consecutive accepted records.
///
/// The caller positions and limits `source` to the observed untrusted file suffix.
/// `file_id` supplies the required stream and first assigned sequence number.
/// `trusted_records` counts the caller-certified prefix within this file (at most
/// `RECORDS_PER_FILE`), and `start_position` is its absolute exclusive byte end.
/// With no trusted prefix, both are zero. `ends` starts empty and receives only
/// newly accepted absolute exclusive ends; trusted records are not inserted.
///
/// # Outcomes and caller responsibilities
///
/// - `Ok(None)` means clean source EOF OR the total accepted count reached
///   `RECORDS_PER_FILE`. It does not prove EOF or absence of trailing file bytes.
/// - `Ok(Some(error))` reports the first content violation. `ends` retains every
///   preceding accepted record, excluding the offending record and all later data.
/// - `Err(error)` is an operational failure, not permission to truncate. Partial
///   offsets may remain in `ends`, but do not establish a completed validation.
///
/// The caller knows the physical file length and must compare it with the last
/// accepted end (or `start_position` if none were added). Bytes beyond a full
/// 100,000-record prefix become `LogTailError::ExtraData` in `validate`, including
/// partial headers or arbitrary bytes. No EOF probe is needed here after the cap.
/// A clean EOF before the observed extent of a partial file is instead an external
/// file-change error. Reader read-ahead can move the source cursor past the accepted
/// end; the collected offsets, not that cursor, define the recovery boundary.
///
/// RecordReader yields the valid prefix before reporting corrupt input. Apply
/// sequence/file policy to each record before the next wait can report a later
/// encoding error, preserving the earliest invalid boundary in one forward scan.
async fn scan_records<R: AsyncRead + Unpin>(
    source: R,
    file_id: LogFileId,
    trusted_records: u64,
    start_position: u64,
    ends: &mut Vec<u64>,
) -> Result<Option<LogTailError>, Error> {
    let mut reader = RecordReader::new(source);
    loop {
        // A full prefix is sufficient, including one supplied entirely as trusted.
        // The caller checks for trailing bytes using the observed file length.
        if trusted_records + ends.len() as u64 == RECORDS_PER_FILE {
            return Ok(None);
        }
        // A content error identifies a repairable tail after all previously
        // yielded records. An I/O/reader-state error cannot certify that boundary,
        // even if we have collected some offsets, so it fails validation itself.
        // Keep this match exhaustive: do not replace either error group with a
        // catch-all. Adding a RecordReadError variant must fail compilation until
        // its recovery policy is explicitly chosen. A catch-all could silently
        // classify a new operational failure as corrupt content, allowing valid
        // file data to be truncated during subsequent repair.
        match reader.wait_to_read().await {
            Ok(false) => return Ok(None),
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
            ) => return Ok(Some(LogTailError::Record(error))),
        }
        while let Some(record) = reader.try_read_next().map_err(Error::Read)? {
            let count = trusted_records + ends.len() as u64;
            // `count` excludes this just-yielded record. Once the preceding
            // iteration accepted the last assigned record, do not accept another.
            if count == RECORDS_PER_FILE {
                return Ok(None);
            }
            // The accepted count is this record's zero-based position in the
            // file. Derive its sequence from that count rather than maintaining
            // another cursor. Checked addition reports exhaustion as extra data;
            // advancing a local RecordId with next() would panic on overflow.
            let Some(sequence) = file_id
                .first_record_id()
                .sequence_number()
                .get()
                .checked_add(count)
            else {
                return Ok(Some(LogTailError::ExtraData));
            };
            let expected = RecordId::new(file_id.stream_id(), SequenceNumber::new(sequence));
            // RecordReader proves encoding/CRC and the stream-ID domain. This
            // check adds this file's stream assignment and exact sequence order.
            // A mismatch leaves the accepted boundary before this record.
            if record.id() != expected {
                return Ok(Some(LogTailError::UnexpectedRecordId {
                    expected,
                    actual: record.id(),
                }));
            }
            // Accept only after every check passes. The first new record starts
            // at the trusted prefix's end; later ones start at the preceding end.
            // length() includes header, payload and CRC. Saving the exclusive end
            // also advances the accepted count used for the next expected ID.
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
    async fn a_single_checkpointed_record_leaves_the_first_suffix_entry_for_comparison() {
        let file_id = LogFileId::from_record_id(id(42, 100_000));
        let log = encoded(42, 100_000, &[0, 3, 1]).await;
        let start = RecordEndLocation::new(id(42, 100_000), 16);
        let mut wrong = INDEX;
        wrong[8] = 34; // First suffix entry is wrong; the following entry still matches.
        for (index, matching) in [
            (INDEX.as_slice(), 3),
            (&INDEX[..8], 1),
            (wrong.as_slice(), 1),
        ] {
            let (_directory, provider) = pair(file_id, &log, Some(index)).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, Some(start))
                .await
                .unwrap();
            let report = validator.validate().await.unwrap();
            assert_eq!(report.record_count(), 3);
            assert_eq!(report.matching_index_entries(), matching);
            assert_eq!(report.needs_index_repair(), matching != 3);
            assert_eq!(
                report.end(),
                Some(RecordEndLocation::new(id(42, 100_002), 52))
            );
            assert!(report.tail_error().is_none());
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
    }

    #[tokio::test]
    async fn a_checkpoint_cannot_claim_a_record_in_an_empty_log() {
        for first_sequence in [0, 100_000] {
            let record_id = id(7, first_sequence);
            let file_id = LogFileId::from_record_id(record_id);
            for position in [0, 16] {
                // Even an index entry agreeing with the endpoint cannot certify a
                // record in a zero-byte log. Neither file may be repaired on this claim.
                let index = u64::to_le_bytes(position);
                let (_directory, provider) = pair(file_id, &[], Some(&index)).await;
                let mut validator = IndexedLogValidator::open(
                    &provider,
                    file_id,
                    Some(RecordEndLocation::new(record_id, position)),
                )
                .await
                .unwrap();
                assert!(matches!(
                    validator.validate().await,
                    Err(Error::InvalidStart(_))
                ));
                assert!(validator.report().is_none());
                assert!(matches!(
                    validator.repair_index().await,
                    Err(Error::Unusable)
                ));
                assert!(matches!(
                    validator.truncate_invalid_tail().await,
                    Err(Error::Unusable)
                ));
                assert!(matches!(
                    validator.into_writer().await,
                    Err(Error::Unusable)
                ));
                assert!(
                    tokio::fs::read(provider.log_file_path(file_id))
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(
                    tokio::fs::read(provider.index_file_path(file_id))
                        .await
                        .unwrap(),
                    index
                );
            }
        }
    }

    #[tokio::test]
    async fn supplied_boundary_trusts_both_prefixes_and_repairs_only_the_index_suffix() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let mut log = encoded(7, 0, &[0, 3, 1]).await;
        // Deliberately poison certified bytes to prove that recovery honors the
        // supplied boundary. Real checkpoint owners must certify correct records
        // AND indexes; this test does not claim that these are valid checkpoints.
        log[12] ^= 1;
        let start = RecordEndLocation::new(id(7, 1), 35);
        for first_end in [0u64, 16, 35, 36, u64::MAX] {
            let mut expected_index = INDEX;
            expected_index[..8].copy_from_slice(&first_end.to_le_bytes());
            // Keep the checkpoint's final entry intact. A complete, partial or
            // missing first suffix entry must be compared at the correct position.
            for suffix_bytes in 0..=8 {
                let (_directory, provider) =
                    pair(file_id, &log, Some(&expected_index[..16 + suffix_bytes])).await;
                let mut validator = IndexedLogValidator::open(&provider, file_id, Some(start))
                    .await
                    .unwrap();
                let report = validator.validate().await.unwrap();
                assert_eq!(report.record_count(), 3);
                assert_eq!(
                    report.matching_index_entries(),
                    if suffix_bytes == 8 { 3 } else { 2 }
                );
                assert_eq!(report.needs_index_repair(), suffix_bytes != 8);
                assert_eq!(report.end(), Some(RecordEndLocation::new(id(7, 2), 52)));
                assert!(report.tail_error().is_none());
                validator.repair_index().await.unwrap();
                assert_eq!(
                    tokio::fs::read(provider.index_file_path(file_id))
                        .await
                        .unwrap(),
                    expected_index
                );
                assert_eq!(
                    tokio::fs::read(provider.log_file_path(file_id))
                        .await
                        .unwrap(),
                    log
                );
            }
        }
    }

    #[tokio::test]
    async fn no_accepted_suffix_preserves_the_checkpoint_endpoint() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let start = RecordEndLocation::new(id(7, 1), 35);
        let mut corrupt_log = encoded(7, 0, &[0, 3, 1]).await;
        corrupt_log[51] ^= 1;
        for log in [&corrupt_log[..35], corrupt_log.as_slice()] {
            let (_directory, provider) = pair(file_id, log, Some(&INDEX)).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, Some(start))
                .await
                .unwrap();
            let report = validator.validate().await.unwrap();
            assert_eq!(report.end(), Some(start));
            assert_eq!(report.record_count(), 2);
            assert_eq!(report.valid_log_length(), 35);
            assert_eq!(report.matching_index_entries(), 2);
            assert!(report.needs_index_repair());
            if log.len() == 35 {
                assert!(report.tail_error().is_none());
            } else {
                assert!(matches!(
                    report.tail_error(),
                    Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
                ));
            }
            validator.repair_index().await.unwrap();
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                INDEX[..16]
            );
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                log
            );
        }
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
        let mut indexes = vec![
            None,
            Some([16, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 0, 0, 0, 0].to_vec()),
            Some(vec![255; 16]),
        ];
        indexes.extend((0..16).map(|length| Some(INDEX[..length].to_vec())));
        for index in indexes {
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
    async fn the_last_uncheckpointed_record_determines_fullness_and_the_tail_boundary() {
        let file_id = LogFileId::from_record_id(id(9, 0));
        let prefix = encoded(9, 0, &vec![0; 99_999]).await;
        let checkpoint = RecordEndLocation::new(id(9, 99_998), 1_599_984);
        // Independently encode the expected index of these sixteen-byte records.
        let index: Vec<u8> = (1..=99_999u64)
            .flat_map(|n| (n * 16).to_le_bytes())
            .collect();
        let final_record = encoded(9, 99_999, &[0]).await;
        let extra_record = encoded(9, 100_000, &[0]).await;
        for (corrupt_final, append_extra) in [(false, false), (true, false), (false, true)] {
            let mut log = prefix.clone();
            log.extend_from_slice(&final_record);
            if corrupt_final {
                *log.last_mut().unwrap() ^= 1;
            }
            if append_extra {
                // A fully encoded next-file record must still be rejected as extra
                // data, not accepted or classified as a sequence error in this file.
                log.extend_from_slice(&extra_record);
            }
            let (_directory, provider) = pair(file_id, &log, Some(&index)).await;
            let mut validator = IndexedLogValidator::open(&provider, file_id, Some(checkpoint))
                .await
                .unwrap();
            let expected_end = if corrupt_final {
                checkpoint
            } else {
                RecordEndLocation::new(id(9, 99_999), 1_600_000)
            };
            let report = validator.validate().await.unwrap();
            assert_eq!(
                report.record_count(),
                if corrupt_final { 99_999 } else { 100_000 }
            );
            assert_eq!(report.is_full(), !corrupt_final);
            assert_eq!(report.end(), Some(expected_end));
            assert_eq!(report.matching_index_entries(), 99_999);
            assert_eq!(report.needs_index_repair(), !corrupt_final);
            if corrupt_final {
                assert!(matches!(
                    report.tail_error(),
                    Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
                ));
            } else if append_extra {
                assert!(matches!(report.tail_error(), Some(LogTailError::ExtraData)));
            } else {
                assert!(report.tail_error().is_none());
            }
            validator.repair_index().await.unwrap();
            validator.truncate_invalid_tail().await.unwrap();
            let mut expected_index = index.clone();
            if !corrupt_final {
                expected_index.extend_from_slice(&1_600_000u64.to_le_bytes());
            }
            assert_eq!(
                tokio::fs::read(provider.index_file_path(file_id))
                    .await
                    .unwrap(),
                expected_index
            );
            assert_eq!(
                tokio::fs::read(provider.log_file_path(file_id))
                    .await
                    .unwrap(),
                log[..expected_end.position() as usize]
            );
            if corrupt_final {
                assert_eq!(
                    validator.into_writer().await.unwrap().synced_end(),
                    Some(checkpoint)
                );
            } else {
                assert_eq!(
                    validator.into_completed().await.unwrap().end(),
                    expected_end
                );
            }
        }
    }

    #[tokio::test]
    async fn index_bytes_beyond_the_read_cap_still_require_repair() {
        let file_id = LogFileId::from_record_id(id(9, 0));
        let log = encoded(9, 0, &vec![0; 100_000]).await;
        let index: Vec<u8> = (1..=100_000u64)
            .flat_map(|n| (n * 16).to_le_bytes())
            .collect();
        let end = RecordEndLocation::new(id(9, 99_999), 1_600_000);
        // Exercise both an entirely scanned file and an entirely certified one.
        // In each, only the physical index length reveals these unread extra bytes.
        for start in [None, Some(end)] {
            for extra_bytes in [1, 7, 8, 9] {
                let mut oversized_index = index.clone();
                oversized_index.resize(800_000 + extra_bytes, 0xff);
                let (_directory, provider) = pair(file_id, &log, Some(&oversized_index)).await;
                let mut validator = IndexedLogValidator::open(&provider, file_id, start)
                    .await
                    .unwrap();
                let report = validator.validate().await.unwrap();
                assert!(report.is_full());
                assert_eq!(report.end(), Some(end));
                assert_eq!(report.index_length(), oversized_index.len() as u64);
                assert_eq!(report.matching_index_entries(), 100_000);
                assert!(report.needs_index_repair());
                assert!(report.tail_error().is_none());
                assert_eq!(
                    tokio::fs::read(provider.index_file_path(file_id))
                        .await
                        .unwrap(),
                    oversized_index
                );
                validator.repair_index().await.unwrap();
                assert_eq!(
                    tokio::fs::read(provider.index_file_path(file_id))
                        .await
                        .unwrap(),
                    index
                );
                assert_eq!(
                    tokio::fs::read(provider.log_file_path(file_id))
                        .await
                        .unwrap(),
                    log
                );
                assert_eq!(validator.into_completed().await.unwrap().end(), end);
            }
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
    async fn repeated_validation_returns_original_findings_before_and_after_repairs() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let mut log = encoded(7, 0, &[0, 3, 1]).await;
        log[51] ^= 1;
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX)).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        // Contents are unchanged. This handle still permits repair but would
        // reject a log read, proving subsequent validate calls use the cached report.
        validator.log = tokio::fs::OpenOptions::new()
            .write(true)
            .open(provider.log_file_path(file_id))
            .await
            .unwrap();
        for phase in 0..3 {
            let report = validator.validate().await.unwrap();
            assert_eq!(report.file_id(), file_id);
            assert_eq!(report.end(), Some(RecordEndLocation::new(id(7, 1), 35)));
            assert_eq!(report.record_count(), 2);
            assert_eq!(report.log_length(), 52);
            assert_eq!(report.index_length(), 24);
            assert_eq!(report.matching_index_entries(), 2);
            assert!(report.needs_index_repair());
            assert!(matches!(
                report.tail_error(),
                Some(LogTailError::Record(RecordReadError::CrcMismatch { .. }))
            ));
            match phase {
                0 => validator.repair_index().await.unwrap(),
                1 => validator.truncate_invalid_tail().await.unwrap(),
                _ => {}
            }
        }
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log[..35]
        );
        assert_eq!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap(),
            INDEX[..16]
        );
        // The old report still describes corruption, but completed repairs permit handover.
        assert_eq!(
            validator.into_writer().await.unwrap().synced_end(),
            Some(RecordEndLocation::new(id(7, 1), 35))
        );
    }

    #[tokio::test]
    async fn failed_log_truncation_after_successful_index_repair_prevents_reuse() {
        let file_id = LogFileId::from_record_id(id(7, 0));
        let mut log = encoded(7, 0, &[0, 3, 1]).await;
        log[51] ^= 1;
        let (_directory, provider) = pair(file_id, &log, Some(&INDEX)).await;
        let mut validator = IndexedLogValidator::open(&provider, file_id, None)
            .await
            .unwrap();
        validator.validate().await.unwrap();
        // Replace only this owner's log handle with a read-only one. Index repair
        // can complete, then set_len must fail without changing the log's contents.
        validator.log = File::open(provider.log_file_path(file_id)).await.unwrap();
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::LogIo(_))
        ));
        assert_eq!(
            tokio::fs::read(provider.index_file_path(file_id))
                .await
                .unwrap(),
            INDEX[..16]
        );
        assert_eq!(
            tokio::fs::read(provider.log_file_path(file_id))
                .await
                .unwrap(),
            log
        );
        let report = validator.report().unwrap();
        assert_eq!(report.end(), Some(RecordEndLocation::new(id(7, 1), 35)));
        assert_eq!(report.index_length(), 24);
        assert!(report.tail_error().is_some());
        assert!(matches!(validator.validate().await, Err(Error::Unusable)));
        assert!(matches!(
            validator.repair_index().await,
            Err(Error::Unusable)
        ));
        assert!(matches!(
            validator.truncate_invalid_tail().await,
            Err(Error::Unusable)
        ));
        assert!(matches!(
            validator.into_writer().await,
            Err(Error::Unusable)
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

# Indexed log validator

`IndexedLogValidator` owns recovery of one existing log/index pair. It validates
from a caller-supplied trusted endpoint, repairs untrusted index suffixes,
supports explicit invalid-log-tail removal, and hands over either a synchronized
partial writer or completion metadata for a full file with its handles closed.
Public callers continue to import `transaction_log::streams::IndexedLogValidator`.

## Source map

| Source | Responsibility |
| --- | --- |
| `indexed_log_validator.rs` | Acquisition, supplied-boundary validation, index repair, explicit tail truncation, both handover paths and same-file tests. |
| `log_validation_report.rs` | Immutable original findings: accepted prefix, observed lengths, matching index entries and first invalid tail. |
| `log_tail_error.rs` | Malformed records, wrong stream/sequence and bytes beyond a full file. |
| `log_validation_error.rs` | Recovery precondition and operational failures, distinct from corrupt content. |
| `completed_indexed_log.rs` | Full-file completion metadata returned after synchronization and closing the pair. |
| `mod.rs` | This Rustdoc specification, declarations and re-exports. |

Read the [shared stream contracts](../README.md#shared-contracts),
[storage specification](../../storage/README.md) and
[record specification](../../../../transaction-log-exports/src/record/README.md)
for file acquisition, trusted locations and record validity. Repair uses the
[index writer](../index_writer/README.md); partial handover transfers ownership to
the [indexed log writer](../indexed_log_writer/README.md). These remain sibling
components so recovery reuses their output contracts.

## Validation, repair and handover

`IndexedLogValidator::open(&provider, file_id, start).await` accepts an explicit
`Option<RecordEndLocation>` boundary. `None` means the beginning; `Some` means the
last trusted record in this file and the byte position immediately after it.
The caller certifies **both the log and matching index prefix**. The validator
never loads a checkpoint, chooses a boundary, or moves that boundary backwards.
A full-file checkpoint remains in its own file and needs no next-record location.
Checkpoint advancement requires correct records and their exact index offsets
through that endpoint. The checkpoint owner must synchronize the covered log data,
then the corresponding index data, before durably publishing the checkpoint.
Recovery trusts that certification; checkpoint persistence remains future work.

Opening uses `StorageProvider::open_log_for_validation` first, then
`open_index_for_repair`. The log must exist. A missing derived index is created
empty; existing bytes are preserved. Creating that empty file is distinct from
the subsequent non-mutating validation pass. With a nonempty trusted boundary,
a missing index prefix or mismatching final entry produces `TrustedIndexUnavailable`. The
caller must supply an earlier trustworthy boundary for a separate attempt.

### Validation and precise corruption boundaries

`open` checks the endpoint's file identity and counts its record inclusively.
`None` means zero trusted records and byte position zero; no record ID acts as an
empty-file sentinel. `validate` checks that the supplied byte position fits both
the observed log length and the possible encoded lengths of that record count.
An empty log with no trusted prefix is valid; claiming a trusted record in an empty
log is rejected during validation.

Validation loads the index from byte zero, capped at the 800,000 bytes needed for
a full file. One iterator decodes offsets as needed. With a trusted prefix, it
skips directly to the final certified entry, checks that the entry exists and
equals the supplied byte position, then remains positioned at the suffix. It does
not validate earlier certified entries, even for plausible record lengths. The
checkpoint already guarantees their correctness, just as it guarantees the log
prefix's CRC and sequence validity. The retained presence and endpoint checks
establish that the supplied boundary applies to this file pair; they do not
recertify its history. A missing trusted prefix cannot be silently replaced by
scanning earlier log records. Without a checkpoint, no index entries are skipped.

The suffix is read through the production `RecordReader` for framing, stream-ID
domain and CRC checks. The validator adds exact stream/sequence checks and the
100,000-record limit. It records each accepted exclusive end and compares it with
the index, finding the first incorrect, missing or partial entry. Index contents
never choose untrusted record boundaries. Extra index entries are also repairable.

The log scan is bounded to the suffix within the initially observed file length.
Accepted ends, rather than the read-ahead cursor, define its valid extent. Index
comparison preserves only the consecutive matching entries after the trusted
prefix, stopping at the first mismatch or missing complete entry. Later matches
do not extend that prefix. Readiness also requires the actual index length to
equal eight bytes per accepted record, catching extra entries and partial trailing
bytes even when the comparison does not inspect them. An oversized index's actual
length is retained despite the input cap.
One accepted `RecordEndLocation` supplies both the trailing-data check and report.
If no new records pass validation, that endpoint remains the supplied boundary,
or `None` when no record is accepted at all.

The expected ID comes from the file's first sequence plus the total accepted
record count, including the trusted prefix. Keeping that calculation tied to the
accepted offsets avoids a separate expected-ID cursor that must advance with
them. The addition is checked: input beyond the representable sequence range is
reported as extra data. `RecordId::next()` intentionally panics on overflow and
would need a separate exhaustion check to preserve this validation behavior.

The private `scan_records` helper returns `Ok(None)` both at clean source EOF and
when the accepted count reaches 100,000; this result alone does not certify a
clean file end. Its caller, `validate`, compares the accepted byte end with the
observed log length. Any bytes beyond a full prefix become `ExtraData`, even if
they do not form a complete record. Clean EOF short of the observed length in a
partial file instead reports `FilesChanged`. Reader read-ahead is not an accepted
boundary, and reaching the record cap does not require another source read.

The reader publishes every valid record before the first encoding failure in a
batch. The validator drains that prefix, checking sequence/stream policy and
recording accepted ends, before the next wait reports the encoding error. An
earlier sequence error therefore takes precedence over later corrupt input.
Scanning stops at the first format or sequence failure and never salvages records
across a gap. Operational read failures return an error, not a corrupt-tail report.
The reader-error match lists every variant explicitly in one of those two groups.
Adding a `RecordReadError` variant must force a compile-time decision about its
recovery policy; a catch-all content-error arm could silently allow truncation
after a newly introduced operational failure.

Validation makes one forward pass over the supplied file suffix. The reader's
existing buffer and one split per prepared batch preserve throughput on valid
input; no file reread or second buffering/delivery layer is needed on corruption.
The reader may repeat the failing validation from its retained bytes to report
the error after its valid batch drains. This does not repeat source I/O or
validation of accepted records. Record framing and CRC remain owned by the
production reader, with no duplicate codec or disabled checks here.

Memory holds only suffix offsets (at most 100,000 `u64` values), at most 800,000
bytes of index input, and the reader's buffer. Vector capacity may exceed the
initialized data length as buffers grow. Repair can temporarily hold another encoded index batch.
Payloads are released during scanning; allocation never follows an arbitrarily
large/corrupt index length. There are no recovery-throughput measurements yet.

`LogValidationReport` is a snapshot of original findings: accepted endpoint/count,
original lengths, matching index-prefix count and first `LogTailError`. OS I/O
failures return `LogValidationError`, not truncation authority. Report fields stay
unchanged after repair. Repeating successful validation returns that report without
rescanning while the validator remains usable; it is not a live filesystem-status
query. The final file-length check must succeed before publication. Publishing a
report restores usability even when corruption was found: usability permits
explicit repair, while the independent readiness flags gate handover and do not
certify durability.

### Internal state and lifecycle

Every field is documented beside `IndexedLogValidator` in
[`indexed_log_validator.rs`](indexed_log_validator.rs). Keep those local invariants
with the implementation. The state separates three different kinds of information:

- `file_id`, `start` and `trusted_records` fix the caller's scope and trusted prefix.
  They do not advance as validation accepts more records.
- `ends` collects only newly accepted suffix offsets; `report` publishes the
  completed original findings. Repair does not rewrite that report's history.
- `log_length` and `index_length` track expected physical lengths, while
  `log_ready` and `index_ready` track successful validation or repair. They describe
  the current pair only while the validator remains usable; they do not certify sync.

`usable` guards in-place operations across I/O, cancellation and unwinding. A
new usable validator still needs validation. A completed report may describe
corruption and permit explicit repair. Handover requires a usable validator,
a completed report and both readiness flags, then supplies synchronization.
After an operational failure, old findings remain inspectable but cannot authorize
further repair or handover. Length comparisons detect size changes, not same-size
overwrites, so exclusive write ownership remains a caller responsibility.

### Explicit repair and two handover paths

| Operation | Contract |
| --- | --- |
| `validate()` | Inspect bytes and report the accepted prefix; no repair. |
| `repair_index()` | Preserve matching entries and rewrite/truncate only the untrusted suffix through `IndexWriter`; never change log bytes. |
| `truncate_invalid_tail()` | Explicitly authorize removal of the reported invalid log suffix, repairing its index first if necessary. |
| `into_writer()` | Require a clean partial pair, seek verified append ends, synchronize both files and transfer the same handles to `IndexedLogWriter<File>`. |
| `into_completed()` | Require a clean full pair, synchronize log then index, close both handles and return `CompletedIndexedLog`. |
| `into_inner()` | Extract raw handles without validation, repair, synchronization or handover authority. |

Index repair checks unchanged file lengths, completes the log's destination flush,
truncates the index at its first mismatch, seeks there, and uses `IndexWriter` to
encode and flush the replacement suffix. Correct index prefixes and trusted log
bytes remain untouched. Only accepted log records receive entries, even when a
corrupt log tail remains. Completed repairs are idempotent. Repair success means
completed output; handover separately supplies durability.

Log truncation is never automatic during validation, index repair or handover.
The owner must explicitly call `truncate_invalid_tail` after inspecting the report.
An I/O error cannot authorize this. Bytes after 100,000 valid records report
`ExtraData`; explicit truncation removes them before full-file handover. Partial
completion returns `NotFull`; full-file writer handover returns `Full`.

`CompletedIndexedLog` exposes the file ID and last `RecordEndLocation`, with no
append API or open handles. It does not publish a checkpoint, acquire the next
file, rename files or implement historical reads. Partial handover likewise leaves
checkpoint publication to its owner, while its writer's `synced_end` covers the
accepted/repaired prefix. For example:

```no_run
use tokio::fs::File;
use transaction_log::{storage::{LogFileId, RecordEndLocation, StorageProvider},
    streams::{IndexedLogValidator, IndexedLogWriter, LogValidationError}};

async fn resume_partial(
    provider: &StorageProvider,
    file_id: LogFileId,
    trusted_end: Option<RecordEndLocation>,
) -> Result<IndexedLogWriter<File>, LogValidationError> {
    let mut validator = IndexedLogValidator::open(provider, file_id, trusted_end).await?;
    if validator.validate().await?.tail_error().is_some() {
        // Application policy must decide whether to delete this suffix.
        return Err(LogValidationError::LogRepairRequired);
    }
    validator.repair_index().await?;
    validator.into_writer().await
}
```

The caller excludes concurrent writes and withholds the recovering pair from
historical requests. Length checks catch some changes between phases, but are
not locks and cannot detect same-length overwrites. Repairs are not atomic across
two files. Startup readiness, checkpoint publication, directory-entry durability
and coordination with replication remain owner responsibilities.

Validation/repair marks the instance unusable before I/O and restores it only on
success. Errors, panic and cancellation cannot grant handover of a partly processed
pair. Unpolled futures are inert. Handover consumes the validator, so cancellation
cannot return reusable authority. Underlying file I/O may continue after a future
is dropped; `into_inner` allows the owner to drain/quiesce outstanding operations
before another recovery attempt. Dropping or reopening alone is not repair.

## Scope

Validation, repair and partial/full handover are implemented. The owner still
chooses the trusted boundary and whether to remove corrupt data. Startup-wide
coordination, checkpoint persistence/publication, replication decisions, file
rotation and historical queries remain future work. This component does not
silently extend its authority to any of those responsibilities.

## Verification

Validator tests prove that certified log/index contents are trusted while missing
prefixes and mismatching endpoint entries are rejected. They cover complete,
partial and missing first suffix entries, preservation of the supplied endpoint
when no suffix record is accepted, and supplied full-file boundaries. They cover
missing files/indexes, every incomplete index and final-record prefix of small
fixtures, incorrect/extra entries, framing/CRC/domain errors, wrong streams and
duplicate/skipped sequences. They preserve valid records before corruption in the
same batch and across maximum-size records. Full-file tests validate and index
100,000 records, remove extra log bytes, close handles and return completion
metadata, including handover from a full trusted boundary. Partial handover tests
append at the verified original-handle positions. Tests also cover a one-record
checkpoint, rejection of a checkpoint claiming a record in an empty log, and the
99,999-record checkpoint boundary with a valid or corrupt final record and a
complete extra record. Indexes beyond the 800,000-byte input cap still require
repair for both scanned and fully checkpointed files.

Repeated successful validation is tested before and after repair: it must return
the original findings without another log read. A read-only log handle exercises
truncation failure after successful index repair, preserving the log and refusing
further recovery or handover through that instance. These tests assume exclusive
write ownership during validation; they do not depend on concurrent file changes.
I/O failures, changed lengths
and deterministically cancelled validation/repair never permit truncation or
handover. Provider acquisition tests live with the provider.

Tests remain in `indexed_log_validator.rs`, beside the behavior they exercise.
Follow the [module verification guidance](../README.md#verification-and-performance).
No recovery-throughput measurement is claimed. Buffering and the single forward
scan describe the implementation, not measured disk-performance gains.

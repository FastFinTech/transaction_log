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

Opening uses `StorageProvider::open_log_for_validation` first, then
`open_index_for_repair`. The log must exist. A missing derived index is created
empty; existing bytes are preserved. Creating that empty file is distinct from
the subsequent non-mutating validation pass. With a nonempty trusted boundary,
a missing/inconsistent index prefix produces `TrustedIndexUnavailable`. The
caller must supply an earlier trustworthy boundary for a separate attempt.

### Validation and precise corruption boundaries

`validate` checks the endpoint's file identity and possible byte extent. It checks
that the trusted index prefix exists, has plausible increasing offsets, and ends
at the supplied position. These are handover consistency checks, not proof of old
CRC validity or intermediate index agreement. Those remain the caller's trust.

The suffix is read through the production `RecordReader` for framing, stream-ID
domain and CRC checks. The validator adds exact stream/sequence checks and the
100,000-record limit. It records each accepted exclusive end and compares it with
the index, finding the first incorrect, missing or partial entry. Index contents
never choose untrusted record boundaries. Extra index entries are also repairable.

The reader publishes every valid record before the first encoding failure in a
batch. The validator drains that prefix, checking sequence/stream policy and
recording accepted ends, before the next wait reports the encoding error. An
earlier sequence error therefore takes precedence over later corrupt input.
Scanning stops at the first format or sequence failure and never salvages records
across a gap. Operational read failures return an error, not a corrupt-tail report.

Validation makes one forward pass over the supplied file suffix. The reader's
existing buffer and one split per prepared batch preserve throughput on valid
input; no file reread or second buffering/delivery layer is needed on corruption.
The reader may repeat the failing validation from its retained bytes to report
the error after its valid batch drains. This does not repeat source I/O or
validation of accepted records. Record framing and CRC remain owned by the
production reader, with no duplicate codec or disabled checks here.

Memory holds only suffix offsets (at most 800 KB), at most 800 KB of loaded index
bytes, and the reader's buffer. Repair can temporarily hold another encoded index batch.
Payloads are released during scanning; allocation never follows an arbitrarily
large/corrupt index length. There are no recovery-throughput measurements yet.

`LogValidationReport` is a snapshot of original findings: accepted endpoint/count,
original lengths, matching index-prefix count and first `LogTailError`. OS I/O
failures return `LogValidationError`, not truncation authority. Report fields stay
unchanged after repair. Repeating successful validation returns that report without
rescanning; it is not a live filesystem-status query.

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

Validator tests cover supplied boundaries and rejected trusted index prefixes,
missing files/indexes, every incomplete index and final-record prefix of small
fixtures, incorrect/extra entries, framing/CRC/domain errors, wrong streams and
duplicate/skipped sequences. They preserve valid records before corruption in the
same batch and across maximum-size records. Full-file tests validate and index
100,000 records, remove extra log bytes, close handles and return completion
metadata, including handover from a full trusted boundary. Partial handover tests
append at the verified original-handle positions. I/O failures, changed lengths
and deterministically cancelled validation/repair never permit truncation or
handover. Provider acquisition tests live with the provider.

Tests remain in `indexed_log_validator.rs`, beside the behavior they exercise.
Follow the [module verification guidance](../README.md#verification-and-performance).
No recovery-throughput measurement is claimed. Buffering and the single forward
scan describe the implementation, not measured disk-performance gains.

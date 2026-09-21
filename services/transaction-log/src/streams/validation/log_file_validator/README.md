# Log file validator

`LogFileValidator` implements standalone startup recovery for one log file. It
checks the supplied trusted boundary, scans the following records, removes a corrupt
tail and synchronizes the file. Its associated `validate` function takes ownership
of the file and returns a `ValidatedLogFile<F>` only after successful completion.
The unit struct holds no state and needs no construction step. Public types are
exported from both `streams::validation` and `streams`.

[`IndexedLogValidator`](../indexed_log_validator/README.md) pairs it with
[`IndexFileValidator`](../index_file_validator/README.md) and returns a synchronized
pair's endpoint plus append-positioned handles for a partial file. Application
startup does not call it.

## Source map

- `log_file_validator.rs`: the associated validation function, its private
  `LogScanResult`, the scanner and same-file tests. The small internal result stays
  beside its producer.
- `validated_log_file.rs`: the owned synchronized file and read-only recovery findings.
- `log_file_validation_error.rs`: invalid-start and operational errors.
- `log_tail_error.rs`: content-corruption diagnostics produced by log scanning.
- `mod.rs`: declarations, re-exports and this README's Rustdoc inclusion.

The component owns `LogTailError`, re-exported through `streams::validation` and
`streams`. Its record-content, unexpected-ID and extra-data variants describe corruption; the
scanner never puts an operational read error into a tail diagnostic.

## Inputs and public lifecycle

`LogFileValidator::validate<F: ValidationFile>(file, file_id, last_trusted).await`
receives an already-open owned file, its `LogFileId` and an optional caller-trusted
`RecordEndLocation` directly. The shared
[file-capability contract](../README.md#shared-file-capabilities) supplies reading,
seeking, length inspection, truncation and data synchronization. Log validation
does not require `AsyncWrite` or write record bytes. Tokio files and explicitly
supplied wrappers use the same generic implementation. The caller completes earlier writes and
excludes all other file access throughout recovery. There are no file locks or
external-change guards.

Validation checks that the trusted endpoint belongs to the supplied
file and that its byte position is within the file and plausible for the trusted
record count, using the bounds from `RecordLength::MIN` and `RecordLength::MAX`.
It then seeks to that position. A mismatched or impossible endpoint
returns an error without truncation. Earlier checkpoint-certified bytes are not
rescanned. `None` means `LogFilePosition::START` and the file's first assigned record ID.

The raw length returned by `ValidationFile::length()` is converted once into
`file_end: LogFilePosition`, the whole file's exclusive EOF position. Start,
plausible-boundary and accepted-end comparisons stay typed. Raw offsets are
extracted with `get()` for seeking and truncation; the underlying file capability
continues to express byte lengths as `u64`.

Reuse `RecordReader` for framing, stream-ID domain and CRC validation; the
[record specification](../../../../../transaction-log-exports/src/record/README.md)
owns those contracts. This layer adds the expected stream, exact sequence order
and per-file record count from the
[location specification](../../location/README.md). Every valid record before the
first content violation is retained, including those delivered before an error
in the same reader batch. The scan never skips corruption to find later records.

## Complete scanner handoff

The private `scan_records` owns its offset vector and returns one `LogScanResult`:

- `validated_end`: the last accepted record, initialized to the trusted endpoint;
- `suffix_ends: Vec<LogFilePosition>`: absolute exclusive byte ends of newly
  accepted records only;
- `tail_error`: the first content violation, or `None` for a clean end.

The accepted endpoint supplies both the next expected ID and its byte start.
The scanner constructs that start from `end.record_id().next()` and
`end.position()` directly: its `count < RECORDS_PER_FILE` guard ensures the next
record remains in the current file, so the general file-rotation calculation in
`next_record_start()` is unnecessary here. The supplied trusted file identity is
checked before scanning; record acceptance advances the count and endpoint together.
With no accepted endpoint, the start is the file's first assigned ID at
`LogFilePosition::START`. After checking the
incoming ID, `RecordStartLocation::to_end(record.length())` supplies the accepted
end and its suffix-index offset. The scanner keeps no separate mutable byte
position or expected ID. It already needs each accepted end for the index, so
it does not also call `to_next()` to recompute that end.

The caller does not reconstruct IDs from counts or offsets. Empty input with no
checkpoint gives no endpoint; a trusted prefix with no accepted suffix keeps
its original endpoint and an empty vector.

The scanner receives the initial whole-file EOF position, including trusted bytes. At
100,000 accepted records it uses that position to diagnose every extra byte without
probing EOF, including when the entire file was already trusted. This comparison
uses the accepted end in the current file, not the next record's start, which
would reset to zero at file rotation. A full file necessarily has an accepted
endpoint; the scanner asserts that invariant instead of supplying a fallback zero.
`None` is a complete
clean-tail diagnosis, rather than an instruction for the caller to distinguish EOF
from reaching the record cap. EOF is not rechecked for external changes.

Operational read failures return `Err` and drop partial findings. Error classification
is exhaustive so a newly added reader error requires an explicit recovery policy.
The public validator receives a complete content finding: it truncates only when
`tail_error` is present, to the endpoint's exclusive position or zero, then calls
`sync_data` even for a clean file. Only after successful synchronization does that
diagnostic become the public result's `removed_tail`.

## Result and index pairing

`ValidatedLogFile<F = tokio::fs::File>` owns the synchronized file, final endpoint,
suffix offsets and optional removed-tail diagnostic. `F` preserves the concrete
input file type; the default keeps plain `ValidatedLogFile` usable for Tokio files.
Read-only accessors expose the findings; `into_inner()` returns `F` and discards
the findings without more I/O.
The cursor may reflect reader read-ahead: callers seek explicitly before subsequent
reading or appending. The result has no unchecked public constructor.

The `suffix_ends()` accessor borrows `&[LogFilePosition]`, without converting or
copying the vector. The combined validator passes this typed slice directly to
the index validator.

The combined validator supplies the same original checkpoint-certified
endpoint to both validation operations. The checkpoint certifies that the covered
log and index were previously validated and synchronized; log validation does not
derive its trusted endpoint by inspecting the index. The combined validator passes
`suffix_ends()` directly to `IndexFileValidator::validate` along with the index file
and that original endpoint. These offsets exclude the trusted prefix and include
only records accepted by this scan. An empty slice still matters: it directs the
index validator to remove everything after that trusted prefix.

This component does not inspect or modify indexes, choose or publish checkpoints,
open storage paths, hand over an active writer, or perform cluster coordination.
A validated log alone does not establish a ready log/index pair or publish a stream.

## Ownership and failure

Passing the file by value transfers ownership into the validation future. There is
no validator instance to reuse and no cached report, readiness flag or usability
flag. A successful result returns the file; a failed call drops it. Read and input
failures do not permit truncation. Truncation and synchronization failures return no successful
result, even when some file changes have completed. The original concrete file
or reader error remains available through `LogFileValidationError`.

An unpolled validation future owns the file but starts no I/O; dropping it
releases the file. Cancellation after I/O begins can leave partial repair, and
underlying OS operations may still finish. The owner must quiesce outstanding
operations before another recovery attempt. Ownership transfer does not make recovery
atomic. Synchronizing this file does not publish a checkpoint or synchronize its
parent directory or paired index.

## Validation and performance

Tests in `log_file_validator.rs` cover empty and trusted starts, independent encoded
record fixtures, every partial byte length of a sample tail record, invalid length
and stream-domain fields, CRC failures, wrong streams, duplicate/skipped sequences,
and preservation of the earliest failure within a batch. Fragmented inputs verify
that accepted endpoints and offsets do not depend on read chunk boundaries.

Record-cap tests cover complete scans across reader refills, nearly full and fully
trusted prefixes, clean full files and extra complete or partial bytes. Public-API
tests also verify the actual removal of those extra bytes, including for fully
trusted files. Maximum-size records span reader refills; clean and truncated inputs
check exact endpoints, suffix offsets and retained bytes with and without a trusted
prefix. Invalid-start cases cover the wrong stream or file, an absent trusted
record, a position beyond EOF, and positions below or above the plausible encoded
extent. Each rejected input leaves all bytes unchanged.

Scripted read errors after valid records produce no scan result. File tests check
trusted-prefix preservation, public result contents and failed truncation without
publishing successful findings. The parent module's
[shared test file](../README.md#shared-test-file) implements `ValidationFile` and
observes real Tokio file operations. Tests explicitly pass it with their own
`FileProbe` to pause or fail operations at entry boundaries. They verify that
empty, clean, fully trusted and repaired files await synchronization before success,
and that a synchronization failure returns no successful result even when truncation
has already completed. Ordinary filesystem tests pass a Tokio file directly;
the validator contains no test-specific branches or thread-local setup.

Cancellation tests cover an unpolled public operation, a pending scanner, and the
public operation paused before length lookup, truncation or synchronization. They verify
which operations completed and the exact bytes left behind, including a completed
truncation before cancelled synchronization. These deterministic pauses occur before
the underlying OS call; they do not simulate cancellation inside a kernel operation,
power loss or failures at every filesystem operation.

The scanner uses the production reader and releases payloads as it advances. The
result retains at most 100,000 offsets (800,000 bytes of values, with vector capacity
possibly larger). `LogFilePosition` retains the `u64` representation, so typed
suffix entries introduce no extra per-entry storage or conversion buffer. The
validator moves that vector into the successful result without copying it and
never retains the whole log. Removing the general rotation calculation and using
typed bounds simplify the source; no runtime speedup has been measured. There
are no recovery-performance measurements.

Run:

```powershell
cargo test -p transaction-log log_file_validator --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

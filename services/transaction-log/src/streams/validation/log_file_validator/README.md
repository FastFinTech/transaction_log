# Log file validator

Recover one log file by preserving its trusted prefix, accepting valid records
that follow, and removing the first corrupt record and everything after it.
Success returns the synchronized file, its accepted endpoint and the new record
ends needed to rebuild its index. This is the log recovery step used by the
combined log/index validator.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`LogFileValidator`] | Validate and repair an owned log through the associated `validate` operation. |
| [`ValidatedLogFile`] | Own the synchronized file and expose its accepted endpoint, suffix offsets and removed-tail diagnostic. |
| [`LogFileValidationError`] | Report an invalid trusted boundary or an operational failure that prevents successful recovery. |
| [`LogTailError`] | Describe the corrupt content removed after the last accepted record. |

The shared [`ValidationFile`](crate::streams::ValidationFile) trait supplies file
capabilities. [`RecordReader`](transaction_log_exports::RecordReader) validates
record encodings; this module adds file identity, sequence order and recovery.
[`IndexFileValidator`](crate::streams::IndexFileValidator) consumes the new record
ends, and [`IndexedLogValidator`](crate::streams::IndexedLogValidator) coordinates
recovery of a stored pair.

## Usage

Given an exclusively accessed file opened for reading and repair, validate from
its beginning with `None`. The file identity supplies the expected stream and
sequence range:

```rust
use transaction_log::streams::{LogFileId, LogFileValidator};
use transaction_log_exports::StreamId;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let directory = tempfile::tempdir()?;
# let path = directory.path().join("recover.log");
# // One complete record for stream 7, sequence 0, followed by a partial header.
# let first = [16, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 226, 50, 73];
# tokio::fs::write(&path, [&first[..], &[0xaa]].concat()).await?;
# let file = tokio::fs::OpenOptions::new().read(true).write(true).open(&path).await?;
let file_id = LogFileId::first(StreamId::new(7)?);
let validated = LogFileValidator::validate(file, file_id, None).await?;
let accepted_end = validated.validated_end();
let suffix_ends = validated.suffix_ends();

if let Some(reason) = validated.removed_tail() {
    eprintln!("Removed invalid log tail: {reason}");
}
# assert_eq!(accepted_end.unwrap().position().get(), 16);
# assert_eq!(suffix_ends, &[transaction_log::streams::LogFilePosition::new(16)]);
# assert!(validated.removed_tail().is_some());

let file = validated.into_inner();
// Seek explicitly before reading or appending; recovery may have read ahead.
# drop(file);
# assert_eq!(tokio::fs::read(&path).await?, first);
# Ok(())
# }
```

The caller finishes flushing earlier writes and excludes other access throughout
recovery. A successful result can include a removed-tail diagnostic: finding
corruption is a recoverable outcome when truncation and synchronization succeed.

<details>
<summary>Design and maintenance notes</summary>

**Pairing with index recovery.** Both focused validators receive the same original
checkpoint-certified endpoint. The log result contains only the offsets newly
accepted after that boundary, so the index can borrow them directly:

```no_run
use tokio::fs::File;
use transaction_log::streams::{
    IndexFileValidator, LogFileId, LogFileValidator, RecordEndLocation,
};

async fn recover_open_pair(
    log: File,
    index: File,
    file_id: LogFileId,
    last_trusted: Option<RecordEndLocation>,
) -> Result<(File, File), Box<dyn std::error::Error>> {
    let validated = LogFileValidator::validate(log, file_id, last_trusted).await?;
    let index = IndexFileValidator::validate(index, last_trusted, validated.suffix_ends()).await?;
    Ok((validated.into_inner(), index))
}
```

The newly discovered `validated_end()` cannot replace `last_trusted` in the index
call: that would ask the old index to certify records that still need indexing.
An empty suffix still tells the index validator to remove obsolete entries after
the trusted prefix. The slice excludes trusted records and needs no copied or
converted offset vector.

For recovery through storage paths, `IndexedLogValidator` already performs this
coordination and positions partial-file handles for appending. The focused calls
above return synchronized handles without an append-cursor guarantee. A validated
log alone does not establish a synchronized pair or publish a stream checkpoint.

</details>

## Behavior and guarantees

### Inputs and trusted boundary

[`validate`](LogFileValidator::validate) owns a concrete `F: ValidationFile` and
returns `Result<ValidatedLogFile<F>, LogFileValidationError>`. Tokio files satisfy
the bound directly; the result keeps the input's concrete type.

| Input | Required meaning |
| --- | --- |
| `file` | The exclusively accessed log, with earlier writes flushed and operations available for reading, seeking, truncation and synchronization. |
| `file_id` | The stream and assigned sequence range expected in this file. |
| `last_trusted` | An optional checkpoint-certified record end belonging to this file. `None` starts at byte zero and the file's first assigned record ID, including for a nonzero file number. |

The validator checks that the trusted endpoint belongs to `file_id`, lies within
the file and has a plausible byte position for its trusted record count. Invalid
input returns an error without truncation. These checks do not recertify earlier
records: the prefix remains trusted, and its bytes are not rescanned.

<details>
<summary>Design and maintenance notes</summary>

**Plausible extent and typed positions.** A trusted count of `n` permits an end
between `n * RecordLength::MIN` and `n * RecordLength::MAX`, inclusive. The boundary
must also be no later than EOF. These bounds reject impossible metadata without
pretending to prove framing, CRC or record existence inside the certified prefix.
The checkpoint supplies that prior validation and synchronization guarantee.

The file's raw length is converted once into `file_end: LogFilePosition`. Start,
plausible-boundary and accepted-end comparisons remain in the log-position domain;
`get()` extracts byte counts at seek and truncation calls. `ValidationFile` itself
continues to express file lengths as `u64`.

**Required capabilities.** Log recovery reads, seeks, truncates and synchronizes;
it does not write record bytes or require `AsyncWrite`. The shared file trait
supports those operations for Tokio files and explicit wrappers through the same
generic implementation, with static dispatch and no mandatory `Send`/`Sync` bounds.
It supplies capabilities, not exclusivity or checkpoint certification. The unit
struct has no constructor, locks or persistent recovery state.

</details>

### Accepted records and repair

1. Check the supplied boundary and seek to it.
2. Accept each following record only after encoding, CRC, expected stream and exact
   next-sequence checks succeed.
3. Stop at clean EOF, the first content violation or the file's 100,000-record cap.
   Preserve every record accepted before that boundary; never skip corruption.
4. If content is invalid, truncate to the last accepted record's exclusive end,
   or zero when no record was accepted. Synchronize the file before returning.

Clean, empty and fully trusted files also complete synchronization before success.
Bytes after the assigned record range are an invalid tail, even if they contain
complete, individually valid records. Payloads remain opaque.

| Finding | Recovery outcome |
| --- | --- |
| Clean EOF at an accepted boundary | Keep the file and return no removed-tail diagnostic. |
| Short header/record, invalid length or stream domain, or CRC failure | Remove that record and all following bytes; report `LogTailError::Record`. |
| Wrong stream, duplicate or skipped sequence | Remove that record and all following bytes; report `LogTailError::UnexpectedRecordId`. |
| Any bytes beyond the file's assigned record count | Remove the extra bytes; report `LogTailError::ExtraData`. |
| Operational read failure | Return an error with no successful findings and no truncation. |

<details>
<summary>Design and maintenance notes</summary>

**Validation responsibilities.** The production `RecordReader` owns framing,
stream-ID domain and CRC checks under the shared
[record specification](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/src/record/README.md#wire-and-file-contract).
The [reader's valid-prefix behavior](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/src/record_reader/README.md#reading-eof-and-failures)
exposes good records before a later content error in the same batch. Log recovery
adds the expected identity and file count from the
[location specification](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/location/README.md#assigned-sequence-ranges).
An earlier sequence violation therefore wins over a later encoding violation,
and accepted endpoints do not depend on read fragmentation.

Reader errors are classified exhaustively. Framing, domain and CRC failures are
content findings; `Io` and `ReaderFailed` abort the scan and discard its partial
findings. An inability to read bytes supplies no evidence that deleting them is
correct. The complete content diagnosis reaches the public validator before any
truncation is attempted.

**One accepted endpoint drives the scan.** The private `scan_records` owns the
suffix vector and returns one `LogScanResult` with the accepted endpoint, new
record ends and optional first tail error. The endpoint starts at `last_trusted`.
Its record ID supplies the next expected ID, and its exclusive position supplies
the next record's byte start. With no endpoint, those are the file's first ID and
`LogFilePosition::START`.

The `count < RECORDS_PER_FILE` guard keeps that next record in the current file,
so constructing its start directly avoids the general rotation calculation in
`next_record_start()`. After matching its ID, `RecordStartLocation::to_end` computes
the end once from the actual encoded length. Acceptance advances the count and
endpoint together and appends that end to the suffix vector. No separate mutable
byte position or expected ID can drift out of agreement, and no second `to_next()`
calculation is needed. The caller receives the complete findings rather than
reconstructing an endpoint from a count or offsets.

**Diagnosing a full file.** At the record cap, the scanner compares the accepted
end with the original whole-file EOF, including trusted bytes. This detects every
extra byte without an EOF probe, including when the entire file was already
trusted. The comparison uses the last record's end in this file; its next start
would rotate to byte zero in another file. A full file must have an accepted
endpoint, so the implementation asserts that invariant rather than supplying a
fallback position.

A missing tail error is a complete clean-end diagnosis; the caller need not
interpret whether scanning stopped at EOF or the cap. EOF is not rechecked for
external changes, which the exclusive-access contract excludes.

</details>

### Results, ownership and failure

`ValidatedLogFile<F = tokio::fs::File>` exposes findings through read-only accessors.
Only the validator can construct it, after any required truncation and successful
synchronization.

| Accessor | Meaning |
| --- | --- |
| [`validated_end()`](ValidatedLogFile::validated_end) | Last accepted record, including the trusted prefix; `None` for an empty log. |
| [`suffix_ends()`](ValidatedLogFile::suffix_ends) | Borrowed absolute exclusive ends of newly accepted records only, in order. An empty slice can accompany a nonempty trusted prefix. |
| [`removed_tail()`](ValidatedLogFile::removed_tail) | The content violation whose tail was successfully removed, or `None` for a clean file. |
| [`into_inner()`](ValidatedLogFile::into_inner) | Return the owned file and discard the findings without more I/O. Seek explicitly before subsequent reads or appends. |

`LogFileValidationError` distinguishes `InvalidStart`, direct file-operation `Io`
and operational record-reading `Read` errors, retaining the original concrete
sources. Invalid inputs and read failures do not permit truncation. Repair or
synchronization failures return no successful result even after changes complete.

The validation future owns the file until success. Error, panic or cancellation
drops that ownership; an unpolled future drops the file without starting I/O.
Cancellation can leave completed repair and outstanding OS work, which the owner
must quiesce before another recovery attempt. Recovery is not atomic, and log data
synchronization does not synchronize the paired index or parent directory or
publish a checkpoint.

<details>
<summary>Design and maintenance notes</summary>

**Findings become public only after completion.** The scanner's `tail_error` is
provisional: truncation can fail, and a successfully shortened file can still fail
synchronization. Only after both succeed does it become `removed_tail` in a public
result. This keeps content diagnosis separate from a claim that repair completed.
There is no reusable validator instance, cached report or readiness flag.

The result moves the scanner's offset vector without copying it. Extracting the
file consumes the result and drops the findings; it makes no new durability claim.
Reader read-ahead can leave the cursor beyond the accepted end, and truncation does
not reposition it, so explicit seeking is part of the caller's next operation.
The same concrete file type is retained throughout, with Tokio's `File` as the
result's default type argument.

</details>

## Performance

The scanner uses the production reader and releases records as it advances. It
retains at most 100,000 new offsets, or 800,000 bytes of values, plus vector capacity
and the reader's buffering; it never retains the whole log. Trusted records need
no offset entries or repeated scan. A fully trusted file needs no record reads,
but still requires length inspection, seeking and synchronization.

`LogFilePosition` has the `u64` representation, so typed suffix entries add no
per-entry storage or conversion buffer. The vector moves into the result and the
index validator borrows it directly. Computing each accepted end once and avoiding
file-rotation arithmetic simplify the source; no runtime speedup or recovery
throughput has been measured.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked log_file_validator
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The service is a binary crate, so ordinary Cargo tests skip documentation examples.
The [stream documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
describes checking them with `rustdoc --test` against a temporary library build.

<details>
<summary>Design and maintenance notes</summary>

Independent encoded fixtures distinguish recovery expectations from production
encoding. The coverage establishes:

| Contract | What the tests establish |
| --- | --- |
| Trusted starts | Wrong stream/file, absent trusted extent, positions beyond EOF and implausible minimum/maximum extents leave all bytes unchanged. Certified prefix bytes are not rescanned. |
| Content boundaries | Every partial length of a sample tail record, invalid length/domain fields and CRC failures preserve exactly the preceding valid records. |
| Identity and error ordering | Wrong streams, duplicate/skipped sequences and multiple violations retain the earliest content diagnosis, including within one reader batch. |
| Fragmentation | Different read chunk sizes produce identical accepted endpoints, suffix offsets and diagnostics. |
| File capacity | Full scans across refills, nonzero file numbers, nearly full and fully trusted prefixes distinguish clean full files from extra complete or partial bytes. Public calls actually remove those bytes. |
| Maximum record size | Clean and truncated records spanning refills preserve exact offsets and retained bytes, with and without a trusted prefix. |
| Operational failures | Read failures after valid records discard scan findings; failed truncation returns no validated result. Concrete error sources survive. |
| Synchronization | Empty, clean, fully trusted and repaired files wait for sync. Sync failure returns no validated file, even after truncation has completed. |
| Cancellation | Unpolled validation starts no I/O; cancellation during scanning or at length, truncation and sync boundaries cannot publish successful findings. Completed progress remains observable. |

The parent module's
[shared test file](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/validation/README.md#shared-test-file)
uses explicit probes over real Tokio files to pause or fail operations at API
entry boundaries. Ordinary filesystem cases pass Tokio files directly. Tests
check exact retained bytes and completed operations, including truncation before
cancelled synchronization; the usage example checks a repaired file against a
literal valid record. These checks do not simulate cancellation within kernel
operations, power loss or every filesystem failure.

</details>

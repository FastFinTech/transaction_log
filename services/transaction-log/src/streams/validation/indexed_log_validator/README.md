# Indexed log validator

`IndexedLogValidator` recovers one existing log and its paired dense index by
composing `LogFileValidator` and `IndexFileValidator`. It has no constructor or
persistent state: one associated async method opens the pair, runs both validators
and returns a trusted file-local endpoint. Partial files also return their handles
positioned for appending. Complete files release both handles.

The [stream initializer](../../stream_initializer/README.md) uses this component
to validate successive pairs and then cleans up discarded pairs. Fresh-pair
preparation, checkpoint advancement and startup integration remain unfinished there. This component
does not enumerate files, create new logs, delete future files,
load or publish checkpoints, or decide when a stream is ready.

## Source map

- `indexed_log_validator.rs`: the stateless type, its one operation and
  same-file integration tests.
- `validated_file_pair.rs`: the complete/partial result with ownership of partial
  file handles.
- `indexed_log_validation_error.rs`: storage, log and index failures, retaining
  the original typed errors.
- `mod.rs`: declarations, re-exports and this README's Rustdoc inclusion.

The types are re-exported through both `streams::validation` and `streams`.
The [storage specification](../../../storage/README.md) owns paths and opening
semantics. The [log validator](../log_file_validator/README.md) owns record scanning
and truncation; the [index validator](../index_file_validator/README.md) owns trusted
entry checks and suffix output. This component does not duplicate either algorithm.

## API and trusted boundary

```no_run
use transaction_log::{
    storage::StorageProvider,
    streams::{LogFileId, IndexedLogValidator, RecordEndLocation, ValidatedFilePair},
};

async fn recover_one(
    storage: &StorageProvider,
    file_id: LogFileId,
    last_trusted: Option<RecordEndLocation>,
) -> Result<ValidatedFilePair, transaction_log::streams::IndexedLogValidationError> {
    IndexedLogValidator::validate(storage, file_id, last_trusted).await
}
```

`last_trusted` comes from a checkpoint certifying prior validation and synchronization
of both files. It must belong to the requested `file_id`. The trusted log prefix
is checked for plausible extent, and its last index entry must contain the trusted
byte position; preceding certified contents are not rescanned. `None` starts at
this file's first assigned record, including for a nonzero file number.

Both validators receive the **same original trusted endpoint**. The index validator
also receives `ValidatedLogFile::suffix_ends()`, which contains only newly accepted
records. Passing the newly discovered final endpoint as the trusted boundary would
incorrectly require the existing index to certify records that still need indexing.
An empty suffix still removes obsolete index entries after the trusted boundary.

## Operation and result

1. Borrow `StorageProvider` to open the existing log with read/write access, then
   open or create its index. Opening both precedes content recovery. A missing log
   returns the provider's path-bearing `NotFound` error without creating an index;
   interpreting a file gap belongs to the caller.
2. Validate the log, remove its corrupt tail and synchronize its data. Operational
   read failures remain errors rather than authorization to truncate.
3. Check the trusted index entry, replace its entire following suffix with the
   newly accepted ends, flush the output and synchronize the index. A missing index
   can be rebuilt when `last_trusted` is `None`. With a trusted endpoint, an absent
   or mismatched trusted entry is an error; there is no implicit checkpoint fallback.
4. Return `ValidatedFilePair::Complete { end }` for exactly 100,000 accepted records,
   releasing both handles. Otherwise return `Partial { end, log, index }`, seeking
   the log to its accepted byte end and the index to its accepted entry count times
   the shared entry width. Both cursors are at EOF, including zero for empty files.

The returned endpoint describes the last accepted record in this file and the
exclusive byte end in this file's log. It certifies the synchronized pair, but does
not itself publish stream state. Corrupt-tail diagnostics and the temporary suffix
vector are not retained in this result.
The partial variant owns both `File` values directly. This result is handled one
file at a time, so a larger enum is preferable to boxing handles just to reduce
the complete variant's unused space.

A caller recovering successive files supplies the checkpoint only to its own file
and `None` for following files. Each complete result advances the caller's trusted
stream endpoint. A partial result stops that caller's scan and supplies the files
to continue. If the partial file is empty, its `end` is `None`: the caller retains
the preceding complete file's endpoint for the stream. If every existing file is
complete, choosing and creating the next log is also caller work. No range loop or
stream-wide endpoint state is implemented here.

## Ownership, durability and failure

The caller must finish earlier I/O and exclude concurrent file access throughout
recovery and handover. Borrowing the provider does not lock storage. The operation
owns each opened handle and relies on the lower validators' exclusive-access
contract; there are no external-change guards or shared mutable validator state.

Success requires log synchronization followed by index output, flushing and
synchronization. Partial success additionally requires both final seeks. Complete
results retain neither handle nor offsets. Directory entries are not synchronized
here: a newly created index's directory durability and checkpoint publication remain
the recovery owner's responsibility. File synchronization is not an atomic pair commit.

`IndexedLogValidationError` preserves storage errors (including paths), log
errors and index errors. Final cursor errors use the corresponding log/index I/O
error. Failure returns no pair, but the index may have been created or the log may
already have been truncated and synchronized before index validation fails. There
is no rollback and no automatic retry. A cancelled operation can leave partial
changes and outstanding OS I/O; the caller must quiesce it before a new attempt.
An unpolled future performs no file opening or mutation.

## Testing and cost

Same-file tests use the real storage provider and temporary files. Independent
record fixtures and expected little-endian index bytes verify the handoff, with
tests for every trusted boundary in a sample pair, empty/corrupt logs, stale or
missing indexes, append positions, repeated recovery, full-file completion and
nonzero file numbers. Returned handles are used to append a record and its index
without seeking. Boundary tests include nearly full and fully trusted files and
extra bytes beyond a full log.

Error tests preserve missing-log and index-open causes, reject invalid trusted log
boundaries, and verify missing/mismatched trusted index entries after successful log
repair. Scope tests ensure checkpoints and later file pairs are untouched. The
lower validators' [shared test file](../README.md#shared-test-file) separately covers
short I/O, failures, cancellation and synchronization before success. These tests
do not simulate power loss or failures inside every kernel operation.

This is startup recovery. It scans through the existing record reader, borrows the
discovered offsets for index output without copying their vector, and drops findings
before returning. It retains no stream-wide list or full log image. Index suffixes
are unconditionally rewritten even when already correct. No recovery throughput
measurements or hot-path performance claims are made.

```powershell
cargo test -p transaction-log streams::validation --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

Follow the [streams validation guidance](../../README.md#verification-and-performance)
for release checks and running documentation examples in this binary crate.

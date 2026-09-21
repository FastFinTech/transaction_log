# Stream validation

Recover persisted log/index pairs from an explicitly trusted checkpoint boundary.
The focused validators own inspection and repair of each file; the combined
validator returns a synchronized pair ready for handover. Stream-wide recovery
and checkpoint publication belong to the stream initializer.

## Types and modules

| Type or module | Responsibility |
| --- | --- |
| [`ValidationFile`] | Shared reading, seeking, length, truncation and synchronization capabilities for focused recovery. |
| [`LogFileValidator`] / [`ValidatedLogFile`] | Scan a log, remove a corrupt tail and return the synchronized file with accepted record ends. |
| [`IndexFileValidator`] | Check the trusted index entry and replace its following suffix from accepted log ends. |
| [`IndexedLogValidator`] / [`ValidatedFilePair`] | Open and recover one stored pair; return append-positioned handles when partial. |
| [`LogTailError`] | Describe content corruption that was removed successfully. |
| [`LogFileValidationError`], [`IndexFileValidationError`], [`IndexedLogValidationError`] | Preserve invalid-boundary and operational failures at the appropriate recovery layer. |

The [log](self::log_file_validator), [index](self::index_file_validator) and
[pair](self::indexed_log_validator) specifications own their detailed algorithms
and completion guarantees. [`StreamInitializer`](crate::streams::StreamInitializer)
coordinates successive pairs, cleanup, checkpoint advancement and active-pair
preparation. Application startup integration remains unimplemented.

## Usage

The shared file trait lets a focused validator recover a Tokio file or a concrete
wrapper while preserving its type:

```no_run
use transaction_log::streams::{
    LogFileId, LogFileValidationError, LogFileValidator, RecordEndLocation,
    ValidatedLogFile, ValidationFile,
};

async fn recover_log<F: ValidationFile>(
    file: F,
    file_id: LogFileId,
    last_trusted: Option<RecordEndLocation>,
) -> Result<ValidatedLogFile<F>, LogFileValidationError> {
    LogFileValidator::validate(file, file_id, last_trusted).await
}
```

Index recovery additionally requires `AsyncWrite`. For a pair acquired through
[`StorageProvider`](crate::storage::StorageProvider), use `IndexedLogValidator` to
coordinate both operations and establish append positions.

<details>
<summary>Design and maintenance notes</summary>

**Capabilities follow the work.** The focused operations receive already-open
files; the combined operation acquires those files through storage. This keeps
path selection and opening semantics with the provider while letting file wrappers
exercise the same validation code as ordinary Tokio handles.

Log recovery needs truncation but writes no record bytes. Index recovery writes
replacement entries, so only that operation adds `AsyncWrite`. A shared file trait
avoids separate validator-specific abstractions for the same length and completion
contracts. Generic calls use static dispatch and concrete futures without boxing
or mandatory `Send`/`Sync` bounds.

</details>

## Behavior and guarantees

### Shared file capabilities

`ValidationFile` combines Tokio `AsyncRead`, `AsyncSeek`, the exports crate's
`AsyncSyncData`, and `Unpin`. It adds two operations:

| Operation | Contract |
| --- | --- |
| `length()` | Return the byte length without changing contents or cursor. |
| `set_len(length)` | Set the exact byte length without repositioning the cursor. Shrinking removes the tail; extending adds zero bytes. Success means the change has completed. |

All operations address the same underlying file and cursor, defer I/O until polled
and preserve original I/O errors. Resizing does not establish durability; each
validator synchronizes separately before success. Cancellation need not stop
underlying OS work.

The caller supplies exclusive access, finishes flushing earlier writes and
establishes checkpoint certification. The trait itself guarantees none of those
conditions. The focused validators return ownership of the same concrete type
only on success; recovery failure can leave partial file changes.

<details>
<summary>Design and maintenance notes</summary>

**Completion belongs in the capability contract.** A validator can rely on a
successful resize before its next seek or synchronization only if the length
change has actually completed. Merely scheduling a resize would break that order.
The Tokio implementation delegates to metadata and the inherent `set_len` method,
and uses the existing `AsyncSyncData` implementation for data synchronization.
No alternate file semantics are introduced by the wrapper boundary.

</details>

### Trust and recovery order

A checkpoint certifies prior validation and synchronization of the covered log
and index. Both focused validators receive that same original endpoint; earlier
certified contents are trusted. Log validation supplies only the newly accepted
suffix ends to index recovery. Operational read failures remain errors and never
authorize deletion of unread content.

The combined operation synchronizes the recovered log before replacing, flushing
and synchronizing the index. Its partial result additionally positions both
handles for appending. Synchronizing files does not make recovery atomic across
the pair or publish a checkpoint; the stream initializer owns the later publication.

<details>
<summary>Design and maintenance notes</summary>

**Typed handoff.** Log recovery retains its accepted endpoint and a vector of
`LogFilePosition` values. Index recovery borrows that vector as a slice without
conversion or copying. The original trusted boundary selects the index prefix;
substituting the log's newly discovered final endpoint would demand trust in
entries that still need to be written.

Log EOF, accepted ends and decoded index values all describe positions in a log.
Index-file addresses and lengths remain raw `u64` byte counts in the index.
Preserving those domains through recovery prevents an index cursor from being
mistaken for a log position. The combined validator extracts raw log offsets only
when seeking, while the initializer passes typed endpoints through publication
and handover.

</details>

## Performance

Recovery reuses the production record reader and index writer. It scans one pair
at a time, borrows accepted offsets for index output and retains no whole log image
or stream-wide file list. Index suffix replacement is unconditional, including
already-correct entries. Recovery throughput has not been measured; the focused
specifications describe their buffering and synchronization costs.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::validation
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The [stream validation guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
also covers release checks and documentation examples in this binary crate.

### Shared test file

The focused suites use explicit file probes to observe partial progress and
synchronization. The combined suite uses real provider paths to check file
acquisition, repaired bytes and append positions together.

<details>
<summary>Design and maintenance notes</summary>

**Controlled operations over real files.** The test-only `TestFile` implements
`ValidationFile` around a Tokio file. Each case supplies its own `Rc<FileProbe>`;
there is no global or thread-local registration and no test-specific branch in
either validator. `FileOperation` identifies length lookup, truncation, writing,
flushing and synchronization gates. Reads and seeks pass through directly.

The wrapper can pause or fail operations at API entry boundaries and limit
accepted write sizes. It records operation starts and successful completions;
write completion means acceptance, not flushing or durability. Repeated write or
flush polls can produce repeated start observations. Each file has its own probe.

`wait_for_pause` drives a validator until the configured gate, failing if it
completes first. Tests explicitly resume and poll the operation, making completion
order observable. A dropped wrapper retains its handle in the probe so pending
writes can be quiesced before retained bytes are inspected. This cleanup is a test
mechanism, not a guarantee that cancellation stops OS work or makes repair atomic.
The wrapper controls I/O; recovery decisions stay in the validators.

The suites check short writes, concrete errors, cancellation, exact surviving
bytes and synchronization before success. Ordinary filesystem cases pass Tokio
files directly. Combined recovery uses owned storage fixtures for isolation and
cleanup while checking complete/partial results, cursor positions and error
propagation. These tests do not simulate power loss or cancellation inside kernel
operations. The component specifications explain which observable contracts each
suite establishes.

</details>

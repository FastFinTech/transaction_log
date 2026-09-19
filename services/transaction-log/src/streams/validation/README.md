# Stream validation

This module collects focused components that validate and repair persisted stream
files. `IndexFileValidator::validate` owns an index file, checks the checkpoint's
trusted entry, unconditionally replaces its following suffix and returns the file
after synchronization. It has no constructor or persistent state.

[`LogFileValidator`](log_file_validator/README.md) scans and removes corrupt log
tails through an associated async function that owns its input file and returns
the synchronized file and accepted record ends. Its scanner produces the complete
content findings, including any extra bytes beyond the record limit. Both
validators receive the same trusted endpoint from the stream checkpoint, which
certifies prior validation and synchronization of the covered log and index.
[`IndexedLogValidator`](indexed_log_validator/README.md) passes the
accepted suffix offsets to `IndexFileValidator::validate`, returning a synchronized
pair's endpoint and, for partial files, handles positioned for appending. It opens
one existing log/index pair through a borrowed `StorageProvider`. Checkpoint
publication and stream/startup orchestration remain separate work.

The focused log/index validators receive already-open files and domain values;
the combined validator acquires those files through `StorageProvider`. Storage path
selection and opening semantics remain in the provider; log-record decoding remains
with the record reader. Components here must preserve exclusive ownership across
inspection and repair, fail closed after incomplete
I/O, and distinguish content mismatch from operational failure.

See the
[index-file validator specification](index_file_validator/README.md) for its API,
dense-index format, ownership, suffix replacement and tests.

## Shared file capabilities

`validation_file.rs` owns `ValidationFile` and its Tokio file implementation. The
trait is re-exported through `streams::validation` and `streams`. The log and index
validators are generic over the supplied file and return ownership of the same concrete type;
Tokio files and explicitly supplied controlled test files run through the same
implementation. Type inference keeps ordinary calls unchanged.

`ValidationFile` combines Tokio's `AsyncRead` and `AsyncSeek`, the exports crate's
existing `AsyncSyncData`, and `Unpin`. It adds only `length()` and `set_len(length)`.
Length inspection must preserve contents and cursor. Resizing truncates or extends
with zero bytes without repositioning the cursor; success means the length change
has completed, not merely been scheduled. It does not establish durability. All
operations must address the same underlying file and cursor, preserve concrete
I/O errors and defer I/O until polled. Cancellation may leave outstanding work.

The Tokio implementation delegates to metadata and the inherent `set_len` method,
and uses the existing `AsyncSyncData` implementation. Index validation additionally
requires `AsyncWrite` to output its suffix. Log validation only reads, seeks,
truncates and synchronizes. This separates required capabilities without separate
validator-specific file traits.

Generic calls use static dispatch and concrete futures without boxing or mandatory
`Send`/`Sync` bounds. The traits supply I/O capabilities; trusted records, valid
offsets, exclusive access and checkpoint certification remain caller responsibilities.
Recovery throughput has not been measured.

## Shared test file

`test_file.rs` is compiled only for unit tests. It contains `TestFile`, `FileProbe`,
`FileOperation` and `wait_for_pause`, shared by both validators' same-file tests.
Each test explicitly constructs a file with its own `Rc<FileProbe>` and passes it
to the generic validator. There is no global or thread-local registration and no
test-specific branch in either validator.

The wrapper delegates successful operations to a real Tokio file. It can pause or
fail length lookup, truncation, writes, flushing and synchronization at API entry
boundaries, and cap accepted write sizes. Reads and seeks pass through directly.
It records starts and successful completions; write completion means acceptance,
not flushing or durability. Repeated write/flush polls can appear more than once
in the start observations. Each file gets a separate probe.

Tests drive these futures locally and explicitly resume/poll them after a pause.
`wait_for_pause` drives either validator until its configured gate, failing if the
operation returns first. A dropped wrapper retains its handle in the probe so tests
can quiesce pending writes before inspecting bytes. This cleanup is test-only and
does not promise atomic repair or cancellation of OS work. The wrapper implements
file controls, never recovery decisions. Component tests cover short writes,
errors, cancellation, exact retained bytes and synchronization before success;
they do not simulate power loss or cancellation inside a kernel operation.

The combined validator's tests use real storage paths and files to exercise the
handoff, complete/partial results, cursor positions and error propagation. It does
not introduce a storage abstraction or test-specific execution path.

Run all validation suites with:

```powershell
cargo test -p transaction-log streams::validation --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

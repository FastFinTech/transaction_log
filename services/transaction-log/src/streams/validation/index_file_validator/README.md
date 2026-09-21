# Index file validator

Check the trusted boundary of an index, replace its suffix with record ends
supplied by log validation, and return the synchronized file. This is the index
recovery step used by the combined log/index validator.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`IndexFileValidator`] | Check the trusted entry and rebuild the following index suffix through one associated `validate` operation. |
| [`IndexFileValidationError`] | Distinguish excessive counts, a missing or mismatched trusted entry, and file/output failures. |

The shared [`ValidationFile`](crate::streams::ValidationFile) trait supplies file
capabilities. [`IndexWriter`](crate::streams::IndexWriter) owns the dense encoding
and buffered output; [`LogFileValidator`](crate::streams::LogFileValidator) supplies
validated record ends, and [`IndexedLogValidator`](crate::streams::IndexedLogValidator)
coordinates both directions of recovery for one stored pair.

## Usage

To rebuild an index with no trusted prefix, pass the consecutive record ends
already established by log validation. The operation replaces the entire index,
including any stale entries or partial trailing bytes:

```rust
use transaction_log::streams::{IndexFileValidator, LogFilePosition};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let directory = tempfile::tempdir()?;
# let path = directory.path().join("rebuild.idx");
# tokio::fs::write(&path, [0xaa; 25]).await?;
# let file = tokio::fs::OpenOptions::new().read(true).write(true).open(&path).await?;
let suffix_ends = [LogFilePosition::new(16), LogFilePosition::new(35)];
let repaired_file = IndexFileValidator::validate(file, None, &suffix_ends).await?;
// Ownership returns only after the replacement has been flushed and synchronized.
# drop(repaired_file);
# assert_eq!(tokio::fs::read(&path).await?, [
#     16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0,
# ]);
# Ok(())
# }
```

The caller supplies an already-open file, finishes flushing earlier writes, and
excludes other access throughout recovery. The input offsets are trusted results
from the log; this operation does not establish their record validity.

<details>
<summary>Design and maintenance notes</summary>

**Continuing from a checkpoint.** Both focused validators receive the same original
checkpoint-certified endpoint. The log result's `suffix_ends()` contains only the
newly accepted records after that boundary and can be borrowed directly:

```no_run
use tokio::fs::File;
use transaction_log::streams::{
    IndexFileValidationError, IndexFileValidator, RecordEndLocation, ValidatedLogFile,
};

async fn repair_index(
    index: File,
    last_trusted: Option<RecordEndLocation>,
    validated_log: &ValidatedLogFile,
) -> Result<File, IndexFileValidationError> {
    IndexFileValidator::validate(index, last_trusted, validated_log.suffix_ends()).await
}
```

Passing `validated_log.validated_end()` as the trusted boundary would instead ask
the old index to certify records that still need indexing. The unchanged original
boundary identifies the prefix that may be trusted; the borrowed slice describes
its replacement suffix. No copied offset vector or separate repair plan is needed.

</details>

## Behavior and guarantees

### Inputs and trust

[`validate`](IndexFileValidator::validate) takes ownership of a concrete
`F: ValidationFile + AsyncWrite` and returns `Result<F, IndexFileValidationError>`.
Tokio files satisfy the bound directly, and callers normally infer the file type.

| Input | Required meaning |
| --- | --- |
| `file` | The exclusively accessed index file, with earlier writes flushed and operations available for seeking, truncation, writing and synchronization. A trusted-entry check also needs read access. |
| `last_trusted` | An optional endpoint certified by prior validation and synchronization of this log/index prefix. `None` means there is no trusted prefix. |
| `suffix_ends` | One absolute exclusive log end for each consecutive record accepted after that same trusted boundary. |

The trusted endpoint must describe this file. The caller establishes checkpoint
certification and the suffix's agreement with the log. This validator checks the
combined entry count and, when present, the trusted entry's stored value; it does
not rescan the earlier prefix or validate records and offsets independently.

An empty suffix removes everything after the trusted prefix. With no trusted
prefix and no suffix, the result is an empty synchronized index. All entries use
the shared [dense index format](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/index_writer/README.md#dense-index-layout).

<details>
<summary>Design and maintenance notes</summary>

**Capabilities and ownership.** The parent module's
[`ValidationFile`](crate::streams::ValidationFile) combines reading, seeking and
data synchronization with length inspection and resizing. Index recovery adds
`AsyncWrite` for the replacement bytes. All operations refer to the same file and
cursor; the trait does not establish exclusive access or trusted contents.

The handle needs OS read permission only when checking a trusted entry. Without
one, whole-file replacement reads neither existing bytes nor file metadata, so a
write-only Tokio handle can perform it. A read-only handle cannot repair the index,
even if its existing entries happen to match.

The operation accepts file wrappers through the same generic path and returns the
same concrete type. Static dispatch and concrete futures require no boxing or
mandatory `Send`/`Sync` bounds. The public unit struct groups the operation without
a constructor or persistent validation state. It neither locks the file nor detects
external changes; the recovery owner excludes concurrent access.

</details>

### Recovery sequence

1. Reject a trusted-prefix count plus suffix count above 100,000 before any I/O.
2. If a trusted endpoint exists, require its complete index entry to contain the
   certified log position. Reject a missing or unequal entry before changing bytes.
3. Truncate to the trusted prefix, seek to its end, and write every supplied suffix
   offset through `IndexWriter`. Existing suffix values are never read or compared.
4. Complete buffer output, flush the file, synchronize its data, and return ownership.

Empty and already-correct indexes also complete synchronization before success.
A successful file can be passed to another call with the same inputs: the bytes
remain identical, although replacement and synchronization run again. The API
makes no append-cursor guarantee; callers seek explicitly before later use.

<details>
<summary>Design and maintenance notes</summary>

**Locating the trusted entry.** `LogFileId::record_count_through` derives the
one-based count within the trusted record's assigned file, including nonzero file
numbers. The count minus one selects the boundary entry. The private lookup checks
whether the file length covers all eight bytes before seeking and reading exactly
that entry. A missing or partial entry becomes `None`; seek/read failures, including
a short read after the length check, remain operational errors.

The decoded value is a `LogFilePosition`, while the index entry address and
truncation length remain `u64` byte counts in the index file. Keeping those domains
separate avoids treating an index address as a log position. The shared entry-width
constant supplies their encoding width; the validator introduces no alternate
format. File length and trusted count are local to the operation.

**Why replace the whole suffix.** The log is authoritative, and its validation
already discovers every needed end position. Unconditional replacement removes
old-tail reads, value comparison, matching-prefix tracking and a separate repair
plan. Stale complete entries and partial trailing bytes disappear through the same
truncation and output sequence. Rewriting correct entries and synchronizing every
processed file is the deliberate cost of this simpler recovery algorithm.

**Pair and stream coordination.** `IndexedLogValidator` supplies the same original
trusted endpoint to both validators and passes the log's accepted suffix directly
to this operation. It returns synchronized pair results without publishing a
checkpoint. The implemented [`StreamInitializer`](crate::streams::StreamInitializer)
coordinates successive pairs, cleanup and checkpoint advancement. Application
startup integration remains separate work. This focused operation neither chooses
a checkpoint nor opens storage paths or invokes log validation itself.

</details>

### Completion, errors and cancellation

| Error | Meaning and effect |
| --- | --- |
| `TooManyEntries` | The combined count exceeds one file's assigned capacity; reject before I/O. |
| `TrustedIndexMismatch` | The certified entry is absent or unequal; retain its expected and optional actual position and leave file bytes unchanged. |
| `Io` | Preserve a metadata, seek, read, truncation or synchronization error. Replacement may already have changed bytes. |
| `Write` | Preserve an `IndexWriter` failure during suffix output or file flushing. A prefix may already have been accepted. |

Any error returns no file. Panic or cancellation drops the operation's ownership;
there is no successfully validated handle to reuse. Dropping an unpolled future
also drops its owned handle, without starting I/O.

Replacement is not atomic. After an interrupted repair, recovery must coordinate
outstanding file operations before retrying from the unchanged trusted checkpoint.
Failure cannot authorize checkpoint advancement or stream readiness. Successful
index synchronization alone neither commits a log/index pair atomically nor
synchronizes its parent directory or publishes a checkpoint.

<details>
<summary>Design and maintenance notes</summary>

**Progress remains inside the owned operation.** There is no validator usability
flag or reusable instance: the future owns the file until success. Count rejection
and a trusted-entry mismatch preserve bytes, but still do not return the handle.
Later errors can leave a truncated or partly rewritten suffix, and even completed
replacement bytes do not establish success if synchronization fails.

A pending future may continue its current operation. Cancelling it does not promise
that underlying OS work stopped, so the file-owning layer must quiesce outstanding
work before reopening or observing the repaired state. The log can supply the
suffix again after that coordination. This recovery retry differs from blindly
replaying the in-flight writer's buffer on an uncertain cursor.

Typed errors retain the original concrete I/O or writer source. A trusted-entry
mismatch retains its zero-based entry index, expected typed log position and
optional actual value; diagnostics display full-width numeric offsets and
separate an absent entry from a stored value.

</details>

## Performance

The operation reads at most one trusted index entry, then rewrites the entire
supplied suffix. With no trusted boundary, it reads no old index data or metadata.
A complete ordinary index contains at most 100,000 entries, or 800,000 encoded
bytes, bounding the replacement work for one file.

The offset slice is borrowed directly and encoded into `IndexWriter`'s reusable
batch buffer. There is no input buffer for old index bytes or separate owned repair
plan. No recovery-throughput measurements are available; the rewrite and sync
costs are design tradeoffs rather than measured performance gains.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked index_file_validator
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The service is a binary crate, so ordinary Cargo tests skip documentation examples.
The [stream documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
describes checking them with `rustdoc --test` against a temporary library build.

<details>
<summary>Design and maintenance notes</summary>

Independent byte fixtures distinguish the expected encoding from typed input
positions and the production writer. The coverage includes:

| Contract | What the tests establish |
| --- | --- |
| Trusted boundary | Every incomplete prefix length, absent/unequal entries and original read errors preserve existing bytes. |
| Suffix replacement | Missing, partial, matching, wrong and excessive tails are replaced while the trusted prefix remains unchanged. |
| Repeated calls | The explicitly supplied checkpoint governs each call, including shorter, empty and whole-file replacements. |
| Encoding and errors | Literal little-endian bytes preserve full-width offsets; mismatch diagnostics distinguish absence from a stored position. |
| Count limits | Empty, nearly full and full prefixes respect the exact 100,000-entry limit; rejection occurs before mutation and permits a corrected call after reopening. |
| File permissions | Whole-file replacement works without reading old entries; read-only replacement fails even for matching contents and returns no file. |
| Short and interrupted output | Short writes cross entry boundaries correctly; errors and cancellation at every incomplete prefix of two entries return no successful file. |
| Repair boundaries | Truncation, flushing and synchronization failures preserve concrete errors and the bytes actually completed. |
| Synchronization | Empty, matching, trusted and rebuilt indexes all wait for sync; sync failure returns no file even after replacement completes. |
| Future lifecycle | Unpolled futures perform no I/O; cancellation during trusted inspection or repair cannot publish successful validation. |

The parent module's
[shared test file](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/validation/README.md#shared-test-file)
supplies explicit probes for short writes, pauses and errors over real Tokio files.
Tests quiesce abandoned handles before inspecting bytes after partial output.
Pauses occur at API boundaries rather than inside kernel operations. Ordinary
filesystem tests use Tokio files directly, and the usage example checks a complete
replacement against literal bytes. These checks do not simulate power loss,
exhaust every OS failure or measure recovery throughput.

</details>

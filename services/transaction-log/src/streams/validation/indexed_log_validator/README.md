# Indexed log validator

Recover one stored log/index pair and return its synchronized endpoint. A partial
file also returns both handles positioned for appending; a full file releases
them. The stream initializer uses this operation to recover successive pairs
before publishing the stream checkpoint.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`IndexedLogValidator`] | Open one pair, coordinate log and index recovery, and prepare its result through the associated `validate` operation. |
| [`ValidatedFilePair`] | Distinguish a complete file from a partial file with owned append-positioned handles. |
| [`IndexedLogValidationError`] | Preserve storage, log and index failures, including failures during final positioning. |

[`StorageProvider`](crate::storage::StorageProvider) owns paths and file opening.
[`LogFileValidator`](crate::streams::LogFileValidator) owns scanning and tail removal;
[`IndexFileValidator`](crate::streams::IndexFileValidator) owns trusted-entry checks
and suffix replacement. [`StreamInitializer`](crate::streams::StreamInitializer)
owns recovery across files and checkpoint publication.

## Usage

Pass a provider, the existing log's identity and its optional checkpoint-certified
endpoint. Use `None` to recover the whole file:

```no_run
use transaction_log::{
    storage::StorageProvider,
    streams::{IndexedLogValidator, LogFileId, RecordEndLocation, ValidatedFilePair},
};

async fn recover_one(
    storage: &StorageProvider,
    file_id: LogFileId,
    last_trusted: Option<RecordEndLocation>,
) -> Result<ValidatedFilePair, transaction_log::streams::IndexedLogValidationError> {
    IndexedLogValidator::validate(storage, file_id, last_trusted).await
}
```

The caller finishes earlier I/O and excludes other access to the pair during
recovery and handover. The operation opens the files itself; borrowing the provider
does not lock storage. A missing log is an error, while a missing index can be
rebuilt when no trusted prefix is required.

<details>
<summary>Design and maintenance notes</summary>

**Recovering successive files.** The checkpoint applies only to its own file;
following files receive `None`. A complete result advances the recovery owner's
stream endpoint. A partial result stops the scan and supplies the handles to
continue writing. If that partial file is empty, its `end` is `None`, while the
stream still retains the preceding complete file's endpoint. For example, an
empty file 8 does not erase the progress established by complete file 7.

The implemented `StreamInitializer` owns this loop, interprets file gaps, removes
discarded later pairs, publishes the checkpoint and prepares an active pair. If
all existing files are complete, it selects and creates the next pair. Keeping
those decisions there lets this operation report exactly what one existing pair
contains without retaining stream-wide state. Application startup does not call
the initializer yet.

</details>

## Behavior and guarantees

### Inputs and trusted boundary

[`validate`](IndexedLogValidator::validate) borrows `&StorageProvider`, takes a
`LogFileId` and `Option<RecordEndLocation>`, and returns
`Result<ValidatedFilePair, IndexedLogValidationError>`.

| Input | Required meaning |
| --- | --- |
| `storage` | The provider for an existing, durably established directory hierarchy, under the caller's exclusive recovery access. |
| `file_id` | The existing log and paired index to recover. |
| `last_trusted` | An endpoint in this file certified by prior validation and synchronization of both files, or `None` to start at this file's first assigned record. |

The trusted log boundary must have a plausible extent, and its index entry must
contain the certified log position. Earlier bytes and entries remain trusted.
A checkpoint from a preceding file cannot be passed here. Starting with `None`
works for nonzero file numbers as well as file zero.

An absent or mismatched trusted index entry is an error, even after log recovery
succeeds. There is no automatic fallback to an earlier checkpoint or full rebuild.

<details>
<summary>Design and maintenance notes</summary>

**One original boundary, two validators.** Both focused validators receive the
same original `last_trusted`. The log result's `suffix_ends()` contains only records
accepted after it. Passing the newly discovered final endpoint to the index
validator would ask old index contents to certify records that still need indexing.
An empty suffix also carries a repair instruction: remove everything after the
trusted prefix.

The log never derives trust by inspecting the index. Prior checkpoint certification
permits both validators to skip the earlier prefix, while their boundary checks
reject supplied metadata that no longer agrees with the files. The combined
operation passes the typed `&[LogFilePosition]` directly between them without a
conversion or copied vector.

</details>

### Recovery order and handover

1. Open the existing log, then open or create its index. Both opens complete before
   content recovery starts. A missing log preserves the provider's path-bearing
   `NotFound` error and leaves the index untouched.
2. Validate the log, remove any corrupt tail and synchronize the accepted contents.
   Operational read failures abort without authorizing truncation.
3. Check the trusted index entry, replace the entire following suffix from the
   newly accepted log ends, flush its output and synchronize the index.
4. Return a complete result or position both partial-file handles at their accepted
   ends before returning them.

| Result | Contents and guarantee |
| --- | --- |
| [`Complete`](ValidatedFilePair::Complete) | Exactly 100,000 accepted records. `end` identifies the last record and its exclusive log end; both handles have been released. |
| [`Partial`](ValidatedFilePair::Partial) | Fewer than 100,000 accepted records. `end` is the last record or `None` for an empty file; owned `log` and `index` handles are positioned at their respective EOFs. |

Both results describe this file only and require successful log and index
synchronization. Partial handles can append without another seek. The result
retains neither the corrupt-tail diagnostic nor the temporary suffix-offset vector,
and it does not publish stream state.

<details>
<summary>Design and maintenance notes</summary>

**Opening before repairing.** The provider opens the log with read/write access
without create, truncate or append mode. It opens the index with read/write access,
creating it only when missing. Opening the log first prevents a missing log from
creating an orphan index; opening the index before validation prevents an index-open
failure from following an avoidable log repair. An index can nevertheless be
created before an invalid trusted boundary is rejected.

Index opening synchronizes the immediate parent on Unix before returning, including
when the index already exists: a prior interrupted recovery may have created that
entry. The caller supplies durability of the existing ancestor hierarchy. Windows
has no directory-durability guarantee here. These details belong to the provider's
[file-acquisition contract](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#file-acquisition-for-validation-and-repair);
file data still requires the subsequent validation and synchronization steps.

**Two cursor domains.** The log seek uses the accepted `LogFilePosition`, or
`LogFilePosition::START` for an empty file, extracting its raw value only at the
seek. The index seek uses the file-local accepted count multiplied by the shared
index entry width. That address is a `u64` byte count in the index, not a position
in the log. Explicit seeks remove any dependence on reader read-ahead or lower
validators' final cursor positions.

Fullness comes from the returned record's assigned position within its file,
using `LogFileId::record_count_through`; it does not use the global sequence as an
index length. A full file needs no append cursors, so its handles and findings are
dropped. The partial variant stores both Tokio `File` handles directly. One result
is processed at a time, so avoiding boxing is preferable to reducing the enum's
size solely for its complete variant.

</details>

### Ownership, durability and failure

The operation owns each opened handle until it is returned in a partial result or
released. It has no persistent validator state or external-change guards.

| Error | Meaning and possible progress |
| --- | --- |
| `Storage` | Opening or provider synchronization failed; preserve the path and concrete cause. Index creation may already have occurred. |
| `Log` | Log validation, repair, synchronization or final positioning failed. File changes may already have completed. |
| `Index` | Index validation, replacement, synchronization or final positioning failed. The log may already be repaired and synchronized. |

Every error returns no pair. Final cursor errors retain the corresponding log or
index I/O error. There is no rollback or automatic retry. Cancellation can leave
partial changes and outstanding OS work; the owner must quiesce it before trying
again. An unpolled future performs no file opening or mutation.

File synchronization does not make the pair an atomic commit or publish a
checkpoint. The caller owns directory-hierarchy durability and stream readiness;
only the provider's immediate-parent synchronization on Unix occurs here as part
of opening the index.

<details>
<summary>Design and maintenance notes</summary>

**Successful lower steps do not imply a returned pair.** Log synchronization
precedes index repair. A missing trusted index entry can therefore reject the pair
after a corrupt log tail has already been removed and synchronized. Likewise,
both files can be synchronized before a final seek fails. Returning no pair
prevents these partial outcomes from being mistaken for a completed handover,
without claiming that earlier work was undone.

The error enum wraps existing storage, log and index errors instead of flattening
their sources. This preserves path-based diagnosis for acquisition failures and
typed content-boundary or I/O details for the focused validators. Recovery of the
same pair is possible only after outstanding work has been coordinated and the
caller supplies a valid trusted boundary again.

</details>

## Performance

Recovery processes one pair at a time using the production record reader. The
combined operation borrows the discovered suffix offsets for index output and
drops those findings before returning. It retains no stream-wide list or whole
log image.

Index suffixes are rewritten even when already correct; clean and empty files
still require synchronization, and partial pairs require two final seeks. These
are recovery costs, not ingestion throughput measurements. No recovery-performance
measurements are available.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::validation
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The [stream validation guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
also covers release checks and running documentation examples in this binary crate.

<details>
<summary>Design and maintenance notes</summary>

Tests use real provider paths and files so they exercise opening order, paired
recovery and actual append positions together. Independent record fixtures and
expected little-endian index bytes make the accepted endpoint and index contents
checkable without relying on a matching encoder/decoder pair.

| Contract | What the tests establish |
| --- | --- |
| Trusted handoff | Every trusted boundary in a sample pair sends only new offsets with the unchanged original checkpoint. |
| Empty and corrupt files | Empty or entirely corrupt logs yield empty appendable pairs; later corruption removes its index entry and everything after it. |
| Missing indexes and cursor positions | Untrusted indexes are rebuilt; returned handles append a record and its index entry without seeking. |
| Repeated recovery | Reopening with the same original boundary preserves the already repaired pair. |
| Capacity and file identity | Full files return complete, nearly full files remain partial, and nonzero file numbers use file-local index lengths. Extra log bytes are removed. |
| Opening order | Missing logs retain `NotFound` without creating or changing an index; index-open errors retain their path and precede log repair. |
| Rejected trust | Invalid log boundaries leave the index unchanged. Missing or wrong trusted index entries remain errors even after log repair succeeds. |
| Scope | Checkpoints and other file pairs remain untouched. |

[Shared test storage](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#shared-test-storage)
constructs and clears the provider once per process. Each independent case,
including parameter-loop cases, owns a distinct stream guard from the allocator
shared with provider and initializer tests. That isolation lets cases run
concurrently without a stream mutex. Fixtures populate only their claimed streams;
additional streams have their own guards, and root-level metadata remains outside
their ownership.

The guards outlive file handles and completed I/O so cleanup can remove each
stream's contents on normal scope exit or panic unwinding. They preserve the empty
directory skeleton for later tests. Literal record templates are adapted to the
allocated stream before corruption is introduced; full-file fixtures use that
case's own identity.

The focused validators' [shared test file](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/validation/README.md#shared-test-file)
separately exercises partial I/O, errors, cancellation and synchronization before
success. Together these checks cover the composition and its underlying failure
contracts; they do not simulate power loss or failures within every kernel operation.

</details>

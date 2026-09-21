# Stream initializer

Recover one stream, publish its recovered checkpoint and return the active
log/index pair ready for appending. Initialization reuses a recovered partial pair
or creates an empty successor when needed. The per-stream operation is implemented;
application startup does not call it yet.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`StreamInitializer`] | Run recovery for one stream through the associated `initialize` operation. |
| [`InitializedStream`] | Own the active pair, its identity and its file-local endpoint for handover to the writer. |

[`IndexedLogValidator`](crate::streams::IndexedLogValidator) recovers individual
pairs. [`StorageProvider`](crate::storage::StorageProvider) owns file discovery,
acquisition, removal and checkpoint persistence.
[`IndexedLogWriter`](crate::streams::IndexedLogWriter) consumes the initialized
result to continue appending.

## Usage

Supply a provider and exclusive recovery access to the selected stream:

```no_run
use transaction_log::{
    storage::StorageProvider,
    streams::{InitializedStream, StreamInitializer},
};
use transaction_log_exports::StreamId;

async fn initialize_one(
    stream_id: StreamId,
    storage: &StorageProvider,
) -> anyhow::Result<InitializedStream> {
    StreamInitializer::initialize(stream_id, storage).await
}
```

The caller finishes earlier I/O and supplies an existing, durably established
directory hierarchy. The result has room for another record, with both cursors
positioned for appending. Consume it with `IndexedLogWriter::from(initialized)`
when ready to construct the writer.

<details>
<summary>Design and maintenance notes</summary>

**Handover preserves recovered progress.** The writer's `From<InitializedStream>`
conversion moves the existing handles without I/O and uses the recovered file end
as buffered, flushed and synchronized progress. Contents, cursors and exclusive
write ownership remain intact until this move; borrowed file getters do not grant
permission to change the initialized pair before handover. The
[writer contract](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/indexed_log_writer/README.md#ownership-and-construction)
owns that conversion.

The public initializer has no constructor or reusable state. Its operation borrows
the provider only until completion, and the returned result owns its files without
retaining that borrow. `anyhow::Result` suits this application-level coordination:
concrete storage and validator errors remain available without another enum
repeating their variants.

</details>

## Behavior and guarantees

### Recovery sequence

Every fallible step must succeed before the next begins:

| Step | Effect |
| --- | --- |
| Load checkpoint | Read optional metadata and require its stream ID to match the requested stream. |
| Discover logs | Find the maximum existing log and require it to cover the checkpoint's file, when present. |
| Validate pairs | Recover the contiguous prefix from the checkpoint's file or file zero, stopping at a partial pair or uncheckpointed gap. |
| Remove later files | Delete discarded log/index pairs through the discovered maximum. |
| Advance checkpoint | Publish the recovered stream endpoint when present and different from the original checkpoint. |
| Prepare active pair | Retain the recovered partial pair or create an empty pair after the recovered prefix. |
| Return ownership | Move the prepared pair to the caller without more I/O. |

Initialization may repair and remove files and publish a checkpoint. It is not an
atomic operation: later failure does not undo earlier completed work. A missing
checkpointed log or invalid trusted boundary is an error; the initializer does
not silently fall back to an earlier checkpoint.

<details>
<summary>Design and maintenance notes</summary>

**Private state follows the sequence.** `InitializationState` retains the stream
ID, provider reference, original checkpoint, discovered maximum, latest recovered
stream endpoint, optional active pair and first pair to remove. Its constructor
sets optional fields to `None` without I/O. The public operation invokes
`load_checkpoint`, `discover_log_files`, `validate_files`, `remove_later_files`,
`advance_checkpoint`, `prepare_active_pair`, then `finish` in order.

The original checkpoint remains available throughout recovery; there is no second
mutable copy of publication state. File enumeration is lazy, with no retained file
list or collection of completed pairs. The state holds at most one active pair.
Typed endpoints pass through validation, publication and handover without repeated
byte arithmetic; pair validation owns cursor positioning.

</details>

### Checkpoint loading and discovery

Absent metadata means no checkpoint. A present checkpoint must identify the
selected stream. Discovery accepts no logs only when no checkpoint exists;
otherwise its maximum file number must be at least the checkpoint's file number.
Neither step changes files or establishes stream readiness.

<details>
<summary>Design and maintenance notes</summary>

**Metadata and storage agreement are separate.** Loading delegates parsing and
read limits to `StorageProvider::read_checkpoint`, then checks the decoded stream
ID. A mismatch reports the checkpoint path and both IDs. Assignment happens only
after reading and checking succeed, so errors preserve previous state. Loading
neither requires log/index existence nor advances the recovered endpoint; actual
agreement with those files is checked during validation.

Discovery delegates to `maximum_log_file` for the same stream. Empty logs count;
orphan indexes do not establish log existence. A successful search records only
the optional maximum, not a file list. The checkpoint comparison uses file numbers
because loading and search scope already establish a common stream.

A maximum is an upper bound, not proof of continuity or checkpoint-file presence:
a later log can exist while the checkpointed log is missing. Length, trusted-index
checks and gaps are deferred to pair validation. Discovery assigns state only
after the search and comparison succeed, retaining the previous maximum on errors.
Provider failures keep their concrete `StorageError` and path through `anyhow`.

</details>

### Contiguous recovery and cleanup

Recovery starts at the checkpoint's file, or file zero without a checkpoint, and
visits through the discovered maximum. The original trusted endpoint applies only
to its own file. Following files are validated from their beginnings.

A complete pair advances recovered progress. A partial pair stops the scan and is
retained for appending; later pairs are discarded. A missing uncheckpointed log
also stops the scan, and cleanup includes that gap's orphan index and later pairs.
Files before the starting checkpoint file remain untouched.

<details>
<summary>Design and maintenance notes</summary>

**Stopping defines the contiguous prefix.** `LogFileId::iter_to` enumerates the
bounded range lazily. With no maximum, validation performs no file operations.
Every visited pair delegates to `IndexedLogValidator`, which has already repaired
and synchronized its contents before returning.

Complete results update `recovered_end` and release their handles. A partial result
stores synchronized, append-positioned files in `active_pair`. Its local `end` may
be `None`; in that case the prior complete file still supplies `recovered_end`.
If the partial pair precedes the maximum, its successor is the cleanup boundary.
When all discovered files are complete, no active pair is retained and the
recovered endpoint belongs to the maximum file.

Only a provider `NotFound` at the exact requested log path, with no trusted
endpoint for that file, establishes a gap. A missing checkpointed log is an error.
Index-open failures, invalid trusted boundaries and other operational failures
propagate as `IndexedLogValidationError` through `anyhow`, without marking cleanup.
A missing untrusted index is rebuilt by pair recovery.

Validation records the cleanup boundary but does not delete later files, create
an active log or publish a checkpoint. Earlier pair repairs and progress updates
may survive a later failure or cancellation. The step is used once per
initialization; retrying that partial working state is not supported.

**Cleanup uses the discovered bound.** Without a recorded boundary, cleanup does
no I/O. Otherwise one provider call receives the inclusive range from
`first_file_to_remove` through `maximum_log_file`. A cleanup boundary requires a
maximum; absence is an internal invariant failure, not an empty-range fallback.
Pairs beyond the stopping point cannot represent a contiguous local prefix;
future cluster coordination will recover required data.

The provider tolerates absent logs and indexes, deletes the log before its index,
and synchronizes each distinct changed immediate parent once on Unix. Directories,
unrelated entries and files outside the selected stream/range remain. Cleanup
stops at the first error, retaining its path and concrete cause; even one half of
a pair may already be removed.

The bounds, retained pair and recovered endpoint are unchanged by cleanup. After
outstanding I/O is quiescent and an obstruction resolved, the same cleanup range
can be repeated because absent files are accepted. This repeatable provider work
does not imply rollback or automatic retry of the initialization operation.

</details>

### Publication and active-pair preparation

A checkpoint is published only after validation and cleanup succeed. Empty streams
and unchanged endpoints need no checkpoint write. Publication uses the recovered
stream endpoint, even when the retained active file is empty.

An existing partial pair is kept without more I/O. Otherwise initialization creates
file zero when no records survived, or the successor of the recovered complete
file. New empty pairs are not explicitly synchronized; recovered records and
indexes have already been synchronized before checkpoint publication.

<details>
<summary>Design and maintenance notes</summary>

**Durability precedes publication.** Index opening synchronizes its immediate
parent on Unix, including recreated indexes. Pair validation synchronizes accepted
log data, then the index, and cleanup synchronizes changed parents. Only then can
checkpoint publication begin. The provider synchronizes staging JSON, renames it
over the checkpoint and synchronizes the checkpoint's immediate parent on Unix.
Ancestors are not synchronized; the caller supplies an established hierarchy.
Windows directory durability follows the limited
[storage contract](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#stream-checkpoints).

`advance_checkpoint` wraps `recovered_end` in `StreamCheckpoint`. No recovered end,
or equality with the original checkpoint, avoids publication I/O. It preserves
the original checkpoint and other working state. A publication error stops the
sequence before new-pair creation; staging or an already renamed checkpoint may
remain. A new attempt requires quiescent I/O and rereading metadata.

**The next empty pair is independent of the recovered checkpoint.** Preparation
chooses the successor from recovered progress, not the discovered maximum; files
beyond a gap have already been discarded. The provider's
[fresh-pair operation](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#fresh-logindex-pair-creation)
creates missing range directories and two new read/write files. Existing logs or
orphan indexes are preserved and rejected. The pair is installed only after both
opens succeed, with zero cursors and `end == None`.

No records exist in the new pair, and the recovered checkpoint covers only the
preceding prefix. Losing those empty files on a crash loses no records; they can
be recreated. Once appending starts, the live owner's synchronization schedule
establishes durability. That scheduling and live rollover remain future work.

Publishing the recovered checkpoint before creating its empty successor means a
creation failure can leave a valid new checkpoint in place. It still certifies the
recovered prefix. Failed or cancelled creation can also leave directories or one
or both new files without installing a pair; existing files must be recovered
after outstanding I/O is quiescent before retrying. There is no rollback or
automatic retry. Repeated preparation after success simply retains the installed
pair, including an empty recovered partial pair.

</details>

### Returned ownership and errors

`InitializedStream` owns the active pair and exposes read-only getters:

| Getter | Meaning |
| --- | --- |
| [`file_id()`](InitializedStream::file_id) | Identity of the active pair, including when it contains no records. |
| [`end()`](InitializedStream::end) | Last accepted record in this file, or `None` when this file is empty. |
| [`log()`](InitializedStream::log) | Borrow the log handle positioned after its accepted records. |
| [`index()`](InitializedStream::index) | Borrow the index handle positioned after its accepted entries. |

The returned end is file-local. If file 7 is complete and file 8 is empty, the
result identifies file 8 with `end == None`, while the stream checkpoint covers
file 7. Sequence zero remains a valid record, not an absence sentinel.

Any failed step returns no initialized stream. Storage and validator errors retain
their concrete sources through `anyhow`; identity/order checks report the violated
recovery condition. Failure or cancellation can follow repair, deletion, publication
or creation, so a new attempt must account for completed work and outstanding I/O.

<details>
<summary>Design and maintenance notes</summary>

**Consuming the working state.** `finish(self)` moves the prepared pair after
publication and preparation succeed, preserving handles, cursors and the local
endpoint without I/O. A missing active pair is an internal ordering violation and
panics with a specific message, not a recoverable storage error.

The result has no public unchecked constructor. Its fields are visible within
`streams` for named ownership transfer into the writer; any internal construction
must establish the same invariants. `getset` supplies copy getters for the identity
and endpoint and borrowed getters for the files. Keeping stream-wide recovered
progress in private state prevents an empty active pair from erasing the checkpoint
boundary while letting the writer begin with accurate file-local progress.

</details>

## Performance

Initialization is recovery work, outside the per-record append path. Discovery
retains only a maximum, validation enumerates files lazily and at most one active
pair survives. There is no stream-wide file list, completed-file collection or
copy of recovered record payloads.

Unchanged checkpoint publication and reuse of a retained partial pair avoid I/O.
Cleanup delegates its range so the provider can synchronize each changed parent
once. Recovery throughput has not been measured; these are implementation costs
and choices, not measured speedups.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked stream_initializer
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The [stream validation guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
also covers release checks and documentation examples in this binary crate.

<details>
<summary>Design and maintenance notes</summary>

The public initializer and focused private-step tests check observable files,
checkpoints and ownership, including where later steps must not have started:

| Contract | What the tests establish |
| --- | --- |
| Checkpoint loading | Absence, literal sequence-zero/later-file metadata, wrong streams, JSON/I/O errors, unchanged storage and retained state on failure. |
| Discovery | Empty/index-only storage, stream isolation, empty maximum logs, maxima around the checkpoint, absent checkpointed logs and preserved state/errors. |
| Pair validation | Multiple full files, partial/empty/corrupt tails, checkpoint starting points, trust passed only to its own file, gaps, index-open errors, append positions and retained earlier progress on failure. No cleanup, creation or publication occurs in this step. |
| Cleanup | No-op cases, inclusive ranges crossing directories, gaps/orphan indexes, missing pairs, unrelated-file/checkpoint preservation and repeatable removal. Log/index deletion errors stop later work while exposing completed progress. |
| Preparation and handover | Unpolled work, empty storage, early gaps, retained partial pairs, completed-file successors, directory boundaries and repeated preparation preserve exact file-local ends and cursors. Orphan indexes cause partial creation without installing a pair. |
| Public recovery | Empty/corrupt streams, first publication including sequence zero, checkpoint advancement, tail/index repair, cleanup, restart and unchanged checkpoints without writes. Empty or discarded successors preserve the recovered stream endpoint. |
| Failure ordering | Cleanup failure blocks publication; publication failure blocks successor creation; successor-creation failure preserves the new valid checkpoint. |

[Shared test storage](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#shared-test-storage)
provides one initialized provider and an atomic allocator shared with provider and
writer/validator tests. Each independent case owns a distinct stream guard,
including parameter-loop cases; extra streams have their own guards. They outlive
file handles and completed I/O, clean only their streams on scope exit or unwinding,
and preserve empty bases for reuse. Cases run without a stream mutex. The case
that deliberately damages a stream base uses an isolated temporary provider.

Literal record templates keep fixed framing, sequences and payloads; a fixture
helper adapts the stream ID and CRC independently of the production encoder. Known
bytes check that adaptation, and expected endpoints/index bytes remain explicit.
Full-file fixtures use each case's own stream. Provider suites cover metadata
read limits, parsing and directory search; focused validators cover detailed
synchronization, I/O failures and cancellation. These tests do not simulate power
loss or cancellation inside filesystem deletion.

</details>

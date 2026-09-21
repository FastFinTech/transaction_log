# Stream initializer

This module implements startup recovery for **one stream**. `StreamInitializer` is
an empty public type with one associated async method. It owns a private
`InitializationState` for the duration of initialization and returns an
`InitializedStream` containing the active log/index files and their file-local end.
Both public types are re-exported from `streams`.

**Current status:** `initialize` creates private state and delegates to the steps
in the order below, propagating errors before proceeding. The private constructor
stores the stream ID and provider reference and sets all optional fields to `None`
without I/O. Checkpoint loading reads the optional metadata and checks its stream ID.
Discovery records the highest existing log and rejects a maximum below the
checkpoint's file, including absent logs when a checkpoint exists. Validation
recovers successive pairs, retaining the first partial pair or marking a gap for
later cleanup. Cleanup removes the marked range through the discovered maximum.
Checkpoint advancement publishes the recovered endpoint when it differs from the
original checkpoint. Preparation then retains the validated partial pair or creates
an empty pair. Final handover consumes the state and returns that pair. All steps
are implemented; the result uses `getset` getters, and the writer consumes it
through named field destructuring.
Application startup does not call the initializer yet.

## Source map

- `stream_initializer.rs`: the empty public entry point and private working state.
  The private state stays beside its entry point so its fields and step methods
  can remain private to that implementation file.
- `initialized_stream.rs`: the owned result and derived read-only getters.
  Its fields are visible within `streams` for named ownership transfer; stream
  components constructing it must establish the documented initialization guarantees.
- `mod.rs`: declarations, re-exports and this README's Rustdoc inclusion.

The following describes the implemented per-stream operation. Startup-wide
coordination and live rotation remain separate work. The caller can consume the
result through `IndexedLogWriter::from(initialized)` to prepare its writer.

## Public API and result

```no_run
use transaction_log::{
    storage::StorageProvider,
    streams::{InitializedStream, StreamInitializer},
};
use transaction_log_exports::StreamId;

async fn initialize_one(
    stream_id: StreamId,
    storage_provider: &StorageProvider,
) -> anyhow::Result<InitializedStream> {
    let initialized = StreamInitializer::initialize(stream_id, storage_provider).await?;
    let _file_id = initialized.file_id();
    let _file_end = initialized.end();
    let _log = initialized.log();
    let _index = initialized.index();
    Ok(initialized)
}
```

`initialize(stream_id, storage_provider)` returns `anyhow::Result<InitializedStream>`.
This application-level result preserves concrete provider/validator errors
without duplicating their error variants in an orchestration-specific enum.
The public initializer has no constructor, fields, lifetime parameter or reusable
operation state. The provider is borrowed only during initialization.

The result owns `file_id: LogFileId`, `log: tokio::fs::File`,
`index: tokio::fs::File` and `end: Option<RecordEndLocation>`. `getset` supplies:

- `file_id(&self) -> LogFileId` identifies the pair even when it is empty.
- `end(&self) -> Option<RecordEndLocation>` identifies its last accepted record.
- `log(&self) -> &File` borrows the owned log handle.
- `index(&self) -> &File` borrows the owned index handle.

Successful initialization will return files positioned for appending, with no
invalid log bytes or index entries after their accepted ends. Recovered records
are synchronized; freshly created empty pairs require no initial synchronization.
The pair has room for another record: a completed final file requires preparing
its successor. The initializer returns the pair; the caller chooses when to
consume it as an `IndexedLogWriter<File>` through the writer's `From` conversion.
That conversion moves the existing handles without I/O and preserves the recovered
endpoint as buffered, flushed and synchronized progress. Borrowed file getters
must not be used to mutate contents or cursors before this handover. See the
[writer specification](../indexed_log_writer/README.md#ownership-and-construction).

The returned endpoint belongs **only to the returned file**. If file 7 is complete
and file 8 is empty, the result identifies file 8 with `end == None`, while the
stream checkpoint still covers the final record in file 7. The private
`recovered_end` preserves that stream-wide progress. `None` is never a sentinel
record ID; sequence zero remains a valid first record.

Checkpoint, recovered and returned endpoints carry `LogFilePosition` through
`RecordEndLocation`. The initializer passes these values between storage and
validation without extracting offsets or doing byte arithmetic. The pair validator
owns append positioning. Tests unwrap positions with `get()` only when comparing
against raw file cursors; independent JSON and index-byte fixtures remain numeric.

## Private state and step signatures

`InitializationState<'a>` owns the selected stream ID, the borrowed provider,
original checkpoint, discovered maximum file, latest recovered stream endpoint,
optional active pair and first file to discard. The original checkpoint remains
available throughout recovery. The first file to inspect is derived from it, or
from `LogFileId::first(stream_id)` (file zero) without a checkpoint. The same helper
selects a new active file when no records survived. No file list or completed-file collection is
retained. The optional active pair holds at most one set of open files.

The public operation creates state, calls these steps in order, then consumes it.
Construction and every recovery step are implemented:

| Private method | Return type | Intended responsibility |
| --- | --- | --- |
| `new(stream_id: StreamId, storage_provider: &'a StorageProvider)` | `Self` | Implemented: store the inputs and initialize optional state to `None`, without I/O. |
| `async load_checkpoint(&mut self)` | `Result<()>` | Implemented: load the optional checkpoint and verify its stream ID. |
| `async discover_log_files(&mut self)` | `Result<()>` | Implemented: find the maximum log and reject absence or a maximum below a checkpointed file. |
| `async validate_files(&mut self)` | `Result<()>` | Implemented: recover successive pairs; continue through complete files and stop at a partial pair or missing uncheckpointed log. |
| `async remove_later_files(&mut self)` | `Result<()>` | Implemented: remove discarded pairs through the discovered maximum. |
| `async advance_checkpoint(&mut self)` | `Result<()>` | Implemented: publish the recovered stream endpoint when present and changed. |
| `async prepare_active_pair(&mut self)` | `Result<()>` | Implemented: retain a partial pair or create an empty pair ready for appending. |
| `finish(self)` | `InitializedStream` | Implemented: consume the state and transfer the prepared pair without more I/O. |

Here `Result<T>` means `anyhow::Result<T>`. Checkpoint advancement is asynchronous
to await `StorageProvider::write_checkpoint`, which owns checkpoint publication
and synchronization of its immediate parent directory through Tokio. Publication
precedes fresh-pair preparation because it certifies the recovered prefix,
independently of the next empty pair. If creation later fails, the published
checkpoint remains valid. The successful sequence establishes the state needed
by the infallible `finish`.

## Checkpoint loading (implemented)

`load_checkpoint` delegates reading and metadata deserialization to
`StorageProvider::read_checkpoint`. A missing checkpoint sets `checkpoint` to
`None`; provider construction has already created the stream base. A present
checkpoint must identify the requested stream; a mismatch reports the checkpoint
path and both stream IDs.
Provider failures retain their concrete `StorageError` and path through `anyhow`.

State is assigned only after reading and the stream check succeed, so an error
leaves the previous checkpoint untouched. This step creates or modifies no files,
does not require logs or indexes to exist, and does not advance `recovered_end`.
Agreement with actual log/index contents belongs to the later validation step;
loading the checkpoint alone does not establish stream readiness.

## Log-file discovery (implemented)

After checkpoint loading succeeds, `discover_log_files` delegates to
`StorageProvider::maximum_log_file` for the selected stream. It records only the
optional maximum ID, without collecting a file list. Empty logs count; orphan
indexes do not establish log existence. Without a checkpoint, absent logs are valid
and store `None`. With a checkpoint, the maximum must exist and its file number
must be at least the checkpoint's file number. Both IDs belong to the selected
stream, established by checkpoint loading and the provider's search scope.

Discovery changes no files and assigns `maximum_log_file` only on success.
Provider errors retain their concrete `StorageError` and path. A failed search
or checkpoint comparison leaves the previous maximum untouched. This check
establishes an upper bound, not continuity or checkpoint-file presence: a higher
log can exist despite a missing checkpointed log. Opening the checkpointed file,
checking its length and trusted index entry, and detecting gaps belong to the
later validation step. Discovery does not advance `recovered_end` or prepare files.

## Sequential pair validation (implemented)

After loading and discovery, `validate_files` uses `LogFileId::iter_to` for lazy
enumeration from the checkpoint's file, or file zero, through the discovered
maximum. No maximum means an empty stream and no file operations. The original
trusted endpoint is supplied only to its own file; later files start without a
trusted prefix. Every pair is delegated to
[`IndexedLogValidator`](../validation/indexed_log_validator/README.md).

Complete results advance the stream endpoint and release their handles. A partial
result stops scanning and retains the pair in `active_pair`, with synchronized
files positioned for appending. Its file-local endpoint remains `None` when empty,
while `recovered_end` preserves the preceding complete file's endpoint. Corrupt log
tails are already removed by the validator. If the partial file precedes the
discovered maximum, its successor becomes `first_file_to_remove`; otherwise no
cleanup is needed. When every discovered file is complete, `active_pair` stays
absent and `recovered_end` identifies the final record of the maximum file.

A missing log in the checkpoint's own file is an error. A missing uncheckpointed
log stops recovery and becomes `first_file_to_remove`, including file zero when
no checkpoint exists. Only a provider `NotFound` at that exact log path establishes
a gap. Index-open failures, invalid trusted boundaries and other operational
errors propagate as the original `IndexedLogValidationError` through `anyhow`,
without marking cleanup. A missing untrusted index is rebuilt by the pair validator.

This step neither deletes later files nor creates an active log nor publishes a
checkpoint. Earlier files before the checkpoint's file and files beyond the first
partial pair or gap are untouched. Call it once per initialization: failures or
cancellation can leave completed pair repairs and earlier `recovered_end` updates.
There is no rollback or retry on the same working state.

## Later-file cleanup (implemented)

After successful validation, `remove_later_files` returns without I/O when no
cleanup boundary was recorded. Otherwise it passes the inclusive range
`first_file_to_remove..=maximum_log_file` to `StorageProvider::remove_log_and_index`
in one call. The provider owns enumeration and directory synchronization.
Validation only records a
cleanup boundary when a maximum exists; a missing maximum here is an internal
invariant violation, not an empty range.

The range includes a missing log's orphan index after a gap, or starts immediately
after a retained partial pair. Discarded pairs cannot describe a contiguous
prefix; cluster coordination will later recover required data. The provider
tolerates missing logs/indexes, deletes the log before the index, and synchronizes
each distinct immediate parent directory once on Unix using Tokio's async API.
It leaves directories and unrelated entries in place.
No files outside the selected stream and inclusive range are removed.

Cleanup stops at the first error, preserving the concrete `StorageError` and path
through `anyhow`. Earlier removals, including just one half of a pair, can already
have completed. It retains the original bounds, active pair and recovered endpoint
on success or failure, and never publishes a checkpoint. After outstanding I/O
is quiescent and an obstruction is resolved, the same cleanup range can be
repeated; already-absent files are accepted. There is no automatic retry or rollback.

## Checkpoint advancement (implemented)

After validation and cleanup succeed, `advance_checkpoint` wraps `recovered_end`
in `StreamCheckpoint` and publishes it through `StorageProvider::write_checkpoint`.
No recovered records means no checkpoint and no publication I/O. An endpoint equal
to the original checkpoint also requires no write. The step uses stream-wide
progress, so an empty retained active file does not erase the preceding complete
file's endpoint. It preserves the original `checkpoint` field and other working
state; there is no second mutable copy of the publication state.

The owner excludes concurrent access, finishes earlier I/O and supplies an existing,
durably established directory hierarchy. Index opening synchronizes its immediate
parent on Unix, including indexes recreated during recovery. The validators
synchronize accepted log data and then rebuilt index data; cleanup synchronizes
its changed parent directories. Only after those operations succeed can checkpoint
publication start. The provider writes and synchronizes staging JSON, renames it
over the checkpoint, and synchronizes the checkpoint's immediate parent on Unix.
No ancestor synchronization is added. Windows directory durability remains limited
as described by [storage](../../storage/README.md#stream-checkpoints).

A publication error preserves the concrete `StorageError` through `anyhow` and
stops the public sequence before new-pair creation. Errors or cancellation can
leave staging or an already published checkpoint; they do not imply rollback.
Quiesce outstanding I/O and reread metadata before another recovery attempt.
A subsequent pair-creation failure may leave this successfully published checkpoint
in place, correctly certifying the recovered prefix.

## Active-pair preparation (implemented)

After checkpoint advancement succeeds, `prepare_active_pair` retains an existing
`active_pair` without I/O: validation already synchronized its files and positioned
their cursors for appending. This includes an empty partial file, whose file-local
endpoint is `None` even when earlier complete files established `recovered_end`.

Without a retained pair, preparation chooses file zero when no records survived,
or the successor of the recovered complete file. It derives that choice from the
recovered prefix, not the discovered maximum: files beyond a gap have already been
discarded. `StorageProvider::create_log_and_index` creates the pair and any missing
range directories. Preparation installs the returned pair without synchronizing
the empty files. Both cursors are at byte zero and
`end == None`; the original checkpoint and stream-wide `recovered_end` are preserved.

There are no records to make durable in a new empty pair, and the recovered
checkpoint covers only the preceding prefix. Losing the empty pair on a crash
loses no records; absent files can be recreated. Once records are appended, the
live stream owner's synchronization schedule establishes their durability.
That scheduling remains future work. Recovery repairs still synchronize accepted
records and indexes before their progress can be checkpointed.

Creation errors preserve their concrete `StorageError`, path
and I/O cause through `anyhow`. State is updated only after success, but failure or
cancellation can leave directories and one or both files on disk. There is no
rollback or automatic retry; existing destinations are preserved and rejected by
creation. Quiesce outstanding I/O and recover existing files before retrying after
an interrupted operation. Calling preparation again after success simply retains
the installed pair.

The provider's creation operation can also serve eventual live rollover; this
private step makes startup's reuse-or-create decision. It publishes no checkpoint
and does not change the certified recovered prefix.

## Final handover (implemented)

`finish(self)` consumes the private state and moves its prepared `InitializedStream`
to the caller. It preserves the owned file handles, cursor positions and file-local
endpoint without further I/O. The public sequence calls it only after checkpoint
advancement and pair preparation succeed. A missing active pair is an internal ordering error and
panics with a specific message; it is not a recoverable storage failure.

## Storage provider readiness

Existing operations cover checkpoint read/write, maximum-log discovery, fresh-pair
creation, log/index opening for recovery and pair removal. Index opening, checkpoint
publication and range removal own their directory synchronization. File data synchronization is available on
the returned Tokio files.

`StorageProvider::create_log_and_index(id).await` now creates missing range
directories and returns an empty `(log, index)` pair with read/write handles at
byte zero. Both files must be new: existing logs or orphan indexes are preserved
and reported as errors. Index-creation failure may leave a new empty log;
interrupted creation must be inspected/recovered before retrying. The
[storage contract](../../storage/README.md#fresh-logindex-pair-creation) owns the
details. This operation creates files without synchronizing files or directories.
`prepare_active_pair` uses those empty files directly after checkpoint advancement.
Storage and validators own the filesystem work; the initializer selects the prefix
and orders those operations. No workers, locks, timers, startup wiring, cluster
coordination or live rotation are part of this component.

## Validation and performance

Same-file tests use [shared test storage](../../storage/README.md#shared-test-storage)
alongside the provider, indexed-log writer and indexed-log validator tests. They share
one initialized provider and one atomic stream-ID allocator. Each independent
test case and parameter-loop iteration owns a fixture guard for its stream;
additional streams need additional guards from the same allocator. Guards are
declared before file handles and state, and clean only their stream's contents
when the scope ends. No stream mutex is held during execution. The storage specification owns
the fixture's initialization, caching, isolation and cancellation contracts.
Cleanup works with ordinary `cargo test`, including panic unwinding, preserving
the empty stream bases for reuse.

Literal record templates retain their fixed framing, sequences and payloads.
The fixture helper replaces only the stream ID and CRC-32C trailer, independently
of the production encoder. Known byte fixtures check that adaptation, while
expected record ends and index bytes remain explicit. Full-file fixtures are
encoded for each case's own stream.

The test that deliberately replaces a stream base with a file retains an isolated
temporary provider because it damages the base layout. The other cases rely on
the shared fixture. Production initialization and provider behavior are unchanged.

Build and lint the component, compile the README example and exercise the public
initializer as well as the focused private-step tests.
Same-file checkpoint-loading tests cover absent metadata, independent literal
metadata (including sequence zero and a later file), wrong-stream metadata, JSON
and I/O failures, retained state on errors, and unchanged storage. Discovery tests
cover empty/index-only storage, stream isolation, empty maximum logs, maxima
equal to/after/before the checkpointed file, absent logs with a checkpoint, deferred
pair checks, and preserved state and concrete errors on failure. Provider tests
cover the read-size limit, metadata parsing and directory-search details.
Validation tests cover multiple complete files, partial/empty/corrupt tails,
checkpoint-file starting points and trust handoff, missing logs with and without
checkpoints, index-open and trusted-boundary errors, append positions, and retained
earlier progress after failure. Independent record/index fixtures and expected
endpoints check the handoff; lower validator suites cover detailed I/O failures,
cancellation and synchronization. Tests also verify that cleanup, fresh-log
creation and checkpoint publication have not occurred during validation.
Cleanup tests cover empty storage, no-op cleanup with a retained pair, inclusive
removal across range directories, gaps/orphan indexes, absent pairs, preservation
of retained/earlier/other-stream files and checkpoints, and repeatable cleanup.
Log and index deletion failures verify partial progress, stopping before later
pairs and preserved concrete errors. These tests do not simulate power loss or
cancel in-flight filesystem deletion.
Preparation tests cover unpolled work, empty storage, gaps before any records,
retained empty/nonempty partial pairs, repeated preparation, completed-file
successors, range-directory boundaries and gaps beyond a recovered complete file.
They check append positions, empty new files, preserved recovered endpoints and
checkpoints, and orphan-index errors that leave partial creation without installing
a pair. Handover is exercised by these same fixtures: retained partial pairs and
new empty successors keep their file-local endpoints and append positions after
the state is consumed. These tests do not simulate power loss; detailed recovery
synchronization failures remain covered by the validator suites.
End-to-end tests cover empty/corrupt streams, first publication including sequence
zero, advancement from an existing checkpoint, tail/index repair, later-file
cleanup, restart from the new checkpoint, and unchanged checkpoints without writes.
Complete files followed by absent, empty or discarded successors retain the correct
stream endpoint. Failure tests verify that cleanup blocks publication, publication
failure blocks successor creation, and successor-creation failure preserves the
new valid checkpoint. Preserve independent fixtures and
observable file/checkpoint results. Recovery throughput has not been measured;
this is startup work rather than a per-record hot path.

```powershell
cargo build -p transaction-log --locked
cargo test -p transaction-log --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

Follow the [streams validation guidance](../README.md#verification-and-performance)
to compile documentation examples for this binary crate.

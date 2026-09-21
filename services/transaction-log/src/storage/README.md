# Storage

This application module groups storage types for log, index and other file types.
It implements immutable startup configuration, asynchronous construction of the
root and stream directories, and deterministic log/index/checkpoint paths.
The provider creates fresh log/index pairs and opens existing files for repair. The sibling
[streams module](../streams/README.md) implements indexed appends, validation from
a supplied trusted boundary, index repair and explicit invalid-tail recovery.
Stored clustering configuration read/write operations are also
implemented, along with checkpoint read/write, recovery pair deletion and Unix
directory synchronization. Startup integration and management of live files
remain future work.

Provider implementation, root configuration and shared errors live directly in this directory, one type per file. Logical file identities, sequence grouping, endpoints and ranges belong to [streams::location](../streams/location/README.md). The thin `mod.rs` re-exports `StorageProvider`, `StorageConfig` and `StorageError` and includes this specification in Rustdoc.

| Source | Responsibility |
| --- | --- |
| `storage_error.rs` | Shared provider I/O/JSON failures and bounded-read limits, retaining paths and original causes. |
| `storage_config.rs` | Immutable startup configuration and absolute root resolution. |
| `storage_provider.rs` | All provider methods: configuration ownership, paths, directory initialization, maximum-log discovery, file acquisition and metadata I/O, with private JSON read/write and directory-sync helpers. |
| `test_support.rs` | Compiled only under `cfg(test)`: one shared initialized provider, a stream-ID allocator, and literal-record adaptation for filesystem tests. Its contracts and tests are covered by this README. |

Storage policy belongs to the application, not the public record I/O exports
crate. `LogFileId` describes where a record belongs in a stream's sequence;
`StorageProvider` maps that identity to a physical location. Neither an ID nor
a calculated path establishes that a file or record exists.

The provider exposes storage configuration read/write operations. Startup does not yet call them.
Setup, networking and recovery remain future integration work.

## Clustering configuration persistence

The application-owned
[`EstablishedClusteringConfiguration`](../clustering/configuration/README.md#established-configuration)
combines validated deployment configuration with its permanent UUID in either
mode. Clustering owns its value and Serde contracts; storage introduces no
intermediate persistence type.

`StorageProvider::read_clustering_configuration` and
`write_clustering_configuration` directly deserialize/serialize that type in root
`clustering.json`. Reads are bounded to 4 KiB and validate through the domain
types. Writes atomically replace the supplied metadata. Enforcing write-once
establishment and retaining the permanent UUID/configuration belong to the
clustering lifecycle caller, whose startup integration remains planned. See the
[provider metadata contract](#provider-metadata-operations) for concrete errors, interrupted writes
and durability limits. UUID establishment, configuration comparison and startup
integration remain deferred and are not provider responsibilities.

## Provider metadata operations

The public `log_file_path` and `index_file_path` methods share the private
`log_or_index_file_path` helper for their numeric directory layout and basename.
Checkpoint and clustering metadata paths do not use that helper.
All provider methods live together in `storage_provider.rs`.
Both metadata readers and writers use Tokio's asynchronous filesystem API,
delegating bounded reads to `read_json` and publication to `write_json_atomic`.
The provider implementation is ordered by purpose: construction and root access,
public path queries, clustering metadata, checkpoint metadata, log/index discovery
and file operations, then private path helpers. Shared JSON read/write helpers
follow the implementation. Test modules come last, grouped into provider operations,
clustering metadata, checkpoints, JSON reads and JSON writes. Pair-deletion tests
live with the other log/index operations rather than checkpoint tests.
Published filenames are constants local to `checkpoint_file_path` and the private
`clustering_configuration_file_path` helper; both clustering read and write methods
use that helper. Pending paths append `.pending` to the published path.
Metadata read limits are named constants scoped to their respective read methods; independent
size-limit fixtures in tests use the specified byte counts.
`storage_error.rs` retains path and concrete I/O/JSON
errors. `read_clustering_configuration().await` reads root `clustering.json`,
bounds reads to 4 KiB and deserializes through the model's validation. Only the
published file is read: an absent file returns `None` even when
`clustering.json.pending` exists. Pending files are neither inspected nor recovered;
other I/O and JSON errors propagate. It creates or modifies nothing.
`write_clustering_configuration(&configuration).await` writes the supplied
configuration/UUID, replacing any existing document. It uses the root established
by provider construction and the shared JSON writer to stage
`clustering.json.pending` and rename it over
`clustering.json`. Unix then synchronizes the root directory;
Windows directory durability is not guaranteed and ancestors are not synced.
Errors/cancellation may leave pending or published metadata; callers must quiesce
outstanding I/O and reread/inspect storage before another attempt. A failed write
does not imply that nothing was published. Exclusive root ownership is required.
Write-once policy, UUID generation and startup integration are not part of these
storage operations. Temporary filesystem tests cover both deployment modes, all
slots, independent JSON fixtures, malformed/oversized input, replacement of
existing metadata and stale staging.

## Bounded JSON reading

The private async `read_json<T: DeserializeOwned>(path, max_bytes)` helper lives
beside the atomic writer in `storage_provider.rs`. It returns `Result<Option<T>,
StorageError>`: only `NotFound` when opening the requested path becomes `None`,
including an absent parent. It creates or modifies nothing and never inspects
`.pending` siblings. Cancellation produces no value or file modifications.

The caller supplies an inclusive byte limit. Tokio reads at most one byte beyond
that limit (saturating at `u64::MAX`), then rejects oversized input before JSON
parsing. Limits count all encoded bytes, including whitespace. Empty files,
malformed JSON, trailing non-whitespace and model validation failures are errors.
Deserialization returns an owned value; a valid JSON `null` for an optional target
is `Some(None)`, distinct from an absent file. I/O and JSON failures retain the
requested path and their concrete cause through the existing `StorageError`.

Both checkpoint and clustering readers use this helper and ignore pending staging.
Read limits remain caller policy, with each provider method owning its locally
named constant. Recovery decisions do not belong in the generic helper.

Same-file tests use independent JSON fixtures to cover owned values, missing
files/parents, ignored staging, exact/over/zero limits, malformed and invalid UTF-8
input, target-type validation and filesystem errors. Reads preserve file contents.
These metadata operations are outside the record hot path; no throughput is claimed.

## Shared atomic JSON publication

The private `write_json_atomic<T: Serialize + ?Sized>(path, &value)` function lives
beside the provider in `storage_provider.rs`. Both metadata writers delegate to
this one operation, which always replaces the destination. It borrows the value
and serializes once into an owned byte buffer before any filesystem I/O. It then
opens `.pending` staging with `create(true)` and `truncate(true)`, writes and
flushes the bytes, synchronizes the file and closes it. Publication
renames the staged file over the destination and synchronizes the parent directory.
Flushing before synchronization surfaces pending Tokio write errors.

Truncation discards any interrupted staging contents before writing the new
document, including leftover bytes when the replacement is shorter. Write-once
rules belong to callers, not this helper.

All helper filesystem operations use Tokio. Directory synchronization is Unix-only
and applies only to the immediate parent obtained from the destination path.
The helper creates no directories. Provider construction establishes the root
and stream bases; both publication methods require their parent to still exist.
The provider supplies absolute file paths with parents. Callers must establish ancestor
durability and exclude concurrent access to the destination and pending path.

Atomic publication makes a complete document visible; it does not promise rollback
on error or cancellation. Staging can remain, and failure after publication can
leave the new document visible. Quiesce outstanding I/O before
recovery or retry. `StorageError` retains the final path for serialization and
publication failures, the pending path for staging failures, and the
parent path for directory-sync failures. No new error layer or startup policy is
introduced.

Same-file helper tests use independent expected bytes and verify unsized borrowed
values, unchanged files on serialization failure, first publication, shorter
replacement without stale bytes, and missing-parent errors without directory
creation. Provider tests retain
model round trips, staging/rename failures and interrupted-publication coverage.
These tests do not simulate power loss or cancellation during filesystem I/O.
This is metadata work outside the record hot path; no throughput is claimed.

## Shared errors

`StorageError` is the common error for directory initialization, purpose-specific
file opening and JSON metadata operations. `Io { path, source }` retains the
concrete filesystem error; `Json { path, source }` retains Serde encoding or
parsing errors, including domain validation during deserialization;
`TooLarge { path, max_bytes }` reports a bounded-read limit. `path()` borrows the
requested path for every variant. I/O and JSON causes remain available through
`std::error::Error::source` and can be matched/downcast without string parsing.
There are no deployment mismatch or startup policy variants. `StorageConfig`
path resolution still returns `io::Error` before provider construction.
Provider and validator tests check paths, preserved causes and failure propagation;
metadata tests cover JSON rejection and size limits. The same error contract applies to every provider operation.

## Stream checkpoints

The [streams checkpoint module](../streams/README.md#stream-checkpoint-model) owns
`StreamCheckpoint`, its Serde representation, certification requirements and
publication ordering. `read_checkpoint(stream_id).await` reads its JSON file with
a 4 KiB inclusive limit, returning `None` only for a `NotFound` open failure
(including an absent parent). It creates or modifies nothing. Other I/O, JSON and
size failures use the shared `StorageError` with the requested path.
Deserialization validates the model; stream-identity and log/index agreement
checks belong to recovery. Cancellation publishes no result or storage changes.
Startup integration remains deferred. Same-file provider
tests cover literal JSON, exact/over-limit input, absent paths, malformed metadata
and a directory in place of a checkpoint file.

Checkpoint endpoints expose `LogFilePosition` in Rust. Its transparent Serde
representation keeps the JSON `position` field numeric, so these provider
operations need no offset conversion or new persistence format. Literal-fixture
tests can extract the decoded offset with `.get()` to compare it with raw
expected JSON values.

`write_checkpoint(&checkpoint).await` uses Tokio filesystem I/O. It derives the
stream path from the model, requires an existing, durably established stream
directory and exclusive ownership, and serializes before touching files. It
writes a created or truncated `checkpoint.json.pending` (the checkpoint path with
`.pending` appended), flushes pending writes to surface their errors, synchronizes its
contents and metadata, closes it, then renames it over
`checkpoint.json`. A stale pending file is never read as certified metadata;
its contents are truncated before staging a replacement. Errors before rename preserve the
previous checkpoint; errors after rename may leave the new checkpoint visible.
Cancellation can leave filesystem work in flight; quiesce outstanding I/O and
reread after uncertain publication before recovery or another write. Model
construction and storage writing do not certify log/index validity, compare
deployment settings or advance recovery state.

Unix publication synchronizes only the checkpoint's immediate parent (the stream
directory), derived from the checkpoint path, using Tokio after the rename.
The existing directory hierarchy is not
changed and its ancestors are not synchronized. Windows has no directory-durability
guarantee here. The caller must first establish the covered log/index prefix and
its directory-entry durability; publishing a checkpoint does not synchronize
those separate files or their parent directories. Tests verify first publication,
replacement, stale staging truncation, staging/rename failures and retry after a failed
rename; they do not simulate power loss or cancellation during filesystem I/O.

`remove_log_and_index(first..=last).await` accepts an inclusive range of
`LogFileId`s from one stream. It validates both endpoints through `iter_to` before
any I/O: different streams or reversed bounds return `StorageError::Io` at the
first log's path, with `InvalidInput` and the original `LogFileIdRangeError` as
its cause. It enumerates lazily in ascending order, removes each log before its
index, accepts `NotFound` and preserves other concrete filesystem errors.
It removes no directories, creates nothing and leaves entries outside the range
untouched.

After all deletions, Unix opens and synchronizes each distinct immediate parent
directory once using Tokio's asynchronous file API. No ancestors are synchronized:
removing files changes their parent entries, not the existing directory hierarchy.
Existing parents are synchronized even when their requested files were already
absent, so repeating a range after an interrupted synchronization still establishes
durability. Missing parents are skipped. The method retains one path per distinct
parent, not a list of every file ID. Index opening, JSON publication and range removal
share the private `sync_directory(path)` helper. It synchronizes exactly the supplied
directory through Tokio on Unix and performs no I/O on other platforms. It creates
nothing, traverses no ancestors and preserves path-bearing errors; each caller
decides whether a missing directory is acceptable. Range deletion tolerates missing
parents; index opening and JSON publication propagate failures. Tokio moves
filesystem work off the async executor; completion still awaits synchronization.
Windows has no directory-durability guarantee here.

Cleanup requires exclusive recovery access and a durably established directory
hierarchy; directory initialization alone does not supply that durability.
Errors/cancellation can leave partial deletion or unsynchronized directory changes;
repeat the original range after quiescing outstanding I/O. Checkpoint publication
is separate and must wait for successful cleanup. Tests cover invalid bounds
before deletion, inclusive ranges spanning directories, preserved neighbors and
other streams, absent pairs, orphan indexes, repeated deletion and directory
collisions. They do not simulate power loss.

## Dense index format

The [index writer specification](../streams/index_writer/README.md#dense-index-layout)
owns the shared log/index file encoding used by appending and recovery.
Storage constructs `.idx` paths and acquires handles; it does not encode entries.

## Configuration and ownership

`StorageConfig::new(root_directory: PathBuf) -> io::Result<StorageConfig>` resolves
the root with `std::path::absolute` and owns the result. A relative path is anchored
to the working directory at configuration construction, so later working-directory
changes do not redirect storage. The root is private and exposed as a borrowed
`&Path`; it cannot be changed through the configuration API.

Absolute-path resolution does not create or inspect the root, resolve symlinks,
or require it to exist. Platform-specific lexical rules apply; this is not
filesystem canonicalization. Empty or otherwise rejected paths and failures to
obtain the working directory return the underlying `io::Error`. Root paths remain
native `PathBuf` values, including spaces, Unicode and OS-specific non-UTF-8 data.

`StorageProvider::new(config: StorageConfig)` is async and returns
`Result<StorageProvider, StorageError>`. It takes ownership and creates the root
and all stream base directories before returning a provider.
`root_directory()` borrows the absolute configured root.
The provider is immutable, has no interior mutability or background tasks, and
can be shared through `Arc<StorageProvider>` when multiple components need it.
The configuration does not need its own `Arc` inside the provider.

Callers construct configuration and await provider construction before using its
methods. There is no separate initialization method or partially initialized
provider exposed to callers. The current startup scaffold does not construct a
provider. No storage-worker architecture or startup recovery coordinator is
implemented. There is no initialization flag or per-call readiness check;
actual file operations must still handle their own I/O errors.

## Fixed path layout

All numeric components use zero-padded decimal notation. A stream ID occupies
four digits; a file number occupies fifteen. The first twelve digits of the file
number form four directory components of three digits each. The full fifteen
digits remain the basename, followed by `.log` or `.idx`.

For stream 42 and file 1,234:

```text
{root}/streams/0042/000/000/000/001/000000000001234.log
{root}/streams/0042/000/000/000/001/000000000001234.idx
{root}/streams/0042/checkpoint.json

File number:  000 000 000 001 234
Directories:  000/000/000/001/
```

At the maximum stream ID and file number:

```text
{root}/streams/4095/184/467/440/737/184467440737095.log
{root}/streams/4095/184/467/440/737/184467440737095.idx
{root}/streams/4095/checkpoint.json
```

The slashes above describe path components. Production construction uses native
`PathBuf` operations on both Windows and Linux, without converting the configured
root to a string. The bounded `LogFileId` guarantees that the file number fits
the fifteen-digit representation.

| Directory | Maximum assigned contents |
| --- | ---: |
| `streams/` | 4,096 stream directories, `0000` through `4095`. |
| A stream base | 185 top-level range directories, `000` through `184`, plus `checkpoint.json`. |
| An intermediate range directory | 1,000 child directories, `000` through `999`. |
| A leaf range directory | 1,000 log/index pairs, or 2,000 files. |

The directory-entry target is flexible, not a hard 1,000-entry limit. The fixed
stream count needs no extra grouping level; log/index pairs stay together. These
bounds describe assigned names, not a scan or enforcement of actual directory
contents. Existing metadata or unrelated files are not counted or removed.

Consecutive file numbers share leaf directories, supporting range-based discovery
and future retention work. Fixed-width names sort in numeric order within their
respective groups. The hierarchy covers the entire file-number domain from the
start, so growing sequences never require moving earlier files to add levels.
Early paths intentionally include leading `000` directories.

Active and finalized pairs use these same permanent paths and file formats.
They are not relocated, stitched together or converted when full. Finalization
ends appending after synchronization; the future stream owner publishes that
state. Path shape alone does not establish a file's completeness or readiness.

This is a fixed storage-layout contract, not a runtime directory-size setting.
Changing digit widths, grouping, extensions, the checkpoint filename or the
`streams` component changes where files are found. Keep naming logic in the
provider rather than duplicating it in future readers, writers, index maintenance
or recovery code.

## Path construction and performance

### Maximum log discovery

`maximum_log_file(stream_id).await` returns the highest existing canonical
`LogFileId`, or `None` for an absent stream directory or a tree without logs.
It searches the four range-directory levels in descending numeric order and
backtracks through empty or index-only branches. At a leaf it selects the highest
regular `.log` file whose fifteen-digit name, directory prefix and number domain
match the provider's path contract. Empty logs count; indexes do not establish
log existence. Unrelated/noncanonical entries and range/log symbolic links are
ignored. The method performs no content validation, continuity checks, repair,
deletion or checkpoint operations. The initializer records this maximum and
checks it against the checkpoint's file number. Its validation step enumerates
lazily from the recovery boundary through this maximum, stopping at a partial
pair or gap.

Pending siblings are retained only along the fixed-depth search, rather than
collecting every log ID. A method-local `PendingDirectory` names each pending
branch's path, depth and accumulated numeric prefix. Two local async helpers
separate recognizing and sorting child range directories (`range_directories`)
from selecting a leaf's highest canonical log (`maximum_log_in_directory`).
The main loop controls descending traversal and backtracking.
A populated highest branch avoids scanning older leaf directories.
Empty high branches require backtracking and may cause a wider
scan; directory-entry counts for unrelated contents are not capped. No discovery
performance measurement is claimed.

Callers must exclude concurrent tree changes: this is an existence snapshot,
not a lock or durable publication marker. Only absence of the initial stream
directory is treated as empty; enumeration, entry-type and disappearing child
directory failures propagate as path-bearing `StorageError::Io`. Cancellation
changes nothing. Same-file tests cover sparse/empty/index-only branches, stream
isolation, empty logs, numeric carries and the terminal number, noncanonical
entries, and an obstructed stream directory.

### Constructed paths

- `log_file_path(id: LogFileId) -> PathBuf` returns the complete absolute log path.
- `index_file_path(id: LogFileId) -> PathBuf` returns the paired index path with
  the same directory and numeric basename.
- `checkpoint_file_path(stream_id: StreamId) -> PathBuf` returns the stream's
  `checkpoint.json` path directly inside its base directory.

All three methods are synchronous and deterministic. They allocate a new path
without scanning directories, testing existence, creating parents, opening handles
or checking file contents. The caller retains ownership of the result independently
of the provider's lifetime.

Path construction is not expected to occur frequently enough to justify caching
filenames or directory names. Ordinary string/path allocation is an explicit
design choice. Do not add caches, shared scratch buffers, fixed-size formatting
machinery or caller-supplied output-buffer APIs without a demonstrated need.
The log and index methods share one private formatter so their paths cannot
silently diverge. Initialization and all path methods share the stream-base helper.
That helper accepts a validated `StreamId`, preserving the type's domain contract
through path construction rather than accepting arbitrary raw integers.

There is no provider or file-recovery throughput measurement yet. Existing record
benchmarks do not measure these file operations.

## File acquisition for validation and repair

`open_log_for_validation(id)` opens the named existing log with read/write access,
without create, truncate or append mode. A missing log is an error. The caller
needs write access for explicit tail repair and eventual writer handover. The
validator seeks to verified append boundaries before transferring these handles.

`open_index_for_repair(id)` opens the index with read/write access, creating a
missing empty index and preserving an existing one. It also avoids append mode,
because repair must be able to replace a bad suffix. Parent directories must
already exist and their hierarchy must be durably established. A validator opens
the log successfully first, so a missing log does not create an orphan index.
After opening the index, Unix synchronizes its immediate parent before returning
the handle. This also applies to an existing index, which may have been created
by an interrupted recovery. Errors/cancellation can leave a created index; a
directory-sync failure preserves the directory path and original I/O cause.
Windows has no directory-durability guarantee here. The validator still must
establish and synchronize the index contents before publishing a checkpoint.
Neither method invents valid records or entries.

`StorageError` preserves the requested path and concrete OS error. Returned
handles belong to the caller; the provider keeps no cache, task or mutex. Default
file sharing permits independent readers, but these methods do not coordinate
them, acquire exclusive recovery locks, or make the pair queryable. The caller
must exclude other writers and withhold recovering indexes from readers. Opening
files and synchronizing an index's parent do not publish stream state.

## Fresh log/index pair creation

`create_log_and_index(id).await` returns `Result<(tokio::fs::File, tokio::fs::File),
StorageError>`, ordered `(log, index)`. It creates any missing parent directories
using Tokio's `create_dir_all`, then creates the log followed by the index. Both
handles are empty, readable/writable and positioned at byte zero. Append mode is
not enabled, so subsequent seeks control where writes land. The caller owns the
handles and must exclude concurrent access to the pair and its directory tree.

Both opens use `create_new(true)`. An existing log, including an empty one, is an
error before the index is touched. An existing orphan index is also an error;
it is preserved rather than silently discarded or adopted. This keeps recovery
decisions with the recovery owner. No existence precheck is needed: each create
operation itself refuses an existing destination, including a dangling file link.

The two creations are not atomic. A directory-creation failure can leave some
parents created; an index-creation failure leaves the newly created empty log.
For example, an orphan index remains unchanged alongside that empty log. A direct
retry then fails at the existing log. There is no rollback or automatic retry:
quiesce outstanding I/O and inspect/recover partial creation before another
attempt. Cancelling a polled future can leave directories, one file or both files;
an unpolled future performs no I/O. Failures use the existing `StorageError::Io`,
retaining the requested directory or file path and the underlying OS error.

Success establishes open empty files, not durability or a validated stream
boundary. This method does not synchronize files or directories, choose the
next file ID, or publish a checkpoint. Before publishing recovered progress, the
recovery owner must establish the required file and directory-entry durability
for the prefix covered by that checkpoint. The initializer's `prepare_active_pair`
uses the empty files without synchronizing them: they contain no records and are
outside the recovered checkpoint's prefix. Future live-stream writes follow the
owner's synchronization schedule. Recovery index opening establishes its immediate
parent's directory durability on Unix; the initializer publishes recovered progress
before preparing a new empty pair.

Same-file tests use the shared stream fixtures. They cover unpolled creation,
missing and reused directories, zero lengths/cursors, readable/writable/seekable
handles, preserved neighbors/checkpoints, existing empty/nonempty logs, orphan
indexes, partial creation and retry, and directory obstructions at each range
level and either file path. Unix tests additionally cover dangling file links.
Tests check paths and concrete I/O causes, but do not simulate power loss or
cancellation during kernel I/O. Pair creation has no throughput measurement.

## Stream-directory initialization and failures

`StorageProvider::new(config).await` performs directory initialization. It uses
`StreamId::all()` to visit every supported, typed stream ID in ascending order,
calling Tokio's `create_dir_all` for each stream base. This ensures the root,
`streams`, and `0000` through `4095` exist.
There is no preceding existence check: creation itself accepts existing
directories and reports failures, avoiding a redundant filesystem lookup and
check-then-create race.

Construction is safe to repeat for an existing root. Existing files and directory
contents are left intact; construction does not scan, validate, truncate, overwrite
or clean them up. The four deeper range directories and actual log/index/checkpoint files are
not created. `create_log_and_index` creates those parents only as needed.

The first creation failure stops construction without returning a provider.
`StorageError::path()` identifies the full stream-directory path requested; the actual obstacle may be
one of its parents. Its standard error source preserves the original `io::Error`,
and its display includes both path and cause. For example, a regular file at
`streams/0007` prevents that directory from being created and is left untouched.

Failure or cancellation may leave earlier directories created. There is no
rollback, and constructing again after the cause is resolved accepts that partial
progress. An unpolled constructor future performs no I/O. Tests cover this lazy
behavior and a failure after several successful stream creations followed by retry.

Success establishes directory existence during the operation. It does not grant
exclusive ownership, guarantee future writes, validate existing records, or sync
directory entries to durable storage. File operations and startup recovery must
establish their own stronger guarantees.

## Storage boundaries and future work

The [streams module](../streams/README.md) owns one log/index pair exclusively,
enforces sequence continuity on append, orders output and reports separate flushed
and synchronized endpoints. It finalizes a full, synchronized pair without
changing paths. Its validator obtains handles through the provider, checks the
untrusted suffix, removes corrupt tails, repairs its index and returns a synchronized
endpoint, plus append-positioned file handles when partial.

Handle caching, coordination with independent readers, historical/sealed-file
reads, startup integration and live file rotation remain unimplemented.
Checkpoint persistence and fresh log/index-pair creation are implemented;
the per-stream initializer advances recovered checkpoints after validation and
cleanup, before preparing the active pair. `prepare_active_pair` retains
a validated partial pair or creates a new empty pair through `create_log_and_index`
without an initial file sync. Index opening synchronizes its immediate parent on
Unix before validation, and checkpoint publication synchronizes its own parent
after renaming. The caller supplies a durably established directory hierarchy;
neither operation synchronizes ancestors. Interrupted creation requires
recovery before retrying.
Directory initialization alone grants no validation, durability or read readiness.

Read the [record specification](../../../transaction-log-exports/src/record/README.md)
before integrating record I/O. The provider does not change record framing or
the reader's validation responsibilities.

## Verification and maintenance

Location and checkpoint tests are specified in their owning
[location module](../streams/location/README.md#verification) and
[streams module](../streams/README.md#checkpoint-validation).

Tests remain in the corresponding source files. Configuration tests cover relative
root resolution, nonexistent roots and empty input. Provider tests use independent
path fixtures at every three-digit carry boundary and the numeric maximum, along
with stream limits, Unicode/space-containing roots, and no filesystem side effects
from path construction. Checkpoint fixtures cover stream limits, the stable base
directory and filename, and paths before any files exist. Relative-root and
Unix-only non-UTF-8-root checks cover checkpoint paths as well.

Construction, layout-damage and special-root tests use isolated temporary
directories. They verify all 4,096 stream bases, absence of eagerly created range
directories, repeated construction, preservation of existing contents, collisions
at the root/parent/stream levels, retained causes and retry after partial
initialization. Their temporary directory owners clean up after handles close.
Tests do not change the process-wide working directory. Routine filesystem tests
use the shared fixture below instead of repeatedly creating/removing all bases.

`StorageProvider::clear_all(&mut self)` is available only under `cfg(test)` for
reusing initialized test storage. It removes all contents except the root,
`streams`, and the canonical 4,096 stream bases. This includes metadata, staging,
range directories and unrelated entries. It does not delete/recreate the retained
directories, sync disposable data or repair a damaged base layout. Callers must
keep that layout intact, close file handles and exclude other access throughout
cleanup and use. Errors preserve the affected path and can follow partial removal.
Directory links are not traversed. The scan runs as one Tokio blocking task to
avoid thousands of async dispatches for empty bases; callers must await completion
before releasing their fixture. Same-file tests check repeated clearing, reuse,
all stream bases and unrelated content; Unix tests also protect an outside sentinel
behind symlinks.

### Shared test storage

The crate-private, test-only `test_support` module serves provider, initializer,
indexed-log writer and indexed-log validator tests. `storage_fixture().await` returns an owned
`StorageFixture` guard with read-only `provider()` and `stream_id()` getters.
A Tokio `OnceCell` constructs the real provider and awaits `clear_all` exactly
once before publishing it. Construction is not bypassed. An atomic counter
assigns distinct IDs across all consuming suites; claim another fixture when a case
needs another stream. Independent parameter-loop cases claim separate guards.
Allocation order and numeric IDs are not assumptions tests may rely on.

Each guard owns only its stream's contents. Its `Drop` removes files and nested
range directories, preserving the stream base. Declare the guard before the
file handles and state that use it, retain it for their entire lifetime, and
await all filesystem work before leaving its scope. Copying its provider
reference or stream ID does not extend the guard's lifetime. Drop performs small,
synchronous filesystem cleanup on the test thread; it does not spawn async work
or depend on a Tokio runtime remaining alive. A damaged or missing stream base
is an error, and cleanup never follows directory links into another tree.

No per-test mutex is held for streams, so independent Tokio test runtimes can
use the provider concurrently. Path-only tests may query fixed boundary IDs
without inspecting or changing those streams' contents; filesystem assertions
use an owned fixture's ID. Tests that alter base directories, exercise
construction/reset itself, or require a particular root retain isolated providers.

Clustering configuration is root-wide, so stream IDs cannot isolate it. The
provider's clustering tests borrow the shared provider and own a metadata guard
holding the existing async mutex. It removes `clustering.json` and its pending
sibling before releasing that mutex, including during panic unwinding. Only these
guarded tests may modify clustering metadata; stream tests proceed independently.

Cleanup runs through normal Rust scope exit under `cargo test` and IDE test runs,
including assertion failures that unwind. A removal failure panics during normal
exit, failing the test. During an existing panic it reports the path and cause to
stderr without a second panic, preserving the original test failure. Removal can
be partial on I/O failure. Startup clearing remains the fallback for leftover data
after such errors, aborted processes, forced termination or machine restarts;
`Drop` cannot guarantee cleanup when a process does not unwind.

The cache lives below `test-storage/log data 東京/replica` beside the test executable
in Cargo's build output, exercising spaces and Unicode as part of ordinary tests.
Successful guard cleanup leaves only the empty directory skeleton. Later runs
reuse that layout; `cargo clean` removes it. An OS file lock held for the process
lifetime excludes competing test processes using the cache. The small lock file
remains beside it so another process cannot bypass exclusion by replacing the
lock file. Lock acquisition runs on a blocking worker. Await shared fixture
initialization to completion: cancellation does not stop spawned filesystem work.
The static provider is intentionally retained; file cleanup belongs to the owned
guards, and needs no runner script or suite-teardown hook.

Same-file guard tests check distinct IDs, cleanup of nested contents, preservation
of another live stream and the base directory, and cleanup during panic unwinding.
Windows tests deny deletion to check error reporting and preservation of an
existing panic; Unix tests check that a linked outside sentinel is untouched.
The metadata tests check published/staging cleanup on success and panic before
another case acquires the lock.

`record_fixture` adapts independent literal records to an allocated stream by
changing only the ID and CRC-32C trailer, without invoking the production encoder.
Known complete byte fixtures, including a stream ID with a nonzero high byte,
check that adaptation. A same-file fixture test verifies a second claim gets a
different ID in the same root and preserves the first case's files. The consuming
suites keep explicit expected endpoints/index bytes and run in parallel to expose
accidental shared IDs or resets. This support changes test setup only; it makes no
production recovery-throughput claim.

From the workspace root:

```sh
cargo test -p transaction-log --locked
cargo test -p transaction-log --release --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo doc --workspace --no-deps --locked
```

Run filesystem tests on both Windows and Linux when those environments are available;
OS-specific error codes and path rules need not be identical. Update API contracts,
same-file tests and this specification together when storage path or metadata
rules intentionally change. Keep implemented behavior distinct from planned file
lifecycle and recovery behavior.

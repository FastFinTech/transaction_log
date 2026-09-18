# Storage

This application module groups storage types for log, index and other file types.
It implements immutable startup configuration,
deterministic log/index/checkpoint paths and stream-directory initialization.
The provider also opens existing logs and their repairable indexes. The sibling
[streams module](../streams/README.md) implements indexed appends, validation from
a supplied trusted boundary, index repair and explicit invalid-tail recovery.
Stored clustering configuration read/write operations are also
implemented. Checkpoint persistence, startup log recovery and management of live files remain
future work.

Provider implementation, root configuration and shared errors live directly in this directory, one type per file. Logical file identities, sequence grouping, endpoints and ranges belong to [streams::location](../streams/location/README.md). The thin `mod.rs` re-exports `StorageProvider`, `StorageConfig` and `StorageError` and includes this specification in Rustdoc.

| Source | Responsibility |
| --- | --- |
| `storage_error.rs` | Shared provider I/O/JSON failures and bounded-read limits, retaining paths and original causes. |
| `storage_config.rs` | Immutable startup configuration and absolute root resolution. |
| `storage_provider.rs` | All provider methods: configuration ownership, paths, directory initialization, file acquisition, stored clustering configuration I/O. |

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
types. Publication never overwrites existing metadata. See the
[provider metadata contract](#provider-metadata-operations) for concrete errors, interrupted writes
and durability limits. UUID establishment, configuration comparison and startup
integration remain deferred and are not provider responsibilities.

## Provider metadata operations

The public `log_file_path` and `index_file_path` methods share the private
`log_or_index_file_path` helper for their numeric directory layout and basename.
Checkpoint and clustering metadata paths do not use that helper.
All provider methods live together in `storage_provider.rs`, including blocking startup read/write
methods for `EstablishedClusteringConfiguration`;
`storage_error.rs` retains path and concrete I/O/JSON
errors. `read_clustering_configuration` reads root `clustering.json`, returns
`None` for absent metadata, rejects unpublished staging files, bounds reads to
4 KiB and deserializes through the model's validation. It creates nothing.
`write_clustering_configuration` publishes a supplied configuration/UUID once:
existing files are never overwritten. It creates the root, synchronizes a
create-new `.clustering.pending` file and publishes it using a hard link.
Unix synchronizes the root directory before and after staging-file removal;
Windows directory durability is not guaranteed and ancestors are not synced.
Errors may leave pending or published metadata; callers must reread/inspect it
and must not assume a failed write assigned nothing. Exclusive root ownership
and hard-link support are required. UUID generation and startup integration
are not part of these operations. Temporary filesystem tests cover both modes,
all slots, independent JSON fixtures, malformed/oversized input, no-overwrite
behavior and interrupted publication.

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

The [streams checkpoint module](../streams/README.md#stream-checkpoint-model) owns `StreamCheckpoint`, its Serde representation, certification requirements and publication ordering. Storage owns only the checkpoint file path; checkpoint persistence and startup integration remain deferred.

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

`StorageProvider::new(config: StorageConfig) -> StorageProvider` takes ownership
without filesystem I/O. `root_directory()` borrows the absolute configured root.
The provider is immutable, has no interior mutability or background tasks, and
can be shared through `Arc<StorageProvider>` when multiple components need it.
The configuration does not need its own `Arc` inside the provider.

Callers construct configuration and the provider before using its methods.
`initialize()` creates stream base directories when explicitly requested; it is
not called by the current startup scaffold. No storage-worker architecture or
startup recovery coordinator is implemented. There is no
initialization flag or per-call readiness check. Calculating a path is valid
before initialization; actual file operations must handle their own I/O errors.

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
already exist. A validator opens the log successfully first, so a missing log
does not create an orphan index. Neither method invents valid records or entries.

`StorageError` preserves the requested path and concrete OS error. Returned
handles belong to the caller; the provider keeps no cache, task or mutex. Default
file sharing permits independent readers, but these methods do not coordinate
them, acquire exclusive recovery locks, or make the pair queryable. The caller
must exclude other writers and withhold recovering indexes from readers. Creating
a file does not durably synchronize its parent directory or publish stream state.

## Stream-directory initialization and failures

`initialize(&self) -> Result<(), StorageError>` is asynchronous. It uses
`StreamId::all()` to visit every supported, typed stream ID in ascending order,
calling Tokio's `create_dir_all` for each stream base. This ensures the root,
`streams`, and `0000` through `4095` exist.
There is no preceding existence check: creation itself accepts existing
directories and reports failures, avoiding a redundant filesystem lookup and
check-then-create race.

Initialization is safe to repeat. Existing files and directory contents are left
intact; initialization does not scan, validate, truncate, overwrite or clean them
up. The four deeper range directories and actual log/index/checkpoint files are
not created. Future file creation will ensure those parents only as needed.

The first creation failure stops initialization. `StorageError::path()`
identifies the full stream-directory path requested; the actual obstacle may be
one of its parents. Its standard error source preserves the original `io::Error`,
and its display includes both path and cause. For example, a regular file at
`streams/0007` prevents that directory from being created and is left untouched.

Failure or cancellation may leave earlier directories created. There is no
rollback, and retrying after the cause is resolved accepts that partial progress.
The provider stores no success/failure state, so a failed attempt does not poison
the object. Tests cover a failure after several successful stream creations.

Success establishes directory existence during the operation. It does not grant
exclusive ownership, guarantee future writes, validate existing records, or sync
directory entries to durable storage. File operations and startup recovery must
establish their own stronger guarantees.

## Storage boundaries and future work

The [streams module](../streams/README.md) owns one log/index pair exclusively,
enforces sequence continuity on append, orders output and reports separate flushed
and synchronized endpoints. It finalizes a full, synchronized pair without
changing paths. Its validator obtains handles through the provider, checks the
untrusted suffix, repairs its index, supports explicitly authorized log truncation,
and hands over either a synchronized partial writer or full completion metadata.

Handle caching, coordination with independent readers, historical/sealed-file
reads, checkpoint persistence and advancement, durable checkpoint publication and
startup-wide recovery orchestration remain unimplemented. Directory initialization
alone grants no validation, durability or read readiness.

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
directory and filename, and paths before initialization. Relative-root and
Unix-only non-UTF-8-root checks cover checkpoint paths as well.

Filesystem tests use isolated temporary directories and verify all 4,096 stream
bases, absence of eagerly created range directories, repeated initialization,
preservation of existing log/index/checkpoint contents, collisions at the
root/parent/stream levels, retained error causes, and retry after partial initialization. They do not
change the process-wide working directory. Temporary test data is cleaned up by
the test directory owner.

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

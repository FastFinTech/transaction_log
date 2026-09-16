# Storage

This application module groups storage types for log, index and other file types.
It currently implements log-file identities, a JSON-serializable stream-checkpoint
model, immutable startup configuration, deterministic log/index paths and stream-directory
initialization. File persistence, index encoding, recovery and management of open
files remain future work.

Each type has its own source file. The thin `mod.rs` re-exports the public types
and `RECORDS_PER_FILE` and includes this specification in Rustdoc:

| Source | Responsibility |
| --- | --- |
| `log_file_id.rs` | A stream/file-number pair, its assigned record range and derived Serde representation. |
| `log_file_number.rs` | Validated file numbers, sequence-to-file grouping, checked successors, Serde integer representation and the records-per-file constant. |
| `log_file_number_error.rs` | Raw file-number validation failures. |
| `storage_config.rs` | Immutable startup configuration and absolute root resolution. |
| `storage_provider.rs` | Configuration ownership, path construction and asynchronous directory initialization. |
| `storage_provider_error.rs` | A failed directory-creation request and its underlying I/O error. |
| `stream_checkpoint.rs` | Immutable last-record identity and its exclusive end position in a log file, with JSON serialization. |

Storage policy belongs to the application, not the public record I/O exports
crate. `LogFileId` describes where a record belongs in a stream's sequence;
`StorageProvider` maps that identity to a physical location. Neither an ID nor
a calculated path establishes that a file or record exists.

The executable still has its minimal entry point. Choosing configuration inputs
and calling the provider from service startup remain future integration work.

## Assigned sequence ranges

A file belongs to exactly one `StreamId`. Its zero-based `file_number` assigns
100,000 consecutive sequence positions using:

```text
file_number = sequence_number / RECORDS_PER_FILE
first_sequence = file_number * RECORDS_PER_FILE
last_sequence = min(first_sequence + RECORDS_PER_FILE - 1, u64::MAX)
record_index = sequence_number - first_sequence  (for a record in this file)
```

| File number | First sequence | Last sequence |
| ---: | ---: | ---: |
| 0 | 0 | 99,999 |
| 1 | 100,000 | 199,999 |
| 2 | 200,000 | 299,999 |
| 184,467,440,737,094 | 18,446,744,073,709,400,000 | 18,446,744,073,709,499,999 |
| 184,467,440,737,095 | 18,446,744,073,709,500,000 | 18,446,744,073,709,551,615 (`u64::MAX`) |

The mapping is anchored at sequence zero. Changing the divisor or starting offset
changes file identities and is a storage-layout change, not an independently
configurable process setting. `RECORDS_PER_FILE` in `log_file_number.rs` is the single
production definition. The exports crate's `SequenceNumber` continues to accept
every `u64`; storage grouping is application policy, not a wire-format change.

A completed file must hold exactly 100,000 records in its assigned consecutive
range. Counting 100,000 arbitrary records is insufficient: the future writer and
recovery code must enforce the stream and sequence values. These identity helpers
do not validate file contents or sequence continuity. A partial file's
`last_record_id()` is its assigned endpoint, not its last written, validated,
received or durable record.

## Numeric limits and construction

`LogFileNumber` is a read-only `u64` wrapper. Its typed `MIN` is zero and its typed
`MAX` is `u64::MAX / RECORDS_PER_FILE`, inclusive. Because
numbering starts at zero, the number of addressable files per stream is one more
than this maximum: 184,467,440,737,096. The cap ensures every constructible file ID
has at least one representable sequence position, and its first sequence fits in
`u64`.

The final range has only 51,616 representable positions. Its last record ID is
`SequenceNumber::MAX`; endpoint arithmetic deliberately caps there rather than
wrapping, rejecting that otherwise valid record ID, or panicking on a getter.
It cannot become a completed 100,000-record file. Sealing or stream retirement
at numeric exhaustion is future lifecycle policy; the ID type does not introduce
an exception to the completion rule or claim that this terminal file is sealed.

- `LogFileNumber::new(raw)` and `TryFrom<u64>` validate the numeric bound and
  return `LogFileNumberError` for invalid input. The error retains the rejected
  number through a read-only `value()` getter; no clamping or truncation occurs.
- `LogFileNumber::from_sequence_number(sequence)` and `From<SequenceNumber>` are
  infallible: division by the record count proves the bound without revalidation.
  These are sequence-to-file grouping, distinct from validating a raw file number.
- `LogFileNumber::get()` and `From<LogFileNumber> for u64` expose the validated raw
  value. Decimal display forwards formatting options, including the zero padding
  used by storage paths. The wrapper retains the native size/alignment of `u64`.
- `LogFileNumber::next()` checks the domain cap and panics at `MAX` in debug and
  release builds. Checking `u64` overflow alone would allow invalid file numbers.
  Exhaustion is a programming error, consistent with `RecordId::next()`.
- `LogFileId::new(stream_id: StreamId, file_number: LogFileNumber)` combines already
  valid identifiers and returns `Self`. It needs no validation or error type.
- `LogFileId::from_record_id(record_id)` delegates grouping to `LogFileNumber` and
  preserves the validated stream ID, without another validation pass.
- `LogFileId::stream_id()` and `file_number()` are copy getters returning the typed
  components. Neither type has setters or unchecked public constructors.
- `LogFileId::first_record_id()` and `last_record_id()` return inclusive assigned
  endpoints. `record_index(record_id)` returns `Some(ordinal)` only for a record
  in this stream and range; otherwise it returns `None`.
- `LogFileId::next()` preserves the stream and delegates to the file number's
  checked successor. It does not duplicate the range check.

IDs are `Copy`, equatable, ordered and hashable. Ordering compares stream first,
then file number; it is not chronology across streams. The compiler-selected
native layout is not a file encoding, filename convention or persisted format.

`LogFileNumber` uses `#[serde(try_from = "u64", into = "u64")]`, so it serializes
as an integer and deserializes through its checked conversion. Do not replace
this with transparent deserialization, which would bypass the domain bound.
The range constraint belongs to this primitive wherever it is used.

`LogFileId` derives Serde serialization/deserialization as named integer fields,
for example `{"stream_id":42,"file_number":1234}`. Each field validates its own
domain, so no custom deserializer or temporary field representation is required.
Missing, duplicate and unknown object fields are rejected. The validation works
through Serde's format-independent traits, with no separate parser for JSON or
a compatible binary format. These types establish numeric identity, not file
existence, record validity or durability.

## Stream checkpoint model

`StreamCheckpoint` stores two read-only fields with `getset` copy getters:

- `last_record_id: RecordId`: the last complete record included in the checkpoint.
  Its identity already includes the stream ID.
- `end_position: u64`: the byte offset from the beginning of that record's log
  file to immediately after the complete encoded record, including its CRC.
  The boundary is exclusive: following bytes have not been checkpointed by this
  snapshot. It is not the position of the last byte, an index-file position, or
  a cumulative byte count across multiple files.

`log_file_id()` derives the file identity through `LogFileId::from_record_id`, so
the model does not store redundant stream/file identifiers that could disagree.
At sequence 99,999 the endpoint still belongs to file zero; sequence 100,000
belongs to file one and has a position within that new file. The helper never
increments the sequence and remains usable at `u64::MAX`. An endpoint may precede
the physical end of a file containing newer, uncheckpointed records.

Use `Option<StreamCheckpoint>` for an absent checkpoint, including an empty stream;
do not reserve sequence zero as a sentinel. Positions use `u64` because log files
can exceed 4 GiB. The model is a small owned `Copy` value without allocation,
mutable accessors or filesystem access. Its native layout does not define its
serialized representation.

`new(last_record_id, end_position)` is an infallible data constructor. It does not
claim to validate the relationship between the record and its supplied position,
and it does not add a partial numeric check in place of inspecting actual storage.
The future checkpoint owner must establish the complete contract when producing
or loading a trusted recovery checkpoint.

A checkpoint published for recovery must describe a contiguous prefix of valid,
durable records on the local replica. It may lag durable data, but must never lead
it. Validity includes record framing, CRC, stream identity and sequence continuity.
The owner must capture a complete boundary and make the covered data durable before
durably publishing that checkpoint. Constructing this value establishes none of
those facts. Checkpoint file loading, persistence, advancement, index recovery
and startup integration remain future work; this step implements the model and
its JSON representation.

### JSON representation

Checkpoint metadata uses human-readable JSON. It is small and updated outside
the per-record hot path, so a compact binary encoding is not necessary. Serde
derives describe the fields, and `serde_json` provides the JSON codec:

```json
{
  "last_record_id": {
    "stream_id": 42,
    "sequence_number": 99999
  },
  "end_position": 6553500000
}
```

Use `serde_json::to_vec_pretty(&checkpoint)` to prepare bytes for asynchronous
file output, and `serde_json::from_slice::<StreamCheckpoint>(&bytes)` to decode
them. The same traits also support `to_writer_pretty` and `from_reader` for
synchronous `std::io` destinations/sources. These codecs do not flush, sync or
atomically publish a checkpoint file. Those operations belong to the future
persistence owner, which must publish only after covered log data is durable.

Fields are required; missing, duplicate or unknown object fields are errors,
including inside the nested record ID. There are no default-zero substitutions.
`StreamId` deserialization uses its existing checked conversion, so JSON cannot
introduce an ID outside `0..=4095`. Sequence numbers and positions decode as `u64`:
negative, fractional and overflowing values are rejected. JSON integer text
preserves the full `u64` range through `serde_json`; tooling that rewrites these
files must preserve integers rather than round them through floating point.

These checks establish typed metadata only. A syntactically valid checkpoint
can still point to missing, mismatched or non-durable records; file-level recovery
must establish those facts before trusting it. Deserialization does not add
filesystem I/O or partial record/offset validation to this model.

The serialization traits are format-independent and reuse the identifier types'
validation. There are no per-format custom deserializers or separate JSON DTOs.
JSON is the selected metadata encoding; no binary checkpoint codec or persisted
file envelope has been introduced. Metadata serialization does not change the
binary record protocol, identifier layouts, getters or hot-path validation.

## Storage and performance boundaries

Direct arithmetic makes record-to-file lookup independent of directory scans or
record byte lengths. It performs no allocation, I/O, locking or shared-state
access. Raw file-number validation happens in `LogFileNumber` at construction or
deserialization; sequence conversion proves the bound arithmetically. `LogFileId`
construction and typed getters reuse those invariants. After matching the stream,
`record_index` explicitly rejects a sequence below the file's first sequence,
subtracts that first sequence, then rejects an index at or above `RECORDS_PER_FILE`.
The first check prevents underflow; the second enforces the upper bound. This
avoids another division or computing a saturating last-sequence bound, while
keeping both checks visible in ordinary control flow.

On 2026-09-16, standalone wrappers using the production types were compared with
Rust 1.98.1, LLVM 22.1.8, `x86_64-pc-windows-msvc`, and `-C opt-level=3`. These
explicit checks compiled identically to checked subtraction followed by
`then_some`; the compiler merged the wrappers into one function. This is evidence
for that compiler and target, not a portable code-generation guarantee or a timed
throughput result. No benchmark has been added for this primitive; storage
benchmarks will follow actual file operations.

The ordinal can locate an index entry, but it cannot locate a variable-length
record's byte offset without index/file information. A full file contains 100,000
records irrespective of their byte sizes. The provider owns the file-path layout;
index encoding and offset width, open handles and read/write ownership remain
responsibilities of the future storage implementation. Range endpoints must not
be substituted for a durability or validation checkpoint.

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

The intended startup order is: construct configuration, construct the provider,
await `initialize()`, and then hand the provider to storage workers. There is no
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

File number:  000 000 000 001 234
Directories:  000/000/000/001/
```

At the maximum stream ID and file number:

```text
{root}/streams/4095/184/467/440/737/184467440737095.log
{root}/streams/4095/184/467/440/737/184467440737095.idx
```

The slashes above describe path components. Production construction uses native
`PathBuf` operations on both Windows and Linux, without converting the configured
root to a string. The bounded `LogFileId` guarantees that the file number fits
the fifteen-digit representation.

| Directory | Maximum assigned contents |
| --- | ---: |
| `streams/` | 4,096 stream directories, `0000` through `4095`. |
| A stream base | 185 top-level range directories, `000` through `184`. |
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

This is a fixed storage-layout contract, not a runtime directory-size setting.
Changing digit widths, grouping, extensions or the `streams` component changes
where files are found. Keep naming logic in the provider rather than duplicating
it in future readers, writers, index maintenance or recovery code.

## Path construction and performance

- `log_file_path(id: LogFileId) -> PathBuf` returns the complete absolute log path.
- `index_file_path(id: LogFileId) -> PathBuf` returns the paired index path with
  the same directory and numeric basename.

Both methods are synchronous and deterministic. They allocate a new path without
scanning directories, testing existence, creating parents, opening handles or
checking file contents. The caller retains ownership of the result independently
of the provider's lifetime.

Path construction is not expected to occur frequently enough to justify caching
filenames or directory names. Ordinary string/path allocation is an explicit
design choice. Do not add caches, shared scratch buffers, fixed-size formatting
machinery or caller-supplied output-buffer APIs without a demonstrated need.
The two public methods share one private formatter so log/index paths cannot
silently diverge. Initialization and formatting share the stream-base helper.
That helper accepts a validated `StreamId`, preserving the type's domain contract
through path construction rather than accepting arbitrary raw integers.

There is no provider throughput claim or benchmark yet. Record-processing hot
paths and file-handle ownership will be measured when file operations exist.

## Initialization and failures

`initialize(&self) -> Result<(), StorageProviderError>` is asynchronous. It uses
`StreamId::all()` to visit every supported, typed stream ID in ascending order,
calling Tokio's `create_dir_all` for each stream base. This ensures the root,
`streams`, and `0000` through `4095` exist.
There is no preceding existence check: creation itself accepts existing
directories and reports failures, avoiding a redundant filesystem lookup and
check-then-create race.

Initialization is safe to repeat. Existing files and directory contents are left
intact; initialization does not scan, validate, truncate, overwrite or clean them
up. The four deeper range directories and actual log/index files are not created.
Future file creation will ensure those parents only as needed.

The first creation failure stops initialization. `StorageProviderError::path()`
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

File opening, handle caching, the single writer/multiple readers model, index
encoding, sealed-file reads, sequence continuity, checkpoint persistence and
advancement, durable publication and invalid-tail recovery remain unimplemented.
This module must not make directory initialization appear to provide any of those
behaviors.

Read the [record specification](../../../transaction-log-exports/src/record/README.md)
before integrating record I/O. The provider does not change record framing or
the reader's validation responsibilities.

## Verification and maintenance

Same-file tests in `log_file_number.rs` cover raw construction/conversions, domain
limits, rejected values, sequence grouping, numeric display/zero padding, native
layout, integer JSON and checked successor exhaustion. Tests in `log_file_id.rs`
cover independent expected ranges, exact file boundaries, zero and maximum
sequences, all ordinals in an ordinary and the terminal range, mismatched
streams/ranges, and successor delegation. Expected boundary values are literal
fixtures so a mistaken production constant or calculation cannot redefine the
expected results.
JSON tests cover the exact named-field representation, domain endpoints, invalid
stream/file numbers, propagated constructor errors and malformed object fields.

`stream_checkpoint.rs` tests the exclusive endpoint's associated file at sequence
zero, file rotation, a position above 4 GiB and sequence exhaustion. These tests
exercise the metadata model, not validation or durability of real stored records.
JSON fixtures check pretty output and I/O round trips, exact maximum sequence
values, large offsets, missing/duplicate/unknown fields, invalid numeric values,
truncation and trailing garbage. Expected JSON is literal, so serialization and
deserialization cannot silently agree on an unintended field shape.

Tests remain in the corresponding source files. Configuration tests cover relative
root resolution, nonexistent roots and empty input. Provider tests use independent
path fixtures at every three-digit carry boundary and the numeric maximum, along
with stream limits, Unicode/space-containing roots, and no filesystem side effects
from path construction. A Unix-only test checks a non-UTF-8 root component.

Filesystem tests use isolated temporary directories and verify all 4,096 stream
bases, absence of eagerly created range directories, repeated initialization,
preservation of existing log/index contents, collisions at the root/parent/stream
levels, retained error causes, and retry after partial initialization. They do not
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

Both debug and release identity tests protect the no-wraparound contract. Run
filesystem tests on both Windows and Linux when those environments are available;
OS-specific error codes and path rules need not be identical. Update API contracts,
same-file tests and this specification together when storage identity or layout
rules intentionally change. Keep implemented behavior distinct from planned file
lifecycle and recovery behavior.

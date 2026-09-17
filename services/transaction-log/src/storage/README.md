# Storage

This application module groups storage types for log, index and other file types.
It currently implements log-file identities, record-range location models, a
JSON-serializable stream-checkpoint model, immutable startup configuration,
deterministic log/index/checkpoint paths and stream-directory initialization.
The provider also opens existing logs and their repairable indexes. The sibling
[streams module](../streams/README.md) implements indexed appends, validation from
a supplied trusted boundary, index repair and explicit invalid-tail recovery.
Checkpoint persistence, startup orchestration and management of live files remain
future work.

Each type has its own source file. The thin `mod.rs` re-exports the public types
and `RECORDS_PER_FILE` and includes this specification in Rustdoc:

| Source | Responsibility |
| --- | --- |
| `log_file_id.rs` | A stream/file-number pair, its assigned record range and derived Serde representation. |
| `log_file_number.rs` | Validated file numbers, sequence-to-file grouping, checked successors, Serde integer representation and the records-per-file constant. |
| `log_file_number_error.rs` | Raw file-number validation failures. |
| `log_file_range.rs` | The portion of an individual log file needed for a range: bounded span, postfix, whole file or prefix. |
| `record_start_location.rs` | A record ID with its inclusive byte start, derived file identity and Serde metadata representation. |
| `record_end_location.rs` | A record ID with its exclusive byte end, derived file identity and Serde metadata representation. |
| `record_range_location.rs` | A typed start/end pair, checks on their relationship and lazy iteration over the required files and their portions. |
| `record_range_location_error.rs` | Inconsistent stream, sequence or known byte-boundary metadata. |
| `storage_config.rs` | Immutable startup configuration and absolute root resolution. |
| `storage_provider.rs` | Configuration ownership, paths, directory initialization and owned file acquisition for validation/repair. |
| `storage_provider_error.rs` | A failed directory-creation request and its underlying I/O error. |
| `storage_file_open_error.rs` | The requested log/index path and original file-opening error. |
| `stream_checkpoint.rs` | A checkpoint boundary held as a typed record end location, with JSON serialization. |

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
range. Counting 100,000 arbitrary records is insufficient: `IndexedLogWriter`
enforces the stream and sequence on append; `IndexedLogValidator` checks them
during recovery scanning. These identity helpers do not validate file contents or
sequence continuity. A partial file's
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
  endpoints.
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

`StreamCheckpoint` stores one read-only `end: RecordEndLocation`, exposed through
a `getset` copy getter. The endpoint keeps the last included record and its byte
position together:

- `checkpoint.end().record_id()` identifies the last complete record included in
  the checkpoint. Its identity already includes the stream ID.
- `checkpoint.end().position()` is the byte offset from the beginning of that
  record's log file to immediately after the complete encoded record, including its CRC.
  The boundary is exclusive: following bytes have not been checkpointed by this
  snapshot. It is not the position of the last byte, an index-file position, or
  a cumulative byte count across multiple files.

`checkpoint.end().log_file_id()` derives the file identity through the endpoint,
so the model does not store redundant stream/file identifiers that could disagree.
Endpoint access and file lookup belong to `RecordEndLocation`; the checkpoint
does not duplicate those methods. The separate checkpoint type expresses the
recovery boundary's meaning, which a general record end location does not carry.
At sequence 99,999 the endpoint still belongs to file zero; sequence 100,000
belongs to file one and has a position within that new file. The helper never
increments the sequence and remains usable at `u64::MAX`. An endpoint may precede
the physical end of a file containing newer, uncheckpointed records.

Use `Option<StreamCheckpoint>` for an absent checkpoint, including an empty stream;
do not reserve sequence zero as a sentinel. Positions use `u64` because log files
can exceed 4 GiB. The model is a small owned `Copy` value without allocation,
mutable accessors or filesystem access. Its native layout does not define its
serialized representation.

`StreamCheckpoint::new(end)` is an infallible data constructor. It does not
claim to validate the relationship between the record and its supplied position,
and it does not add a partial numeric check in place of inspecting actual storage.
The future checkpoint owner must establish the complete contract when producing
or loading a trusted recovery checkpoint.

A checkpoint published for recovery certifies a contiguous prefix of valid records
AND correct index entries on the local replica. It cannot advance until framing,
CRC, stream identity, sequence continuity and the corresponding exact record end
offsets are established. Synchronize the covered log data, then the index data,
before durably publishing the checkpoint. It may lag this durable pair, but must
never lead either file. Constructing the model establishes none of those facts.
Checkpoint loading, persistence, advancement and startup integration remain future
work. `IndexedLogValidator` accepts the explicitly supplied trusted endpoint; it
does not load or choose the checkpoint itself. It checks prefix presence and the
final certified index entry's agreement with that endpoint, without revalidating
earlier records or index entries. It validates and repairs the suffix. A missing
trusted index prefix or mismatching endpoint requires the caller to supply an
earlier trustworthy boundary for a separate attempt.

```rust
use transaction_log::storage::{RecordEndLocation, StreamCheckpoint};
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(99_999));
let end = RecordEndLocation::new(id, 6_553_500_000);
let checkpoint = StreamCheckpoint::new(end);
assert_eq!(checkpoint.end().record_id(), id);
assert_eq!(checkpoint.end().position(), 6_553_500_000);
assert_eq!(checkpoint.end().log_file_id().file_number().get(), 0);

let bytes = serde_json::to_vec_pretty(&checkpoint)?;
assert_eq!(serde_json::from_slice::<StreamCheckpoint>(&bytes)?, checkpoint);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`StorageProvider::checkpoint_file_path(stream_id)` names this stream's checkpoint
at `{root}/streams/{stream_id:04}/checkpoint.json`. The path depends only on the
stream, so advancing across log-file ranges does not change the checkpoint's
location. Initialization creates its parent directory but no checkpoint file.
The future loader must check that the decoded checkpoint's stream ID matches the
requested stream, as well as establishing its agreement with storage.

### Planned historical-read availability

Historical requests require a published contiguous prefix of validated, fully
indexed records. A ready recovery checkpoint can establish a durable serving
boundary. For the active file, `IndexedLogWriter` also reports a flushed boundary:
independent read-only handles can read completed writes through the OS cache
before durable synchronization. Whether to expose that newer prefix or require
durability remains the serving owner's policy; the writer reports both boundaries.
On startup, loading checkpoint metadata alone does not make a stream ready;
required indexes must also be available and consistent, with rebuilding where
necessary. Raw index length is not a substitute for this readiness contract.

Provide a per-stream endpoint reporting the **last queryable sequence number**
so historical clients can discover this limit before requesting data. Its value
comes from the serving owner's ready, published endpoint. It may lag the last
received or appended record; those newer records are not the advertised historical
limit. Represent absence explicitly when no queryable prefix exists, rather
than using sequence zero as a sentinel. A stream still recovering must not advertise
an unready checkpoint as available history.

The reported limit is a snapshot of the serving replica's availability. Each
historical request still checks its range against that replica's ready prefix;
an earlier status response does not establish readiness on another replica or
guarantee that older files remain retained. Requests beyond the limit must not
be reported as successful shortened ranges. Whether they return unavailable or
explicitly wait, along with the endpoint's name and wire response format, remains
future protocol design. These are development notes; no endpoint or historical
request handling is implemented yet.

### JSON representation

Checkpoint metadata uses human-readable JSON. It is small and updated outside
the per-record hot path, so a compact binary encoding is not necessary. Serde
derives describe the fields, and `serde_json` provides the JSON codec:

```json
{
  "end": {
    "record_id": {
      "stream_id": 42,
      "sequence_number": 99999
    },
    "position": 6553500000
  }
}
```

Use `serde_json::to_vec_pretty(&checkpoint)` to prepare bytes for asynchronous
file output, and `serde_json::from_slice::<StreamCheckpoint>(&bytes)` to decode
them. The same traits also support `to_writer_pretty` and `from_reader` for
synchronous `std::io` destinations/sources. These codecs do not flush, sync or
atomically publish a checkpoint file. Those operations belong to the future
persistence owner, which must publish only after covered log and index data are durable.

Fields are required; missing, duplicate or unknown object fields are errors,
including inside the endpoint and its nested record ID. There are no default-zero
substitutions.
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

## Dense index layout

The agreed index is a dense list of exclusive record end positions: one `u64`
for every record in a contiguous prefix of its log file. No separate index-entry
or collection model type is needed. `IndexWriter`, composed by `IndexedLogWriter`,
encodes entries into a reusable byte buffer containing only its pending batch.
Future index readers can seek directly to entries or load a list as `Vec<u64>`;
they need not retain the entire index merely to resolve a range.

The `.idx` file contains consecutive eight-byte little-endian integers, with
no per-entry record ID, padding, header or collection-length prefix. Its paired
`LogFileId` comes from the requested storage path. The entry's ordinal supplies
its sequence number, so storing that identity again would be redundant. This is
a binary format, distinct from the human-readable checkpoint JSON. A Rust vector's
native memory representation must not be treated as its portable file encoding.

For a record belonging to the requested file:

```text
n = record.sequence_number - log_file_id.first_record_id().sequence_number
index_entry_byte_position = n * 8
record_start = 0 if n == 0, otherwise end_positions[n - 1]
record_end = end_positions[n]
record_byte_range = record_start..record_end
```

Positions count bytes from the beginning of the log file. Each end includes the
complete record's CRC trailer; the next record starts immediately there. Log
records begin at byte zero with no file header or inter-record padding. Looking
up an end position needs one entry; finding both start and end needs two adjacent
entries, except for the first record. No index scan or binary search is needed.
These formulas describe layout and lookup work, not measured disk latency.

The initial zero boundary is implicit and is not stored. A completed file's
last record end is in entry 99,999 (the 100,000th entry), at byte offset 799,992
in the index. Reading that entry supplies the log file's complete record end
without querying log-file metadata; no extra terminal entry is required.

A full 100,000-record file has 800,000 bytes of index entries (about 781.25 KiB).
A partial index contains only its actual prefix's entries; unused slots must not
be interpreted as records. An empty index has no entries, and sequence zero
remains a real record. The terminal file range permits only 51,616 entries because
sequences end at `u64::MAX`. End positions need `u64`: 100,000 maximum-size records
occupy 6,553,500,000 log bytes, exceeding a `u32` offset.

Index data is derived from the log and is rebuildable. `IndexedLogValidator`
checks whole eight-byte entries, prefix plausibility and actual record boundaries
and IDs for the newly scanned suffix. The supplied starting boundary certifies
both earlier prefixes; structural checks on that trusted prefix do not establish
its original validity. Structural checks
alone cannot detect every stale or corrupt offset. The log remains authoritative;
an index endpoint is not evidence of CRC validity, sequence continuity or durable
storage, and cannot replace a trusted recovery checkpoint. An index may lag the
log, so a missing entry alone does not establish that the record is absent.

`IndexWriter` owns encoding and file output; `IndexedLogWriter` supplies the offsets
of accepted records and coordinates output with the paired log. The pair writer
completes the log batch and its destination flush before beginning index output,
then flushes the index. Concurrent index readers must account for a partial last
entry and respect the owner's published boundary. Ordered flushes establish read
visibility, not an atomic two-file commit or durability. The validator uses the
same `IndexWriter` to repair a bad/missing index suffix before handover. Historical
index lookup and range-serving APIs remain future work.

## Record range locations

Two distinct endpoint types describe a record's byte boundaries:

- `RecordStartLocation` stores a `record_id: RecordId` and its inclusive byte
  `position: u64` within that record's file.
- `RecordEndLocation` stores a `record_id: RecordId` and its exclusive byte
  `position: u64`, immediately after that record's CRC trailer in its file.

Both expose read-only `record_id()` and `position()` copy getters, plus
`log_file_id()` derived through `LogFileId::from_record_id`. Their
`new(record_id, position)` constructors are infallible metadata constructors:
they store the resolver's supplied values without inspecting files or adding
partial numeric validation. A location alone does not prove that a record exists
at that offset, is valid or is durable. Endpoint getters perform no validation.

Both endpoint types derive Serde with named `record_id` and `position` fields.
For example, an end location has this JSON representation:

```json
{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553500000}
```

A start location uses the same field names, with its inclusive start position.
The enclosing field or chosen Rust type establishes the endpoint's role; the
JSON has no start/end type tag. Distinct Rust types prevent swapped constructor
arguments, but do not make arbitrary serialized bytes trustworthy.
Deserialization delegates to `RecordId` and its validated identifier types,
requires a `u64` position, and rejects missing, duplicate or unknown object fields
at either level. All `u64` positions are representable metadata, including zero
and `u64::MAX`; storage validation must still establish real record boundaries.
In particular, decoding a zero end does not make it a valid record end.

The Serde traits are format-independent. This metadata representation does not
change the dense index's eight-byte end entries or the binary record protocol.
It adds no validation to typed getters or constructors. `RecordRangeLocation`
has no Serde implementation; its checked constructor remains the boundary for
establishing the relationship between endpoints.

A start lookup uses the preceding record's dense-index entry, or zero for the
first record in a file. Its stored ID still identifies the **requested record**,
not the preceding one. An end lookup uses the requested record's own index entry.
The end remains in that record's file even when it completes the file, and its
helper never increments the sequence number, including at `u64::MAX`.

`RecordRangeLocation` stores only `start: RecordStartLocation` and
`end: RecordEndLocation`, exposed through read-only copy getters. Distinct types
make swapped constructor arguments a compile-time error. There is no implicit
conversion between the endpoint roles. Use `range.start().record_id()`,
`range.start().position()` and `range.end().log_file_id()` to inspect them;
endpoint behavior belongs to those types rather than duplicate range accessors.

The two record IDs are **inclusive**. Equal IDs describe one complete record,
so a separate complete-record `RecordLocation` type is unnecessary. An empty
result is represented outside the range type, rather than reserving sequence
zero or constructing an empty byte span. Single-file ranges can contain several
records and exceed 65,535 bytes; positions and byte differences use `u64`, even
though an individual encoded length fits `u16`.

Positions are relative to their respective log files. The final position can be
smaller than the initial position when the range spans files; subtracting those
two values does not give the total transfer size. Each endpoint derives its own
file identity; there are no redundant stored file IDs, paths or per-record entries.

`RecordRangeLocation::new(start, end)` checks the metadata relationships needed
for enumeration and returns `RecordRangeLocationError`
for different streams, decreasing sequences, zero byte ends, or an end at/before
the start in a single file. The final prefix of a multi-file range is checked
against zero, never against the start offset in another file. Constructors do
not read indexes or validate that offsets agree with actual record boundaries,
encoded sizes or readable file extents. These structural checks are not record
validation; the future file/index resolver must establish agreement with storage.
The value is location metadata, not proof of CRC validity or durable persistence.

### Iterating over files

`iter()` owns a copy of the endpoints and lazily returns `(LogFileId, LogFileRange)`
items in ascending file-number order:

| Case | Enum value | Bytes requested from that file |
| --- | --- | --- |
| Both endpoints in one file | `FileRange { start_position, end_position }` | The explicit `start..end` span. |
| First of multiple files | `FilePostfix { start_position }` | From the explicit start through the file's validated record end. |
| Intermediate file | `EntireFile` | From zero through the file's validated record end. |
| Last of multiple files | `FilePrefix { end_position }` | From zero up to the explicit exclusive end. |

For example, a request from sequence 99,998 to 300,001 yields file zero's postfix,
files one and two in their entirety, then file three's prefix. Two adjacent files
produce no `EntireFile` item. A single record produces exactly one `FileRange`.
Explicit outer bounds are preserved even when they coincide with whole-file
boundaries; there is no need to infer EOF in place of a known end position.

The enum leaves intermediate byte ends unresolved because 100,000 variable-length
records do not determine a file's byte length. The future reader resolves those
ends while opening each file. It must establish the complete, validated extent
of that file's assigned records, not guess a maximum size or copy an unvalidated
physical tail. The last file is always bounded by the supplied end, even if
newer records have since been appended. The iterator itself does not open, seek,
copy, validate or synchronize any file, and creates no asynchronous tasks.

The endpoint and range models are `Copy` and have no heap storage. Construction
does constant work; iteration uses constant memory and advances one file per item,
without allocating a list or resolving all file ends upfront. It stops before asking for a successor
to the final file, including `LogFileNumber::MAX`, and remains exhausted thereafter.
The full sequence domain can therefore be represented and traversed progressively.
These are algorithmic properties, not benchmark results. No `repr(C)`, cache,
lock or shared ownership is needed for these value types. Serde metadata is
separate from their compiler-selected memory layout.

Owning a location does not keep files open, prevent retention or stabilize file
contents. Future request handling must acquire suitable read handles and preserve
the readable boundaries while copying. Historical sealed-file reads and current-file
publication remain file-layer work; a location is not a recovery checkpoint.

## Storage and performance boundaries

Direct arithmetic makes record-to-file lookup independent of directory scans or
record byte lengths. It performs no allocation, I/O, locking or shared-state
access. Raw file-number validation happens in `LogFileNumber` at construction or
deserialization; sequence conversion proves the bound arithmetically. `LogFileId`
construction and typed getters reuse those invariants. These are implementation
choices, not measured throughput claims. Storage benchmarks will follow actual
file operations.

`LogFileId` describes file identity and assigned record-range boundaries.
Resolving record positions belongs to future file/index APIs using the dense
layout above; `RecordRangeLocation` carries their outer endpoints and enumerates
the required file portions. A full file contains 100,000 records irrespective of
their byte sizes. The provider owns the file-path layout; the streams module owns
append and validation/repair operations on a supplied pair. Named provider methods
acquire recovery handles; historical lookup and coordination of live writer/read
handles remain future integration work.
Range endpoints must not be substituted for a durability or validation checkpoint.

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

`StorageFileOpenError` preserves the requested path and concrete OS error. Returned
handles belong to the caller; the provider keeps no cache, task or mutex. Default
file sharing permits independent readers, but these methods do not coordinate
them, acquire exclusive recovery locks, or make the pair queryable. The caller
must exclude other writers and withhold recovering indexes from readers. Creating
a file does not durably synchronize its parent directory or publish stream state.

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
up. The four deeper range directories and actual log/index/checkpoint files are
not created. Future file creation will ensure those parents only as needed.

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

The [streams module](../streams/README.md) owns one log/index pair exclusively,
enforces sequence continuity on append, orders output and reports separate flushed
and synchronized endpoints. It finalizes a full, synchronized pair without
changing paths. Its validator obtains handles through the provider, checks the
untrusted suffix, repairs its index, supports explicitly authorized log truncation,
and hands over either a synchronized partial writer or full completion metadata.

Handle caching, coordination with independent readers, historical/sealed-file
reads, checkpoint persistence and advancement, directory-durable publication and
startup-wide recovery orchestration remain unimplemented. Directory initialization
alone grants no validation, durability or read readiness.

Read the [record specification](../../../transaction-log-exports/src/record/README.md)
before integrating record I/O. The provider does not change record framing or
the reader's validation responsibilities.

## Verification and maintenance

Same-file tests in `log_file_number.rs` cover raw construction/conversions, domain
limits, rejected values, sequence grouping, numeric display/zero padding, native
layout, integer JSON and checked successor exhaustion. Tests in `log_file_id.rs`
cover independent expected ranges, record-to-file mapping at exact file and
integer boundaries, preserved stream IDs, zero and maximum sequences, and
successor delegation. Expected boundary values are literal fixtures so a mistaken
production constant or calculation cannot redefine the expected results.
JSON tests cover the exact named-field representation, domain endpoints, invalid
stream/file numbers, propagated constructor errors and malformed object fields.

`stream_checkpoint.rs` tests preservation of the supplied typed endpoint at sequence
zero, file rotation, a position above 4 GiB and sequence exhaustion. These tests
exercise the metadata model, not validation or durability of real stored records.
JSON fixtures check pretty output and I/O round trips, exact maximum sequence
values, large offsets, missing/duplicate/unknown fields at all three object levels,
invalid numeric values, truncation and trailing garbage. Expected JSON is literal, so serialization and
deserialization cannot silently agree on an unintended field shape.

`record_start_location.rs` and `record_end_location.rs` test independent endpoint
identity and position preservation, both stream limits, positions above 4 GiB,
file rotation and the maximum sequence. Literal fixtures distinguish a start in
the new file from the preceding record's end in the completed file.
Their JSON tests cover literal field shapes and I/O round trips, zero and maximum
numeric metadata, invalid stream IDs and numeric types, missing/duplicate/unknown
fields, truncation and trailing garbage. Acceptance of raw offset limits protects
the distinction between metadata decoding and actual storage validation.

`record_range_location.rs` tests the typed endpoint pair and `LogFileRange` enum contract:
single records, partial single-file requests, every multi-file role, adjacent and
exact file boundaries, independent offsets across files, positions above 4 GiB,
stream limits, the terminal sequence/file and repeated iterator exhaustion.
Invalid stream/order/known byte-span metadata exercises each typed error.
A full-domain test consumes only a few iterator items, protecting lazy enumeration
without allocating or visiting the entire file range. Literal expected file numbers
and enum values keep these tests independent of the iterator's implementation.

The checkpoint example exercises typed construction, endpoint access and JSON
round trips. The range constructor's Rustdoc includes a working typed-endpoint
example and a compile-fail example for swapped roles. The application is currently
a binary target, so ordinary `cargo test` does not discover these documentation tests.
They can be checked with `rustdoc --test` against a temporary library build of
the application source using Cargo's compiled dependencies. Keep that library
and other compiler-check artifacts under the ignored `target/` directory.

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

Both debug and release identity tests protect the no-wraparound contract. Run
filesystem tests on both Windows and Linux when those environments are available;
OS-specific error codes and path rules need not be identical. Update API contracts,
same-file tests and this specification together when storage identity or layout
rules intentionally change. Keep implemented behavior distinct from planned file
lifecycle and recovery behavior.

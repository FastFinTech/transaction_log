# Stream locations

This streams module owns typed logical file identities and record byte locations.
`log_file_number.rs` and its error own numeric validation and the authoritative
`RECORDS_PER_FILE` constant; `log_file_id.rs` combines a file number with a stream,
maps file-local record positions and provides inclusive file-ID iteration.
`log_file_record_index_error.rs` and `log_file_id_range_error.rs` own its position
and range errors.
`log_file_position.rs` distinguishes a log byte offset from lengths, record counts
and index-file offsets. It is used by the record endpoints and explicit file-range
boundaries, index-entry values, and writer/recovery progress.
`record_start_location.rs` and `record_end_location.rs` distinguish inclusive
starts from exclusive ends. `record_range_location.rs` and its error validate
endpoint relationships and lazily enumerate `log_file_range.rs` portions.
`mod.rs` only declares and re-exports the types.

These values do not open files, prove record existence, validate stored bytes or
establish durability. Private fields preserve constructor/Serde validity.
Sequence zero anchors fixed 100,000-record file ranges; the last numeric file is
shorter. Successors must not wrap. Range endpoints must share a stream, have
ordered sequences and consistent known byte boundaries. Byte offsets remain
metadata rather than proof of an actual file's size.

`RecordStartLocation::to_end(record_length)` derives the same record's exclusive
end from a validated total encoded `RecordLength`. It preserves the record ID
and file, adding the length to the start position without inspecting storage.
`to_next(record_length)` instead derives the following record's start, advancing
the ID and resetting the byte position to zero when it belongs to the next file.

`RecordEndLocation::next_record_start()` derives the following record's inclusive
start: it preserves the exclusive end position within a file, resets to zero on
file rotation, and panics at sequence exhaustion through `RecordId::next()`. It performs no I/O and
does not prove record existence. There is no reverse conversion: a first record
in a new file does not reveal the preceding file's end position.

The sections below own the detailed [sequence mapping](#assigned-sequence-ranges),
numeric, endpoint, range and Serde contracts and their rationale. Read those
sections before changes; changing the range divisor changes the storage layout.
Physical paths belong to the [storage provider](../../storage/README.md), and stream continuity belongs to
the [streams module](../README.md), not these individual value objects.

Types are fixed-size metadata; range iteration is lazy and does not allocate an
entire file list. Preserve numeric overflow checks and independent literal fixtures.
Same-file tests cover boundaries, terminal sequences, malformed metadata, known
byte spans and repeated iterator exhaustion. Run
`cargo test -p transaction-log streams::location` (also with `--release` for
overflow contracts) and `cargo clippy -p transaction-log --all-targets -- -D warnings`.
Swapped endpoint roles have compile-fail Rustdoc examples; the [stream validation guidance](../README.md#verification-and-performance) explains checking documentation tests for this binary application.

## Assigned sequence ranges

A file belongs to exactly one `StreamId`. Its zero-based `file_number` assigns
100,000 consecutive sequence positions using:

```text
file_number = sequence_number / RECORDS_PER_FILE
first_sequence = file_number * RECORDS_PER_FILE
last_sequence = min(first_sequence + RECORDS_PER_FILE - 1, u64::MAX)
inclusive_count_through_record = sequence_number % RECORDS_PER_FILE + 1
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
- `LogFileId::first(stream_id)` is a `const` constructor for file zero in the given
  stream, beginning at sequence zero. It constructs the logical first file ID;
  it does not find the earliest file currently present in storage.
- `LogFileId::from_record_id(record_id)` delegates grouping to `LogFileNumber` and
  preserves the validated stream ID, without another validation pass.
- `LogFileId::record_count_through(record_id)` returns the one-based inclusive
  count from the beginning of whichever file naturally contains the record. It
  is infallible because it accepts no independent file ID that could disagree;
  it performs no I/O and does not claim the record exists or has been validated.
- `LogFileId::record_id_at(index)` performs the inverse mapping for a specific
  file using a zero-based index. It rejects indexes at or above 100,000 before
  sequence addition can cross an ordinary file boundary. Sequence exhaustion
  follows the repository-wide panic policy; no terminal-file accommodation is
  part of this API. The crate-private helper and its error currently serve
  location and validation tests, so they permit unused production declarations
  without widening their visibility.
- `LogFileId::stream_id()` and `file_number()` are copy getters returning the typed
  components. Neither type has setters or unchecked public constructors.
- `LogFileId::first_record_id()` and `last_record_id()` return inclusive assigned
  endpoints.
- `LogFileId::next()` preserves the stream and delegates to the file number's
  checked successor. It does not duplicate the range check.
- `LogFileId::iter_to(last)` returns `Result<impl FusedIterator<Item = LogFileId>,
  LogFileIdRangeError>`. It validates matching streams and nondecreasing file
  numbers before returning an inclusive, allocation-free standard-library
  `successors` iterator. Equal endpoints yield one ID. It stops before requesting
  the final file's successor, including at `MAX`, and remains exhausted thereafter.
  Iteration owns its bounds and performs no I/O or existence checks. Typed errors
  retain both endpoints and distinguish different streams from reversed files.
  Same-file tests cover equal/multiple bounds, stream preservation, rejected
  endpoints, large lazy ranges and repeated terminal exhaustion.

File IDs are `Copy`, equatable and hashable, with no `Ord` or `PartialOrd`.
Cross-stream ordering has no domain meaning. Establish matching streams before
comparing their ordered `LogFileNumber` components; `iter_to` checks this boundary
explicitly. The compiler-selected
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

## Log-file byte positions

`LogFilePosition` is a private `u64` wrapper exposed from both `streams::location`
and `streams`. Its infallible `new(u64)` constructor and read-only `get()` accessor
preserve every raw value, including zero and `u64::MAX`. The associated constant
`START` names byte zero, the start of any log file; it carries no file identity
and performs no I/O. It distinguishes a log
offset from an encoded record length, a record count, a file number or an offset
into the index file. It carries no file identity or start/end role; enclosing
location models own those meanings. Numeric ordering compares byte offsets only,
so callers must establish a common file before interpreting it as file order.

Construction and access perform no validation, allocation or I/O. An offset alone
cannot prove a record boundary, readable extent or durability. Its transparent native
layout is not an encoded representation. Raw extraction is explicit at I/O and
encoding boundaries; no arithmetic traits or numeric conversion implementations
are provided. Transparent Serde derives represent a position as a single `u64`,
preserving the endpoint models' numeric JSON fields without additional validation.

`advance(RecordLength)` and `retreat(RecordLength)` return new positions by adding
or subtracting the total encoded record length; the original position is unchanged.
Both are `const` methods with explicit overflow/underflow checks that panic in
debug and release rather than wrapping. Retreating by exactly the current offset
returns zero. They reuse the length type's protocol bounds without checking them
again. These are operations within one file: they do not advance record IDs,
discover record boundaries or rotate files. Callers supply the appropriate length
and retain responsibility for interpreting the resulting position.

`RecordStartLocation`, `RecordEndLocation` and `LogFileRange` use the primitive
for their positions. `RecordRangeLocation` compares and propagates those typed
positions, and its `InvalidByteSpan` error retains them. Writers, validators and
the initializer preserve these typed values. Index-entry values are log positions;
index-file byte addresses remain `u64`. File APIs and explicit index encoding
extract the raw offset with `get()` at their boundaries.

## Record range locations

Two distinct endpoint types describe a record's byte boundaries:

- `RecordStartLocation` stores a `record_id: RecordId` and its inclusive byte
  `position: LogFilePosition` within that record's file.
- `RecordEndLocation` stores a `record_id: RecordId` and its exclusive byte
  `position: LogFilePosition`, immediately after that record's CRC trailer in its file.

Both expose read-only `record_id()` and `position()` copy getters, plus
`log_file_id()` derived through `LogFileId::from_record_id`. Their
`new(record_id, position: LogFilePosition)` constructors are infallible metadata constructors:
they store the resolver's supplied values without inspecting files or adding
partial numeric validation. A location alone does not prove that a record exists
at that offset, is valid or is durable. Endpoint getters perform no validation.

`RecordStartLocation::to_end(record_length: RecordLength)` returns a
`RecordEndLocation` for the same record. Its exclusive position is the start plus
the complete encoded length, including header, payload and CRC. `RecordLength`
comes from the exports crate and already enforces the protocol bounds, so this
method does not repeat length validation. The caller must provide the length of
the record identified by the start; numeric validity alone does not prove that
relationship. No bytes are read or written, and the result certifies neither
storage validity nor durability.

The helper delegates byte arithmetic to `LogFilePosition::advance`. It does not
advance the record ID or reset the position at file rotation:
the last record's end stays in its own file. Byte-position addition panics on
`u64` overflow in both debug and release, rather than wrapping malformed metadata.
Valid resolved file positions cannot reach that overflow. The log-file validator
and indexed log writer use this helper after checking the incoming record ID,
sharing the resulting endpoint with their index-offset and progress handling.

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
decodes the position through its transparent `u64` representation, and rejects missing, duplicate or unknown object fields
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

The separate `RecordEndLocation::next_record_start()` method derives the next
record ID and its start position. It preserves the exclusive end offset when
the next record remains in the same file, uses zero when it belongs to a new
file, and panics at sequence exhaustion through `RecordId::next()`. It does not validate stored
bytes or prove that the next record exists. No reverse method is provided,
because a file's first record start cannot reveal the preceding file's end.

`RecordStartLocation::to_next(record_length: RecordLength)` composes `to_end()`
with `RecordEndLocation::next_record_start()`. It uses the **current** record's
encoded length to derive the following record's inclusive start. The next ID
stays in the same stream; its position is the current end within a file, or zero
at file rotation. Composition keeps the arithmetic and rotation rules in their
existing helpers, including their panic contracts for byte-position overflow
and sequence exhaustion. It performs no allocation, I/O or repeated length
validation, and does not establish that the next record exists.

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
records and exceed 65,535 bytes; positions retain the full `u64` width through
`LogFilePosition`, even though an individual encoded length fits `u16` on disk.

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

The comparisons stay in `LogFilePosition`, including the zero start of a later
file's prefix. `RecordRangeLocationError::InvalidByteSpan` retains the affected
file and both typed positions; error display extracts their numeric offsets to
keep diagnostics readable. No conversion to raw integers is needed for range
validation or enumeration.

### Iterating over files

`iter()` owns a copy of the endpoints and lazily returns `(LogFileId, LogFileRange)`
items in ascending file-number order:

| Case | Enum value | Bytes requested from that file |
| --- | --- | --- |
| Both endpoints in one file | `FileRange { start_position, end_position }` | The explicit `start..end` span. |
| First of multiple files | `FilePostfix { start_position }` | From the explicit start through the file's validated record end. |
| Intermediate file | `EntireFile` | From zero through the file's validated record end. |
| Last of multiple files | `FilePrefix { end_position }` | From zero up to the explicit exclusive end. |

Every explicit `start_position` or `end_position` is a `LogFilePosition`.
The enum accepts supplied metadata without checking endpoint relationships;
the range owner remains responsible for their validity.

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

## Location ownership and performance

Direct arithmetic makes record-to-file lookup independent of directory scans or
record byte lengths. It performs no allocation, I/O, locking or shared-state
access. Raw file-number validation happens in `LogFileNumber` at construction or
deserialization; sequence conversion proves the bound arithmetically. `LogFileId`
construction and typed getters reuse those invariants. These are implementation
choices, not measured throughput claims. Storage benchmarks will follow actual
file operations.

`LogFileId` describes file identity and assigned record-range boundaries.
Resolving record positions belongs to future file/index APIs using the dense
[dense index layout](../index_writer/README.md#dense-index-layout); `RecordRangeLocation` carries their outer endpoints and enumerates
the required file portions. A full file contains 100,000 records irrespective of
their byte sizes. The provider owns the file-path layout; the streams module owns
append and validation/repair operations on a supplied pair. Named provider methods
acquire recovery handles; historical lookup and coordination of live writer/read
handles remain future integration work.
Range endpoints must not be substituted for a durability or validation checkpoint.

## Verification

`log_file_position.rs` tests full-width offset preservation, including values above
4 GiB and the raw numeric limits. Arithmetic tests cover minimum/maximum record
lengths, exact zero and `u64::MAX` results, unchanged originals, const use and
overflow/underflow panics in both debug and release. Its Rustdoc examples show the
public API and reject passing a `RecordLength` where a `LogFilePosition` is required.

Same-file tests in `log_file_number.rs` cover raw construction/conversions, domain
limits, rejected values, sequence grouping, numeric display/zero padding, native
layout, integer JSON and checked successor exhaustion. Tests in `log_file_id.rs`
cover independent expected ranges, record-to-file mapping at exact file and
integer boundaries, preserved stream IDs, zero and maximum sequences, and
successor delegation. Expected boundary values are literal fixtures so a mistaken
production constant or calculation cannot redefine the expected results.
JSON tests cover the exact named-field representation, domain endpoints, invalid
stream/file numbers, propagated constructor errors and malformed object fields.

`record_start_location.rs` and `record_end_location.rs` test independent endpoint
identity and position preservation, both stream limits, positions above 4 GiB,
file rotation and the maximum sequence. Literal fixtures distinguish a start in
the new file from the preceding record's end in the completed file.
Their JSON tests cover literal field shapes and I/O round trips, zero and maximum
numeric metadata, invalid stream IDs and numeric types, missing/duplicate/unknown
fields, truncation and trailing garbage. Acceptance of raw offset limits protects
the distinction between metadata decoding and actual storage validation.
Those fixtures also verify that adopting `LogFilePosition` preserves the numeric
JSON representation rather than nesting an object around each offset.

`to_end()` tests cover minimum/maximum encoded lengths, nonzero offsets, positions
above 4 GiB, both stream limits and the last/first records on either side of a file
boundary. Literal expected ends verify exclusive-end arithmetic and preserved
identity. Raw byte-position overflow is rejected in both debug and release.
`to_next()` tests verify the following record ID and position with minimum/maximum
lengths, offsets above 4 GiB and progression before, across and after a file
boundary, preserving both stream limits. Expected endpoints use literal values
rather than the helper composition being tested.

`log_file_range.rs` has Rustdoc examples for typed boundaries above 4 GiB and
rejecting a record length in place of an offset. Its enum has no runtime behavior
apart from storing the chosen variant and its positions.

`record_range_location.rs` contains the combined endpoint and `LogFileRange` behavior tests:
single records, partial single-file requests, every multi-file role, adjacent and
exact file boundaries, independent offsets across files, positions above 4 GiB,
stream limits, the terminal sequence/file and repeated iterator exhaustion.
Invalid stream/order/known byte-span metadata exercises each typed error.
A full-domain test consumes only a few iterator items, protecting lazy enumeration
without allocating or visiting the entire file range. Literal expected file numbers
and enum values keep these tests independent of the iterator's implementation.
`record_range_location_error.rs` checks that typed offsets still display as full-width
decimal numbers. Run the same-file tests in debug and release using Cargo, and
check Rustdoc examples using the stream documentation-test guidance below.

`next_record_start()` tests use literal expectations for same-file offsets,
file rotation, the last representable successor and exhaustion. They compare the
exhaustion panic with `RecordId::next()` in debug and release. Range constructor
Rustdoc includes a working example and a compile-fail example for swapped roles.
Follow the [stream documentation-test guidance](../README.md#verification-and-performance)
for this binary application.

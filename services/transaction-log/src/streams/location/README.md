# Stream locations

Identify a stream's log files, track record byte boundaries, and split a record
range into the portions needed from each file. These typed values support indexed
appends and recovery; resolving historical read requests from indexes remains
planned.

## Types and modules

| Type or constant | Responsibility |
| --- | --- |
| [`LogFileNumber`] / [`LogFileNumberError`] | Represent a checked file number and report rejected raw numbers. |
| [`LogFileId`] / [`LogFileIdRangeError`] | Combine stream and file identities, map assigned record ranges, and enumerate matching files. |
| [`RECORDS_PER_FILE`] | Assign 100,000 consecutive sequence positions to each ordinary log file. |
| [`LogFilePosition`] | Distinguish a byte offset within a log file from a length, record count or index-file offset. |
| [`RecordStartLocation`] | Pair a record ID with its inclusive byte start. |
| [`RecordEndLocation`] | Pair a record ID with its exclusive byte end, after the CRC trailer. |
| [`RecordRangeLocation`] / [`RecordRangeLocationError`] | Check the relationship between outer endpoints and enumerate the required file portions. |
| [`LogFileRange`] | Describe an explicit byte span, a file prefix, a file postfix or an entire file. |

[`RecordId`](transaction_log_exports::RecordId) and
[`RecordLength`](transaction_log_exports::RecordLength) come from the shared
record layer. Physical paths belong to [`storage`](crate::storage); the enclosing
[`streams`](crate::streams) module coordinates appends, validation and recovery.

## Usage

Starting with a resolved byte position and the record's encoded length, derive
its exclusive end and locate its assigned file:

```rust
use transaction_log::streams::{LogFilePosition, RecordRangeLocation, RecordStartLocation};
use transaction_log_exports::{RecordId, RecordLength, SequenceNumber, StreamId};

let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(100_123));
let start = RecordStartLocation::new(id, LogFilePosition::new(4_096));
let end = start.to_end(RecordLength::new(2_064)?);

assert_eq!(start.log_file_id().file_number().get(), 1);
assert_eq!(end.record_id(), id);
assert_eq!(end.position().get(), 6_160);

let range = RecordRangeLocation::new(start, end)?;
assert_eq!(range.iter().count(), 1); // Equal record IDs describe one whole record.
# Ok::<(), Box<dyn std::error::Error>>(())
```

The example supplies illustrative metadata. Constructing these values checks
numeric domains and range relationships; it does not read storage or prove that
the record exists at the supplied offset.

<details>
<summary>Design and maintenance notes</summary>

**Ranges spanning files.** A request from sequence 99,998 through 300,001 needs
file zero's postfix, files one and two, then file three's prefix. The outer byte
positions belong to different files, so the final position can be smaller:

```rust
use transaction_log::streams::{
    LogFilePosition, LogFileRange, RecordEndLocation, RecordRangeLocation, RecordStartLocation,
};
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

let id = |sequence| RecordId::new(StreamId::MIN, SequenceNumber::new(sequence));
let start = RecordStartLocation::new(id(99_998), LogFilePosition::new(6_000_000));
let end = RecordEndLocation::new(id(300_001), LogFilePosition::new(128));
let range = RecordRangeLocation::new(start, end)?;
let portions: Vec<_> = range.iter()
    .map(|(file, portion)| (file.file_number().get(), portion))
    .collect();

assert_eq!(portions, [
    (0, LogFileRange::FilePostfix { start_position: LogFilePosition::new(6_000_000) }),
    (1, LogFileRange::EntireFile),
    (2, LogFileRange::EntireFile),
    (3, LogFileRange::FilePrefix { end_position: LogFilePosition::new(128) }),
]);
# Ok::<(), transaction_log::streams::RecordRangeLocationError>(())
```

Collecting makes the four results easy to inspect here; callers can consume the
iterator one file at a time without allocating a list. A future reader must
resolve the missing file ends from validated storage. The explicit final end
stays fixed even if newer records are appended after the request is resolved.

</details>

## Behavior and guarantees

### Assigned sequence ranges

Each file belongs to one stream and is assigned 100,000 consecutive sequence
positions, anchored at zero. [`LogFileId::from_record_id`] maps a record to its
file by dividing the sequence number by [`RECORDS_PER_FILE`].

| File number | First assigned sequence | Last assigned sequence |
| ---: | ---: | ---: |
| 0 | 0 | 99,999 |
| 1 | 100,000 | 199,999 |
| 2 | 200,000 | 299,999 |

`first_record_id()` and `last_record_id()` describe assigned endpoints, including
for a partial or nonexistent file. `LogFileId::first(stream)` means logical file
zero. `iter_to(last)` includes both file IDs and rejects different streams or
reversed file numbers.

<details>
<summary>Design and maintenance notes</summary>

**Mapping and layout.** The fixed divisor determines storage identities, so
changing it or the sequence-zero anchor changes the storage layout. It is not a
per-process tuning parameter. `RECORDS_PER_FILE` has one authoritative definition
alongside `LogFileNumber`; the exports crate's `SequenceNumber` still accepts every
`u64`, independently of this application grouping policy.

```text
file_number = sequence_number / RECORDS_PER_FILE
first_sequence = file_number * RECORDS_PER_FILE
last_sequence = min(first_sequence + RECORDS_PER_FILE - 1, u64::MAX)
inclusive_count_through_record = sequence_number % RECORDS_PER_FILE + 1
```

A completed file contains the assigned consecutive records in order. Merely
counting 100,000 arbitrary records is insufficient. The indexed writer checks
stream and sequence on append, and the indexed validator checks them during
recovery; identity values alone cannot establish file completeness.

**Numeric validity.** `LogFileNumber` is a read-only `u64` wrapper with bounds
`MIN = 0` and `MAX = u64::MAX / RECORDS_PER_FILE`, or 184,467,440,737,095. This
permits 184,467,440,737,096 file identities per stream. The cap proves that each
file has at least one representable sequence and that its first sequence fits
in `u64`.

`new(raw)` and `TryFrom<u64>` reject out-of-domain numbers without clamping or
truncation; `LogFileNumberError::value()` retains the rejected input. Conversion
from `SequenceNumber` is infallible because division establishes the bound.
`get()` and conversion into `u64` expose the validated value. Decimal display
forwards formatting options, including the zero padding used by storage paths.
The wrapper preserves the native size and alignment of `u64`.

`LogFileId::new` combines already-valid identifiers without repeated validation.
`stream_id()` and `file_number()` return typed copies. IDs are `Copy`, equatable
and hashable, with no ordering traits: file order is meaningful only after stream
agreement. `iter_to` checks that agreement and owns its inclusive bounds.

The crate-private `record_count_through(record_id)` gives the one-based count
through the record in its naturally assigned file. Accepting no independent file
ID avoids a possible mismatch. The inverse helper, `record_id_at(index)`, uses a
zero-based index within a specified file; it rejects indexes at or above 100,000
before adding them to the first sequence. Its `LogFileRecordIndexError` retains
the file, rejected index and maximum index. This inverse helper currently serves
location and validation tests without becoming a public construction API.

**Numeric exhaustion.** The last two representable ranges are:

| File number | First sequence | Last sequence |
| ---: | ---: | ---: |
| 184,467,440,737,094 | 18,446,744,073,709,400,000 | 18,446,744,073,709,499,999 |
| 184,467,440,737,095 | 18,446,744,073,709,500,000 | 18,446,744,073,709,551,615 (`u64::MAX`) |

The final numeric range has 51,616 representable positions. Its assigned last ID
is capped at `SequenceNumber::MAX`; that getter does not declare the shortened
file complete or sealed. Exhaustion is operationally unreachable at the intended
write rate, and no special terminal-file lifecycle is part of the current contract.
Existing successors panic at exhaustion in debug and release instead of wrapping.
`LogFileNumber::next` checks its domain cap, and `LogFileId::next` delegates while
preserving the stream. Iteration stops before requesting its last file's successor
and stays exhausted afterward, including at the numeric maximum.

</details>

### Log-file byte positions

[`LogFilePosition`] preserves every `u64` offset, including positions above 4 GiB.
`START` means byte zero; `new(raw)` and `get()` store and expose the value without
I/O or storage validation. A position carries no file identity or endpoint role.
Its numeric ordering has file-order meaning only when the file is known to match.

`advance(RecordLength)` and `retreat(RecordLength)` return a new offset within the
same file. They use the total encoded length, leave the original unchanged, and
panic on overflow or underflow in both debug and release. They do not rotate files
or discover record boundaries.

<details>
<summary>Design and maintenance notes</summary>

**Offsets and lengths.** An encoded record length has protocol bounds; a log-file
position can exceed 4 GiB because a file contains many variable-length records.
The distinct types prevent accidentally substituting a length, record count or
file number for an offset. Index-entry values are log positions, while addresses
within the index file remain separate `u64` offsets.

Arithmetic is limited to the named, `const` methods rather than general numeric
operators or implicit conversions. Both reuse the validity established by
`RecordLength`; subtracting exactly the current offset returns zero. Callers supply
the length of the intended record. Raw values are extracted explicitly with `get()`
at file I/O and index-encoding boundaries.

A raw offset cannot establish a readable extent or record boundary. The position
therefore accepts all `u64` values, and enclosing endpoint/range types establish
roles and relationships. Its transparent native layout does not define a file
encoding.

</details>

### Record endpoints

A start is inclusive, and an end is exclusive: it points just after the record's
CRC trailer. Both retain the ID of that record, expose read-only `record_id()` and
`position()` getters, and derive `log_file_id()` from the ID.

| Operation | Result |
| --- | --- |
| `RecordStartLocation::new(id, position)` / `RecordEndLocation::new(id, position)` | Store supplied metadata without checking it against storage. |
| `start.to_end(length)` | Keep the same record ID and file; add its full encoded length to the byte start. |
| `end.next_record_start()` | Advance the record ID; keep the end offset in the same file or reset to zero in the next file. |
| `start.to_next(length)` | Derive the current record's end, then the next record's start. |

A file's last record ends in that same file. Advancing to the next record is a
separate operation. The caller supplies the current record's actual length;
these helpers compute metadata and inherit the position/sequence panic contracts.

<details>
<summary>Design and maintenance notes</summary>

**Resolving a boundary.** A start lookup uses the preceding record's
[dense-index entry](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/index_writer/README.md#dense-index-layout),
or zero for the first record in a file. The start's ID still identifies the
requested record. An end lookup uses that record's own index entry. No redundant
file ID or path is stored alongside the record ID.

`to_end` delegates byte arithmetic to `LogFilePosition::advance`; the validator
and indexed writer use the same helper for their index offsets and progress.
It never advances the sequence or rotates the file. `to_next` composes it with
`next_record_start`, keeping those rules in one place. There is no reverse
conversion from the next start to the previous end: byte zero in a new file
cannot reveal the preceding file's length.

**Type roles and storage validity.** Separate endpoint types prevent swapped
range arguments without implicit conversions. Infallible endpoint constructors
retain the resolver's result; a partial numeric check could not prove its
agreement with storage. The full relationship belongs to the layer that reads
and validates file/index contents. A location alone does not establish record
existence, CRC validity or durability, and is not a recovery checkpoint.

</details>

### Record range locations

[`RecordRangeLocation::new`] accepts an inclusive first record and inclusive last
record, represented by their outer byte boundaries. Equal IDs describe one whole
record. It rejects different streams, decreasing sequences, a zero end position,
and an end at or before the start within the same file.

Its [`iter()`](RecordRangeLocation::iter) yields `(LogFileId, LogFileRange)` pairs
in file-number order:

| Part of the request | Portion | Byte boundaries |
| --- | --- | --- |
| Both endpoints in one file | `FileRange` | Explicit `start..end`. |
| First of multiple files | `FilePostfix` | Explicit start through the file's validated record end. |
| Intermediate file | `EntireFile` | Zero through the file's validated record end. |
| Last of multiple files | `FilePrefix` | Zero through the explicit exclusive end. |

Positions are relative to their own files. Subtracting endpoints from different
files does not give the transfer size. Unspecified file ends require validated
storage metadata; physical EOF alone is insufficient. Owning a range keeps no
files open and does not protect them from retention, replacement or truncation.

<details>
<summary>Design and maintenance notes</summary>

**Structural validation.** Matching streams and ordered record IDs establish the
file order. For a single file, the constructor compares the two supplied byte
positions. For multiple files, it checks the last file's end against zero,
independently of the first file's start. `InvalidByteSpan` retains the affected
file and both typed positions, with full-width decimal offsets in diagnostics.
The other error variants retain the conflicting stream IDs or sequences.

These checks establish the relationships required for enumeration, but do not
match offsets to encoded sizes, readable file extents or actual record boundaries.
A directly constructed `LogFileRange` stores its variant and positions without
those relationship checks; its owner must establish their validity.

**Explicit and unresolved bounds.** The iterator preserves both outer positions,
even if they coincide with whole-file boundaries. Two adjacent files need no
`EntireFile` item. One record needs only a `FileRange`, so there is no separate
single-record location type. An empty result belongs outside this nonempty range
model; sequence zero is never a sentinel. A single-file span can contain many
records and exceed the maximum encoded length of one record.

Intermediate byte ends stay unresolved because a record count does not determine
a file's length. The future reader must establish the complete validated extent
of each file's assigned records instead of guessing a maximum byte size or copying
an unvalidated physical tail. The final end remains bounded by the supplied
endpoint even when newer records exist.

The range stores only two endpoints and exposes them through copy getters;
inspection of IDs, files and positions stays on those endpoint types. Iteration
owns copies, advances one file per item, stops before the last successor, and
remains exhausted. It does not open, seek, validate, copy or synchronize files.
Historical lookup and coordination of live read/write handles remain future
integration work. That layer must preserve the resolved readable boundaries
while consuming the range.

</details>

### Metadata serialization

`LogFileNumber` and `LogFilePosition` serialize as integers. `LogFileId` uses named
`stream_id` and `file_number` fields. Both endpoint roles use the same named
`record_id` and `position` fields, for example:

```json
{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553500000}
```

Deserialization validates identifier domains and rejects missing, duplicate or
unknown object fields. Positions retain their full `u64` range without checking
storage. [`RecordRangeLocation`] has no Serde implementation; its constructor
establishes endpoint relationships.

<details>
<summary>Design and maintenance notes</summary>

**Preserving validity through decoding.** `LogFileNumber` deserializes through its
checked `u64` conversion. Transparent decoding there would bypass its upper bound.
`LogFileId` can derive Serde because each typed field establishes its own numeric
validity; a separate temporary representation or custom parser is unnecessary.
For example, its JSON shape is `{"stream_id":42,"file_number":1234}`.

`LogFilePosition` can use transparent Serde because every `u64` is representable
metadata. It preserves a numeric endpoint field without wrapping it in another
object. Endpoint decoding delegates to the nested `RecordId` and typed position;
it adds no checks to constructors or getters. Decoding an end position of zero
does not establish a valid record end.

The enclosing field or Rust type establishes a start/end role; the serialized
object has no role tag. Rust's distinct types prevent accidental role swaps in
code, but cannot make arbitrary serialized metadata trustworthy. These
format-independent Serde contracts do not change the eight-byte dense-index
entries, record encoding or compiler-selected native layouts.

</details>

## Performance

Location values are fixed-size `Copy` metadata. Record-to-file lookup is direct
arithmetic, independent of directory scans or record byte lengths. Construction
and getters do constant work without allocation or I/O; range iteration uses
constant memory and advances one file per item.

Raw numeric validation occurs at construction or deserialization. Typed getters
reuse those invariants, and sequence-to-file conversion proves its bound through
division. These are implementation properties; no storage-throughput measurement
is claimed. Measurements of file operations remain future work.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::location
cargo test -p transaction-log --release --locked streams::location
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The service is a binary crate, so ordinary Cargo tests skip its documentation
examples. The
[stream documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
describes checking them with `rustdoc --test` against a temporary library build.

<details>
<summary>Design and maintenance notes</summary>

Independent literal fixtures keep a mistaken divisor, offset calculation or
iterator from redefining its own expected results. The existing coverage includes:

| Contract | What the tests establish |
| --- | --- |
| File-number domains and mapping | Raw rejection, sequence conversion, assigned ranges, stream preservation, display/zero padding, native layout and checked successors. |
| Inclusive file enumeration | Equal and multiple bounds, different-stream/reversed-range errors, lazy traversal and repeated exhaustion. |
| Position arithmetic | Full-width offsets above 4 GiB, minimum/maximum record lengths, unchanged originals, const use, exact zero/maximum results and checked overflow/underflow. |
| Endpoint identity and rotation | Inclusive starts and exclusive ends preserve their records; only the next-start operation resets to zero across a file boundary. |
| `to_end` and `to_next` | Literal expected positions and IDs across file boundaries, nonzero/high offsets and both stream limits, independently of the helper composition. |
| Range validation and portions | Single records, partial single-file requests, every multi-file role, adjacent/exact boundaries, independently sized endpoint files and each typed rejection. |
| Large ranges | A full-domain test consumes only a few items, establishing lazy enumeration without a file list; existing numeric-end tests establish termination. |
| Serde metadata | Literal field shapes, integer/domain limits, propagated constructor errors, missing/duplicate/unknown fields, invalid numeric types, truncation and trailing garbage. |
| Diagnostics and type separation | Full-width offsets remain readable in errors; compile-fail examples reject swapped endpoint roles and lengths supplied as positions. |

Accepting raw position limits during decoding protects the distinction between
metadata representation and storage validation. JSON fixtures also establish that
typed positions retain the original numeric field shape. Debug and release checks
exercise explicit arithmetic guards in both profiles. The examples cover ordinary
use and compile-time restrictions; these tests make no claim about file existence,
read availability or durable storage.

</details>

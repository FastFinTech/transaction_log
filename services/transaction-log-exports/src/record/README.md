# Records

A record carries a stream-local identity and an opaque payload between transaction
log producers, byte streams and files. Records own validated, immutable bytes and
remain usable after their reader advances or is dropped.

## Types and modules

| Type or module | Responsibility |
| --- | --- |
| [`Record`] | Own one validated encoding and expose its identity, payload and checksum. |
| [`RecordHeader`] | Retain the decoded length and identity independently of the record's bytes. |
| [`RecordId`] | Pair a stream ID with a sequence number and derive the next ID in that stream. |
| [`StreamId`] / [`StreamIdError`] | Identify a logical stream in `0..=4095` and report rejected raw IDs. |
| [`SequenceNumber`] | Represent a stream-local sequence as a `u64`. |
| [`RecordLength`] / [`RecordLengthError`] | Represent a total encoded length in `16..=65_535` and report rejected lengths. |
| [`record_protocol`] | Define the shared encoding, limits and field/CRC operations. |

The sibling [`record_reader`](crate::record_reader) module validates incoming
bytes and returns records. [`record_writer`](crate::record_writer) creates
compatible encodings. Their specifications cover input and output lifecycles.

## Usage

Create an encoding with the writer, then read it as a validated record. The
returned record retains its payload after the reader is dropped:

```rust
use std::io::Write;
use transaction_log_exports::{RecordId, RecordReader, RecordWriter, SequenceNumber, StreamId};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(123));
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
writer.write(id, |body| body.write_all(b"payload"))?;
writer.flush_buffer().await?;

let encoded = writer.into_inner();
let mut reader = RecordReader::new(encoded.as_slice());
assert!(reader.wait_to_read().await?);
let record = reader.try_read_next()?.unwrap();
drop(reader);

assert_eq!(record.id(), id);
assert_eq!(record.body(), b"payload");
# Ok(())
# }
```

The [`reader specification`](crate::record_reader) explains consuming a complete
stream, including batching, EOF and error handling.

<details>
<summary>Design and maintenance notes</summary>

**Retaining the right data.** A `Record` owns an immutable `bytes::Bytes` handle.
Moving it transfers the handle, and cloning it shares the encoded bytes. A small
record may keep a larger batch allocation alive; that is the tradeoff for retaining
records without copying each payload.

| Need | Operation and ownership |
| --- | --- |
| Inspect the payload | `body()` borrows bytes excluding the header and CRC. |
| Pass the full encoding to a byte-slice API | `AsRef<[u8]>` borrows the complete record. |
| Retain or slice the encoded storage | Clone or slice the handle borrowed by `as_bytes()`. |
| Transfer the encoded storage | `into_bytes()` moves the handle without cloning it. |
| Retain only decoded header fields | `get_header()` returns an owned snapshot without retaining byte storage. |

Borrowed views remain tied to their record. A header snapshot can outlive it, and
individual getters let callers decode only the fields they need.

**Copying a record to another destination.** The writer's existing-record mode
copies an already-validated encoding into its own batch. The borrow ends when
`write_record(&record)` returns, so the original can be dropped before output.
That intentional copy preserves synchronous appends and record order without
revalidating fields or recomputing CRC. Sending the batch remains an explicit
operation; details are in [`record_writer`](crate::record_writer).

</details>

## Behavior and guarantees

### Wire and file contract

All encoded integers are little-endian. Records have no padding between fields
or between consecutive encodings, on the wire or in files. The length includes
the header, payload and CRC trailer.

| Byte offset | Size | Field |
| --- | --- | --- |
| 0 | 2 | Total record length, `u16` |
| 2 | 2 | Stream ID, encoded as `u16` |
| 4 | 8 | Sequence number, encoded as `u64` |
| 12 | Variable | Opaque payload |
| length minus 4 | 4 | CRC-32C trailer, `u32` |

The 12-byte header and four-byte trailer give a minimum encoded size of 16 bytes
and a maximum of 65,535 bytes. Payloads may be empty and contain at most 65,519
bytes. These fixed limits apply to each record; a buffer or batch can be larger.
The reader has no configurable maximum.

CRC-32C covers the finalized header and payload, excluding the trailer itself.
[`Record::crc()`](Record::crc) reads the stored checksum; the reader has already
verified it. The payload remains opaque to the record I/O layer.

<details>
<summary>Design and maintenance notes</summary>

**Shared encoding.** The [`record_protocol`] constants define production sizes
independently of native Rust layout. Its internal read and write operations share
field offsets and limits, so both directions use the same representation.
Grouping the operations by direction separates their initialization requirements
without introducing dispatch or duplicate codecs. `StreamId::COUNT` is a domain
limit, independent of the field's encoded width.

**CRC placement and initialization.** A trailer permits CRC calculation over one
contiguous header-and-payload prefix. It is not a zeroed field included in the
checksum. The protocol distinguishes three operations:

- `read::crc` decodes the stored trailer after its caller establishes the extent.
- `read::compute_crc` calculates CRC over a fully initialized frame, excluding
  its trailer, for input validation.
- `write::crc` calculates and initializes a writable trailer after the header
  and payload have been initialized.

Both calculations use the same `crc32c` implementation. The writing operation
accepts a raw pointer because creating a full-frame slice would expose an
uninitialized trailer. A separate content-only CRC or trailer-borrow layer would
add no needed capability.

The raw header writer initializes all 12 header bytes with unaligned
little-endian stores without first creating a reference to uninitialized storage.
CRC finalization requires an initialized, finalized header and payload plus
writable trailer storage in the same allocation. The caller establishes byte
extents and exclusive access; these protocol operations neither validate framing
nor grow storage. The [`writer specification`](crate::record_writer) explains
how its builder establishes those properties before publishing the encoding.

**Native layout and alignment.** `RecordHeader` and `RecordId` use Rust's default,
compiler-selected layout. The integer wrappers use `repr(transparent)`, but their
native sizes and padding do not define serialization. The explicit wire offsets
remain independent of `size_of` and struct memory layout.

Records can begin at arbitrary byte addresses. A borrowed native header view
would require alignment that byte buffers and packed record boundaries do not
provide. Padding records would cost up to seven bytes on both wire and disk;
adding padding during reads would require data movement. Fixed-width
little-endian decoding avoids those costs without a packed native header or a
dynamically sized native record.

</details>

### Identity and encoded length

`StreamId` identifies one of 4,096 logical virtual shards. Zero is valid, and
replicas share the same stream ID. `new` and `TryFrom<u16>` check the domain;
`StreamIdError::value()` retains the rejected integer. `StreamId::all()` lazily
enumerates every supported ID in ascending order, rather than discovering which
streams exist in storage.

Every `u64` is a valid raw `SequenceNumber`, including zero. `RecordId` combines
an already-typed stream and sequence. Its ordering compares stream ID first,
then sequence; it does not define chronology across streams. `next()` preserves
the stream and performs a checked increment, panicking on exhaustion in both
debug and release builds. It neither reserves an ID nor establishes continuity.

`RecordLength` represents the complete encoded byte count as a private `u64`.
`new` and `TryFrom<u64>` validate `16..=65_535` before narrowing, and
`RecordLengthError::value()` retains the full rejected input. `get()` extracts the
number explicitly. A valid length alone does not establish that bytes exist or
that framing, identity and CRC are valid.

`Record::length()` and `RecordHeader::length()` return `RecordLength`. Value
fields are read-only; header snapshots are obtained through `Record::get_header()`.

`StreamId`, `SequenceNumber` and `RecordId` support Serde metadata serialization.
In JSON the wrappers are integers, and an ID has the shape
`{"stream_id":42,"sequence_number":123}`. Both fields are required; duplicate and
unknown object fields are rejected. This metadata representation is separate
from the binary record encoding.

<details>
<summary>Design and maintenance notes</summary>

**Logical identity.** A user or another chosen domain boundary stays on its
logical shard even when the machine hosting that shard changes. The ID therefore
carries no physical host or execution-context identity. Routing may distribute
virtual shards over contexts associated with CPU cores; the value itself performs
no routing or scheduling. `all()` constructs valid wrappers directly from its
bounded range, without allocating a collection or repeating validation.

The command handler assigns sequence numbers. Continuity needs per-stream state,
which a generic reader of mixed streams does not own. The planned ingestion
contract rejects out-of-sequence input, reports stream/expected/received values,
and disconnects that client. Replay treats such a record and the remaining tail
as invalid. Those stateful checks belong to the ingestion or replay owner;
raw identifier validity cannot establish them.

**Length representation.** A native `u64` fits file-position arithmetic while the
wire length remains a `u16`. Buffer lengths and batch cursors use `usize`, so
conversions are explicit. Typed `MIN` and `MAX` derive from the protocol limits,
and `Record` computes the length from its byte handle instead of storing another
field. Downstream location arithmetic can accept a `RecordLength` without
confusing it with a payload size, file offset or batch length.

`RecordLength::new_unchecked` is restricted to the record module and only
`Record::length()` may call it. The reader has established the supported length,
exact extent and equality with the encoded field before freezing the bytes. The
getter can therefore wrap `Bytes::len()` without repeating a range check. Raw
protocol decoding and reader validation continue to use raw lengths until those
properties are established.

**Read-only values.** Private fields and copy getters keep IDs, header snapshots
and error diagnostics stable after construction. `RecordHeader::new` is restricted
to the record module and takes the length and identity of the same validated
record. Getters copy fields without allocation or repeated validation; replacing
an entire value remains possible. `getset::CopyGetters` implements native field
access, while the record keeps its specialized byte decoding and ownership APIs.

**Validated metadata.** `StreamId` deserialization uses its checked `u16`
conversion; a transparent derive would bypass the domain bound. `SequenceNumber`
can be transparent because every `u64` is valid. Integer decoding preserves the
full range without floating-point conversion. The selected Serde format retains
numeric and domain errors; it does not prove record existence or sequence
continuity. Metadata serialization adds no fields or validation to getters and
is not used by the record reader/writer codecs.

</details>

### Immutable validity

Public records come from [`RecordReader`](crate::RecordReader), which validates
the complete encoding before exposing it. Its checks establish length, stream
ID and CRC validity. They do not establish sequence acceptance, replication or
durable persistence.

Records remain valid independently of subsequent reader activity. The
[`reader specification`](crate::record_reader) owns how input is validated and
what happens at EOF, cancellation or failure.

<details>
<summary>Design and maintenance notes</summary>

**Immutable validity.** The crate-private unsafe record constructor accepts a
byte handle only after all of these properties have been established:

1. It contains exactly one complete record of 16 through 65,535 bytes.
2. Its byte length equals the encoded length field.
3. Its stream ID is in `0..=4095`.
4. Its stored CRC matches CRC-32C over the entire header and payload.
5. The handle retains initialized, immutable bytes for the record's lifetime.

Immutable shared ownership preserves these properties across reader refills and
record clones. A public constructor doing only partial validation could not
establish this contract, so there is no partial `from_bytes` API or separate
`RecordError`. Getters rely on complete validity. Any future construction path
that exposes a `Record` would need to establish the same properties.

**Raw byte access.** Protocol helpers decode raw values, including values that
the reader may reject. Unchecked header views use byte arrays with alignment one
and valid bit patterns for every initialized byte. CRC reads use unaligned loads
and explicit little-endian conversion. Their callers establish the byte extent;
no reference to an unaligned native integer or header is created.

</details>

## Performance

Record getters decode only requested fields. They perform no allocation, storage
cloning, checksum calculation or repeated integrity validation. Cloned records
share payload bytes, although shared handles still carry reference-count costs.
The [`reader specification`](crate::record_reader) describes batching and
receive-buffer costs.

The design target has been 100 million records per minute for record I/O.
That target and the assembly observations below are not durable-service throughput
guarantees. The opt-in reader benchmark measures TCP input, parsing and validation;
its workload and saved results are described in the
[benchmark documentation](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/benches/README.md).

<details>
<summary>Design and maintenance notes</summary>

**Access costs.** Validated equality between encoded length and handle length lets
`Record::length()` use `Bytes::len()` without dereferencing record bytes. Validated
IDs can be wrapped directly, and native copy getters add no integrity checks.
Ordinary bounds checks in safe slices can still exist; their elimination depends
on the method and compiler. Inlining may reuse a loaded buffer pointer or header
fields, so standalone getter instruction counts do not describe every call site.

**Code-generation observations.**

On 2026-09-14, optimized standalone getter wrappers were inspected with Rust
1.98.0, LLVM 22.1.8, targeting `x86_64-pc-windows-msvc`. Excluding the return,
`length()` used one load, `stream_id()` and `sequence_number()` each used two,
and `crc()` used three. The stream and sequence getters include fetching the
buffer pointer from the record; their actual field decoding takes one load.
Those wrappers contained no validation branches, helper calls, or byte swaps.
`StreamId` added no instructions. This is compiler evidence, not a throughput
benchmark or a portable instruction-count contract. `Bytes`' internal field
offsets and Rust calling conventions must not become assumptions in source code.

The read-only accessors were checked on the same date and toolchain with
`getset` 0.1.7, Rust's default representation for `RecordId` and `RecordHeader`,
and `-C opt-level=3`. Each `RecordId` component getter,
`RecordHeader::length()`, and `StreamIdError::value()` compiled to one load.
Reading `header.id().stream_id()` or `header.id().sequence_number()` also used
one load, without materializing the intermediate ID. Reading either identity
component through `record.get_header().id()` produced the same two-load code as
the corresponding direct record getter; the compiler merged the equivalent
probe functions. The existing record getter load counts above were unchanged.
These probes measure optimized scalar access, not the cost of copying or
retaining a whole header or ID, and do not establish throughput.

These historical measurements predate the `RecordLength` return type on `Record`
and `RecordHeader`.

On 2026-09-21, the typed header length was checked with Rust 1.98.1, LLVM 22.1.8,
targeting `x86_64-pc-windows-msvc` with `-C opt-level=3` and a release-built exports
library. Wrappers returning `RecordLength` from `header.length()` and
`record.get_header().length()` each compiled to one 64-bit load followed by a
return, without validation branches or helper calls. This checks those access
paths only; it is not a throughput measurement or a portable layout guarantee.

The following probe, saved as `target/record_getters.rs`, exposes
current code generation:

```rust
use transaction_log_exports::{
    Record, RecordHeader, RecordId, RecordLength, SequenceNumber, StreamId, StreamIdError,
};

#[unsafe(no_mangle)]
pub fn probe_length(record: &Record) -> RecordLength { record.length() }
#[unsafe(no_mangle)]
pub fn probe_stream_id(record: &Record) -> StreamId { record.stream_id() }
#[unsafe(no_mangle)]
pub fn probe_sequence_number(record: &Record) -> SequenceNumber { record.sequence_number() }
#[unsafe(no_mangle)]
pub fn probe_crc(record: &Record) -> u32 { record.crc() }
#[unsafe(no_mangle)]
pub fn probe_id_stream_id(id: &RecordId) -> StreamId { id.stream_id() }
#[unsafe(no_mangle)]
pub fn probe_id_sequence_number(id: &RecordId) -> SequenceNumber { id.sequence_number() }
#[unsafe(no_mangle)]
pub fn probe_header_length(header: &RecordHeader) -> RecordLength { header.length() }
#[unsafe(no_mangle)]
pub fn probe_record_header_length(record: &Record) -> RecordLength { record.get_header().length() }
#[unsafe(no_mangle)]
pub fn probe_header_stream_id(header: &RecordHeader) -> StreamId { header.id().stream_id() }
#[unsafe(no_mangle)]
pub fn probe_header_sequence_number(header: &RecordHeader) -> SequenceNumber { header.id().sequence_number() }
#[unsafe(no_mangle)]
pub fn probe_record_header_stream_id(record: &Record) -> StreamId { record.get_header().id().stream_id() }
#[unsafe(no_mangle)]
pub fn probe_record_header_sequence_number(record: &Record) -> SequenceNumber { record.get_header().id().sequence_number() }
#[unsafe(no_mangle)]
pub fn probe_error_value(error: &StreamIdError) -> u16 { error.value() }
```

Run from the workspace root:

```powershell
cargo build --package transaction-log-exports --release --locked
rustc --edition=2024 --crate-type=lib -C opt-level=3 --emit=asm --extern transaction_log_exports=target/release/libtransaction_log_exports.rlib -L dependency=target/release/deps target/record_getters.rs -o target/record_getters.s
```

The resulting assembly describes these access paths on the selected compiler,
target and optimization level. Throughput also depends on payload size and
retention/batching patterns: byte loads alone do not capture CRC work, cache
misses, reference counting, allocation or I/O.

</details>

## Validation

From the repository root:

```sh
cargo test -p transaction-log-exports --locked
cargo test -p transaction-log-exports --release --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo rustdoc -p transaction-log-exports --locked -- -D warnings
```

These checks cover the record values, their reader/writer integration and
executable documentation, including examples inside collapsed notes.

<details>
<summary>Design and maintenance notes</summary>

Independent encoded fixtures and a bitwise CRC reference detect shared mistakes
that matching production encoders and decoders could hide. The reference encoder
and CRC calculation do not call production decoding or checksum helpers.

| Area | What the tests establish |
| --- | --- |
| Wire encoding and CRC | Literal offsets and endianness, known checksum vectors, every covered bit and exclusion of trailer bytes. |
| Byte alignment and initialization | Access works at all eight address residues; raw header/CRC writes preserve guard bytes around their writable extent. |
| Record length | Every wire-representable input and oversized `u64` values are checked before narrowing; typed limits, const construction and errors preserve the expected values. |
| Stream and sequence values | The full raw stream domain is checked, sequence values retain their full width, and successor arithmetic preserves the stream with checked overflow. |
| Metadata | JSON uses exact integer values and the named record-ID shape; missing, duplicate, unknown and invalid fields are rejected. |
| Record ownership | Payload bounds, empty/maximum bodies, shared storage, retained records and independent header lifetimes match the public contract. |
| API restrictions | Compile-fail examples prevent field mutation, direct header construction and unchecked public length construction. |

Unsafe record constructors receive only independently established valid fixtures.
Malformed frames and invalid IDs enter through reader validation, where rejection
can be tested without violating constructor preconditions. Raw protocol tests can
exercise invalid numeric fields while still supplying valid byte extents.

The [`reader`](crate::record_reader) and [`writer`](crate::record_writer)
specifications describe their integration and lifecycle tests against this
shared format.

</details>

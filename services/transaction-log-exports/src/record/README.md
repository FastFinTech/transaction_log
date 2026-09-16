# Record requirements and design

This is the maintained specification for record values, their encoded format,
and the boundaries between records, readers, and writers. It is for both
people and coding agents. `mod.rs` includes this file in the module's generated
Rust documentation; edit this source rather than creating another design summary.

The requirements below describe the agreed behavior. The performance observations
describe one compiler and target, and must be rechecked when relevant code changes.
Planned components are explicitly identified. An implementation change alone does
not revise a requirement: keep this document, API contracts, and tests consistent
with intentional design changes.

## Purpose and scope

A record carries a stream-local identity and an opaque binary payload between
event producers, sockets, and transaction log files. Returned records must remain
usable after the next read, after the receive buffer changes, and after the reader
is dropped. Reading header fields and producing records are hot paths. The design
discussion used 100 million records per minute as a throughput target, not a
benchmark result or a guarantee made by this crate.

| Component | Responsibility and status |
| --- | --- |
| `Record` | Implemented: immutable ownership of exactly one validated encoded record and inexpensive accessors. |
| `RecordHeader` | Implemented: an independently owned, read-only snapshot of length and identity. |
| `RecordId` | Implemented: a read-only `StreamId` and `SequenceNumber` pair with a checked successor operation. |
| `StreamId` / `StreamIdError` | Implemented: a validated logical stream identifier, enumeration of its supported domain, and its construction error. |
| `SequenceNumber` | Implemented: a typed integer with no raw-value validation. |
| `record_protocol` | Implemented: encoded sizes, offsets, raw header/CRC initialization, field decoding, and CRC calculation shared by consumers. |
| `RecordReader` in the sibling `record_reader.rs` | Implemented: asynchronous input, complete wire validation, and batch preparation. |
| `RecordBuilder` in the sibling `record_writer` module | Implemented: concrete callback destination with bounded body serialization; construction and finalization remain module-private. |
| `RecordWriter` | Implemented: constructor-selected modes for synchronous serialization or existing-record copies, one reusable buffer and owned async destination, explicit buffer output, separate destination flushing and optional data synchronization, and terminal handling of failed/cancelled I/O. Scheduling and durability policy belong to higher layers. |
| `AsyncSyncData` in the sibling `record_writer` module | Implemented: optional destination capability that makes data synchronization available on a suitably bounded `RecordWriter`. Tokio files have an implementation; arbitrary `AsyncWrite` destinations do not. |
| Stream ingestion and file replay | Planned: enforce sequence continuity using previously known stream state. |

Keep `mod.rs` thin and keep each record type in its own file. The reader is a
separate module. Tests belong in the same source file as the behavior they test.

`record_protocol.rs` groups operations into inline `read` and `write` modules.
This README covers both groups, their shared layout, and test support. Keeping
them inline makes the groups collapsible in the editor while their common format
definitions remain nearby. They can move to separate files later without changing
call paths. The grouping introduces no runtime dispatch, allocation, or new types.

| Protocol group | Responsibility |
| --- | --- |
| Root | One set of field offsets, field sizes, and record limits shared by both directions. Public constants keep paths such as `record_protocol::MAX_RECORD_LEN`. |
| `read` | Crate-private field decoding, header borrowing, stored-CRC reading, and CRC calculation used by both `Record` and `RecordReader`. `read_field` is private to this group; validation decisions remain in `RecordReader`. |
| `write` | Crate-private header and CRC initialization into exclusively held storage. Capacity and publication of the completed buffer remain the builder's responsibility. |
| Root test support | Same-file tests and independent fixtures outside both groups, shared with record, reader, and builder tests. |

Use directional paths such as `protocol::read::stream_id(header)` and
`protocol::write::header(pointer, length, id)`. Do not add flat aliases that obscure
the split or duplicate shared layout definitions inside either group.

## Wire and file contract

All encoded integers are little-endian. There is no padding between fields or
between consecutive records, on the wire, on disk, or in the retained byte buffer.
The length field describes the entire record, including itself, the payload,
and the CRC trailer.

| Byte offset | Size | Field |
| --- | --- | --- |
| 0 | 2 | Total record length, `u16` |
| 2 | 2 | Stream ID, encoded as `u16` |
| 4 | 8 | Sequence number, encoded as `u64` |
| 12 | Variable | Opaque payload |
| length minus 4 | 4 | CRC-32C trailer, `u32` |

The encoded header is 12 bytes and the CRC trailer is 4 bytes. An empty payload
is valid, giving a minimum record size of 16 bytes. The maximum record size is
65,535 bytes, giving a maximum payload of 65,519 bytes. The limit is fixed by
the protocol; the reader has no configurable maximum. Buffer lengths and batch
cursors use `usize`, and a batch can exceed the maximum size of one record.

Production code gets these sizes from `record_protocol::HEADER_LEN`, `CRC_LEN`,
`MIN_RECORD_LEN`, `MAX_RECORD_LEN`, and `MAX_PAYLOAD_LEN`. Keep byte offsets and
field encoding and decoding in that module, rather than duplicating them in
readers, records, or writers. `StreamId::COUNT` is a domain limit and belongs to
the identifier type, independently of its encoded width.

CRC-32C covers every byte of the header and payload, including the finalized
length and ID. It excludes the last four bytes. Store its result little-endian
in that trailer. Do not place the CRC in the header or include a zeroed
checksum field in the calculation. Trailer placement lets the checksum be
computed over one contiguous prefix. `Record::crc()` reads the stored checksum;
it does not calculate or verify it.

The protocol keeps three CRC operations with distinct contracts. `read::crc(frame)`
locates and decodes the stored trailer without a repeated length check; its caller
must prove the trailer extent. `read::compute_crc(frame)` recalculates the checksum of
a fully initialized input frame, excluding its trailer, for reader validation.
`write::crc(record, record_len)` calculates and initializes the trailer through a
raw pointer whose header and payload are already initialized. Both calculations
use the same `crc32c` implementation. Writers must not create a full frame slice
while the trailer is uninitialized. Separate content-only calculation, CRC-offset,
and trailer-borrow helpers are unnecessary.

## Identity and domain rules

`StreamId` wraps `u16` but accepts only `0..=4095`: 4,096 logical streams. Zero is
a valid stream ID. `StreamId::new` and `TryFrom<u16>` validate external values and
return `StreamIdError` containing the rejected integer. Replicas use the same ID.

`StreamId::all()` returns a fresh, lazy iterator over every supported ID, from
`MIN` through `MAX` inclusive and in ascending order. It allocates no collection
and constructs valid wrappers directly from the bounded range without repeated
validation or unsafe code. Consumers such as storage initialization can remain
typed throughout instead of rebuilding raw integer ranges. This enumerates the
supported domain, not streams discovered on disk or assigned to a particular host.

Streams represent virtual shards, rather than individual users or accounts.
A user or another chosen domain boundary stays on its original logical shard;
the machine hosting a shard can change. A stream ID therefore does not identify
a particular machine or execution context. The broader design distributes many
virtual shards over execution contexts associated with CPU cores. None of that
routing or scheduling is implemented by these value types.

`SequenceNumber` wraps `u64`. Every raw value, including zero and `u64::MAX`, is
representable. Construction is infallible. The command handler assigns sequence
numbers. A raw sequence value does not establish that an append is in order.
`RecordId` combines the two typed values; its ordering is stream ID first, then
sequence number, and does not define chronological order across streams.

Sequence continuity requires external per-stream state. The future client
ingestion path must reject an out-of-sequence record, report stream/expected/
received values, and disconnect that client. File replay must stop at such a
record and treat the remaining file tail as invalid. The generic reader can
consume mixed streams and does not know their previous sequence numbers, so it
must not impose that policy. The first expected sequence and exhaustion behavior
at `u64::MAX` remain decisions for those future components; do not infer a starting
value or introduce wrapping sequence arithmetic from the wrapper's limits.

`RecordId::next()` returns a `RecordId` containing the same stream ID and the
sequence number increased by one. It uses a checked increment and panics at
`u64::MAX` in both debug and release builds, never wrapping to zero or moving to
another stream. Exhaustion is treated as a programming error in this convenience
method, keeping ordinary calls direct while preventing silent wraparound.
It leaves the original ID unchanged and performs no allocation or repeated
stream ID validation. It does not reserve an ID, synchronize producers, or prove
sequence continuity.

### Identifier metadata serialization

`StreamId`, `SequenceNumber` and `RecordId` implement Serde's format-independent
`Serialize` and `Deserialize` traits for metadata such as storage checkpoints.
In JSON the wrappers are integers; a record ID is an object such as
`{"stream_id":42,"sequence_number":123}`. Its two fields are required, with
duplicate and unknown object fields rejected.

`StreamId` uses `#[serde(try_from = "u16", into = "u16")]`, routing loading through
its existing checked conversion. Do not replace this with transparent derived
deserialization: that would allow external metadata to construct an invalid ID.
`SequenceNumber` is transparent to Serde because every `u64` value is valid.
Integer decoding preserves the full range without conversion through floats.
Serde reports invalid numeric types/ranges and stream-domain errors through the
chosen format's error type. It does not establish sequence continuity or record
existence, which still require application state.

These traits describe metadata, not encoded records. They add no stored fields
or validation to getters and are not called by the record reader/writer codecs.
The explicit little-endian protocol remains authoritative for record I/O.

## Ownership, native layout, and accessors

`Record` owns an immutable `bytes::Bytes` handle. It does not borrow the reader's
current buffer. Moving a record transfers that handle; cloning a record or its
byte handle shares storage rather than copying header or payload bytes. A small
retained record can keep a larger backing allocation alive. This is an accepted
tradeoff of shared ownership, not evidence that the record should borrow the
reader or copy every payload.

`body()` and `AsRef<[u8]>` return borrowed slices tied to the record. `body()`
excludes the header and trailer, while `AsRef<[u8]>` exposes the entire encoding
for generic byte-consuming APIs. `as_bytes()` exposes the owning handle by
reference so callers can clone or slice it. `into_bytes()` transfers that handle
without a clone. These operations serve different ownership needs.

`get_header()` returns an owned, native-endian `RecordHeader`. Callers can retain
it independently and reuse it for repeated field access. Its private fields are
exposed by `length()` and `id()`, both returning copies. Its `length()` and
`Record::length()` return `u16`. Individual record getters let callers read only
the fields they need. Callers obtain snapshots through `Record::get_header()`.
The `RecordHeader::new(length, id)` constructor is restricted to the record
module with `pub(super)`. Its inputs must come from an already-validated record,
allowing it to copy the fields without allocation or repeated length validation.
Publicly obtainable headers therefore retain the validated length range.

Record value types expose read-only state. `RecordId::new` constructs an identity,
and `stream_id()` and `sequence_number()` return copies of its typed components.
`StreamIdError::value()` returns the rejected raw ID; errors are constructed by
stream ID validation. `RecordId`, `RecordHeader`, and `StreamIdError` use
`getset::CopyGetters` on private fields, with no setters or mutable accessors.
This keeps an identity, header snapshot, or rejection diagnostic stable after
construction. Callers can still replace an entire value, for example with
`id = id.next()`; read-only fields do not make the caller's binding immutable.

The generated getters inline simple field copies without allocating or repeating
validation. `StreamId` and `SequenceNumber` already have private fields and keep
their handwritten `const get()` methods. `Record` keeps its specialized decoding
and byte-view accessors; generated getters do not replace protocol logic.

`RecordHeader` and `RecordId` use Rust's default representation. Their native
field order, offsets, and padding are compiler-selected and are not part of the
public contract. `StreamId` and `SequenceNumber` use `repr(transparent)` to retain
the layout of their underlying integers. Native layout does not define
serialization: never derive encoded sizes from `size_of`, serialize raw struct
memory, or cast a byte pointer to a native header reference. The protocol's
explicit offsets and little-endian codecs define the wire and file layout.

The design uses fixed-width little-endian decoding from byte arrays at arbitrary
byte addresses. A borrowed native header view would require alignment guarantees
that `BytesMut` and arbitrary record boundaries do not provide. Padding every
record would cost up to seven bytes on both wire and disk; inserting padding
only during reading would require extra movement or
copying. Fixed-width decoding avoids those costs. No dynamically sized native
record or `repr(packed)` representation is part of the design.

## Validation boundary and safety

Production records are currently constructed only after the reader validates
their complete immutable encoding. Before calling `from_validated_bytes`, the
caller must establish all of the following:

1. The bytes represent exactly one complete record, with length in `16..=65_535`.
2. The encoded length equals the supplied byte handle's length.
3. The stream ID is within `0..=4095`.
4. The stored CRC matches CRC-32C over the entire header and payload.
5. The owning handle retains initialized, immutable bytes for the record's life.

`from_validated_bytes` is crate-private and unsafe. There is intentionally no
public `from_bytes` that performs partial validation, no separate `RecordError`,
and no validation pass in the getters. A future writer may establish the same
invariants by construction before exposing a `Record`; it must establish all
of them, not just reserve enough space for the header.

Protocol helpers operate at a different level. They decode raw field values,
including values that a reader may reject. Unchecked header borrows point to
`[u8; N]`, whose alignment is one and whose bit patterns are all valid. CRC reads
use an unaligned integer load and explicit little-endian conversion. The caller
must prove the byte extent; no reference to an unaligned native integer or header
struct is created. The local `# Safety` documentation and each unsafe call's proof
must remain next to the code even though the larger rationale lives here.

The crate-private unsafe `write::header(header: *mut u8, length: u16, id: RecordId)`
initializes all 12 bytes directly with unaligned little-endian stores. The caller
provides writable header storage; it may be uninitialized. It does not create an
ordinary mutable byte-array reference before those writes. The subsequent
`write::crc(record: *mut u8, record_len: usize)` requires an initialized finalized
header and payload and writable trailer storage in the same allocation. Neither
method validates framing or grows the buffer. The builder establishes their
extent and exclusive-access requirements before publishing the completed length.

Do not feed malformed frames to unsafe constructors in tests. Independent known
fixtures and the bounded test encoder establish valid inputs for record tests.
Malformed framing and invalid IDs belong in reader tests; raw protocol tests can
exercise invalid field values while still honoring byte-extent preconditions.

## Reader integration contract

`RecordReader::new(source)` is infallible and takes any suitable source; the
asynchronous methods require Tokio `AsyncRead + Unpin`.

`wait_to_read()` waits until at least one complete record is available, validates
all complete buffered records, then splits and freezes the complete prefix once.
It leaves the incomplete tail in `BytesMut` for the next read. It does not wait
to fill a batch after complete records are available. Repeated waits with unread
records return immediately. Validated pending header state survives fragmented
input and cancellation so header checks are not repeated for the same frame.

`try_read_next()` walks the already validated batch using its encoded lengths.
It performs no I/O, CRC calculation, or stream ID validation, and does not split
the receive buffer per record. Returned records share slices of the frozen
batch. The final record takes over the batch handle instead of cloning it.
`None` means the batch is exhausted; the caller waits again.

If any record in a candidate batch fails validation, the wait returns an error
before exposing any of that batch, including its otherwise valid prefix.
Previously returned records remain valid. Validation and I/O failures are
terminal; subsequent operations report `ReaderFailed`. EOF within a header,
payload, or trailer is an error. Clean EOF between records returns `false` and
is terminal too; this reader does not tail files that grow after EOF.

Refilling can move unfinished data when the mutable buffer grows or reclaims
space. Sharing completed records does not imply that the entire receive path
performs no copies or allocations.

```rust
use tokio::io::AsyncRead;
use transaction_log_exports::{Record, RecordReadError, RecordReader};

async fn read_first<R: AsyncRead + Unpin>(source: R) -> Result<Option<Record>, RecordReadError> {
    let mut reader = RecordReader::new(source);
    if reader.wait_to_read().await? {
        // The returned record owns its storage after this local reader is dropped.
        reader.try_read_next()
    } else {
        Ok(None)
    }
}
```

## Hot-path decisions and evidence

| Decision | Reason and consequence |
| --- | --- |
| Validate at the reader boundary | Repeated field access does not repeat length, stream ID, or CRC validation. |
| Keep completed storage immutable | Once established, those invariants remain true for every retained handle. |
| Decode only requested fields | Getters do not eagerly construct or cache a complete header inside every record. |
| Use `Bytes::len()` for `length()` | The validated equality permits a metadata load and a safe-by-invariant narrowing cast, avoiding a buffer dereference. |
| Use fixed-size little-endian loads | They support arbitrary alignment; on the inspected little-endian target they compile to ordinary loads without byte swapping. |
| Wrap validated IDs without checking again | The typed API retains the domain invariant without adding a range branch to `stream_id()`. |
| Expose native value fields through `getset::CopyGetters` | Private fields preserve read-only APIs; inline field copies avoid allocation, reference counting, and repeated validation. |
| Keep CRC in a trailer | Validation computes CRC over one contiguous header-and-payload slice. |
| Split once per prepared batch | Avoid a mutable-buffer split for every consumed record. |
| Share completed bytes and move the final handle | Avoid per-record payload copies and an unnecessary final reference-count increment. Shared slices can still incur reference-count costs. |

Getters must not allocate, clone storage, recompute checksums, or add integrity
validation. This does not prohibit the ordinary bounds checks of safe slice
operations: whether those checks are optimized away depends on the method and
compiler. Inlining can also let callers reuse a loaded buffer pointer or header
fields; do not assume a standalone getter's instruction count describes every
call site.

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

To inspect current code generation, save a small probe under `target/record_getters.rs`:

```rust
use transaction_log_exports::{
    Record, RecordHeader, RecordId, SequenceNumber, StreamId, StreamIdError,
};

#[unsafe(no_mangle)]
pub fn probe_length(record: &Record) -> u16 { record.length() }
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
pub fn probe_header_length(header: &RecordHeader) -> u16 { header.length() }
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

Inspect the resulting assembly and record the compiler, target, and optimization
level with any new performance claim. For throughput claims, measure realistic
payloads and retention/batching patterns as well; byte loads alone do not capture
CRC work, cache misses, reference counting, allocation, or I/O.

The opt-in [reader benchmark](../../benches/README.md) measures the actual reader
over localhost TCP with 100 million records and 2 KiB payloads by default. Its
README defines the workload, timing boundaries, retention options, and limits
of those measurements. It is separate from correctness tests.

## Writing integration and planned service behavior

The agreed producer flow is: assign the record ID in the command handler, write
the payload, finalize the total length, calculate the CRC, then send the complete
record. The internal `RecordBuilder` owns mutable framing and finalization in a
buffer supplied by the enclosing writer. `Record` remains the completed,
read-only value; adding a `RecordMut` is not part of this design. Accepted body
writes are bounded to 65,519 bytes, proving that adding the fixed 16-byte overhead
fits the encoded length without overflow or a repeated range check.
No provisional header or payload may be sent before finalization.

The sibling [record writer module](../record_writer/README.md) provides synchronous
`RecordWriter::write(id, callback)` with a concrete `&mut RecordBuilder` for static
dispatch into its `std::io::Write` implementation. The builder type is public, but
its constructor, fields, and finalization are module-private. Each builder makes
one record in the writer's `BytesMut`, appending after earlier buffered records.
The caller explicitly sends the accumulated batch with `flush_buffer().await`.
Only successful buffer output clears that allocation for reuse.
Buffering lets the writer compute the final length before emitting the first
header field and reject an unfinished record before any of it reaches a destination.
A serializer's `Write::flush` does not finish or send a record.

Construction reserves a complete maximum-sized record and captures a pointer
covering the spare region. A cached payload count enforces the size limit; body
writes initialize only their final payload positions. The buffer's readable length
stays unchanged until `finish(self, id)` initializes the header and CRC trailer
through the protocol's `write` helpers and publishes the completed length once.
No bytes are prefilled, and serialization/finalization cannot grow the buffer.
The writer module documents the full safety proof and retained performance evidence.

Retained write errors prevent finalization. The caller must independently reject
serializer errors before calling `finish`. Dropping an unfinished builder,
including after rejected finalization or during unwind, preserves earlier records
and buffer capacity. Finalization does not create a public `Record`, check sequence
continuity, send bytes, or establish remote acceptance or durability.

`RecordWriter::for_serialization(destination)` and `RecordWriter::for_records(destination)`
take ownership of an already-open destination and select its append API at compile
time. They infer `RecordWriter<W, SerializeRecords>` and `RecordWriter<W, ExistingRecords>`
respectively. Neither mode can call the other's append method, and there is no mode
conversion. Zero-sized markers require no runtime mode checks. Both modes share
buffer output, destination flushing, optional synchronization and failure handling.
Output requires `AsyncWrite + Unpin`. The caller completes socket handshakes or
chooses file opening mode and position. No Send bound, thread, task, queue, pool
or timer is imposed by this middle layer. The caller drives each async operation.

Serialization mode's `write` immediately runs its callback and completes the builder
without I/O. Existing-record mode's `write_record(&record)` copies an encoding into
its batch without construction, byte-handle cloning, repeated validation or CRC
recalculation. This copy keeps the append synchronous and preserves record order;
the original record can be dropped as soon as the append returns. Each mode's buffer
grows as needed, with limits applied per record rather than per batch. Callers
control batch latency and retained capacity by choosing when to send; the writer
does not impose an automatic send threshold, pool or timer.

`flush_buffer().await` sends all buffered bytes, handling partial writes and
retaining capacity for reuse. `flush().await` flushes only the destination's own
buffering; it does not send the writer's pending records. For durable file output,
perform both operations before `writer.sync_data().await`, available when the
destination implements `AsyncSyncData`. Tokio files implement this capability;
other storage wrappers can opt in. Synchronization does not implicitly send or
flush buffered records and is not a replication acknowledgement. This trait uses
static dispatch without requiring a boxed or Send future. Storage wrappers must
explicitly implement it; the capability is not forwarded automatically.
Applications needing `sync_all` use the file through `get_ref`; those direct
operations bypass the writer's failure tracking and need caller error handling.
`into_inner` returns destination ownership and discards unsent
records without implicit output, flush or shutdown. Applications own batching,
durable-sync cadence, worker sharing and any background handoffs. A single-buffer
writer cannot construct another record while a buffer flush is in progress.

Construction errors and callback unwinding roll back only the current record.
Output errors, panics and cancellation leave the writer unusable, so later writes
cannot replay a partially accepted record prefix. Failed or incomplete data
synchronization also makes the writer unusable because durability is uncertain.
The first I/O error is returned directly; subsequent writes, flushes and syncs
return `Unusable`. A pending send keeps its cursor only inside that future;
although the full batch remains buffered on failure, replaying it is unsafe.
An unpolled future has no effect. Connection setup, stream ordering, acknowledgements, reconnection,
file recovery and command admission remain application responsibilities. See the
writer README for ownership, cancellation and output completion contracts.

The broader planned service is a cluster of transaction logs with prompt
replication to read replicas, roughly one-second durable flushes, and no
traditional per-record producer acknowledgment. Stream storage is intended to
use one record file and one index file per stream. These are service design
constraints, not behavior of this module. Having a validated `Record` proves
neither sequence acceptance nor replication nor durable persistence. Replication
topology, recovery rules, and physical storage implementation remain separate work.

## Maintaining and checking this specification

Keep durable requirements and the reasons for them here. Keep API contracts,
preconditions, and unsafe proofs in Rust documentation and comments. Keep test
expectations independent enough to catch accidental changes to the wire format.
The module enables `deny(missing_docs)` for its public API, and this README's Rust
examples are compiled as documentation tests. These checks cannot detect every
stale rationale; updating affected documentation is part of a design change.

Review changes against the relevant coverage:

| Area | Coverage location |
| --- | --- |
| Accessors, exact payload slices, empty/maximum payloads, typed boundaries, all eight address residues, shared ownership and header lifetime | `record.rs` |
| Encoded sizes, offsets, uninitialized header/CRC writes at byte alignments with guard bytes, endianness, raw numeric limits, known CRC vectors, every covered bit, excluded trailer, oversized test encoding | `record_protocol.rs` |
| Entire raw `u16` range, conversions, representation, limits, rejected value accessor, formatting and validated JSON loading | `stream_id.rs` |
| Raw sequence boundaries, conversions, representation, formatting and exact full-range JSON integers | `sequence_number.rs` |
| Successor arithmetic across numeric boundaries, preserved stream ID, panic on exhaustion in debug and release builds, named-field JSON shape and invalid metadata | `record_id.rs` |
| Public API rejects field mutation and direct header construction; generated accessors remain callable | Rustdoc examples in `record_id.rs`, `record_header.rs`, and `stream_id_error.rs`; existing record and stream tests |
| Partial input, cancellation, batching, fixed size limits, malformed records, error state, retained records across refills | Sibling `record_reader.rs` |
| Bounded serialization, complete framing/CRC, prefix isolation, rollback, preallocated storage, and reader compatibility | Sibling `record_writer/record_builder.rs` |
| Synchronous appends without I/O, batching, callback errors, owned destinations, allocation reuse, existing record copies, partial writes, cancellation, separate send/flush/sync operations, terminal failures, lifecycle, file and handshake-complete socket integration | Sibling `record_writer/record_writer.rs` |
| Constructor-selected append APIs cannot be mixed; synchronization unavailable without its optional trait; public file usage example and builder privacy | Rustdoc examples in sibling `record_writer/README.md` |

The shared test encoder and bitwise CRC reference live in the protocol's
test-only support module. They deliberately do not call production decoding or
checksum helpers. The fixed encoded fixture provides an independent compatibility
check. Do not replace independent expected values with the production function
being tested. Do not create redundant tests for trivial fields or Rust-derived
traits merely to increase the test count.

From the workspace root, run the following for relevant changes:

```powershell
cargo fmt --all --check
cargo test --package transaction-log-exports --lib record:: --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
$env:RUSTDOCFLAGS = '-D warnings'
cargo doc --workspace --no-deps --locked
```

If a change affects getters or unchecked access, also inspect optimized assembly
and check the byte-extent and ownership proofs. When revising the format, update
constants, codecs, fixtures and independent expectations, reader integration,
examples, and this specification together. Preserve the current requirements
until an intentional design change supersedes them, and describe the replacement
rationale instead of silently deleting the old reasoning during cleanup.

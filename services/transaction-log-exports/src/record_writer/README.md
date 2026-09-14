# Record writing

This module owns record construction, the public writer API, and the design
contracts for planned output coordination. It is implemented in small, reviewable
steps. This README is included in Rustdoc; keep it and the code's API documentation
current together.
The [record specification](../record/README.md) defines the encoded format,
limits, checksum coverage, and validated record API.

## Implemented: writer API and body callback

`RecordWriter` owns a reusable `BytesMut`. `new()` leaves storage unallocated until
the first record; `with_capacity(bytes)` can preallocate space for known workloads.
The supplied capacity is an initial size, not a maximum. Each `write(id, write_body)`
creates a private builder that reserves room for one maximum-sized record after
the completed prefix. Delegating that reservation avoids two capacity checks at
the writer/builder boundary. Storage can grow between records, never during
payload serialization or finalization.

The public writing operation is:

```text
write(id, write_body: impl FnOnce(&mut dyn std::io::Write) -> Result<(), E>)
    -> Result<(), RecordWriteError<E>>
```

The callback runs exactly once, synchronously, on the caller's thread. It can
write fields individually, call a serializer, or emit bytes in any other way
supported by `std::io::Write`. The writer understands only the supplied record ID,
body bytes, callback result, and outer framing. Application transactions, event
types, inner headers, and serializer choices remain entirely in the callback's
application code. Callers do not implement a library-specific serialization trait.

`FnOnce` permits captures of borrowed or owned data, including values consumed
during the call. There are no `Send`, `Sync`, `'static`, or `std::error::Error`
bounds on the callback or its error. The callback is generic and is not boxed or
queued. The borrowed `&mut dyn Write` destination keeps `RecordBuilder` private;
it introduces no heap allocation. A trait-object destination can use indirect
method calls, but inlining can expose the concrete builder and eliminate them.
The code-generation evidence below verifies this for the inspected closure
workloads, not for every possible serializer. Keep `write` and the builder
constructor inline to preserve that opportunity across crate boundaries.

The writer itself can move between threads between calls. The private builder,
its pointer, and the borrowed callback destination stay within one synchronous
call, between awaits. The destination cannot be retained after the callback.

This example writes an illustrative opaque body directly into the writer's storage:

```rust
use std::io;
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let value = 42_u64;
let data = b"example";
let mut writer = RecordWriter::new();
let id = RecordId::new(StreamId::MIN, SequenceNumber::new(1));
writer.write(id, |body| {
    body.write_all(&value.to_le_bytes())?;
    body.write_all(data)
})?;
writer.write(id.next(), |_body| Ok::<(), io::Error>(()))?; // Empty body is valid.

// The two bodies have lengths 15 and 0, with 16 bytes of framing per record.
assert_eq!(writer.buffered_bytes().len(), 31 + 16);
let completed = writer.into_bytes(); // Transfer the whole batch without a copy.
assert_eq!(completed.len(), 47);
# Ok(())
# }
```

On success, the writer calls `finish(id)` and publishes exactly one complete
record after earlier records. The ID is supplied by the command handler and is
not advanced or checked for continuity here. On a callback error or panic
unwind, dropping the builder rolls back only the current record. Earlier records
and the current allocation's capacity remain available; another call can retry
the same ID immediately. Callback side effects outside this buffer are not
rolled back. Panics propagate, and process abort does not run cleanup.

`RecordWriteError<E>` preserves a callback failure as `BodyWrite(E)`.
A rejected buffer write is reported as `Build(RecordBuildError)`, whether the
callback propagates, translates, or ignores that failure. A retained build error
takes precedence when both occur. The callback-error path checks retained state
before returning; the successful callback path relies on `finish` to check it
once. No callback failure is passed to `finish`, and no ignored size
error can publish a partial record. Allocation failure uses normal Rust/BytesMut
behavior; it is not a recoverable `RecordWriteError` variant.

`buffered_bytes()` borrows only complete records, with no copy or ownership-count
change. `clear()` explicitly discards those records while retaining capacity; use
it after consumption or intentional abandonment. `into_bytes()` consumes the
writer and freezes the whole buffer without copying record bytes. Neither operation
splits per record. No mutable buffer access or partially constructed record escapes.
The writer's debug representation reports size and capacity, not payload contents.

These operations make the construction API usable independently of transport.
There is no automatic submission, socket flush, queue, acknowledgement, or buffer
pool yet. Completed bytes accumulate until the caller consumes or discards them;
there is currently no automatic batch size or total-memory bound. The planned
`RecordOutput` will handle ownership transfer, backpressure, bounded batching,
and recycling. Do not interpret `write` success as remote acceptance or durability.

## Implemented: serialization and finalization

Each `RecordBuilder` instance constructs exactly one complete record. It is
internal to this module and lives for one body-writing callback.
It exclusively borrows a `BytesMut` owned by the enclosing writer and appends the
record after its existing bytes. Those earlier bytes are inaccessible to the
callback and remain unchanged. The writer creates a fresh builder for each
record; `finish` consumes it, and dropping it unfinished rolls back that record.

The writer and its buffer are the objects intended for reuse across many records.
A fresh builder borrows the existing buffer and starts with an empty body and no
retained error. It owns no allocation, and with sufficient spare buffer capacity
its construction requires no heap allocation. The public writer now owns that
buffer; the pool that will recycle buffers after output remains planned.

Its lifecycle is:

1. `new(buffer)` remembers the original buffer length, reserves 65,535 bytes of
   additional capacity, and caches a pointer covering that complete spare region.
   It starts `payload_len` at zero without prefilling any record bytes. The
   reservation covers the entire maximum-sized record, including its header and
   CRC. The buffer's readable length remains unchanged.
2. The callback appends body bytes through `std::io::Write`, directly
   into their final positions after that header. Only body bytes count towards
   the fixed 65,519-byte payload limit. Many writes can contribute to this one
   body; a write call does not start or complete a record. Each accepted write
   advances only `payload_len`, leaving the whole record outside `BytesMut` length.
3. After the callback succeeds, `finish(self, id: RecordId)` checks the retained
   write error and derives total length from the body. It calls the protocol's
   `write::header` to initialize the header, then `write::crc` to calculate CRC-32C
   over the finalized header and body and initialize the trailer. Only then does
   one `set_len` expose the complete initialized record through `BytesMut`.
4. The completed record stays in the same buffer. The consumed builder cannot
   accept more writes or be finalized again. The next builder borrows the same
   buffer and starts a new record immediately after the completed one, without
   padding or splitting the buffer.

The supplied `RecordId` provides stream ID and sequence number. Length and CRC
come from the bytes actually written, so callers do not supply either. Typed
stream IDs already satisfy their range restriction; finalization does not
validate them again or check sequence continuity. `RecordHeader` remains a
read-only snapshot obtained from a validated record, not a construction input.

The builder owns outer framing and finalization because it already tracks the
body extent and write failures. The enclosing writer owns storage and callback
invocation; bounded batching and output coordination remain planned. The
callback's only interface is `Write`: it does not own `RecordBuilder`, manipulate
the buffer, reserve holes, or patch earlier bytes. Internal `payload_len()` / `is_payload_empty()`
read the cached payload count (`is_payload_empty()` is test-only), and
`check_error()` inspects retained error state.

The type and its methods are restricted to this module with `pub(super)` and are
not re-exported. `RecordWriter::write` is its production caller. Public callers
cannot access the concrete builder:

```compile_fail,E0432
use transaction_log_exports::record_writer::RecordBuilder;
```

The callback cannot retain its borrowed destination:

```compile_fail,E0521
use std::io;
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

let mut writer = RecordWriter::new();
let mut retained = None;
writer.write(RecordId::new(StreamId::MIN, SequenceNumber::MIN), |body| {
    retained = Some(body);
    Ok::<(), io::Error>(())
}).unwrap();
```

An application may encode a complete command transaction as one record body.
That is application policy: transaction headers, event headers, type/version
identifiers, and serialization formats belong to that application. The record
layer has no dependency on event types or a particular serializer and does not
interpret or fill inner headers.

This example's two-byte inner header only illustrates caller-owned serialization;
it does not define a required body format:

```rust
use std::io::{self, Write};

fn serialize_example<W: Write + ?Sized>(writer: &mut W) -> io::Result<()> {
    let payload = b"body data";
    writer.write_all(&(payload.len() as u16).to_le_bytes())?;
    writer.write_all(payload)
}
```

One callback invocation may make many incremental `Write` calls. It does
not require an intermediate allocation containing the whole serialized
body. The enclosing writer owns and reuses the destination storage.

## Error and ownership contracts

`write` and `write_all` append their entire input or return an error without
changing bytes or length. Empty writes are valid at the payload limit. The bound
check compares input length with remaining payload allowance, avoiding overflow
when calculating a requested total length. `write_vectored` currently uses the
standard trait's default behavior; callers must honor its returned byte count.

`RecordBuildError::PayloadTooLarge` reports an exceeded payload limit. The first
error is retained; all subsequent writes and flushes return it, including
operations with empty input. The `Write` methods convert it to `io::Error` with
kind `InvalidInput` and preserve the typed error as its payload.

`finish` checks this retained error itself. Ignoring a failed write and returning
success from a callback cannot make an incomplete body acceptable.
`RecordWriter::write` also checks the callback's own result before calling
`finish`: the builder cannot detect callback failures that never reach
its methods. The writer drops the builder if the callback fails.

Dropping an unfinished builder truncates the buffer to its original length. This
also happens on a rejected `finish` and during panic unwinding, preserving earlier
records and retaining the allocation's current capacity. Successful `finish`
disarms rollback. Until length is published, truncation is a no-op: incomplete
bytes remain outside the readable extent. As with Rust destructors generally,
process abort does not run this cleanup. `flush()` only reports retained errors;
it performs no I/O and does not finish or commit the record.

The exclusive buffer borrow prevents appending another record while this one
is unfinished. No raw mutable view escapes. The completed bytes remain owned by
the enclosing writer's buffer: finalization neither constructs a `Record` nor
sends bytes or establishes remote receipt, replication, or durability.

The callback runs synchronously on the producer's thread. The builder is a
temporary borrow within that operation; it is not queued or transferred between
threads. Its cached `NonNull<u8>` makes it neither `Send` nor `Sync`, and it has
no manual implementations of those traits. The planned output queue transfers
completed buffers after the builder has been consumed. Create and finish the
builder between awaits, rather than retaining it across an await in a `Send`
future. Writer handles can still move independently when no builder exists.

## Hot-path decisions

After construction reserves capacity, payload writes perform no allocation,
buffer growth, splitting, reference-count changes, locks, async operations, or
event boxing. Each append copies the serializer's supplied chunk directly into
its final body position. The builder itself owns no payload allocation.

The `payload_len: usize` field tracks accepted body bytes, excluding the header
and CRC. The size guard lives directly in `write`, beside the copy it protects.
After checking retained errors, the size check is
`incoming_len <= MAX_PAYLOAD_LEN - payload_len`: it does
not derive body length from the buffer or inspect capacity. Subtraction is safe
because accepted writes maintain the bound, and avoids overflowing an addition
for a rejected input. A successful append advances the counter; rejected writes
leave the counter and initialized bytes unchanged.

Construction derives a cached `NonNull<u8>` from `spare_capacity_mut` after the
reservation and before initializing the header. Its permitted region covers the
whole record: header, payload, and trailer. Appends copy directly to
`record_start + HEADER_LEN + payload_len`; they do not access `BytesMut`, obtain
another buffer view, check capacity, or update its length. During serialization,
`buffer.len() == start`, while exactly `payload_len` bytes after the header space
have been initialized in spare capacity. The header and trailer are initialized
only during finalization. The constant header offset does not require another
pointer field or an extra buffer lookup.

The safety proof relies on the full-record reservation, bounded counter, and
exclusive buffer borrow keeping the allocation alive and stationary. Nothing may
resize, split, or access the record through a separate buffer view while the
pointer is in use. No payload view escapes to the serializer, so its source slice
cannot overlap the destination. Copying initializes bytes before advancing the
local count. Capture the pointer from the whole spare region; a pointer obtained
from a narrower payload borrow must not be assumed to permit accessing its header.

Finalization delegates both encoded writes to `record_protocol`. Its unsafe
`write::header(header: *mut u8, length: u16, id: RecordId)` initializes the three
fields through unaligned little-endian stores. It does not create a byte-array
reference to uninitialized storage, so no zeroed header placeholder is needed.
Then `write::crc(record: *mut u8, record_len: usize)` creates a shared slice covering
exactly the initialized header and payload. This slice copies no bytes and
excludes the uninitialized trailer. After the shared view ends, the protocol
helper initializes the four trailer bytes with an unaligned little-endian store.

The builder establishes the extent, initialization order, and exclusive access
required by these helpers, then calls `set_len(start + record_len)` to publish
the complete record. The cached pointer is never used after publication. Keep
the protocol helpers' safety contracts, their local proofs, and the builder's
call-site proof consistent when changing this flow.

Pinning is not needed: the pointer addresses the separate heap allocation, and
moving the builder does not move that allocation. Storage stability comes from
reserving first and preventing operations that relocate storage, including buffer
reclamation. Pinning a `BytesMut` handle would not enforce that restriction.

No header, payload, or trailer bytes are prefilled. Payload writes and finalization
place their final bytes directly into the reserved storage. No payload shift or
second buffer is needed, and unwritten capacity never enters the readable length.

Encoded offsets, header stores, CRC coverage, and trailer stores belong to the
protocol module. Its inline `write` group holds the two initialization operations,
while `read` holds decoding and validation calculations; both share root layout
constants. The builder calls `protocol::write::header` and `protocol::write::crc`
without adding any runtime indirection. `read::compute_crc` remains necessary for
validating received frames;
`read::crc` decodes their stored value. `write::crc` performs calculation and storage
for a record under construction. Both calculation paths use the same `crc32c`
implementation. The writer helper accepts a raw pointer because its trailer may
be uninitialized; the reader helper accepts a fully initialized frame slice.
The builder contains no separate CRC calculation, trailer arithmetic, or store.

The payload bound established by accepted writes proves that adding the fixed
16-byte overhead fits in both `usize` and `u16`. Finalization uses that invariant
when encoding length, without repeating a range validation. The buffer's overall
length can exceed one record's limit; framing always starts at this body's own
saved record offset.

Successful operations allocate no auxiliary storage. Construction can grow or
reclaim the supplied buffer to satisfy its reservation; serialization and
finalization then fit without relocation. The future writer should choose an
existing batch with enough spare capacity or another pooled buffer before
creating the builder. Its reservation will then return without growing storage.
This lets the writer control batching while the builder enforces the capacity
guarantee needed by its append implementation. Error conversion to `io::Error`
can allocate on the failure path.

Keep the concrete builder private and the callback generic and unboxed.
`RecordWriter::write` supplies a scoped `&mut dyn Write` view. Recheck optimized
callback code when changing the invocation path, particularly whether the compiler
can resolve the concrete destination for field writes. Out-of-line functions or
other optimization barriers can retain destination dispatch; do not assume the
trait-object view is always eliminated.

These are implementation properties, not throughput measurements. Tests cover
complete maximum-sized records in preallocated storage and confirm unchanged
pointer and capacity, as well as payload address stability during finalization.
End-to-end writer benchmarks remain planned; the existing reader benchmark does
not measure serialization or writer coordination.

### Cached pointer measurement and selection

On 2026-09-14, an isolated prototype combined a cached payload pointer with
deferring `BytesMut` length updates until `finish`. That strategy is now adopted
in the production builder. On the Intel Core i9-12900HK,
Windows, Rust 1.98.0 / LLVM 22.1.8, `x86_64-pc-windows-msvc`, and the default
Cargo release profile, seven alternating paired samples of one million records
gave these median construction/encoding/CRC times:

| Payload workload | Per-write buffer length updates | Cached pointer + deferred length | Time reduction |
| --- | ---: | ---: | ---: |
| One 2,048-byte write | 188 ns | 191 ns | No demonstrated gain |
| 256 direct `u64` writes, 2,048 bytes | 378 ns | 207 ns | 45.1% |
| Postcard 1.1.3 / Serde 1.0.229, 257 writes, 2,153 bytes | 1,813 ns | 1,760 ns | 2.9% |

The Postcard sample serialized a preconstructed transaction with 32 mixed-field
events, including large variable-length integers and floating-point values. Both
variants used one thread, preallocated buffers, 64-record batches, and 100,000
warm-up records per workload. No CPU affinity was set. Timing includes header
and CRC finalization; it excludes input construction, allocation, queues, socket
I/O, and storage. The Postcard saving ranged from 1.7% to 4.1% across pairs;
the bulk difference changed direction. These are warm-memory measurements of
these inputs, not end-to-end throughput or an isolated measure of pointer caching.

The cached pointer and deferred length were selected because many small writes
are the expected hot path: they improved both measured small-write workloads,
with a much larger saving for direct writes than for the Postcard sample.
Real command-handler transaction types are not yet available. This is the best
measured choice for those workloads, not a guarantee for every serializer or
payload. Recheck with actual transactions when they become available.
Local experimental sources, pinned dependencies, checks, and raw results are
under `target/record-builder-pointer-bench/`; those ignored artifacts may be
removed by a clean build. Preserve these conditions with any new measurement.

### Finalization code generation

The measurements above preceded direct pointer-based finalization; they do not
measure its additional benefit. On 2026-09-14, a standalone wrapper consuming the
actual builder and calling `finish` was inspected with Rust 1.98.0, LLVM 22.1.8,
bytes 1.12.1, `x86_64-pc-windows-msvc`, and `-C opt-level=3`. Successful finalization
had no buffer-capacity checks, reservation calls, or slice-bounds panic paths.
The header used its three field stores, the CRC trailer used one four-byte store,
and the completed buffer length used one metadata store. CRC implementation
selection and calculation still occur, and the retained-error/rollback paths remain.
After moving initialization into `write::header` and `write::crc`, these finalization
properties were rechecked on the same toolchain. A constructor probe showed no
stores to record storage: it reserves capacity and initializes builder bookkeeping.
`Record::crc()` still used three loads with no validation branches after merging
trailer lookup into `read::crc`. No additional throughput claim follows from this
assembly inspection.

The inline `read`/`write` grouping was checked with the same compiler, target,
and optimization settings. Constructor, `u64` append, finalization, and
`Record::crc()` probes retained the same assembly instructions after normalizing
compiler-generated block labels. The grouping changes names and organization,
not those measured code-generation properties.

A `Write::write(&value.to_le_bytes())` probe for `u64` also retained a single
payload store after the error and size guards. The header offset is encoded in
that store's addressing mode, with no buffer metadata access. These observations
verify removed bookkeeping, not an end-to-end throughput gain or a portable
instruction-count guarantee. The local `target/record_finish_probe.rs` includes
the production builder; `record_finish_probe.s` and `record_finish_before.s`
contain the inspected assembly and may be removed by a clean build.

### Public writer code generation

On 2026-09-14, downstream-crate probes were compiled against the actual release
exports library with Rust 1.98.0, LLVM 22.1.8, bytes 1.12.1,
`x86_64-pc-windows-msvc`, `-C opt-level=3`, and no LTO. One callback wrote a
single `u64`; the other wrote each of 256 values from a borrowed `[u64; 256]`,
producing a 2 KiB body. Both used `RecordWriter::write` with an ordinary closure.

The callback and its `dyn Write` destination introduced no indirect calls in
either optimized probe. The inline builder constructor exposed its initial zero
payload length and empty error state across the crate boundary. The single-field payload
became one eight-byte store; the 256-field loop became one 2,048-byte `memcpy`
directly into final payload storage. Both probes eliminated their per-field size
and retained-error branches. Each retained the upfront capacity check and a direct
call to builder finalization. The larger copy is the original input-to-destination
write, not an intermediate serialized buffer or second payload copy.

Keep that constructor hint with the generic callback path. Variable-sized
serializers still need whatever bounds checks cannot be proven redundant, and
serializer behavior may prevent these optimizations. These results establish
code generation for two fixed workloads, not writer throughput or the performance
of a future transaction format. No new throughput benchmark was run for step 2.

The local `target/record_writer_api_probe.rs` and its `.s` output contain these
probes. They are ignored artifacts and may disappear on cleanup. Recreate a probe with
the two workloads above if necessary, expose each outer call with
`#[unsafe(no_mangle)]`, and inspect it with:

```powershell
cargo build -p transaction-log-exports --release --locked
rustc --edition=2024 --crate-type=lib -C opt-level=3 --emit=asm --extern transaction_log_exports=target/release/libtransaction_log_exports.rlib -L dependency=target/release/deps target/record_writer_api_probe.rs -o target/record_writer_api_probe.s
```

## Planned integration and review sequence

1. Body serialization, outer framing, finalization, and rollback: implemented in
   `record_builder.rs`, with its error type in `record_build_error.rs`, shared protocol
   codecs, and same-file tests.
2. Writer API and callback invocation: acquire capacity, invoke one synchronous
   body-writing callback per record, check its result, and call `finish`
   on success. Implemented in `record_writer.rs`, with typed errors in
   `record_write_error.rs`. No partial
   record is exposed through the writer's completed-byte view.
3. Output coordination: a shared `RecordOutput` creates independent writers lazily.
   Each command-handling execution context owns its writer; one socket driver
   consumes completed buffers from a bounded queue. The command handler assigns
   sequence numbers and preserves submission order within each stream. Queue
   ownership, backpressure, cancellation, partial writes, connection errors, and
   drain semantics belong to that step.
4. Buffer recycling and batching: move whole buffers through the output path and
   return them for reuse, bounding active buffer memory independently of the number
   of idle writer handles. A transport batch can contain several complete records.
5. Benchmark realistic transaction serialization and CRC work with multiple writers
   before choosing buffer/queue sizes or asserting performance improvements.

The module has a public `RecordWriter` and synchronous callback runner. It has
no `RecordOutput`, queue, pool, socket task, or service durability behavior yet.

## Verification and maintenance

Keep tests in the file owning the behavior. Current coverage includes a serializer
generic over `Write`, prefix isolation, independent limits across consecutive
records, reservation from insufficient capacity, stable storage across thousands
of small writes, deferred complete-record publication, shared-prefix
preservation, empty and maximum payloads, atomic rejection, retained
errors, and rollback on abandonment, serialization error, and unwinding.
Finalized frames are checked against the fixed golden fixture and independent
encoding/CRC expectations, at arbitrary byte offsets and numeric boundaries.
Guard bytes after the golden frame check that the unaligned trailer store writes
only its four bytes and preserves the following storage.
A mixed-stream batch of finished records also passes the real `RecordReader`.
Protocol tests separately exercise uninitialized header/trailer destinations,
guard bytes, byte alignments, and independent encoded expectations.

The README's callback and serialization examples compile as documentation tests.
Same-file writer tests cover exactly-once synchronous invocation with consumed
non-Send captures, borrowed inputs, typed callback-error
preservation, propagated/translated/ignored build errors, retry and unwind
rollback, allocation reuse, whole-buffer transfer, moving the writer between
threads, and real-reader compatibility with mixed streams and boundary bodies.
They compare frames with the independent test encoder and checksum reference.
The error type's source chain has a same-file test. A compile-fail example verifies
that public callers cannot name the internal builder type or retain the callback's
borrowed destination.

Run workspace tests, `cargo fmt --all --check`,
`cargo clippy --workspace --all-targets --locked -- -D warnings`, and Rustdoc with
warnings denied for relevant changes. Also exercise framing and bounds tests in
release mode. Do not turn a long throughput benchmark into a correctness test.
Update implementation status and contracts here together with the record
specification where they overlap.

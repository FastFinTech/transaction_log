# Record writing

This is the common middle layer for record output to sockets, files and test
destinations. `RecordWriter<W, Mode>` owns an already-open `AsyncWrite` destination
and one reusable buffer. Its constructor selects either serialization of new
records or copying existing records; the type system prevents mixing those append
APIs on one writer. Record appends are synchronous; the caller explicitly sends
the accumulated batch with async `flush_buffer()`. Destination flushing is a
separate operation. The caller chooses batch boundaries, scheduling, durable
synchronization and shutdown.

This README is the maintained specification for the writer module and is included
in Rustdoc. It explains requirements and their reasons; API comments and inline
safety proofs explain how the code upholds them. The
[record specification](../record/README.md) owns framing, limits and CRC coverage;
keep both specifications consistent.

## Where to read and maintain the design

| File | Responsibility |
| --- | --- |
| `record_writer.rs` | Define the two zero-sized public mode types at the top, then own the destination and batch; invoke callbacks; send, flush and conditionally sync; enforce terminal I/O state. Includes writer and file/synchronization integration tests. |
| `record_builder.rs` | Build one record in borrowed spare capacity, finalize header/CRC, and roll back unfinished construction. Contains the raw-pointer safety proofs and builder tests. |
| `async_sync_data.rs` | Define optional data synchronization and adapt Tokio's file method. The generic writer consumes this capability; it is not a new file or socket abstraction. |
| `record_build_error.rs`, `record_write_error.rs`, `record_output_error.rs` | Distinguish rejected payload construction, callback failure and terminal destination I/O failure. |
| `mod.rs` | Include this specification and re-export public items; keep implementation out of this entry point. |

Start with the ownership and lifecycle contracts below, then read the writer.
The two mode markers intentionally live beside `RecordWriter` in its source file,
as an exception to the usual one-type-per-file convention: they only select its
append API and are easiest to read alongside the implementations they select.
Read the builder and shared protocol together before changing serialization or
unsafe storage access. Keep errors and test expectations consistent with the
stage at which a failure occurs. Do not add independent format constants here.

## Why these boundaries exist

The expected producer serializes many fields into a record on a busy executor.
A record can contain an entire application transaction, but the writer treats
its body as opaque bytes. The application owns event types, serializers and IDs.

| Decision | Reason and tradeoff |
| --- | --- |
| Serialize synchronously into an owned `BytesMut` | The final length is unknown until the callback completes, yet it appears first in the encoding. Buffering permits in-place header/CRC finalization and rollback before any incomplete record reaches the destination. |
| Lend a concrete temporary builder | Serializers can write field by field with static dispatch, without queuing boxed event objects or allocating their own payload vector. |
| Reuse one batch allocation | Avoid per-record buffer replacement, splitting, reference-count handoffs and pool lookup. Batches can grow; sending one borrows the writer until completion. |
| Select the append API through the constructor | A writer either serializes new records or copies existing encodings. Separate concrete implementations prevent accidental mixing at compile time, without mode branches or a dispatch trait on the hot path. |
| Copy existing records into an owned batch | Existing-record appends stay synchronous, preserve order and release the record borrow immediately. This intentionally spends a copy to give callers one explicit sending boundary. |
| Keep send, flush and durable sync separate | Applications can choose different cadences for batching, destination flushing and persistence. A synchronous append must not incur I/O latency. |
| Offer synchronization through an optional trait | Files and custom storage can expose persistence without pretending that every socket or byte buffer can provide it. The capability is selected at compile time. |
| Stop after incomplete destination I/O | Replaying a partially accepted batch can duplicate a prefix; failed synchronization leaves durability unknown. Recovery requires application context this layer does not have. |

## Ownership and public API

`RecordWriter::for_serialization(destination)` and
`RecordWriter::for_records(destination)` take the destination by value. For a socket,
the application establishes the connection and completes its handshake first.
For a file, the application chooses creation/append mode and file position.
Construction reserves a single 65,535-byte `BytesMut` and performs no I/O.
The buffer grows as records accumulate and keeps its capacity after output.
The protocol size limit applies to individual records, not the whole batch.
In serialization mode, each builder reserves room for a maximum additional record
before taking its pointer; even a small record can trigger growth if the remaining
spare capacity is insufficient. Building allocates nothing only when enough
capacity exists. Existing-record mode appends the known encoded length without a
builder. Capacity is not readable initialized length.

### Constructor-selected modes

| Constructor | Inferred type | Only append method |
| --- | --- | --- |
| `RecordWriter::for_serialization(destination)` | `RecordWriter<W, SerializeRecords>` | `write(id, callback)` |
| `RecordWriter::for_records(destination)` | `RecordWriter<W, ExistingRecords>` | `write_record(&record)` |

Both constructors work with sockets, files and test destinations. The mode selects
how records enter the buffer, not the application role, destination kind, stream ID,
or output policy. A batch may contain multiple stream IDs in either mode. The
transaction-log application's eventual file-ingestion path may manage received
records itself; this API does not require that application to use this writer.

The two marker types are public and re-exported from the crate root so callers can
name a stored writer's full type. Local variables infer the mode from the named
constructor. There is deliberately no default mode, ambiguous `new` constructor,
or conversion that changes an existing writer's append API.

`SerializeRecords` and `ExistingRecords` contain no data. A private marker field
adds zero bytes for either mode and is never inspected. Mode-specific inherent
implementations expose the appropriate constructor and append method. Shared
generic implementations own initialization, I/O, failure tracking and destination
access. There is no mode trait, vtable, runtime enum or extra mode check. Private
fields and a private common constructor prevent constructing arbitrary modes.

The following examples must fail because the other append method does not exist:

```compile_fail,E0599
use transaction_log_exports::{Record, RecordWriter};
fn append_existing(record: &Record) {
    let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
    writer.write_record(record).unwrap();
}
```

```compile_fail,E0599
use std::io::Write;
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};
let mut writer = RecordWriter::for_records(Vec::<u8>::new());
let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
writer.write(id, |body| body.write_all(b"payload")).unwrap();
```

### Buffering and output shared by both modes

There is no automatic submission, batch size limit or capacity trimming. Callers
choose batch boundaries and therefore control both latency and retained memory.
A large batch can leave a large allocation resident for later reuse. Allocation
failure follows normal `BytesMut` behavior, not a recoverable writer error.

Output requires `W: tokio::io::AsyncWrite + Unpin`. Static dispatch is retained
for concrete destinations. There is no Send requirement, task spawn, runtime
creation, channel, pool, lock or timer in the writer. Async output runs wherever
the caller polls it. The supplied destination determines runtime requirements;
an in-memory implementation can be polled without a Tokio runtime or LocalSet.
Rust's ordinary auto-trait rules govern moving the writer and its futures;
single-owner use does not mean the type is forcibly `!Send`. The temporary builder
is non-Send because it retains a raw pointer only during serialization.
A trait-generic synchronization future is not promised to be Send.

| Method | Contract |
| --- | --- |
| `for_serialization(destination)` | Take ownership and allocate capacity for one maximum record; select `SerializeRecords`. |
| `for_records(destination)` | Take ownership and allocate capacity for one maximum record; select `ExistingRecords`. |
| `write(id, callback)` | Serialization mode only: synchronously serialize and append one complete record, including its header and CRC. No I/O. |
| `write_record(&record)` | Existing-record mode only: synchronously copy a record's encoding into the batch without repeated validation or checksum calculation. No I/O. |
| `flush_buffer().await` | Send all buffered bytes, then clear the buffer while retaining capacity. An empty buffer requires no I/O. |
| `flush().await` | Flush only the destination's own buffering; does not send the writer's buffered records. |
| `sync_data().await` | Synchronize previously flushed data when `W` also implements `AsyncSyncData`; does not send or flush buffered records. |
| `get_ref()` | Borrow the destination for observation or destination-specific operations, such as file synchronization. |
| `into_inner()` | Return destination ownership and discard any unsent records, without implicit flush or shutdown. |

```rust
use std::io::{self, Write};
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
writer.write(id, |body| {
    body.write_all(&42_u64.to_le_bytes())?;
    body.write_all(b"opaque application payload")?;
    Ok::<_, io::Error>(())
})?;
assert!(writer.get_ref().is_empty()); // Still entirely in the writer's buffer.
writer.flush_buffer().await?;
writer.flush().await?;
let encoded = writer.into_inner();
assert_eq!(encoded.len(), 16 + 8 + b"opaque application payload".len());
# Ok(())
# }
```

The body-writing callback receives a concrete `&mut RecordBuilder`, not a trait
object. It may capture local application state and return its own error type.
A usable writer invokes it exactly once, immediately inside the synchronous
`write` call. Success publishes a complete record into the buffer; it says nothing
about destination acceptance. Repeated calls to the selected mode's append method
can build one batch before the caller awaits `flush_buffer`. Both modes expose
that explicit sending operation because both append synchronously into a buffer.

The mutable writer borrow serializes operations. Each builder is consumed before
`write` returns, so no cached pointer survives into asynchronous output or a
later reservation. Records stay in the same buffer until complete output succeeds.
The allocation is then cleared for reuse. There is no spare-buffer lookup, split,
replacement buffer or second staging buffer in the writer.

Synchronous writes do not wait on the destination. However, this single-buffer
writer cannot construct the next record while `flush_buffer` is in progress.
A higher layer needing concurrent production and output must provide its own
ownership and handoff strategy. Do not add mandatory worker threads or timers
to this middle layer to satisfy only one destination's policy.

## Existing records and validation boundaries

Existing-record mode accepts immutable, already-validated `Record` values.
`write_record` copies `record.as_bytes()` into its batch, preserving append order,
length, ID, payload and CRC exactly. It neither creates a builder, clones the
`Bytes` handle, revalidates the record nor recalculates CRC. Its borrow ends on
return; the original record may be dropped before the batch is sent. The writer
cannot also serialize new records through a callback.

```rust
use transaction_log_exports::{Record, RecordOutputError, RecordWriter};

async fn copy_record(record: &Record) -> Result<Vec<u8>, RecordOutputError> {
    let mut writer = RecordWriter::for_records(Vec::new());
    writer.write_record(record)?;
    writer.flush_buffer().await?;
    writer.flush().await?;
    Ok(writer.into_inner())
}
```

This copy is intentional: the append remains synchronous, records retain their
order, and `flush_buffer` is the sole point that sends records in either mode.
There is no direct-output bypass. Destination implementations may also copy into
their own buffering and OS buffers; this is not zero-copy networking or storage.

Typed IDs are already validated, and the reader establishes immutable record
validity. The writer does not repeat those checks, enforce sequence continuity,
authorize stream IDs or interpret the body. Those decisions belong to ingestion,
replay and application code.

## Completion, errors and cancellation

There are three separate completion boundaries after a successful append:

1. `flush_buffer().await` means the destination's `AsyncWrite` accepted all
   encoded bytes. Its own buffers or internal I/O may still be pending.
2. `flush().await` means the destination's flush operation completed. It does
   not read the writer's `BytesMut`, even when that buffer contains newer records.
3. `sync_data().await` means a supporting destination completed its data-sync
   operation under its storage/platform guarantees. It does not implicitly
   perform either preceding step or establish remote replication.

To include every pending record in file synchronization, perform all three in
that order. Their cadences can differ: data already sent and flushed can be synced
while newer records remain buffered. No method tracks an acknowledgement,
replication position or last-durable sequence number. A serializer calling
`RecordBuilder::flush` reaches none of these boundaries; that `io::Write`
operation only checks the builder's retained error.

### Partial output and the usability flag

`flush_buffer` advances a borrowed slice through bytes accepted by the destination.
It retries `Interrupted` at the same position and treats zero progress as
`WriteZero`. The `BytesMut` retains the entire batch; only the output future
holds the cursor. Once every byte is accepted, clearing the buffer preserves
capacity for the next batch. An empty buffer triggers no I/O. Destination flush
and sync still call their destination methods when our buffer is empty, because
earlier accepted bytes may need completion.

The `usable` flag is permission to continue, not a statement that the buffer is
empty, records were received, or data is durable. Each I/O operation sets it false
before invoking the destination, restoring it only on success. During a pending
operation, its exclusive mutable borrow prevents other writer calls. Dropping that
future, unwinding, or returning an I/O error leaves the flag false.

| Outcome | Buffered records and subsequent use |
| --- | --- |
| Callback error, sticky builder error or callback panic | Roll back only the current record. Earlier complete records and capacity remain; the writer is still usable. |
| Successful buffer output | Clear the batch and retain capacity. The writer remains usable; downstream flushing and persistence are separate. |
| Pending future polled again | Continue the same operation. A send retains its current byte cursor and never restarts at zero. |
| Unpolled I/O future dropped | No effect; no destination call or state transition has occurred. |
| Failed, panicking or cancelled send | Some prefix may already be accepted. The whole batch remains buffered but must not be replayed; the writer is unusable. |
| Failed, panicking or cancelled destination flush or sync | Downstream completion or durability is uncertain. Local buffered records are unchanged; the writer is unusable. |
| Any write, send, flush or sync after unusability | Return `Unusable` before invoking a callback or touching the destination, including for an empty buffer. |

This is deliberately conservative: cancelling after a pending poll forbids reuse
even if that destination has not accepted any bytes yet. Cancellation is not an
ordinary retry path. A fresh `flush_buffer` future would have lost the previous
cursor, and an async destination may keep doing internal work after its future is
dropped. There is no reset, cursor reconstruction or automatic recovery here.

The first I/O error is returned intact as `RecordOutputError::Io(io::Error)`.
Only terminal state is stored; errors are not cloned into a shared slot. Later
calls return `Unusable`, not the original error. Construction uses
`RecordWriteError<E>`: `Build` identifies a retained payload-size error,
`Serialize(E)` preserves the callback's own error, and `Output` reports a writer
already made unusable by previous I/O. A sticky build error takes precedence over
a callback error, including when the serializer wraps or ignores a failed write.
Callback panics are not caught, and side effects outside the builder do not roll back.

### Ownership recovery and shutdown

Dropping a writer or calling `into_inner` discards unsent records without sending,
flushing, synchronizing or shutting down the destination. If those records must
be sent, explicitly complete the appropriate operations first. `into_inner`
remains available after failure so the owner can close or recover its destination;
extracting it, or putting it in a new writer, does not repair partial framing or
establish the previous durability boundary.

`get_ref` exposes destination-specific operations such as a file's `sync_all`.
Those operations bypass the writer's usability checks and failure tracking.
Callers must handle their errors and preserve ordering if a destination supports
writing, seeking or cloning a handle through a shared reference. Prefer the
writer's own `sync_data` when data synchronization is sufficient.

## Optional data synchronization

`AsyncSyncData` is a public capability trait in `async_sync_data.rs`, re-exported
from this module and the crate root. It is implemented for `tokio::fs::File` by
delegating to the file's inherent `sync_data` method. The writer exposes
`sync_data(&mut self)` only when `W: AsyncWrite + AsyncSyncData + Unpin`.

```rust
use std::io::Write;
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

# async fn example(file: tokio::fs::File) -> Result<(), Box<dyn std::error::Error>> {
let mut writer = RecordWriter::for_serialization(file);
writer.write(RecordId::new(StreamId::MIN, SequenceNumber::MIN), |body| {
    body.write_all(b"file payload")
})?;
writer.flush_buffer().await?;
writer.flush().await?;
writer.sync_data().await?;
# Ok(())
# }
```

These remain three distinct operations. Calling `sync_data` while our buffer is
nonempty leaves its records untouched; calling it before destination flushing
does not promise that downstream buffered records are durable. Implementations
must synchronize previously flushed data without implicitly draining a wrapper's
pending write buffer. They should defer I/O until polled and propagate I/O errors.
The file implementation delegates to Tokio's inherent method, preserving its
runtime requirements, I/O scheduling, error and platform behavior. It can omit
metadata and may behave like `sync_all` on some platforms. This adapter adds no
thread, timer or extra `AsyncWrite::flush` call. Tokio's `fs` feature is a normal
dependency feature so downstream users can use this implementation outside tests.

Custom storage wrappers and test destinations can implement the trait. It takes
exclusive mutable access so wrappers may maintain local state. Its return type
is an implementation-specific future without a `Send` bound; no boxed future,
`async-trait` dependency or runtime capability check is required. Implementors
can write a native `async fn` to satisfy that signature. The trait does not
promise allocation-free internals for every implementation, nor can generic
callers assume its future can be passed to `tokio::spawn`.
Exclusive `&mut self` supports wrappers with mutable state and matches the writer's
serialized operation order; Tokio's file adapter can pass that as a shared borrow
to the underlying file method. This adds no fields, branches or synchronization
to record construction.

The capability is not inferred from `AsyncWrite` or automatically forwarded
through wrappers such as `BufWriter<File>`. Only Tokio files have a production
implementation here. A custom storage wrapper must implement its own capability
and keep its pending buffer separate from synchronization. An implementation must
await actual sync completion and return failures; reporting success after merely
scheduling work would violate the contract. Test destinations may simulate that
completion, but passing their tests does not prove physical durability.

In-memory byte buffers and sockets do not expose this method:

```compile_fail,E0599
use transaction_log_exports::RecordWriter;
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
let _ = writer.sync_data();
```

Synchronization follows the terminal I/O lifecycle described above. Even after
complete writes, uncertain persistence is an application recovery decision.
The trait itself has no shared error state; the writer owns that lifecycle policy.

## Construction lifecycle

Each builder constructs exactly one complete record. It exclusively borrows a
writer-owned `BytesMut` and preserves the prefix of previously buffered records.
One callback constructs one record; many callbacks can contribute to a batch.
The temporary builder has no reset cycle or owned allocation. Only explicit,
successful buffer output clears the allocation for reuse.

1. `new(buffer)` remembers the original length, reserves 65,535 additional bytes,
   and captures a pointer covering the whole spare record region. Header, payload,
   and trailer storage remain uninitialized. `payload_len` starts at zero and the
   buffer's readable length stays unchanged.
2. `std::io::Write` calls append payload bytes directly after the reserved header.
   Many writes may contribute to one record. Each accepted append advances only
   the cached payload count, bounded to 65,519 bytes.
3. After serialization succeeds, `finish(self, id)` checks retained errors and
   derives total length. The protocol's `write::header` initializes the header;
   `write::crc` calculates CRC-32C over the header and body and initializes the
   trailer. One `set_len` then publishes the complete initialized record.
4. The completed bytes remain in the same buffer without a split, payload move,
   padding, or conversion into an owning `Record`. Finishing consumes the builder.
   Dropping it unfinished preserves the prefix and buffer capacity.

The supplied `RecordId` provides the stream ID and sequence number. Length and
CRC come from the bytes written; callers do not provide them. Typed stream IDs
already satisfy their range restriction. The builder does not revalidate IDs,
check sequence continuity, or advance a sequence number.

Callers can name the builder type but cannot construct or finish one themselves.

```compile_fail,E0624
use bytes::BytesMut;
use transaction_log_exports::RecordBuilder;
let mut buffer = BytesMut::new();
let _ = RecordBuilder::new(&mut buffer);
```

```compile_fail,E0624
use transaction_log_exports::{RecordBuilder, RecordId};
fn finalize(builder: RecordBuilder<'_>, id: RecordId) {
    let _ = builder.finish(id);
}
```

The body is opaque. Transactions, event types, inner headers, type/version IDs,
and serializer selection belong to the caller's application. The builder never
interprets or patches inner headers. The writer exposes a scoped concrete builder
through a synchronous, generic, unboxed callback.
Application code does not need its own intermediate serialized byte vector or
an implementation of a library-specific serialization trait.

## Errors, publication, and ownership

`write` and `write_all` accept their entire input or return an error without
changing the payload or its count. Empty writes are valid, including at the
payload limit. `write_vectored` retains the trait's default behavior, so callers
must honor its returned byte count.

`RecordBuildError::PayloadTooLarge` is sticky: the first failed write prevents
further successful writes, flushes, or finalization, including empty writes.
`Write` methods wrap the typed error in `io::Error` with kind `InvalidInput`.
`check_error()` inspects this retained state; it does not validate framing or CRC.
A fresh builder starts with no error. Allocation failure follows normal
Rust/BytesMut behavior and is not a recoverable build-error variant.

`finish` rejects retained errors even if a serializer ignores a failed write.
The caller must independently check the serializer's own result before invoking
`finish`; failures that never reach the builder's methods cannot be inferred.
Drop the builder when serialization fails. `flush()` reports retained errors
only: it performs no I/O and does not finish a record.

An unfinished builder's drop truncates to the original buffer length, including
after rejected finalization and during unwind. Before publication, incomplete
bytes lie outside that readable extent, so this normally changes no length.
Successful finalization disarms rollback. Earlier records and current buffer
capacity survive; callback side effects elsewhere do not roll back. Process
abort does not run destructor cleanup.

The exclusive buffer borrow prevents another record or external byte view from
accessing the region under construction. The builder stays on the producer's
thread during synchronous serialization. Its cached `NonNull<u8>` makes it
neither `Send` nor `Sync`, and it has no manual implementations of those traits.
The builder is consumed before asynchronous output begins. The writer retains
the allocation across that output, then clears it for reuse. Finalization
establishes neither remote receipt nor replication nor durability.

## Hot-path design and safety

Reservation happens once per record, before deriving the cached pointer. Body
writes and finalization cannot grow, split, reclaim, or otherwise relocate the
allocation. With sufficient spare capacity, builder construction allocates
nothing. Growth between records can allocate and move earlier buffered bytes;
the pointer is captured only after reservation completes. The writer always
holds a buffer before creating the builder.

`payload_len` excludes the header, trailer, and earlier records. The write guard
compares incoming length with `MAX_PAYLOAD_LEN - payload_len`, avoiding overflow
for rejected input. Accepted writes perform a direct copy into
`record_start + HEADER_LEN + payload_len` and advance only the local count.
They do not obtain a fresh buffer view, inspect capacity, update `BytesMut` length,
synchronize, perform I/O, or allocate auxiliary storage. Error conversion may
allocate on the failure path.

The cached pointer is derived from the full spare region, allowing it to address
header, body, and trailer. The exclusive borrow and reservation keep that region
alive and stationary. No destination view escapes to the serializer, so its
source slice cannot overlap it. Copying initializes bytes before the count is
advanced. The readable buffer length remains at its original value until finish.

The protocol helpers own encoded offsets, unaligned little-endian stores, CRC
coverage, and trailer placement. `write::header` accepts uninitialized storage and
initializes every header byte. `write::crc` reads only the initialized header and
body, then writes the uninitialized trailer after that shared view ends. The
builder establishes their capacity, initialization, and exclusive-access
preconditions, then publishes the total length. It never uses the cached pointer
after publication. Keep the local unsafe proofs and protocol contracts together.

Serialized bytes are not prefilled, shifted, or copied into a second record buffer.
Existing records are copied once into the batch by `write_record`. The fixed
payload bound proves that adding 16 framing bytes fits in both `usize` and `u16`,
so finish need not repeat range validation. Builder and writer tests exercise
prefix preservation across successful appends, rejected records and unwinding.

Pinning is unnecessary: the pointer addresses a separate heap allocation, and
moving the builder does not move that allocation. Preventing buffer relocation
while the pointer is live is the actual requirement. Pinning a `BytesMut` handle
would not establish it.

Keep the constructor and write path inline. The callback receives the concrete
builder, so this API introduces no virtual `Write` call. A serializer can still
choose to erase its destination type internally. Variable-sized writes retain
necessary bounds checks unless the compiler proves them redundant. No queue,
task, or shared-state borrow occurs inside the serialization callback. The enclosing
`write` checks its local usability flag before serialization but performs no
I/O. `flush_buffer`, destination `flush` and `sync_data` set the flag false before
I/O and restore it only after success.

The following measurements motivate the retained builder implementation. Its
construction, append, and finalization algorithms are unchanged by this buffered
output integration. These measurements are not current end-to-end throughput claims.

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

### Callback integration evidence

#### Current synchronous API

On 2026-09-16, downstream probes of
`RecordWriter<Vec<u8>, SerializeRecords>::write` were checked with Rust 1.98.0 /
LLVM 22.1.8, bytes 1.12.1, `x86_64-pc-windows-msvc`, `-C opt-level=3` and no LTO.
The single-`u64` callback produces one payload store; 256 `u64` writes become one
2,048-byte `memcpy`. The usability and upfront reservation checks and finalization
call remain. There is no output call, async polling or buffer-clear work inside
these synchronous probes.

The emitted function bodies matched the immediately preceding synchronous API
exactly when adding constructor-selected modes, including field offsets and
branches. This checks that the mode parameter introduced no instructions into
these two serialization workloads. It does not imply that every callback or
destination has identical generated code across compiler versions.
The inspected sources and assembly are in the ignored
`target/record_writer_buffered_probe.rs` and `.s`; the comparison baseline is
`target/record_writer_before_modes.s`. This verifies callback code
generation, not batch throughput, allocation cost or destination performance.

#### Earlier callback probes

On 2026-09-14, downstream synchronous callback probes used Rust 1.98.0,
LLVM 22.1.8, bytes 1.12.1, `x86_64-pc-windows-msvc`, `-C opt-level=3` and no LTO.
One callback wrote a `u64`; another wrote 256 values from a borrowed
`[u64; 256]`. Inlining reduced their payload writes to one eight-byte store or
one 2,048-byte `memcpy` into final storage, with an upfront reserve check and
finalization call. Subsequent synchronous-writer checks on September 15 and 16
retained those encoding properties.

On 2026-09-16, an earlier async middle layer was checked with the same compiler,
target, bytes version, optimization level and no LTO. A downstream probe polled
each public `write` future against a concrete, always-ready sink that observes
the encoded slice through `black_box`. The payload still became one eight-byte
store or one 2,048-byte `memcpy`, with the upfront reservation check and builder
finalization call. The usability guard and buffer-clear bookkeeping remained;
there was no callback or destination vtable dispatch in these probes.

The local `target/record_writer_middle_probe.rs` and `.s` contain that inspection.
This is code-generation evidence for two fixed workloads, not a measurement of
async I/O latency or overall throughput. Pending destinations and real serializers
can produce different code and costs. Earlier `target/record_writer_*probe.*`
artifacts may use removed APIs; all these ignored files can disappear on a clean
build. Preserve compiler, target and measurement conditions with new evidence.

## Higher layers and deferred work

The command-handler application decides event types, transaction serialization,
sequence numbers, connection setup, acknowledgements, admission policy and any
background output workers. The transaction-log application decides stream/file
routing, append position, batching, index updates, durable sync cadence, recovery
and worker sharing across files. Neither application layer is implemented here.

A high-throughput background wrapper must manage multiple buffers if its producer
continues while output is pending. The current middle layer owns one buffer and
appends records synchronously, then sends a batch when explicitly asked. The
requested Crossfire-versus-Tokio benchmark remains deferred until the surrounding
output design is reviewed; neither channel
crate is needed by this implementation.

The opt-in [benchmark suite](../../benches/README.md) distinguishes reader-only,
writer-only and combined TCP paths. Writer targets separately measure synchronous
serialization and copying existing records. The writer-only receiver drains raw
bytes, avoiding timed reader work; the combined receiver validates every record.
Socket completion remains distinct from buffered file output, durable
synchronization and command-response latency. Long performance runs stay opt-in;
the benchmark README records workload, conditions and measured results.

## Verification and maintenance

Builder tests remain in `record_builder.rs`. They cover many small writes,
prefix preservation, reservation from insufficient capacity, stable storage,
deferred publication, empty/maximum payloads, atomic rejection, sticky errors
and rollback. Golden frames, independent CRC expectations, arbitrary byte
offsets and guard bytes protect the protocol and unsafe initialization.

Writer tests are in `record_writer.rs`. They cover owned non-Send sinks without
a runtime, immediate callbacks without I/O, batches exceeding one record's limit,
append order in each mode, allocation reuse, construction errors/unwind,
partial writes, interruption, zero progress, I/O errors, cancellation before and
after partial output, unpolled futures, destination panic, empty buffer output,
separate buffer/destination flushing, discarded unsent records, ownership recovery
and rejection of output after failure. Synchronization tests cover explicit
ordering, no implicit output or destination flush, non-Send pending futures,
original errors, cancellation, panic, and rejection after prior I/O failures.
The same-file edge-case coverage also includes:

| Boundary | What the tests establish |
| --- | --- |
| Every incomplete byte prefix of an existing-record batch | I/O errors, zero progress, cancellation and panic retain the batch, reject all later operations, and never replay bytes when ownership is extracted or dropped. This includes record boundaries and CRC bytes. |
| Mode-specific failure rejection | Both modes preserve the batch and reject appends after zero progress or a partial I/O error. Serialization callbacks are never invoked after terminal output, flush or sync failures. |
| Repeated `Interrupted` and `Pending` results after progress | The same send future resumes at the correct cursor across records and completes after the last byte without an extra destination call. |
| Destination flush lifecycle | Dropping an unpolled future has no effect; pending flushes can resume; errors, cancellation and panic preserve newer unsent records and forbid further output. |
| Buffered destination failure | Partial failure while flushing an actual `BufWriter` does not send the writer's newer batch or replay the earlier accepted prefix. |
| Maximum existing records | Repeated copies can exceed one record's size limit in aggregate, preserve caller-supplied IDs even at `u64::MAX`, and reuse the allocation for a shorter later batch without leaking old bytes. |
| Callback failure after filling the maximum payload | Both error and unwinding roll back after buffer growth; subsequent shorter records reuse the initialized spare storage without exposing abandoned bytes. |
| Failure after a previous successful sync | The earlier durable boundary does not mask later errors, cancellation or panic; the same terminal policy applies to initial and later sync attempts. |

Scripted destinations make failure timing deterministic without sleeps or network
races. Scripts reject unexpected additional writes, so cursor/retry regressions
fail the test instead of silently succeeding against an always-ready sink.
File integration exercises both modes, including durable synchronization, and
handshake-complete socket integration exercises serialization mode. Their bytes
pass through the real reader. Temporary-file tests do not simulate power loss.
Compile-fail examples enforce the absence of the other mode's append method,
builder privacy and the absence of data synchronization without the capability.

For behavioral changes, run workspace tests in debug and release, formatting,
Clippy and Rustdoc with warnings denied:

```powershell
cargo fmt --all --check
cargo test --workspace --locked
cargo test --workspace --release --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
$env:RUSTDOCFLAGS = '-D warnings'
cargo doc --workspace --no-deps --locked
```

For documentation-only changes, compile documentation examples with
`cargo test --workspace --doc --locked` and build Rustdoc with warnings denied;
formatting and checking links/contracts are still necessary. Do not claim a new
performance result from documentation changes.

Recheck optimized callback code generation when changing serialization, its
generic call boundary or pointer/reservation bookkeeping. Preserve compiler,
target, workload and measurement limits with new evidence. Long throughput
benchmarks remain opt-in; routine tests do not run them.

Maintain API comments, inline invariants, this README and the record specification
together. When changing batching or adding a wrapper, recheck all three completion
boundaries, append order in each mode and compile-time API separation. When
changing error handling, recheck callback rollback separately from partial I/O and synchronization
uncertainty. Keep protocol expectations independent of production encoders.

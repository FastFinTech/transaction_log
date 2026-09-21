# Record writing

Build or copy transaction-log records into a reusable batch, then send that batch
to an owned async socket, file or other destination. Appends are synchronous in
both modes; the caller chooses when to send, flush and synchronize output.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`RecordWriter<W, Mode>`](RecordWriter) | Own the destination and batch, append complete records, and track output failures. |
| [`SerializeRecords`] | Select synchronous payload construction through `write(id, callback)`. |
| [`ExistingRecords`] | Select synchronous copying through `write_record(&record)`. |
| [`RecordBuilder`] | Lend a `std::io::Write` destination for one payload and finalize its framing after the callback succeeds. |
| [`AsyncSyncData`] | Expose optional destination data synchronization, including for Tokio files. |
| [`RecordBuildError`] | Identify a payload that exceeds the format's size limit. |
| [`RecordWriteError<E>`](RecordWriteError) | Preserve construction, callback or prior output failures during serialization. |
| [`RecordOutputError`] | Report destination I/O errors and subsequent use of an unusable writer. |

The sibling [`record`](crate::record) module owns the wire format, limits and CRC
contract. [`record_reader`](crate::record_reader) validates incoming encodings and
returns immutable records suitable for the existing-record mode.

## Usage

Choose serialization mode, append a payload, then explicitly send the batch:

```rust
use std::io::{self, Write};
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
let id = RecordId::new(StreamId::MIN, SequenceNumber::MIN);
writer.write(id, |body| {
    body.write_all(&42_u64.to_le_bytes())?;
    body.write_all(b"opaque application payload")?;
    Ok::<_, io::Error>(())
})?;
assert!(writer.get_ref().is_empty()); // The record is still in the writer's batch.

writer.flush_buffer().await?;
writer.flush().await?;
let encoded = writer.into_inner();
assert_eq!(encoded.len(), 16 + 8 + b"opaque application payload".len());
# Ok(())
# }
```

Several appends can precede one `flush_buffer`. Its success means the destination
accepted the bytes; destination flushing and file synchronization remain separate.

<details>
<summary>Design and maintenance notes</summary>

**Copying an existing record.** The record already contains its final header,
payload and CRC. Copying that encoding into the batch ends the source borrow
immediately, so the record can be released before any output begins:

```rust
use transaction_log_exports::{Record, RecordOutputError, RecordWriter};

async fn copy_record(record: Record) -> Result<Vec<u8>, RecordOutputError> {
    let mut writer = RecordWriter::for_records(Vec::<u8>::new());
    writer.write_record(&record)?; // Synchronous; no destination I/O.
    drop(record);
    writer.flush_buffer().await?;
    writer.flush().await?;
    Ok(writer.into_inner())
}
```

An existing record needs no construction buffer, but direct output to an async
destination would make each `write_record` asynchronous and retain its borrow
until output completes. The deliberate copy preserves synchronous appends,
releases source storage promptly and combines records into a caller-chosen batch.
Both modes therefore share the same explicit sending boundary.

**Synchronizing a file.** A Tokio file also exposes `sync_data`. To include all
pending records, send the writer's batch, flush the destination, then synchronize:

```rust
use std::io::Write;
use transaction_log_exports::{RecordId, RecordWriter, SequenceNumber, StreamId};

async fn write_file(file: tokio::fs::File) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RecordWriter::for_serialization(file);
    writer.write(RecordId::new(StreamId::MIN, SequenceNumber::MIN), |body| {
        body.write_all(b"file payload")
    })?;
    writer.flush_buffer().await?;
    writer.flush().await?;
    writer.sync_data().await?;
    Ok(())
}
```

Those cadences can differ: previously sent and flushed data can be synchronized
while newer records remain in the writer's batch. A successful sync covers the
destination's completed data-sync operation under its storage/platform guarantees;
it does not establish remote replication or necessarily synchronize all metadata.

</details>

## Behavior and guarantees

### Modes, ownership and batching

The constructor takes the destination by value and fixes the append API:

| Constructor | Writer type | Append method |
| --- | --- | --- |
| [`for_serialization(destination)`](RecordWriter::for_serialization) | `RecordWriter<W, SerializeRecords>` | `write(id, callback)` |
| [`for_records(destination)`](RecordWriter::for_records) | `RecordWriter<W, ExistingRecords>` | `write_record(&record)` |

Both modes preserve append order and buffer complete records without I/O. Output
requires Tokio `AsyncWrite + Unpin`. The destination is already initialized:
connection setup or file opening and positioning happen before ownership passes
to the writer. One writer can contain records from multiple streams; sequence
continuity and application payload validity belong to the caller.

Each writer initially reserves 65,535 bytes. Batches can grow beyond one record's
size limit, and capacity is retained after sending. There is no automatic send or
batch-size limit, so the caller's chosen batch boundaries control latency and
retained memory. Pending output exclusively borrows the writer; another append
must wait until that operation completes.

<details>
<summary>Design and maintenance notes</summary>

**Compile-time API separation.** The public mode markers let callers name a stored
writer's full type, while local variables infer it from the constructor. They
contain no data, add no storage and are never inspected. Mode-specific inherent
implementations expose the append methods; shared implementations own allocation,
output, failure tracking and destination access. Private fields and construction
prevent arbitrary modes. There is no default mode or conversion between modes.

The markers live beside the writer struct because their sole purpose is to
select its API. Keeping them together makes that relationship visible without
introducing a mode trait, runtime branch or virtual dispatch. The other mode's
append method is unavailable:

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

**Capacity and execution.** Serialization reserves room for one maximum additional
record before lending the builder. Even a small record can cause growth if less
than that amount remains. Existing-record mode appends only the known encoded
length. Allocation failure follows normal `BytesMut` behavior; it has no
recoverable writer error variant. Large batches can leave large allocations
resident because successful output clears length without trimming capacity.

Async output runs where its caller polls it. Runtime requirements come from the
destination; an in-memory destination can be used without a Tokio runtime. The
writer imposes no `Send` bound, and ordinary auto traits govern executor use.
Its exclusive ownership needs no internal lock, queue or worker. A higher layer
that continues producing while a batch is being sent needs its own storage and
handoff strategy, because this writer has one exclusively borrowed batch. Such a
wrapper and its Crossfire-versus-Tokio comparison remain deferred.

</details>

### Construction and existing records

`write` invokes a concrete callback exactly once on a usable writer. The callback
can make multiple `std::io::Write` calls into one payload; the writer derives
length and CRC after it succeeds. Payloads can be empty or contain up to 65,519
bytes, following the shared [`record` format](crate::record).

Construction errors and callback unwinding discard only the current record.
Previously buffered records remain available, and the writer remains usable.
Callback side effects outside the builder do not roll back.

`write_record` copies the complete immutable encoding, preserving its ID, length,
payload and CRC. It neither clones the record's byte handle, revalidates its
contents nor recalculates the checksum. Its borrow ends on return.

<details>
<summary>Design and maintenance notes</summary>

**Building before publication.** The encoded length comes before the payload,
but is unknown until serialization finishes. Reserving the complete record region
lets the builder finalize framing in place and abandon incomplete construction
without exposing any partial record to the destination:

1. Construction remembers the original buffer length, reserves 65,535 additional
   bytes, and captures a pointer into the full spare region. Header, payload and
   trailer storage are uninitialized; the readable length remains unchanged.
2. Each accepted body write initializes bytes after the reserved header and
   advances only the local payload count. The prefix of earlier records remains
   untouched, and no byte view into the new record escapes.
3. After callback success, finalization checks retained errors, derives total
   length, initializes the header, calculates CRC over header and body, and
   initializes the trailer. One `set_len` publishes the complete record.
4. Finalization consumes the builder. The record stays in the same allocation
   without a split, payload move or conversion into an owning `Record`. Dropping
   an unfinished builder restores the original length and preserves capacity.

Each callback gets a fresh builder, so one record's error cannot contaminate the
next. A successful append retains the batch; successful buffer output clears it
for reuse. Typed IDs already establish their raw-value validity. The writer uses
the supplied ID without advancing its sequence number or interpreting the payload.

**Error retention.** `write` and `write_all` accept the whole supplied slice or
return an error without changing the payload or count. Empty writes are valid,
including at the size limit, unless an earlier write failed. `write_vectored`
retains the trait's default behavior, so its returned count remains significant.

The first `PayloadTooLarge` error is retained and prevents later writes, builder
flushes and finalization from succeeding. `Write` wraps it in `io::Error` with
kind `InvalidInput`. A serializer that ignores or wraps a failed write cannot
cause publication of a silently truncated record: the retained build error takes
precedence over its callback result. The writer separately checks callback errors
that never reached the builder. `RecordBuilder::flush` only reports retained
errors; it cannot finalize framing or perform I/O.

| `RecordWriteError` variant | Meaning |
| --- | --- |
| `Build` | The builder rejected a payload write, even if the callback ignored the error. |
| `Serialize(E)` | The callback failed with its original concrete error. |
| `Output` | Earlier I/O made the writer unusable; the callback was not invoked. |

Unfinished construction remains outside the buffer's readable extent. Drop
normally needs no length change, but truncation enforces rollback during errors
and unwinding. Successful publication disarms rollback. Panics propagate; process
abort does not run destructor cleanup.

**Builder access.** A concrete, generic callback gives serializers static dispatch
through `Write` without an intermediate payload vector or a custom serialization
trait. Callers can name the builder type but cannot construct or finish one:

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

Its exclusive buffer borrow prevents concurrent construction or external byte
views. The cached `NonNull<u8>` makes the builder neither `Send` nor `Sync`; it is
consumed on the producer's thread before async output or another reservation.

</details>

### Completion and failures

| Operation | Completion guarantee |
| --- | --- |
| [`flush_buffer().await`](RecordWriter::flush_buffer) | The destination accepted every buffered byte. Clear the batch while retaining capacity; an empty batch performs no I/O. |
| [`flush().await`](RecordWriter::flush) | The destination's own flush completed. Records still in the writer's batch are untouched. |
| [`sync_data().await`](RecordWriter::sync_data) | A destination implementing `AsyncSyncData` completed data synchronization. This does not send or flush either layer's pending buffers. |
| [`get_ref()`](RecordWriter::get_ref) | Borrow the destination; operations through this reference bypass the writer's failure tracking. |
| [`into_inner()`](RecordWriter::into_inner) or drop | Discard unsent records without implicit sending, flushing, synchronization or shutdown. `into_inner` returns destination ownership. |

To synchronize all pending file output, call `flush_buffer`, `flush`, then
`sync_data`, in that order. Destination flush and sync still call the destination
when the writer's batch is empty. None of these methods establishes remote
acknowledgement or maintains a last-durable record ID.

**Failed, panicking or cancelled I/O makes the writer unusable.** A prefix may
already have been accepted, so retrying the batch could duplicate bytes. Later
appends and output operations return `Unusable`. Dropping an unpolled future has
no effect; retaining and polling the same pending future continues its operation.

<details>
<summary>Design and maintenance notes</summary>

**Partial output.** The send future advances a borrowed slice as bytes are
accepted. It retries `Interrupted` at the same position and treats zero progress
as `WriteZero`. The buffer retains the entire batch until every byte is accepted;
only the future holds the progress cursor. Successful output clears the buffer.

Before invoking destination I/O, the writer sets `usable` to false. It restores
the flag only on success. An error, unwind or cancellation therefore leaves a
terminal state without needing a drop guard. The exclusive mutable borrow
prevents other calls while the operation is pending. This flag grants permission
to continue; it does not certify receipt, an empty buffer or durability.

| Outcome | Retained state and consequence |
| --- | --- |
| Pending send polled again | Continue from its cursor, including across record boundaries. |
| Failed, panicking or cancelled send | Retain the whole batch, but forbid replay and further output. |
| Failed, panicking or cancelled flush or sync | Leave newer buffered records unchanged; destination completion is uncertain and further output is forbidden. |
| Any append, send, flush or sync after failure | Reject before invoking a callback or touching the destination, even for an empty batch. |

Cancelling after a pending poll forbids reuse even if no bytes have yet been
accepted. A replacement future would have lost the old cursor, and a destination
may continue internal I/O after its future is dropped. The writer provides no
reset or automatic recovery. The first I/O error is returned intact through
`RecordOutputError::Io`; subsequent calls return `Unusable` because only terminal
state, rather than a cloned error, is retained.

`into_inner` remains available after failure so the owner can close or recover
the destination. Extracting it or placing it in a fresh writer does not repair
partial framing or establish durability. Destination-specific operations through
`get_ref`, such as `sync_all`, require the caller to handle errors and preserve
ordering when shared references permit writing, seeking or handle cloning.

**Optional synchronization.** `AsyncSyncData` separates persistence capability
from byte acceptance. Its Tokio file implementation delegates to the inherent
`sync_data` method, preserving its scheduling and platform guarantees without
adding a destination flush. Metadata can be omitted, and some platforms may
provide the same behavior as `sync_all`.

A custom wrapper's implementation must await actual sync completion and return
its error, without implicitly draining that wrapper's pending write buffer. Work
is deferred until the future is polled. Merely scheduling a sync cannot establish
success. Simulated test destinations can establish ordering and error handling,
but cannot prove physical durability.

The trait takes exclusive mutable access so wrappers can track local state and
participate in the writer's serialized operation order. Its implementation-specific
future needs no boxing or virtual dispatch and has no `Send` bound. A native
`async fn` can implement it; generic callers cannot assume that future can be
spawned on another thread or that every implementation allocates nothing.
The trait itself has no shared failure state; the writer owns that policy.

Only Tokio files have a production implementation here. The capability is not
automatically forwarded through wrappers such as `BufWriter<File>`, and byte
buffers and sockets do not acquire it from `AsyncWrite`:

```compile_fail,E0599
use transaction_log_exports::RecordWriter;
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
let _ = writer.sync_data();
```

**Ordering log and index output.** The service's
[indexed log writer](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/indexed_log_writer/README.md)
combines existing-record mode with an index writer. It completes log sending and
destination flushing before sending the matching index entries, because a Tokio
file may accept bytes before its underlying write finishes. This prevents index
output from preceding the log data it describes. The pair's `sync_data` includes
pending output; this general writer's `sync_data` remains destination-only.

</details>

## Performance

Serialization writes payload chunks into their final batch storage, then
initializes the header and CRC in place. Existing-record mode copies each encoding
once into that batch. Both reuse capacity after sending; growth between records
can allocate and move buffered bytes. Destinations and the operating system may
perform further copies.

The [benchmark suite](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/benches/README.md)
measures serialization and existing-record copying separately. Its writer target
uses a raw byte drain; the combined target uses the production reader and validates
each record. These TCP workloads do not measure durable storage or application
response latency.

<details>
<summary>Design and maintenance notes</summary>

**Work per field and per record.** One upfront reservation per record establishes
stable storage for all body writes and finalization. No operation can grow, split
or relocate the allocation while the cached pointer is live. Growth happens before
the pointer is derived, so it may move earlier records safely. Sufficient spare
capacity avoids that allocation.

Each body write checks the retained error and compares its length with
`MAX_PAYLOAD_LEN - payload_len`, avoiding overflow on rejected input. It copies
directly to `record_start + HEADER_LEN + payload_len`, then advances the local
count. It performs no capacity lookup, buffer-length update, I/O or auxiliary
allocation. Error conversion may allocate on the failure path. The constructor
and write path are inline so generic callbacks expose these operations to the
optimizer; a serializer can still choose to erase its destination type internally.
Variable-sized writes retain bounds checks unless the compiler proves them
redundant.

**Pointer and initialization proof.** The pointer derives from the full spare
region and can address header, body and trailer. Reservation and exclusive access
keep the allocation live and stationary; no destination view escapes, so the
source slice cannot overlap it. Copies initialize bytes before advancing the
payload count. The buffer's readable length stays at its original value until
finalization.

The shared protocol helpers own offsets, unaligned little-endian stores, CRC
coverage and trailer placement. Header initialization establishes every header
byte. CRC reads only the initialized header and payload, then writes the trailer
after that shared view ends. Only then can `set_len` expose the entire record;
the pointer is never used after publication. The payload bound already proves
that adding the 16 framing bytes fits both `usize` and `u16`.

Moving the builder does not move its separate heap allocation. Preventing buffer
relocation during construction is the required guarantee; pinning the `BytesMut`
handle alone would not establish it.

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
Real command-handler transaction types are not yet available, so applicability
to those workloads remains unmeasured. These results support the choice for the
tested inputs without establishing a gain for every serializer or payload.
Local experimental sources, pinned dependencies, checks, and raw results are
under `target/record-builder-pointer-bench/`; those ignored artifacts may be
removed by a clean build.

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
build.

</details>

## Validation

From the workspace root, documentation examples and generated API links can be
checked with:

```powershell
cargo fmt --all --check
cargo test --workspace --doc --locked
cargo rustdoc -p transaction-log-exports --locked -- -D warnings
```

Behavioral checks cover construction, both append modes and destination failures:

```powershell
cargo test --workspace --locked
cargo test --workspace --release --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Independent encoded fixtures and CRC expectations protect the format; scripted
destinations exercise partial progress and cancellation deterministically.

<details>
<summary>Design and maintenance notes</summary>

**Construction and encoding.** Builder tests cover many small writes, prefix
preservation, insufficient initial capacity, stable storage, deferred publication,
empty/maximum payloads, atomic rejection, sticky errors and rollback. Golden
frames, independent CRC values, arbitrary byte offsets and guard bytes establish
encoding and initialization boundaries. Real-reader integration compares decoded
typed lengths with payload length plus an independently specified 16-byte overhead.

**Output and ownership.** Writer tests establish immediate callbacks without I/O,
owned non-Send sinks without a runtime, batches larger than a record, append order,
allocation reuse, empty output, discarded unsent records and destination recovery.
The critical failure cases are:

| Boundary | What the tests establish |
| --- | --- |
| Every incomplete byte prefix of an existing-record batch | I/O errors, zero progress, cancellation and panic retain the batch, reject later operations, and never replay bytes on extraction or drop, including at record boundaries and within CRC bytes. |
| Mode-specific failure rejection | Both modes preserve the batch and reject appends after zero progress or a partial I/O error; serialization callbacks are not invoked after terminal send, flush or sync failure. |
| Repeated `Interrupted` and `Pending` after progress | The same future resumes at its cursor across records and completes without an extra destination call. |
| Unpolled and cancelled operations | Unpolled futures are inert; cancellation before or after partial acceptance makes polled I/O terminal. |
| Destination flush lifecycle | Pending flushes can resume; errors, cancellation and panic preserve newer unsent records and forbid further output. |
| Actual `BufWriter` partial failure | Flushing downstream bytes neither sends the writer's newer batch nor replays the already accepted prefix. |
| Maximum existing records | Repeated copies exceed one record's limit in aggregate, preserve IDs including `u64::MAX`, and reuse capacity for a shorter later batch without leaking old bytes. |
| Callback failure after filling the maximum payload | Error and unwinding roll back after growth; shorter later records reuse spare storage without exposing abandoned bytes. |
| Separate synchronization | Sync performs no implicit sending or flushing, supports non-Send pending futures, preserves original errors and rejects use after prior I/O failure. |
| Failure after a successful sync | An earlier durable boundary does not mask later errors, cancellation or panic. |

Scripted destinations reject unexpected additional writes, so retry/cursor defects
fail instead of silently succeeding against an always-ready sink. File integration
exercises both modes and synchronization; handshake-complete socket integration
exercises serialization. Their bytes pass through the real reader. Temporary-file
tests establish API behavior, not resilience to power loss.

Compile-fail examples establish mode separation, builder privacy and synchronization
capability restrictions. The opt-in benchmarks measure performance separately
from these correctness checks; historical assembly observations describe their
specific compiler and workload rather than portable instruction-count guarantees.

</details>

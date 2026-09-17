# Indexed log writer

`IndexedLogWriter` owns the append lifecycle of one stream's log file and its
paired dense index. It composes `RecordWriter<_, ExistingRecords>` and
`IndexWriter`, enforcing stream/sequence continuity and coordinating their output.
Public callers continue to import `transaction_log::streams::IndexedLogWriter`.

## Source map

| Source | Responsibility |
| --- | --- |
| `indexed_log_writer.rs` | Pair ownership, trusted handover, ordered appends, output ordering, progress, finalization and same-file tests. |
| `indexed_log_write_error.rs` | Rejected record/finalization requests and original log/index output errors. |
| `indexed_log_writer_state.rs` | Internal open, unusable and finalized lifecycle states. |
| `mod.rs` | This Rustdoc specification, declarations and re-exports. |

Read the [shared stream contracts](../README.md#shared-contracts),
[storage specification](../../storage/README.md),
[record specification](../../../../transaction-log-exports/src/record/README.md) and
[record writer specification](../../../../transaction-log-exports/src/record_writer/README.md)
for the underlying formats and ownership contracts. The sibling
[index writer](../index_writer/README.md) owns offset output; the
[validator](../indexed_log_validator/README.md) establishes recovery handovers.

## Ownership and construction

`IndexedLogWriter<L, I = File>` owns two distinct, already-open destinations. The
log output uses `RecordWriter<L, ExistingRecords>` and the index uses
`IndexWriter<I>`. Each lower layer owns its reusable pending buffer; the pair
writer owns record identity, sequencing, progress and lifecycle state.
Both files stay at their permanent storage paths, with identical formats while
active and finalized. Finalization changes permission to append, not placement.

`new(file_id, log, index)` accepts an empty pair positioned at zero, with an owned
`tokio::fs::File` for the index. The caller establishes emptiness and exclusive
write ownership. Construction does no I/O. The default index type lets normal
callers name the pair writer as `IndexedLogWriter<File>`.
The application-internal `from_validated(file_id, log, index, end)` is the handover
point used by `IndexedLogValidator`. `None` means an empty pair. A supplied endpoint
must describe a contiguous validated prefix beginning with the file's first
assigned record, with exactly one correct index entry for each complete record.
Any invalid tail must already be repaired, and both handles must be positioned
at their logical append ends. A buffered reader's physical cursor may be ahead
of its last validated record; the handing-over layer must resolve that difference.
Prefix writes, including rebuilt index entries, must have completed destination
flushing before handover; no unflushed prefix bytes may remain in either output.

The writer derives the existing record count from the endpoint and file identity.
The endpoint's log position is reused; the index append position is the record
count times eight. The internal identity assertion detects an inconsistent
handover, not corruption. No public partial file validator or raw metadata
deserialization can establish a trusted pair. The validator establishes content
readiness; its caller establishes exclusive recovery/write ownership.

The handed-over prefix starts as buffered/flushed progress, with no pending
buffers. The internal constructor starts `synced_end` absent: validation alone
does not establish durability. The validator's public `into_writer` synchronizes
the pair before returning it, so that handover reports the accepted prefix as
synchronized. Direct `new` construction still starts empty.
Explicit synchronization can establish durability for that prefix without
appending another record. The owner may independently retain a trusted recovery
checkpoint; this writer does not overwrite or publish one.

Ordinary Rust exclusive borrows serialize operations. There is no worker, timer,
queue, mutex, `RefCell`, object pool or imposed `Send` bound. Async I/O runs wherever
the owner polls it. The destination determines its runtime requirements. During
an awaited operation, the mutable borrow prevents another append to this writer.

## Appending and memory use

`write_record(&Record)` is synchronous. It checks that the writer remains open,
that the file has capacity, and that the ID is precisely the expected stream and
sequence. New files start at their assigned first ID; resumed files require the
successor of their last record. Rejections leave buffers and progress untouched.
Stream domain, framing and CRC validity come from the immutable `Record`; they
are not checked again. Client disconnection after rejection is the caller's job.

Each successful append copies the complete encoding into the record batch, adds
its absolute exclusive log end as one little-endian `u64` index entry, and advances
the accepted count and `buffered_end`. It does not retain a `Record` or clone its
byte handle. The first record starts at zero. There is no file header, padding,
index header or explicit initial-zero entry.

Only the current batches remain in application memory. There is no payload
history or retained full-file index. After validation, the pair guards both
synchronous appends as one acceptance: an unexpected error or unwinding between
them leaves the pair unusable, so partial buffering cannot be followed by output.
Buffers grow as batches accumulate and retain their capacity after flushing. The
owner must choose suitable batch sizes; buffering a whole large file would consume
that much RAM, and an occasional oversized batch leaves capacity available for reuse.
There is no automatic output or capacity-trimming policy.

The record buffer initially reserves one maximum record through the existing
writer; index storage grows on demand. Dispatch is generic and there is no new
unsafe code. The copy is intentional to end the source borrow synchronously.
These are implementation properties, not measured disk throughput claims.
No special terminal-sequence file lifecycle is introduced in this step.

## Output ordering and progress

| API | Contract |
| --- | --- |
| `file_id()` / `record_count()` / `is_full()` | Read the identity and accepted record count, including buffered records. |
| `buffered_end()` | Last accepted endpoint; `None` for an empty file. |
| `flushed_end()` | Last endpoint whose complete log bytes and index entry have finished flushing. |
| `synced_end()` | Last endpoint covered by successful synchronization of both outputs. |
| `flush().await` | Drain and flush log data completely, then drain and flush its index entries. |
| `sync_data().await` | Include pending output through `flush`, then sync log data followed by index data. |
| `finalize()` | Require a full synchronized pair and permanently end further output; return its endpoint. |
| `into_inner()` | Extract destinations without implicit output, sync, finalization or repair. |

The critical flush sequence is:

1. Send the record buffer to the log destination.
2. Await the log destination's `flush()` until its writes have completed.
3. Send the index writer's buffer; it clears its batch after complete acceptance.
4. Await the index destination's `flush()`.
5. Advance the pair's `flushed_end` only after both writers finish successfully.

Do not run the log and index output concurrently. A Tokio file write can return
before its underlying write finishes; its destination flush must complete before
we expose any corresponding index bytes. A reader can otherwise discover an
offset for data that has not reached the file yet. `flush` combines these steps;
the exports `RecordWriter` deliberately leaves them separate for its general users.

The pair identifies a pending batch by comparing its accepted and flushed
endpoints. It does not inspect either lower writer's private buffer.

`sync_data` first completes that flush sequence if records are pending, then
synchronizes the log followed by the index. Only success of both advances
`synced_end`. This is not an atomic two-file transaction. The owner controls the
cadence; no one-second timer is built into the writer. Empty flushes perform no
I/O. Explicit sync still reaches both destinations with an empty pending batch.
Synchronization is available only when both destinations implement `AsyncSyncData`.

For example, the owner can supply newly created files and records already
validated by the reader. This helper deliberately leaves finalization to the
owner; a partial file is a valid writable prefix.

```no_run
use tokio::fs::File;
use transaction_log::{storage::LogFileId, streams::{IndexedLogWriteError, IndexedLogWriter}};
use transaction_log_exports::Record;

async fn append_batch(
    file_id: LogFileId,
    empty_log: File,
    empty_index: File,
    records: &[Record],
) -> Result<IndexedLogWriter<File>, IndexedLogWriteError> {
    let mut writer = IndexedLogWriter::new(file_id, empty_log, empty_index);
    for record in records {
        writer.write_record(record)?; // Synchronous; no I/O.
    }
    writer.flush().await?; // Log flush finishes before index output begins.
    writer.sync_data().await?; // Both files synchronized before success.
    Ok(writer)
}
```

Calling only `sync_data` would also drain the pending batch. The two calls above
show the distinct completion boundaries when the owner needs to observe each.

Both endpoints are inclusive in record identity and exclusive in byte position.
The getters copy metadata and do not grant permission to read arbitrary file tails.
A failed later operation preserves the last successful flushed/synced boundaries.
`synced_end` may lag `flushed_end`, which may lag `buffered_end`.

## Finalization and failures

Finalization requires exactly `RECORDS_PER_FILE` accepted records and synchronization
through the same endpoint. It performs no flush or sync itself: the owner requests
those first. Premature finalization returns `NotFull` or `NotSynchronized` without
changing state, so the owner can finish the work. Successful finalization returns
the final `RecordEndLocation`; future append/output/finalize calls return `Finalized`.
Closing the files and publishing finalized stream state remain owner responsibilities.

Before output begins, the pair is marked unusable and restored to open only on
success. This guards failure, panic and cancellation between destinations as well
as within them. Do not reset it merely because an inner record writer succeeded.
Unpolled futures do nothing; a pending future may continue to completion, but
dropping it leaves the writer unusable. Further operations return `Unusable`
without more destination calls. The original log/index error is returned intact
through the typed error's source chain; later calls do not replay that error or
try to resend an already accepted prefix.

A successful flush followed by failed sync may advance flushed progress while
leaving synchronized progress unchanged. A failed index output can leave complete
log records with a missing or partial index suffix. Even a zero-progress pending
operation is conservatively terminal if cancelled. Recovery must establish the
pair's actual state before it is handed to a new writer. Dropping or extracting
destinations discards unsent batches and never performs hidden I/O. An underlying
async destination may finish internal work after cancellation; recovery must
first coordinate those outstanding operations through its file-owning layer.

## Scope and owner responsibilities

The implemented writer handles one pair. It has no historical read API, payload
history, live subscription delivery, checkpoint publication, flush timer or file
rotation. The [stream overview](../README.md#historical-reads-and-live-delivery)
describes how future request owners use independent handles and choose flushed
or durable read boundaries. Multi-file startup and lifecycle coordination remain
future work.

## Verification

Pair tests live in `indexed_log_writer.rs`. They use literal expected index bytes,
production-validated input records, observable destinations with delayed flushes,
short writes, failures and cancellation, and actual temporary file pairs. Check
record identity and file bounds, resumption, unchanged bytes, separate progress,
strict log-before-index ordering and explicit finalization. Buffer internals are
tested in their owning layer, not exposed for the pair's tests.
The temporary-file test reads through independent handles after flush and before
any explicit durable sync; it establishes visibility, not absence of incidental
OS writeback. No test should rely on a disk actually remaining unsynchronized.

Follow the [module verification guidance](../README.md#verification-and-performance).
This component has no disk-performance measurements yet. Future benchmarks should
separate buffered appends, cached file output and durable synchronization.

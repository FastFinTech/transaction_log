# Indexed log writer

`IndexedLogWriter` owns the append lifecycle of one stream's log file and its
paired dense index. It composes `RecordWriter<_, ExistingRecords>` and
`IndexWriter`, enforcing stream/sequence continuity and coordinating their output.
Public callers continue to import `transaction_log::streams::IndexedLogWriter`.

## Source map

| Source | Responsibility |
| --- | --- |
| `indexed_log_writer.rs` | Pair ownership, trusted handover, ordered appends, output ordering, progress, finalization and same-file tests. |
| `append_outcome.rs` | Read-only snapshot of an accepted record's endpoint and whether that append filled the file. |
| `indexed_log_write_error.rs` | Rejected record/finalization requests and original log/index output errors. |
| `indexed_log_writer_state.rs` | Internal open, unusable and finalized lifecycle states. |
| `mod.rs` | This Rustdoc specification, declarations and re-exports. |

Read the [shared stream contracts](../README.md#shared-contracts),
[storage specification](../../storage/README.md),
[stream location specification](../location/README.md),
[record specification](../../../../transaction-log-exports/src/record/README.md) and
[record writer specification](../../../../transaction-log-exports/src/record_writer/README.md)
for the underlying formats and ownership contracts. The sibling
[index writer](../index_writer/README.md) owns offset output; the
[stream initializer](../stream_initializer/README.md) supplies the active pair
after recovery and checkpoint publication.

## Ownership and construction

`IndexedLogWriter<L, I = File>` owns two distinct, already-open destinations. The
log output uses `RecordWriter<L, ExistingRecords>` and the index uses
`IndexWriter<I>`. Each lower layer owns its reusable pending buffer; the pair
writer owns record identity, sequencing, progress and lifecycle state.
Both files stay at their permanent storage paths, with identical formats while
active and finalized. Finalization changes permission to append, not placement.

The writer caches `expected_record_id`, initially the file's first assigned ID
for an empty pair or the recovered end's successor. It uses that ID for the
existing stream/sequence comparison and advances it only after both buffers
accept the record. Rejected appends and flush/sync/finalize operations leave it
unchanged. After the last assigned record it points into the successor file;
the full-file guard still rejects another append to this writer.

The existing `RecordStartLocation` calculation remains for the byte offset:
`RecordEndLocation::next_record_start()` for a buffered prefix, or the file's
first ID at position zero for an empty pair. This retains the helper's general
rollover calculation even though the full-file guard prevents crossing files.
The cached ID is additional state, not evidence of a performance improvement.
Checked ID advancement retains the existing panic policy at sequence exhaustion.

After identity validation, `start.to_end(record.length())` computes one typed
accepted endpoint. Its position supplies the index entry, and the same endpoint
becomes buffered progress and the append outcome only after both buffers accept
the record. The writer does not maintain a separate local offset or reconstruct
the endpoint afterward. `to_next()` is unnecessary here: the writer needs the
accepted end and cached successor ID, not a successor file's byte position.

`new(file_id, log, index)` accepts an empty pair positioned at zero, with an owned
`tokio::fs::File` for the index. The caller establishes emptiness and exclusive
write ownership. Construction does no I/O. The default index type lets normal
callers name the pair writer as `IndexedLogWriter<File>`.
`From<InitializedStream> for IndexedLogWriter<File>` consumes the initializer's
result to resume its active pair. The result has no public unchecked constructor:
recovery establishes a contiguous valid log prefix, exactly matching index entries,
tail repair, synchronization of both files and their logical append positions.
The initializer also cleans up later files and publishes recovered progress before
returning the active pair. The owner preserves exclusive write access through
handover; borrowed file getters are for observation, not mutation of that prefix.

Conversion moves both handles without reopening, seeking, revalidating or I/O.
The writer destructures `InitializedStream` by field name, so ownership transfer
does not depend on tuple order. Its fields are visible only within `streams`;
components constructing it must establish the documented initialization guarantees.
The writer derives
the file-local record count using `LogFileId::record_count_through`, and initializes
empty output buffers. No raw endpoint constructor is needed for resumption.

All three endpoints (`buffered_end`, `flushed_end`, `synced_end`) start at the
recovered file-local end, preserving the synchronization already performed by
recovery. All three are `None` for an empty pair, including an empty successor to
a completed file. That earlier file's checkpoint remains stream-wide metadata;
it is not progress within the active writer. Direct `new` construction also starts
empty. Fresh empty files receive no initial synchronization. Subsequent appends
advance buffered progress, flush advances readable progress, and successful
explicit synchronization advances durable progress. This writer never publishes
or overwrites a checkpoint.

```no_run
use tokio::fs::File;
use transaction_log::{
    storage::StorageProvider,
    streams::{IndexedLogWriter, StreamInitializer},
};
use transaction_log_exports::StreamId;

async fn resume(
    stream: StreamId,
    storage: &StorageProvider,
) -> anyhow::Result<IndexedLogWriter<File>> {
    let initialized = StreamInitializer::initialize(stream, storage).await?;
    Ok(IndexedLogWriter::from(initialized))
}
```

Ordinary Rust exclusive borrows serialize operations. There is no worker, timer,
queue, mutex, `RefCell`, object pool or imposed `Send` bound. Async I/O runs wherever
the owner polls it. The destination determines its runtime requirements. During
an awaited operation, the mutable borrow prevents another append to this writer.

## Appending and memory use

`write_record(&Record) -> Result<AppendOutcome, IndexedLogWriteError>` is synchronous.
It checks that the writer remains open,
that the file has capacity, and that the ID is precisely the expected stream and
sequence. New files start at their assigned first ID; resumed files require the
successor of their last record. Rejections leave buffers and progress untouched.
Stream domain, framing and CRC validity come from the immutable `Record`; they
are not checked again. Client disconnection after rejection is the caller's job.

Each successful append copies the complete encoding into the record batch, adds
its absolute exclusive log end as one little-endian `u64` index entry, and advances
the accepted count, `buffered_end` and `expected_record_id`. It does not retain a `Record` or clone its
byte handle. The first record starts at zero. There is no file header, padding,
index header or explicit initial-zero entry.

`AppendOutcome` is a small `Copy` snapshot with private fields and `getset` copy
getters. `end()` returns the accepted record and its exclusive file-local byte
end. `file_full()` is true exactly when that successful append filled the last
assigned slot. The final record was accepted; it was not rejected as `Full`.
A subsequent append is still rejected. The owner can use the result to arrange
synchronization, finalization and rollover without fetching progress separately.
An outcome does not certify flushing, synchronization, checkpoint publication or
permission to read the buffered bytes through another handle. Later appends and
output operations do not change a previously returned snapshot. Errors produce
no outcome and do not advance the expected ID or accepted progress; a failed
paired append still makes the writer unusable even if one buffer changed.

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

### Available optimizations (deferred)

The full defensive contract is deliberately retained: stream/sequence checks,
file-capacity and lifecycle checks, the general next-start calculation, lower
writer usability checks and protection against a partially accepted pair.
The expected-ID cache and append outcome do not introduce a reduced-contract
entry point, remove those checks or establish a measured speedup.

The planned live stream owner will verify stream routing and sequence order
before enqueueing records. Validation and admission must be serialized per stream.
A worker will own the indexed writer and process records, flush commands and
sync commands from the MPSC queue in order, completing a command's operation
before processing the next. This owner and worker are not implemented yet; the
writer currently verifies ordering itself.

Optimization candidates include removing duplicate admission checks once the
implemented upper layer establishes and tests those guarantees, simplifying
the rollover-aware offset calculation under the existing full-file guard, and
examining repeated usability checks across the pair and lower writers. The pair's
error, panic and cancellation guarantees must remain accounted for by whichever
layer owns the operation. A queue's ordering guarantee alone does not establish
file health or make partial writes safe to reuse.

Pursue these changes when benchmarking justifies them, or when the implemented
and tested upper layer permits an explicitly reduced contract. Moving an identical
check to the caller is not itself a reduction in total work. Inspect optimized
code and measure before claiming a performance gain: Rust expression count and
derivable-versus-cached state alone do not establish machine cost. The existing
record I/O benchmarks do not measure this paired append path, and no indexed-writer
performance result is claimed here. Keep these future decisions separate from
the current checked API.

## Output ordering and progress

| API | Contract |
| --- | --- |
| `file_id()` / `record_count()` / `is_full()` | Read the identity and accepted record count, including buffered records. |
| `expected_record_id()` | Read the next ID checked on append; this does not override full, finalized or unusable state. |
| `write_record(&Record)` | Buffer one checked record and its index entry; return `AppendOutcome` with `end()` and `file_full()`. |
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
use transaction_log::streams::{LogFileId, IndexedLogWriteError, IndexedLogWriter};
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

### Preparing the next pair ahead of rollover (planned)

The future live-stream owner can create and open the immediate successor pair
while the current pair is still accepting records. Holding that empty pair ready
would move directory creation and file opening ahead of the rollover boundary,
so the next file's first record need not wait for those operations. Preparation
does not activate the successor: the current pair must still be completed,
synchronized and finalized before writes switch to it.

This belongs to the owner coordinating multiple pairs, using the storage provider's
`create_log_and_index`, rather than to this single-pair writer. After a restart,
startup recovery can retain an empty successor following a complete prefix, or
discard it when an earlier partial file or gap ends that prefix. The scheduling,
failure handling and handover are future work; no background preparation is
implemented, and its latency benefit has not been measured.

## Verification

Pair tests live in `indexed_log_writer.rs`. They use literal expected index bytes,
production-validated input records, observable destinations with delayed flushes,
short writes, failures and cancellation, and actual file pairs in the shared
storage fixture. Check
record identity and file bounds, resumption, unchanged bytes, separate progress,
strict log-before-index ordering and explicit finalization. Buffer internals are
tested in their owning layer, not exposed for the pair's tests.
They also check exact append outcomes, the last-slot/full distinction, preserved
outcome snapshots, expected-ID initialization/advancement and no advancement on
identity/capacity/lifecycle rejection or either lower writer refusing an append.
The independent-reader test reads through separate handles after flush and before
any explicit durable sync; it establishes visibility, not absence of incidental
OS writeback. No test should rely on a disk actually remaining unsynchronized.

Handover tests use the real `StreamInitializer`: empty and entirely corrupt files,
sequence zero, repaired tails after a checkpoint, a nonzero file's local count,
and an empty successor whose checkpoint belongs to the previous completed file.
They append after conversion and check exact bytes, index offsets, cursors and
distinct buffered/flushed/synchronized progress. Each claims a unique stream from
the shared fixture and retains its cleanup guard until all files close. Generic
fault tests construct their private test writers directly; a separate arithmetic
test seeds large-offset progress without allocating a multi-gigabyte prefix.

Follow the [module verification guidance](../README.md#verification-and-performance).
This component has no disk-performance measurements yet. Future benchmarks should
separate buffered appends, cached file output and durable synchronization.

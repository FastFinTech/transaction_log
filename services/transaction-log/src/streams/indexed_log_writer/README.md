# Indexed log writer

Append validated records and their dense index entries to one stream's log/index
pair. The writer checks record order, completes log output before index output,
and reports buffered, flushed and synchronized progress separately.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`IndexedLogWriter<L, I = File>`](IndexedLogWriter) | Own one pair, accept ordered records, coordinate output and finalize a full synchronized file. |
| [`AppendOutcome`] | Snapshot the accepted record's endpoint and whether that append filled the file. |
| [`IndexedLogWriteError`] | Distinguish rejected appends/finalization from terminal log or index failures. |

The [`record writer`](transaction_log_exports::record_writer) owns record
buffering and output; the [`index writer`](crate::streams::index_writer) owns the
dense index format and offset output. The
[`location`](crate::streams::location) module supplies file identities and
endpoints, and [`StreamInitializer`](crate::streams::StreamInitializer) supplies
an active pair after recovery.

## Usage

For a new pair, supply distinct empty files positioned at zero and records
starting at the file's first assigned ID. Appends are synchronous; output is
explicit:

```no_run
use tokio::fs::File;
use transaction_log::streams::{IndexedLogWriteError, IndexedLogWriter, LogFileId};
use transaction_log_exports::Record;

async fn append_batch(
    file_id: LogFileId,
    empty_log: File,
    empty_index: File,
    records: &[Record],
) -> Result<IndexedLogWriter<File>, IndexedLogWriteError> {
    let mut writer = IndexedLogWriter::new(file_id, empty_log, empty_index);
    for record in records {
        writer.write_record(record)?; // Buffer the record and its index entry.
    }
    writer.flush().await?;     // Both files readable through the accepted end.
    writer.sync_data().await?; // Both files synchronized through that end.
    Ok(writer)
}
```

The supplied batch must fit this file's assigned range. The helper returns the
writer so its owner can continue appending or finalize it when full. Calling only
`sync_data` would also drain pending output; separate calls expose the two
completion boundaries when needed.

<details>
<summary>Design and maintenance notes</summary>

**Resuming recovered files.** The initializer establishes and synchronizes the
existing prefix before returning its active pair. Consuming that result preserves
its progress and transfers the already-positioned handles:

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

A direct `new` call represents an empty pair; it cannot reconstruct progress from
existing bytes. The initialized result carries that established state without
requiring a public constructor that accepts an unchecked endpoint.

</details>

## Behavior and guarantees

### Ownership and construction

`new(file_id, log, index)` takes exclusive write ownership of two distinct empty
destinations positioned at zero. The public constructor accepts an owned Tokio
index file; `IndexedLogWriter<File>` names the ordinary two-file writer. Read-only
handles may coexist. Construction performs no file inspection or I/O.

`IndexedLogWriter::from(initialized)` consumes an
[`InitializedStream`](crate::streams::InitializedStream), moving its recovered
files without reopening, seeking or revalidating. All progress starts at that
active file's recovered endpoint, or `None` when it is empty. The caller preserves
exclusive write ownership and the established file positions through handover.

<details>
<summary>Design and maintenance notes</summary>

**Dividing ownership.** `RecordWriter<L, ExistingRecords>` and `IndexWriter<I>` each
own their destination and reusable pending buffer. The pair owns stream/sequence
acceptance, progress and lifecycle state. The default index type is `File`;
internal generic construction supports scripted destinations for deterministic
failure tests. Concrete dispatch and exclusive mutable borrows need no internal
worker, timer, queue, lock or imposed `Send` bound. Async I/O runs where the owner
polls it, with runtime requirements supplied by the destinations.

**Trusted handover.** Recovery establishes a contiguous valid log prefix, exactly
matching index entries, repaired tails, synchronization of both files and their
append positions. The initializer also cleans later files and publishes recovered
progress before returning the active pair. Its result has no public unchecked
constructor; its fields are visible within `streams` for named ownership transfer.
Components producing that result must establish the same guarantees. Borrowed
file getters permit observation without authorizing mutation of the prefix or
cursors before transfer.

Conversion destructures the result by field name, derives its file-local count
with `LogFileId::record_count_through`, and creates empty pending buffers. It sets
`buffered_end`, `flushed_end` and `synced_end` to the recovered end, retaining the
durability already established by recovery. If the active pair is an empty
successor, all three are `None` even when the stream checkpoint covers the previous
completed file. That checkpoint is stream-wide metadata, not progress in this
writer. Fresh empty files receive no initial synchronization. The writer itself
never publishes or overwrites a checkpoint.

</details>

### Appending records

[`write_record(&record)`](IndexedLogWriter::write_record) checks that the writer
is open, has capacity, and received exactly the expected stream and sequence.
A fresh pair expects its file's first assigned ID; a recovered pair expects the
successor of its last record. Identity and capacity rejections leave the buffers
and progress unchanged. Framing and CRC validity come from the immutable
[`Record`](transaction_log_exports::Record).

Success copies the complete encoding into the log batch, buffers its exclusive
byte end as an index entry, and returns an [`AppendOutcome`]:

| Outcome field | Meaning |
| --- | --- |
| `end()` | The accepted record and its exclusive byte end in this log file. |
| `file_full()` | This append accepted the last assigned record. A later append returns `Full`. |

The outcome reports buffering only. It does not certify visibility, durability or
checkpoint publication. It is a read-only `Copy` snapshot that remains unchanged
by later operations. The source record's borrow ends immediately after the append.

<details>
<summary>Design and maintenance notes</summary>

**One endpoint for both outputs.** For an existing buffered prefix, the previous
end's `next_record_start()` supplies the next byte start. For an empty pair, the
first assigned ID begins at `LogFilePosition::START`. After identity validation,
`start.to_end(record.length())` computes the accepted endpoint once. Its typed
position supplies the index entry; the same endpoint becomes buffered progress
and the outcome after both appends succeed. Only the index encoder extracts a
raw `u64` for the existing eight-byte format. There is no second offset calculation
or stored raw position that could disagree.

The general next-start helper retains its rollover calculation even though the
full-file guard prevents this writer from crossing files. The separately cached
`expected_record_id` starts at the assigned first ID or recovered successor and
advances only after both buffers accept the record. Rejections and output or
finalization operations leave it unchanged. After the last accepted slot it points
into the next file, while the full guard rejects further appends here. Checked
successors retain the repository's panic policy at operationally unreachable
sequence exhaustion.

**Paired acceptance.** The record copy and index append form one logical
acceptance. After validation and endpoint calculation, the pair marks itself
unusable before calling either lower writer. Only success of both restores the
open state and advances the count, expected ID and buffered endpoint. An error or
unwind between the two appends cannot leave a partly buffered pair available for
output. Such errors produce no outcome or accepted-progress advancement, even if
one buffer changed.

The lower record writer copies encoded bytes without retaining a `Record`, cloning
its byte handle, reserializing the payload or repeating CRC checks. The index adds
one exclusive end per record. The shared formats contain no file header, padding,
index header or initial-zero entry.

</details>

### Output ordering and progress

| API | Contract |
| --- | --- |
| `file_id()` / `record_count()` / `is_full()` | Identify the pair and count accepted records, including buffered appends. |
| `expected_record_id()` | Report the next checked ID; it does not override full, finalized or unusable state. |
| `buffered_end()` | Last accepted endpoint, including records still buffered locally. |
| `flushed_end()` | Last endpoint whose complete log bytes and index entry have finished flushing. |
| `synced_end()` | Last endpoint covered by successful synchronization of both outputs. |
| [`flush().await`](IndexedLogWriter::flush) | Send and flush the log completely, then send and flush the index. |
| [`sync_data().await`](IndexedLogWriter::sync_data) | Include pending output through `flush`, then synchronize log data followed by index data. |
| [`finalize()`](IndexedLogWriter::finalize) | Require a full synchronized pair, permanently end output, and return the final endpoint. |
| [`into_inner()`](IndexedLogWriter::into_inner) | Return both destinations and discard pending buffers without implicit output or repair. |

The three endpoints are file-local and identify an included record plus its
exclusive byte end. `synced_end` may lag `flushed_end`, which may lag `buffered_end`.
Empty pairs have no endpoint. Empty flushes perform no I/O; explicit sync still
reaches both destinations. Synchronization requires both destinations to implement
[`AsyncSyncData`](transaction_log_exports::AsyncSyncData).

Progress advances only when its corresponding operation completes. A failed later
operation preserves the last successfully flushed and synchronized boundaries.
These values inform the owner's publication policy; they do not authorize reads
from arbitrary file tails or constitute an atomic two-file transaction.

<details>
<summary>Design and maintenance notes</summary>

**Why the log must finish first.** The flush sequence is:

1. Send the record buffer to the log destination.
2. Await the log destination's flush until its writes finish.
3. Send the index buffer to its destination.
4. Await the index destination's flush.
5. Advance `flushed_end` only after both outputs finish successfully.

A Tokio file can accept bytes before its underlying write completes. Starting
index output before the log flush finishes could expose an offset for unavailable
log data. The pair therefore orders the destinations instead of running them
concurrently. It detects pending output by comparing accepted and flushed endpoints
without inspecting the lower writers' private buffers. Its `flush` coordinates the
steps that the reusable record and index writers expose separately.

`sync_data` first completes this sequence if needed, then synchronizes the log
before the index. Only success of both advances `synced_end`. A successful flush
followed by failed sync can therefore leave `flushed_end` ahead of `synced_end`.
The owner chooses the cadence; no timer drives either operation.

**Read visibility.** Separate read handles can observe completed writes through
the OS cache before an explicit sync. The request owner chooses a published flushed
or durable boundary and accounts for preceding files and startup readiness.
Concurrent index readers must handle partial trailing entries and stay within that
boundary rather than treating raw index length as certification. The
[stream read contract](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#historical-reads-and-live-delivery)
owns those serving decisions; historical read APIs remain planned.

</details>

### Finalization and failures

Finalization requires exactly [`RECORDS_PER_FILE`](crate::streams::RECORDS_PER_FILE)
accepted records and synchronization through that same endpoint. It performs no
I/O. `NotFull` and `NotSynchronized` leave the writer open so its owner can finish
the work. Success returns the final endpoint; later append, flush, sync and
finalization calls return `Finalized`.

**I/O errors, panics and cancellation make the whole pair unusable.** The original
log or index error remains in the typed source chain; later operations return
`Unusable` without destination calls. Dropping an unpolled future does nothing.
A pending future can resume, but cancellation after I/O begins forbids reuse.
Dropping or extracting the writer discards unsent batches without sending,
synchronizing or finalizing them.

<details>
<summary>Design and maintenance notes</summary>

**Permission to continue.** Internal state distinguishes open, unusable and
finalized writers. The pair arms its unusable state before I/O and restores it
only after complete success, covering failures between destinations that either
lower writer alone cannot observe. One lower writer's success cannot restore
permission to use a partly processed pair. Even cancellation after a zero-progress
pending poll is conservatively terminal.

| Rejection or failure | Effect |
| --- | --- |
| `UnexpectedRecordId` / `Full` | Reject before buffering; retain the expected ID and all progress. |
| `NotFull` / `NotSynchronized` | Leave finalization pending and the writer open. |
| `Log` / `Index` | Preserve the concrete lower error and forbid further work after incomplete paired acceptance or output. |
| `Unusable` / `Finalized` | Reject append, output and finalization without further destination operations. |

A failed index write may leave complete log records and an incomplete index
suffix. Retrying could replay an accepted prefix. Recovery must establish the
actual pair state before constructing a new writer; extracting its handles does
not repair it. An async destination may continue internal work after cancellation,
so recovery first coordinates outstanding operations through the file-owning layer.

**Finishing a file.** Active and finalized pairs keep identical formats and
permanent storage paths. Finalization changes permission to append; it does not
rename files, close handles or publish finalized stream state. Those operations
remain with the owner coordinating the stream.

### Preparing the next pair ahead of rollover (planned)

A future live-stream owner can create and open the immediate successor while the
current pair still accepts records. Holding it ready would move directory creation
and file opening before the rollover boundary. Preparation does not activate it:
the current pair must still be completed, synchronized and finalized before writes
switch to the successor.

This uses the storage provider's `create_log_and_index` at the layer coordinating
multiple pairs. After restart, per-stream recovery can retain an empty successor
following a complete prefix or discard it when an earlier partial file or gap
ends that prefix. Live preparation, scheduling, failure handling and handover
remain unimplemented, and any latency benefit is unmeasured. Per-stream startup
recovery is implemented; application startup integration remains separate work.

</details>

## Performance

Each append copies the existing record encoding once and adds eight index bytes.
The record buffer initially reserves one maximum record, while index storage grows
on demand. Only pending batches remain in memory; the writer retains no record
history or full-file index after output. Both buffers grow with the chosen batch
and retain capacity after flushing, without automatic sending or trimming.

The copy releases the source borrow synchronously. The pair composes its lower
writers through generic dispatch and contains no unsafe code. The mutable borrow
prevents another append while output is pending. These are implementation
properties; the existing record-I/O benchmarks
do not measure this paired append path, and no disk-throughput result is claimed.

<details>
<summary>Design and maintenance notes</summary>

### Available optimizations (deferred)

The checked contract retains stream/sequence, file-capacity and lifecycle checks,
the general next-start calculation, lower writer usability checks, and protection
against partial paired acceptance. The expected-ID cache and append outcome do not
create a reduced-contract API or establish a measured speedup.

The planned live stream owner will verify routing and sequence order before
enqueueing records, with validation and admission serialized per stream. A worker
will own the writer and process records, flush commands and sync commands from an
MPSC queue in order, completing each operation before the next. That owner and
worker are not implemented; this writer currently verifies ordering itself.

Potential changes include removing duplicate admission checks after an implemented
upper layer establishes those guarantees, simplifying rollover-aware offset
calculation under the full guard, and examining repeated usability checks across
the pair and lower writers. Error, panic and cancellation guarantees still require
an owner: queue ordering alone proves neither file health nor safe reuse after
partial writes.

Those changes need workload measurements or an explicitly established narrower
contract. Moving an identical check to a caller does not reduce total work, and
cached state or fewer Rust expressions do not by themselves imply faster code.
Future measurements need to distinguish buffered appends, cached file output and
durable synchronization.

</details>

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::indexed_log_writer
cargo test -p transaction-log --release --locked streams::indexed_log_writer
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The service is a binary crate, so ordinary Cargo tests skip documentation examples.
The [stream documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
describes compiling them with `rustdoc --test` against a temporary library build.

<details>
<summary>Design and maintenance notes</summary>

Tests use production-validated records, independent literal index bytes, scripted
destinations with delayed flushes, and real file pairs. Observable progress and
bytes establish the pair contract without exposing lower writers' private buffers.

| Coverage | What it establishes |
| --- | --- |
| Accepted records and outcomes | Exact encoded bytes, index ends, accepted count, immutable outcome snapshots and the distinction between accepting the last slot and rejecting a later append. |
| Expected IDs and rejections | Correct initialization/advancement; no advancement on identity, capacity or lifecycle rejection, or either lower writer refusing an append. |
| Output ordering and progress | Log flush finishes before index output; buffered, flushed and synchronized endpoints advance separately. |
| All six I/O stages | Errors, pending/resumed work, cancellation and panic preserve successful progress and forbid replay after failure. |
| Either byte output | Zero progress is terminal instead of permitting a stalled or repeated write. |
| Empty and unpolled operations | Empty flushes avoid I/O; explicit sync reaches both destinations; unpolled futures are inert. |
| Finalization | Exactly 100,000 records and a synchronized matching endpoint are required; premature rejection leaves the writer open and success ends output. |
| Independent read handles | Flushed bytes are visible before an explicit sync, without assuming the OS has not already written them back. |
| Initializer handover | Empty or entirely corrupt files, sequence zero, repaired checkpoint tails, a nonzero file's local count and an empty successor preserve the correct recovered progress. |
| Large offsets | Seeded metadata establishes full-width append arithmetic without allocating a multi-gigabyte prefix. |

Handover tests run the real initializer and storage fixture, then append and check
exact bytes, offsets, cursors and progress. Each owns a unique stream and retains
its cleanup guard until handles close. Generic fault tests construct private test
writers directly. These tests distinguish visibility from requested durable sync;
they do not depend on a disk actually remaining unsynchronized or measure disk
throughput.

</details>

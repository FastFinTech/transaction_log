# Index writer

`IndexWriter` owns a file and a reusable buffer of dense index entries. It supplies
the shared output mechanism for normal indexed appends and recovery index repair;
record identity, valid offsets and publication policy belong to its caller.
Public callers continue to import `transaction_log::streams::IndexWriter`.

## Source map

| Source | Responsibility |
| --- | --- |
| `index_writer.rs` | File ownership, synchronous offset encoding, reusable buffering, separate output/flush/sync operations, the shared entry-width constant and same-file tests. |
| `index_write_error.rs` | Original file I/O failures and rejection after incomplete output. |
| `mod.rs` | This Rustdoc specification, declarations and re-exports. |

Read the [shared stream contracts](../README.md#shared-contracts) and
[dense index layout](#dense-index-layout) below for the file format and the
[storage specification](../../storage/README.md) for physical paths. The
[indexed log writer](../indexed_log_writer/README.md) and
[validator](../indexed_log_validator/README.md) establish valid entries. The
[record writer specification](../../../../transaction-log-exports/src/record_writer/README.md)
explains the completion boundaries this component follows.

## Ownership and operations

`IndexWriter::new(file: tokio::fs::File)` takes an already-open file by value.
The caller establishes an empty or validated/repaired prefix, chooses opening
mode, positions the file for appending, and ensures previous prefix writes have
finished flushing. This layer neither opens nor seeks nor validates the file.
The validator uses it for index rebuilding, supplying offsets and controlling
publication through the pair's recovery lifecycle.

`IndexWriter<F = File>` has a file-only public constructor. Its internal generic
construction allows scripted file operations in tests, including partial writes,
pending flushes and failing synchronization. This preserves static dispatch
without a file trait, boxed destination or runtime mode. It does not offer a
public constructor for sockets or arbitrary streams:

```compile_fail,E0308
use transaction_log::streams::IndexWriter;
fn index_to_stream(stream: tokio::io::DuplexStream) {
    let _ = IndexWriter::new(stream);
}
```

The API follows `RecordWriter`'s separate completion boundaries:

| Method | Contract |
| --- | --- |
| `new(file)` | Own the file without I/O or an initial buffer allocation. |
| `write(end_position: u64)` | Synchronously append eight little-endian bytes; no file I/O. |
| `flush_buffer().await` | Write the complete buffered batch, then clear it while retaining capacity. |
| `flush().await` | Complete the file's own pending writes; do not send entries still in the local buffer. |
| `sync_data().await` | Synchronize previously flushed file data; do not implicitly send or flush entries. |
| `get_ref()` | Borrow the file; file-specific operations bypass this writer's failure tracking. |
| `into_inner()` | Return file ownership and discard unsent entries without implicit I/O. |

There is no index header, initial-zero entry, record ID or collection length in
this encoding. The supplied integer is an absolute exclusive log offset. The
writer preserves it exactly; it cannot establish agreement with records without
their identities and log contents. `IndexedLogWriter` determines correct offsets
and enforces sequence/count policy. No partial validation of raw offsets or copy
of that policy belongs in this lower layer.

One `Vec<u8>` holds only pending entries. It grows on demand and is reused after
output. There is no full-file allocation per open stream, retained index history,
per-entry allocation, zero-filling, unsafe byte reinterpretation or mandatory
buffer handoff. The eight-byte little-endian encoding is explicit and portable.
Batch size and retained capacity remain owner decisions, as with record output.

Empty `flush_buffer` calls perform no I/O. Explicit `flush` and `sync_data` always
reach the file, even with no pending entries. Sending tolerates short writes and
retries `Interrupted` without advancing the cursor; zero progress is `WriteZero`.
Every I/O operation marks the index writer unusable until success. Failed,
panicking or cancelled output must not be retried: an accepted prefix may end
inside an eight-byte entry. Unpolled futures have no effects, and a pending future
can resume from its cursor. `IndexWriteError::Io` preserves the original source;
later appends or output return `Unusable` without file operations. Dropping or
extracting the writer never implicitly sends the remaining batch or repairs it.

To include buffered entries durably, call `flush_buffer`, `flush`, then `sync_data`.
For a paired log/index, the log must finish flushing **before** index buffer output
begins. `IndexedLogWriter` owns that coordination and separately guards the whole
pair, since one lower writer cannot observe failure in the other.

## Scope

This component is implemented for file output only. It does not open files,
validate offsets, retain a queryable index, publish checkpoints or schedule
output. Historical index lookup and multi-file lifecycle coordination remain
future application work; no speculative APIs for them belong in this layer.

## Verification

`index_writer.rs` tests independent literal encodings, full-width offsets,
allocation reuse, operation separation, empty output and inert unpolled futures.
Scripted files cover short writes, repeated interruption, resumable pending I/O,
zero progress, original errors, panic and cancellation at every incomplete byte
prefix of two entries, including their boundary. They verify rejection without
replay after failure and preserve newer buffered entries across file-only flush
and sync. Real-file tests cover append-mode resumption, visibility, sync, discard
on drop/extraction and read-only file errors. The compile-fail example checks the
file-only public constructor. Tests do not simulate power loss.

Follow the [module verification guidance](../README.md#verification-and-performance).
No index-file throughput measurements are available yet. Buffer reuse and generic
dispatch describe the implementation, not measured performance gains.

## Dense index layout

The agreed index is a dense list of exclusive record end positions: one `u64`
for every record in a contiguous prefix of its log file. No separate index-entry
or collection model type is needed. `IndexWriter`, composed by `IndexedLogWriter`,
encodes entries into a reusable byte buffer containing only its pending batch.
Future index readers can seek directly to entries or load a list as `Vec<u64>`;
they need not retain the entire index merely to resolve a range.

The `.idx` file contains consecutive eight-byte little-endian integers, with
no per-entry record ID, padding, header or collection-length prefix. Its paired
`LogFileId` comes from the requested storage path. The entry's ordinal supplies
its sequence number, so storing that identity again would be redundant. This is
a binary format, distinct from the human-readable checkpoint JSON. A Rust vector's
native memory representation must not be treated as its portable file encoding.

For a record belonging to the requested file:

```text
n = record.sequence_number - log_file_id.first_record_id().sequence_number
index_entry_byte_position = n * 8
record_start = 0 if n == 0, otherwise end_positions[n - 1]
record_end = end_positions[n]
record_byte_range = record_start..record_end
```

Positions count bytes from the beginning of the log file. Each end includes the
complete record's CRC trailer; the next record starts immediately there. Log
records begin at byte zero with no file header or inter-record padding. Looking
up an end position needs one entry; finding both start and end needs two adjacent
entries, except for the first record. No index scan or binary search is needed.
These formulas describe layout and lookup work, not measured disk latency.

The initial zero boundary is implicit and is not stored. A completed file's
last record end is in entry 99,999 (the 100,000th entry), at byte offset 799,992
in the index. Reading that entry supplies the log file's complete record end
without querying log-file metadata; no extra terminal entry is required.

A full 100,000-record file has 800,000 bytes of index entries (about 781.25 KiB).
A partial index contains only its actual prefix's entries; unused slots must not
be interpreted as records. An empty index has no entries, and sequence zero
remains a real record. The terminal file range permits only 51,616 entries because
sequences end at `u64::MAX`. End positions need `u64`: 100,000 maximum-size records
occupy 6,553,500,000 log bytes, exceeding a `u32` offset.

Index data is derived from the log and is rebuildable. `IndexedLogValidator`
checks whole eight-byte entries, prefix plausibility and actual record boundaries
and IDs for the newly scanned suffix. The supplied starting boundary certifies
both earlier prefixes; structural checks on that trusted prefix do not establish
its original validity. Structural checks
alone cannot detect every stale or corrupt offset. The log remains authoritative;
an index endpoint is not evidence of CRC validity, sequence continuity or durable
storage, and cannot replace a trusted recovery checkpoint. An index may lag the
log, so a missing entry alone does not establish that the record is absent.

`IndexWriter` owns encoding and file output; `IndexedLogWriter` supplies the offsets
of accepted records and coordinates output with the paired log. The pair writer
completes the log batch and its destination flush before beginning index output,
then flushes the index. Concurrent index readers must account for a partial last
entry and respect the owner's published boundary. Ordered flushes establish read
visibility, not an atomic two-file commit or durability. The validator uses the
same `IndexWriter` to repair a bad/missing index suffix before handover. Historical
index lookup and range-serving APIs remain future work.

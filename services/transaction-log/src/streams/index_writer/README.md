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
[storage specification](../../storage/README.md) for the file format. The
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

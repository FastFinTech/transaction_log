# Index writer

Buffer record end positions synchronously, then send, flush and synchronize an
owned index file. The same writer supports normal indexed appends and rebuilding
an index during recovery.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`IndexWriter`] | Own the file and pending batch, encode dense index entries, and track output failures. |
| [`IndexWriteError`] | Preserve file I/O failures and reject reuse after incomplete output. |

Entries use [`LogFilePosition`](crate::streams::LogFilePosition) from the
[`location`](crate::streams::location) module. The
[`indexed log writer`](crate::streams::IndexedLogWriter) coordinates output with
the log; the [`index validator`](crate::streams::IndexFileValidator) uses this
writer to replace an index suffix during recovery.

## Usage

Given an already-open empty index file, append two known record ends and complete
the three output stages. The corresponding log data must finish flushing before
these index bytes are sent:

```rust
use transaction_log::streams::{IndexWriter, LogFilePosition};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let directory = tempfile::tempdir()?;
# let path = directory.path().join("example.idx");
# let file = tokio::fs::File::create(&path).await?;
let mut writer = IndexWriter::new(file);
writer.write(LogFilePosition::new(16))?; // First record ends at log byte 16.
writer.write(LogFilePosition::new(35))?; // Second record ends at log byte 35.

writer.flush_buffer().await?; // Send the buffered entries.
writer.flush().await?;        // Wait for the file's pending writes to finish.
writer.sync_data().await?;    // Synchronize the index file's data.
# assert_eq!(tokio::fs::read(&path).await?, [
#     16, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 0, 0, 0, 0,
# ]);
# Ok(())
# }
```

Several synchronous appends can share one output batch. Synchronizing this file
alone does not establish durability of the paired log or publish a checkpoint.

<details>
<summary>Design and maintenance notes</summary>

**Resuming an existing index.** The caller supplies an empty file or a validated
or repaired prefix, positions it after the last entry, and finishes flushing any
previous writes before transferring exclusive write ownership. Construction does
no inspection, seeking or I/O. This lets both normal appending and recovery use
the same encoding and output mechanism without reopening the file or inferring
its validity from its length.

**A file-only API.** The public constructor accepts `tokio::fs::File`. The internal
`IndexWriter<F = File>` parameter lets deterministic tests supply scripted files,
including ones with pending writes and synchronization. It preserves static
dispatch without exposing a new file trait or general-purpose destination API.
Sockets cannot be supplied through the public constructor:

```compile_fail,E0308
use transaction_log::streams::IndexWriter;
fn index_to_stream(stream: tokio::io::DuplexStream) {
    let _ = IndexWriter::new(stream);
}
```

</details>

## Behavior and guarantees

### Dense index layout

An index contains one eight-byte little-endian `u64` for each record in a
contiguous prefix of its log file. Each value is that record's absolute,
exclusive byte end in the log, including its CRC trailer.

| Format property | Contract |
| --- | --- |
| Entry order | Record sequence order within the paired file. |
| Entry value | A `LogFilePosition`, distinct from a record length or an index-file address. |
| Framing | Consecutive entries with no header, padding, record IDs or collection-length prefix. |
| Initial boundary | Log byte zero is implicit; there is no initial-zero entry. |
| Complete ordinary index | 100,000 entries occupying 800,000 bytes. |
| Partial or empty index | Only the actual prefix's entries; an empty index has no entries. |

`write` encodes the supplied position without checking it against the log. The
caller establishes correct record identities, consecutive order, increasing
ends and entry count. An index entry alone proves neither record validity nor
durability; a missing entry can mean the index is behind the log.

<details>
<summary>Design and maintenance notes</summary>

**Direct lookup.** The paired `LogFileId` comes from the storage path, and an
entry's ordinal supplies its record sequence. Storing either identity in every
entry would be redundant. Its position and ordinal fully describe an entry, so
no separate entry or collection model is needed. For a record belonging to the
requested file:

```text
n = record.sequence_number - log_file_id.first_record_id().sequence_number
index_entry_byte_position = n * 8
record_start = 0 if n == 0, otherwise end_positions[n - 1]
record_end = end_positions[n]
record_byte_range = record_start..record_end
```

Log records begin at zero and have no file header or inter-record padding. An
end lookup therefore needs one entry; finding both boundaries needs two adjacent
entries, except for the first record. No scan or binary search is required.
The completed file's last end is entry 99,999 at index byte 799,992, so no extra
terminal entry or log-file metadata lookup is needed to obtain that boundary
from a valid complete index.

The shared `INDEX_ENTRY_LEN` constant owns the eight-byte width. Encoding uses
`LogFilePosition::get().to_le_bytes()` explicitly; neither a Rust vector's native
representation nor checkpoint JSON defines these file bytes. The full index is
about 781.25 KiB. Its offsets need `u64` because 100,000 maximum-size records occupy
6,553,500,000 log bytes, exceeding `u32`. The final numeric sequence range has
51,616 possible records; exhaustion remains operationally unreachable under the
repository's policy and introduces no special writer behavior.

**Validity belongs with the log.** The position type accepts every `u64`. A lower
writer that knows neither record IDs nor log contents cannot establish agreement
with records by checking raw offsets alone. The indexed writer supplies ends of
accepted records and enforces sequence/count policy. Keeping that responsibility
there avoids partial validation that might appear to certify the complete index.

Index data is derived and rebuildable. During paired recovery, the indexed
validator checks the supplied trusted boundary's final index entry and the log
prefix's plausible extent, validates the log suffix, and replaces the index
suffix with accepted offsets through this writer. The supplied boundary already
certifies both earlier prefixes; checking its final entry does not revalidate
the preceding entries. The log remains authoritative, and an index endpoint
cannot replace a trusted recovery checkpoint.

Historical lookup and range-serving APIs remain planned. The dense format allows
a reader to seek directly to the needed entries or explicitly decode a list;
resolving a range does not require retaining the whole index. The formulas above
describe lookup work, not measured disk latency.

</details>

### Ownership and completion

[`IndexWriter::new`] takes an append-positioned file by value without allocating
a batch buffer. `write` appends synchronously in call order. The caller chooses
batch boundaries and retains exclusive control of when I/O occurs.

| Operation | Guarantee |
| --- | --- |
| [`write(end_position)`](IndexWriter::write) | Append eight encoded bytes to the pending batch without file I/O. |
| [`flush_buffer().await`](IndexWriter::flush_buffer) | The file accepted the entire batch; clear it while retaining capacity. An empty batch performs no I/O. |
| [`flush().await`](IndexWriter::flush) | Complete the file's pending writes, leaving newer entries in the local batch untouched. |
| [`sync_data().await`](IndexWriter::sync_data) | Synchronize previously flushed file data; do not implicitly send or flush entries. |
| [`get_ref()`](IndexWriter::get_ref) | Borrow the file; operations through this reference bypass the writer's failure tracking. |
| [`into_inner()`](IndexWriter::into_inner) or drop | Discard unsent entries without implicit I/O. `into_inner` returns the file. |

To synchronize all pending entries, call `flush_buffer`, `flush`, then `sync_data`.
Explicit file flush and sync reach the file even when the batch is empty. Their
completion boundaries follow the
[`record writer`](transaction_log_exports::record_writer) contract.

For paired output, the log must finish flushing **before** index sending begins.
`IndexedLogWriter` owns this ordering and tracks the pair's progress; `IndexWriter`
has no visibility into the other file's state.

<details>
<summary>Design and maintenance notes</summary>

**Log before index.** A Tokio file may accept bytes before its underlying write
finishes. Completing the log's destination flush before exposing corresponding
index entries prevents an index offset from preceding the data it describes.
The pair writer then sends and flushes the index. Ordered flushes establish read
visibility, but do not form an atomic two-file commit or establish durability.
Concurrent index readers still need to handle a partial final entry and respect
the owner's published readable boundary.

Synchronization concerns previously flushed data, so newer entries may remain
buffered during it. Its guarantees follow the file and platform; synchronizing
the paired log, directory metadata and checkpoints requires their owning layers.
Scheduling and publication policy remain with those owners.

The mutable writer borrow serializes appends and I/O. Operations made through
`get_ref`, including handle cloning or file-specific synchronization, require the
caller to preserve append position, exclusive write ownership and output order.
Extraction stays available after failure so the owner can close or recover the
file; it does not repair a partial entry or authorize resending the batch.

</details>

### Errors and cancellation

**Failed, panicking or cancelled I/O makes the writer unusable.** Some bytes may
already have been accepted, including part of one entry. Later appends and output
return `IndexWriteError::Unusable` without touching the file. The original failure
returns its concrete source through `IndexWriteError::Io`.

Dropping an unpolled future has no effect. Retaining and polling the same pending
future continues its operation. A failed batch cannot safely be replayed by
starting a new operation or putting the file into another writer.

<details>
<summary>Design and maintenance notes</summary>

**Progress and terminal state.** Buffer output tolerates short writes, retries
`Interrupted` without advancing, and treats zero progress as `WriteZero`. The
future owns the cursor while the buffer retains the whole batch. Only complete
acceptance clears that batch. Dropping the pending future loses its cursor, and
an accepted prefix can stop inside an eight-byte entry.

Each I/O operation sets the usability flag false before invoking the file and
restores it only on success. Errors, cancellation and unwinding therefore leave
the writer terminal, even when no bytes were visibly accepted. The failed batch
remains buffered; file-only flush or sync failure leaves newer pending entries
untouched. Drop and extraction send nothing implicitly.

Only terminal state is retained, rather than a cloned I/O error. The pair writer
also guards its own lifecycle because one lower writer cannot observe a failure
in the other file. Recovery needs that wider context instead of an automatic
retry at the encoding layer.

</details>

## Performance

One reusable `Vec<u8>` holds only pending entries. It starts without an allocation,
grows with the batch and retains capacity after sending. Appends require no
separate allocation per entry while capacity is sufficient; no full-file index
or queryable history is retained.

Encoding copies eight explicitly ordered bytes per entry without zero-filling a
full index, unsafe reinterpretation or a mandatory buffer handoff. The caller's
batch size determines peak and retained capacity. These are implementation costs;
no index-file throughput measurements are available yet.

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::index_writer
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The service is a binary crate, so ordinary Cargo tests skip documentation
examples. The
[stream documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
describes checking them with `rustdoc --test` against a temporary library build,
including dependency artifacts.

<details>
<summary>Design and maintenance notes</summary>

| Coverage | What it establishes |
| --- | --- |
| Literal encodings and typed positions | Eight-byte little-endian output preserves full-width offsets independently of production encoding helpers. |
| Batch storage | Construction allocates no buffer; successful output reuses capacity and appends preserve order. |
| Separate operations | Buffer output, file flush and sync have distinct effects, including empty batches and newer buffered entries. |
| Short, interrupted and pending writes | The same future resumes inside and across entries without skipping or replaying bytes. |
| Every incomplete prefix of two entries | Errors, zero progress, cancellation and panic reject subsequent operations, including when progress stops exactly at an entry boundary. |
| Future lifecycle | Unpolled operations are inert; pending file flush and sync resume without sending newer entries. |
| Real files | Append-mode resumption preserves the prefix; flushing exposes bytes; synchronization, discard on drop/extraction and read-only errors follow the file API. |
| Constructor restriction | The compile-fail example rejects a socket as the public destination. |

Scripted files make failure timing deterministic and reject unexpected operations,
so an extra write cannot silently mask a replay defect. The successful usage
example checks actual file bytes against a literal fixture. Real-file tests verify
API behavior and completion ordering; they do not simulate power loss or establish
throughput.

</details>

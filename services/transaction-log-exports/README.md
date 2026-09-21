# Transaction Log record I/O

Read and write the transaction log's binary records with typed identities, validated
immutable ownership and explicit output completion. Payloads are opaque bytes;
application sequencing, storage policy and replication belong to their callers.

## Types and modules

| API | Responsibility |
| --- | --- |
| [`record`] / [`Record`] | Immutable validated bytes, typed headers/identities and the shared wire/file contract. |
| [`RecordReader`] | Validate input and expose batches of records that survive later reads. |
| [`RecordWriter`] | Build or copy records synchronously into reusable output storage. |
| [`SerializeRecords`] / [`ExistingRecords`] | Constructor-selected append modes that cannot be mixed. |
| [`AsyncSyncData`] | Optional destination data synchronization, including for Tokio files. |

## Usage

Serialize a payload, send the batch and read a validated record:

```rust
use std::io::Write;
use transaction_log_exports::{RecordId, RecordReader, RecordWriter, SequenceNumber, StreamId};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(123));
let mut writer = RecordWriter::for_serialization(Vec::<u8>::new());
writer.write(id, |body| body.write_all(b"payload"))?;
writer.flush_buffer().await?;

let encoded = writer.into_inner();
let mut reader = RecordReader::new(encoded.as_slice());
assert!(reader.wait_to_read().await?);
let record = reader.try_read_next()?.unwrap();
drop(reader);
assert_eq!(record.id(), id);
assert_eq!(record.body(), b"payload");
# Ok(())
# }
```

The [reader](crate::record_reader) and [writer](crate::record_writer) specifications
cover complete input/output lifecycles, additional examples and failure contracts.

## Behavior and guarantees

The reader checks framing, stream-ID domain and CRC before returning records.
Records own immutable bytes; retaining them may retain a larger batch allocation.
Sequence continuity and payload validity require application context.

Writer appends are synchronous. Sending the batch, flushing the destination and
synchronizing file data are separate caller-driven operations; none implies remote
acknowledgement. Partial output and cancellation can leave accepted bytes, so the
writer's failure contract governs reuse.

<details>
<summary>Design and maintenance notes</summary>

Serialization writes into reusable batch storage. Existing-record mode deliberately
copies an already validated encoding into that batch so appending remains synchronous
and the source borrow ends immediately. Constructor-selected types keep the two
append APIs distinct without a runtime mode branch. Detailed ownership, safety and
performance rationale stays with the [record](crate::record),
[reader](crate::record_reader) and [writer](crate::record_writer) specifications.

</details>

## Performance

Independent reader, writer and combined TCP
[benchmarks](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/benches/README.md)
measure this record layer. Their loopback throughput is not a durable-storage or
replicated-service guarantee; each observation retains workload and machine context.

## Validation

```sh
cargo test -p transaction-log-exports --locked
cargo clippy -p transaction-log-exports --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo doc -p transaction-log-exports --no-deps --locked
```

Unit and documentation tests cover independent encodings, fragmentation, retained
ownership, output progress and compile-time API restrictions. Benchmarks remain
opt-in and are skipped by ordinary/all-target test runs.

# Record reading

Read validated transaction-log records from an async socket, file or other byte
source. The reader prepares batches of complete records, and each returned record
owns shared storage that survives later reads or the reader being dropped.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`RecordReader<R>`](RecordReader) | Own the source and receive buffer, validate complete records, and expose the prepared batch. |
| [`RecordReadError`] | Report malformed input, truncation, source I/O failures and subsequent use of a failed reader. |

The sibling [`record`](crate::record) module owns the wire format, limits and
immutable record invariants. [`record_writer`](crate::record_writer) produces
compatible encodings and owns the output-side completion contract.

## Usage

Wait for a prepared batch, then drain it synchronously. The next wait performs
more input work only after that batch is exhausted:

```rust
use tokio::io::AsyncRead;
use transaction_log_exports::Record;
use transaction_log_exports::record_reader::{RecordReadError, RecordReader};

async fn read_records<R: AsyncRead + Unpin>(
    source: R,
    mut handle: impl FnMut(Record),
) -> Result<(), RecordReadError> {
    let mut reader = RecordReader::new(source);
    while reader.wait_to_read().await? {
        while let Some(record) = reader.try_read_next()? {
            handle(record);
        }
    }
    Ok(())
}
```

The handler can inspect or retain each record. Returning from `wait_to_read`
means a complete record is ready, without waiting for the receive buffer to fill.

<details>
<summary>Design and maintenance notes</summary>

**Returning owned records.** Each record retains its immutable bytes independently
of the reader. A helper can return a record without returning its source or
receive buffer:

```rust
use tokio::io::AsyncRead;
use transaction_log_exports::{Record, RecordReadError, RecordReader};

async fn read_first<R: AsyncRead + Unpin>(source: R) -> Result<Option<Record>, RecordReadError> {
    let mut reader = RecordReader::new(source);
    if reader.wait_to_read().await? {
        // The returned record owns its storage after this local reader is dropped.
        reader.try_read_next()
    } else {
        Ok(None)
    }
}
```

This helper intentionally consumes ownership of its source. A reader may have
already received bytes from later records into the same buffer, so dropping it
is appropriate only when the caller is finished with that input. Continuing a
stream means keeping the same reader and draining its subsequent batches.

**Scheduling and ownership.** Waiting requires exclusive mutable access to the
reader. Once a batch is ready, draining it needs no async I/O. The caller chooses
how much processing to do before yielding and which records to retain; the
reader supplies no worker, queue or scheduling policy.

</details>

## Behavior and guarantees

### Input and validation

`RecordReader::new(source)` is infallible and takes the source by value. Async
reading requires Tokio `AsyncRead + Unpin`. The reader checks each complete
record's encoded length, stream ID and CRC before exposing a `Record`, following
the shared [`record` contract](crate::record).

The record-size limit is fixed by that protocol, while a received batch may
contain multiple records and exceed one record's maximum size. Empty payloads
are valid. Payload bytes remain opaque, and a source may contain mixed streams.
Sequence continuity, stream authorization and application payload validation
require caller-owned context.

### Reading, EOF and failures

| Operation or outcome | Contract |
| --- | --- |
| [`wait_to_read().await`](RecordReader::wait_to_read) returns `true` | At least one complete, validated record is ready. |
| [`try_read_next()`](RecordReader::try_read_next) | Return a record without I/O or repeated integrity validation; `None` means the prepared batch is exhausted. |
| Repeated wait with unread records | Return immediately without I/O or validation. |
| Invalid input following valid records | Expose the valid prefix first, then report the error after that batch is drained. |
| Incomplete record at EOF | Report truncation after exposing preceding complete records. |
| Clean EOF between records | Return `false`; later waits also return `false` without further source reads. |
| Reported validation or I/O error | Make the reader terminal; later operations report `ReaderFailed`. |
| Cancelled wait | Preserve received bytes and validated pending header state; another wait continues the pending record. |

The reader never skips an invalid record to expose later ones. Retained records
remain valid after an error or reader drop. Clean EOF is terminal: this reader
does not tail a file that grows after EOF.

`RecordReadError` distinguishes short headers, invalid encoded lengths, invalid
stream IDs, truncated records and CRC mismatches. It retains the relevant byte
counts, rejected value or stored/computed checksums. Source errors retain the
concrete `io::Error` and are reported when encountered; `Interrupted` reads are
retried.

<details>
<summary>Design and maintenance notes</summary>

**Batch preparation.** A wait validates the complete prefix up to the first
incomplete or invalid record, then splits and freezes that prefix once. The tail
stays mutable. A cached length also records that the pending header's stream ID
was validated, so fragmented input and cancellation do not repeat those checks.
The length and stream domain are checked before reading the remainder of a
record; CRC verification requires its complete bytes.

Draining walks the validated batch using its encoded lengths. Records share
slices of that frozen storage instead of splitting the mutable receive buffer
for each record. The final record takes over the batch handle, avoiding another
reference-count increment and releasing the reader's ownership promptly.

**Valid prefixes before errors.** Keeping the offending bytes lets the reader
return every preceding valid record independently of how the source fragmented
its input. Once that batch drains, `try_read_next()` returns `None`. The next wait
rediscovers the error at the beginning of the remaining buffer without reading
more source bytes. Invalid input at the start fails immediately.

This avoids storing a pending error or adding another reader state. Invalid
length/stream checks or a failed CRC may run twice, but successfully validated
records leave the receive buffer and are never rechecked. A CRC-invalid frame
can retain its cached length because its length and stream checks already
succeeded. Source I/O failures are reported immediately rather than rediscovered
as content errors.

**Cancellation and terminal states.** Partial bytes and validated header state
live in the reader, so cancelling a wait preserves the information needed by
the next wait. A returned error is different: it records a terminal failure,
and later calls cannot resume or skip damaged input. Clean EOF has a separate
terminal state, allowing repeated waits to return `false` without another read.

**Shared validity.** The reader establishes all of the record module's immutable
byte invariants before calling its trusted constructor. That ownership boundary
lets record getters and batch draining rely on the checks already performed.
The reader accepts every raw sequence number; deciding which one comes next
requires the state of a particular stream.

</details>

## Performance

The reader starts with a reusable 64 KiB receive buffer. It validates complete
records together and splits once per prepared batch. Draining performs no CRC
calculation or repeated stream-ID validation and does not copy payload bytes.

The opt-in [reader benchmark](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/benches/README.md)
measures this reader over loopback TCP. It includes input, framing and CRC work;
it is not a durable-storage or complete-service benchmark.

<details>
<summary>Design and maintenance notes</summary>

Sharing immutable batch storage avoids mandatory per-record payload copies.
Shared slices can still incur reference-count costs, and a small retained record
can keep a larger backing allocation alive. The final handle transfer avoids a
clone only for the last record in a batch.

The mutable receive buffer can move an unfinished tail when it grows or reclaims
space. Capacity requirements for one incomplete record are separate from the
size of a batch containing many records. These costs mean that the complete
receive path is not allocation-free or copy-free.

The benchmark replays prebuilt encodings so timed writer serialization and CRC
generation do not obscure reader work. The direct benchmark defaults to
100 million records with 2 KiB payloads; its specification documents timing,
retention options and saved measurements. Payload size and retained-record
patterns affect costs that standalone getter instruction counts cannot capture.

</details>

## Validation

From the repository root:

```sh
cargo test -p transaction-log-exports --lib record_reader:: --locked
cargo test -p transaction-log-exports --release --lib record_reader:: --locked
cargo test -p transaction-log-exports --doc --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo rustdoc -p transaction-log-exports --locked -- -D warnings
```

The reader tests cover input and state transitions; documentation examples also
exercise the public module and crate-root import paths.

<details>
<summary>Design and maintenance notes</summary>

Independent encoded fixtures and the protocol's test encoder establish valid
input without relying solely on matching production writer behavior. Literal
expectations cover minimum/maximum records, empty payloads, stream limits and
all representable raw sequence values. Correct-CRC frames with invalid stream
IDs distinguish domain validation from checksum validation.

Fragmentation cases split input across headers, payloads and trailers. Tests
check every truncated extent, cancellation with partial input, mixed record
lengths, batches larger than one record, and records retained across refills.
Byte addresses and retained handles establish sharing without per-record
padding or payload copies.

Corruption tests verify the same valid prefix across source chunk sizes, error
rediscovery without additional source reads, and terminal behavior after the
error is returned. Source-error tests separately check concrete error preservation
and interrupted-read retries. These cases distinguish resumable cancellation,
clean EOF and terminal failure rather than treating all incomplete input alike.

</details>

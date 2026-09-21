# Writer and combined benchmark support

Share producer workloads, startup coordination and reporting between the writer
and combined TCP benchmarks. The reader-only target keeps its independent prebuilt
sender. These are explicitly invoked harnesses; no hooks enter production record I/O.

## Types and modules

| Entry point | Responsibility |
| --- | --- |
| [`ReceiverKind`](mod.rs) | Select a raw byte drain or the production record reader. |
| [`run`](mod.rs) | Parse options, prepare workers, run warmup/measurements and report validated results. |
| [Environment collector](../environment/README.md) | Emit machine metadata before timed work. |

The [benchmark overview](../README.md) owns commands/results. The
[record](../../src/record/README.md) and [writer](../../src/record_writer/README.md)
specifications own the encoding and production API contracts.

## Usage

A short check exercises both serialization and existing-record copying:

```sh
python scripts/benchmarks.py run --targets record_writer record_io --records 10003 --warmup-records 1001 --runs 2 --include-copy
```

Run from the workspace root. Ordinary and all-target tests skip transfers;
`--help` is safe, and benchmarks require the explicit `--bench` run guard.

## Behavior and guarantees

### Measurement boundaries

Workers start from one common clock. Writer-only timing ends at the last sender
or raw-drain receiver completion; combined timing includes the production reader.
Success requires every worker, exact bytes and clean EOF. The combined receiver
also validates and counts records.

<details>
<summary>Design and maintenance notes</summary>

`record_writer` uses the real `RecordWriter` and a blocking TCP receiver that
only drains bytes into one reusable 256 KiB buffer. No `RecordReader`, framing
parser, per-record receiver allocation or receiver CRC calculation is timed.
This isolates the writer from reader implementation changes, not from loopback
transport, memory bandwidth, receiver system calls, scheduling or backpressure.

`record_io` uses the same producer with a real `RecordReader` on the receiving
side. It measures both CRC generation (in serialization mode) and validation,
framing, receive-buffer preparation, ownership of returned records, and their
consumption. Reader retention is optional. The existing `record_reader` target
has a separate prebuilt-byte sender and remains independent of `RecordWriter`.

Each connection has a dedicated sender thread with its own Tokio current-thread
runtime, exclusively owned writer and socket. The combined target additionally
has one reader thread/current-thread runtime per connection. The writer-only
target instead has one blocking drain thread per connection. Eight connections
therefore use sixteen workers plus the waiting coordinator. No global lock,
channel, counter, clock or report is added per record. CPU affinity is not pinned;
sockets use `TCP_NODELAY`, default OS buffers, and ephemeral IPv4 loopback ports.

Sockets, runtimes, writer construction, validated stream-ID tables, payloads,
copy fixtures and receiver buffers/retention windows are prepared before workers
report readiness. The coordinator releases all workers with one common `Instant`.
No record bytes are sent before that release. Both sender and receiver durations
use that same start, including wakeup skew. The result's elapsed time is the
maximum of all sender and receiver durations, not a sum or average of rates.
Sender completion includes all appends, `flush_buffer` calls, final destination
`flush`, write-half shutdown and release of the writer's buffer. Receiver completion
includes clean EOF. Joins/reporting do not determine timestamps. Remaining retained
records are inspected/dropped after the reader timestamp; that cleanup can overlap
other workers still transferring. Evictions during transfer are measured.

The reader-only target reports last-reader completion as its aggregate duration;
the writer and combined targets also include the last sender's completion. Both durations are
printed separately to make that distinction visible. Neither target measures
per-record latency, acknowledgements, disk persistence or command handling.
Sender and receiver work overlap; subtracting their durations does not isolate CPU
time. Amortized nanoseconds per record are throughput-derived, not latency samples.

</details>

### Producer workloads and memory

`serialize` generates each record through the synchronous writer callback;
`copy` borrows prevalidated fixture records and copies their encodings. Both send
reusable batches and flush the destination once at the end. Fixture preparation
is outside timing, but a measured writer's first buffer growth is included.

<details>
<summary>Design and maintenance notes</summary>

`--workload serialize` selects `RecordWriter::for_serialization`. One callback
writes a deterministic payload in `--write-chunk-bytes` chunks (eight bytes by
default), with a shorter final chunk when needed. An empty payload makes no body
write calls. Payload input is allocated once; there is no per-record input vector
or boxed callback. Compiler coalescing of these writes is allowed: this is a
synthetic workload, not a claim about the cost of a specific Serde serializer.
ID selection and sequence generation, header/CRC finalization, buffer growth and
socket output are timed. Stream IDs cycle through the valid range; sequence numbers
advance per stream within each connection. Different connections restart the same
ID progression; the benchmark is not a service-level sequence-acceptance test.

`--workload copy` selects `RecordWriter::for_records`. A bounded batch of frames
is encoded from the public wire contract and validated into immutable `Record`s
before timing. Records are then borrowed cyclically; no handle clone or validation
occurs per append. The timed writer copies encoded bytes and does not recalculate
CRC. IDs repeat across fixture cycles and connections. Supplying a write-chunk
option in this workload is rejected; the default printed chunk size is unused.
The fixture's one-time `RecordReader` work is outside writer-only timing.

The generic `send` loop accepts a statically dispatched append closure and concrete
writer mode. Workload selection happens before readiness, not on each append.
Both modes call `flush_buffer` after `--batch-records` records or the final partial
batch. There is one destination flush at the end, followed by explicit socket
shutdown. There is no per-record destination flush, timer, queue or buffer pool.

Each writer starts with its production constructor's maximum-record reservation.
Its first measured batch may grow the buffer; subsequent sends retain capacity.
Warmup is a separate transfer with fresh sockets/runtimes/writers, so it warms code
and the system without silently preserving measured writers' allocations.
The whole 100-million-record input is never materialized: default encoded traffic
is 206.4 GB, while memory is bounded by fixture size, per-writer batch capacity,
receiver buffers and optional retained records. Large batch/retention settings
can still demand substantial memory, and growth can reserve more than live bytes.

</details>

### Startup, failures and reporting

Arguments are validated before work. Startup failure releases waiting workers;
worker errors produce a failed process, not a successful throughput result. The
raw-drain target checks bytes without claiming record-content validation.

<details>
<summary>Design and maintenance notes</summary>

The count means total records across all connections. Remainders are assigned to
the first connections, preserving the exact requested total for warmup and runs.
Configuration rejects zero connections, batches, chunks or runs, too few records
for the connection count, invalid workload names, oversized payloads, byte-count
overflow, and retention on a byte-only receiver. A zero warmup disables it.

Every worker must succeed. Sender batch counts must match ceiling division of
assigned records by batch size. Every receiver must reach EOF and receive the
exact encoded byte count. In `record_io`, every record additionally passes the
real reader's framing/stream-ID/CRC validation and the parsed count must match.
Its byte counter sums `record.length().get()`, the validated `u64` total encoded
length, including the header and CRC, without repeating length validation.
The byte-drain target deliberately does not verify CRC or record contents: its
reported record count comes from the completed producer, cross-checked against
received bytes. Correctness tests and the combined benchmark complement it.

Startup channels are used only once per worker. A ready sender is dropped before
waiting for the start signal: a setup failure must disconnect the ready channel
instead of leaving the coordinator waiting for an impossible worker. Exiting the
coordinator drops start senders and releases waiting workers. Peer sockets close
on worker errors, releasing ordinary pending socket I/O. Both workers in a pair
are joined before their results are propagated. Errors produce a failed process,
not a successful throughput line for that run. There is no automatic retry or
deadline imposed on an explicitly requested long measurement.

The program checks Cargo's `--bench` flag before configuration, fixture
allocation, runtime creation or socket setup. Without it, or when `--test` is
present, it prints a skip message. `--help` is always safe. The manifest uses `harness = false` and `test = false`; the extra guard remains
necessary because all-target tests can still select custom benchmark executables.

Configuration and `RESULT` lines identify target, workload, record/payload/batch
sizes, chunk size and retention. Individual connection lines use the same timing
origin as the aggregate. `sent_batches` counts `flush_buffer` calls. On the raw
receiver, `receive_batches` counts nonempty `read` calls; on the record receiver
it counts prepared `RecordReader` batches. Those two receive counts have different
meanings and should not be compared as equivalent operations.

</details>

## Performance

These measurements include loopback transport, memory bandwidth, scheduling and
backpressure. Concurrent sender/receiver times cannot be subtracted to isolate CPU
cost, and throughput-derived ns/record is not a latency sample. Results and their
conditions are retained in the [measurement history](../README.md#performance).

## Validation

Small executable runs establish partitioning, exact counts, EOF, workload selection
and common-start maxima. Ordinary/all-target tests verify the skip guard; Clippy
and formatting check both target builds without starting long transfers.

<details>
<summary>Design and maintenance notes</summary>

Coverage uses one/eight connections, uneven record and warmup totals, one record
per connection, partial batches, repeated runs, empty/maximum payloads, non-divisible
body chunks and retained records. Invalid arguments exercise rejection before
setup. Failure exits and independently checked count/timing maxima protect the
coordination contract. Short transfers verify the harness and are not throughput
baselines; full measurements follow the repository's serial measurement policy.

</details>

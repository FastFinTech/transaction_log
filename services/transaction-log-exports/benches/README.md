# Record I/O benchmarks

This directory contains explicitly invoked performance executables for people
and agents. It is separate from the correctness test suite. Three targets use
real TCP connections on IPv4 localhost; none starts the Transaction Log service.

| Target | Timed producer | Timed receiver | Use |
| --- | --- | --- | --- |
| `record_reader` | Replays prebuilt encoded bytes; no `RecordWriter` | Real `RecordReader`, including CRC validation | Compare reader changes independently of writer changes. |
| `record_writer` | Real `RecordWriter` | Raw byte drain; no `RecordReader` or CRC validation | Compare writer changes independently of reader changes. |
| `record_io` | Real `RecordWriter` | Real `RecordReader`, including CRC validation | Check the combined path after either side changes. |

Each target still includes TCP, memory and scheduling costs. An individual target
isolates it from the other production component, not from the entire system.
Compare a target against its own before/after baseline; do not subtract durations
or rates between targets to infer isolated CPU cost, since their work overlaps.
Read the [record specification](../src/record/README.md) before changing the
workload or interpreting a result.

## Run, archive and compare the suite

From the workspace root, `python scripts/benchmarks.py run` builds the three
targets once and runs them serially, using eight connections and three measured
100-million-record samples per target. `--include-copy` adds existing-record
copying; use smaller record counts first when changing the harness. Standalone
Cargo commands below continue to default to one connection and one sample.

Each target emits an `ENVIRONMENT` JSON line before warmup, with CPU model,
physical/logical cores, available parallelism, RAM, OS and power plan/governor
where available. The shared [environment collector](environment/README.md)
has no timed or production role. Ordinary test/help invocations skip it.

The runner preserves raw logs, machine/build/source identity, per-sample and
per-connection results in a versioned JSON bundle, plus CSV and generated Markdown.
It can regenerate tables and compare median throughput against an approved
baseline without rerunning it. See [automation documentation](../../../scripts/README.md)
for commands, validation, exit codes and artifact retention. No regression budget
is silently chosen, and matching machine/workload settings are required by default.
The [root README](../../../README.md) presents the selected results for repository
readers; this document owns workload contracts and historical measurements.

## Writer and combined runs

The default workload for every target is **100 million records with 2,048-byte
payloads**, after a separate one-million-record warmup. These new commands divide
that total across eight writer/receiver pairs:

```powershell
cargo bench -p transaction-log-exports --bench record_writer --locked -- --connections 8
cargo bench -p transaction-log-exports --bench record_io --locked -- --connections 8
```

Use one connection by omitting `--connections`, or specify it explicitly. The new
targets share their producer and coordination code in [support](support/README.md),
whose README documents timing, ownership, failure behavior and verification.
The reader-only implementation stays independent so writer changes do not change
its timed workload.

Both writer targets select a workload before timing:

- `--workload serialize` (default): invoke the public synchronous callback for
  every record, write its payload in eight-byte chunks, and compute its header/CRC.
  Payload input is reused; there is no input `Vec` allocation per record. This is
  a synthetic field-writing workload, not a benchmark of a specific serializer.
- `--workload copy`: synchronously call `write_record` on prevalidated records.
  Fixture generation/validation is outside timing; copying into the writer's
  batch and sending it are timed. Fixture IDs repeat between batches/connections.

Both call async `flush_buffer()` after each batch, reuse its allocation, and
explicitly flush/shut down the socket at the end. They do not use a worker queue,
buffer pool, timer, or file synchronization. Each writer owns its destination.

```powershell
# Small checks before a full measurement:
cargo bench -p transaction-log-exports --bench record_writer --locked -- --records 100000 --warmup-records 0
cargo bench -p transaction-log-exports --bench record_io --locked -- --connections 8 --records 100003 --warmup-records 0

# Existing-record copying, or different batching/serialization granularity:
cargo bench -p transaction-log-exports --bench record_writer --locked -- --connections 8 --workload copy
cargo bench -p transaction-log-exports --bench record_io --locked -- --connections 8 --batch-records 256
cargo bench -p transaction-log-exports --bench record_writer --locked -- --write-chunk-bytes 2048
```

The options below in the reader section also apply to the new targets, with these
differences: `--batch-records` counts records per `flush_buffer`, `--retain-records`
is available only on targets with a real reader, and the writer targets additionally
accept `--workload serialize|copy` and `--write-chunk-bytes N` (positive, default 8).
An explicit chunk option with the copy workload is rejected. Each run prints its
configuration and per-connection/aggregate `RESULT` data. The new targets time a
common worker start through the last sender **or** receiver completion, print both
durations, and verify encoded byte counts and clean EOF. The combined target also
validates and counts every record. The raw-drain target derives its record count
from completed writer calls and does not claim to validate record contents.

At the defaults, 100 million records are **206.4 GB of encoded traffic**. Input is
generated progressively, not materialized in full. Memory scales with per-writer
batch capacity, copy fixtures and receiver retention. Sixteen workers run for eight
connections, competing for the same machine's CPU and memory resources. Fresh
writers may grow their batches during timing; the warmup uses separate writers.

These are opt-in executables, with the same early skip guard as `record_reader`.
Ordinary tests and all-target test runs must never launch a long transfer.
Build without measuring using `cargo bench -p transaction-log-exports --no-run --locked`.
Run comparisons serially under comparable conditions and retain raw results;
see the maintenance guidance below. The executables enforce no fixed performance
threshold; the suite comparison command accepts an explicit regression budget.

## Running the reader benchmark

Run from the workspace root. The default is **100 million records with 2,048-byte
payloads**, one measured transfer following a one-million-record warmup:

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked
```

To divide the same total across **eight connections and eight reader threads**:

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8
```

Each reader receives 12,500,000 records. The warmup total is also divided, giving
125,000 warmup records per connection. `--records` always means the total across
all connections, not the count per connection. For uneven totals, the first
remainder connections receive one additional record, preserving the exact total.

Each encoded record is 2,064 bytes, including the header and CRC. The measured
transfer therefore carries 206,400,000,000 encoded bytes (206.4 decimal GB).
All senders share one immutable 8,192-record batch of 16,908,288 bytes; it does
not allocate or write a 206 GB fixture file. Each reader owns its receive buffer
and retention window. The warmup is an additional untimed transfer.

For a short check, omit warmup and reduce the count:

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked -- --records 100000 --warmup-records 0
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8 --records 100003 --warmup-records 0
```

For repeated measurements or a different ownership pattern:

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked -- --runs 3
cargo bench -p transaction-log-exports --bench record_reader --locked -- --retain-records 8192
cargo bench -p transaction-log-exports --bench record_reader --locked -- --help
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--records N` | 100000000 | Total records per measured transfer; at least the number of connections. |
| `--connections N` | 1 | Positive number of concurrent TCP connections, each with its own sender and reader thread. |
| `--payload-bytes N` | 2048 | Fixed payload size, including neither header nor CRC; 0 through 65519. |
| `--batch-records N` | 8192 | Positive number of prebuilt records per sender write batch. |
| `--retain-records N` | 0 | Keep the most recent N owning records alive **per reader**; zero immediately drops each record. |
| `--warmup-records N` | 1000000 | Total records for a separate transfer before measurements; zero disables it, otherwise at least the number of connections. |
| `--runs N` | 1 | Positive number of measured transfers; each uses fresh connections and runtimes. |

All numbers are plain decimal integers without separators. A final partial
sender batch sends exactly the requested number of records. Small write batches
can deliberately expose syscall and scheduling costs; they do not dictate TCP
packet boundaries or the reader's batch size. Payload size and retained records
affect memory usage, so keep them explicit when comparing measurements.

The target uses a custom `main`, with `harness = false` and `test = false`.
Ordinary `cargo test --workspace` does not execute it. Although
`cargo test --workspace --all-targets` selects benchmark targets, this executable
checks for Cargo's `--bench` argument before doing any workload setup. Without
that argument, or when `--test` is present, it prints a skip message and exits.
This rule must stay intact: routine tests must never launch a long measurement.
An optimized build is required for measurements. `cargo bench --no-run` and
`cargo clippy --workspace --all-targets` can check compilation without running it.

## Workload and timing contract

Each sender is a dedicated blocking OS thread using `TcpStream::write_all`.
Each reader runs the real `RecordReader` on its own OS thread and Tokio
current-thread runtime. The main thread coordinates startup and joins workers;
it does not read records. Eight connections therefore have eight sender threads
and eight reader threads plus the waiting coordinator. There is no shared
executor serializing reads, global per-record counter, or lock between readers.
CPU affinity is not pinned. Sockets use `TCP_NODELAY` and the default OS buffer
settings. Each listener binds only to `127.0.0.1` on an automatically assigned port.

Before timing, the executable builds the immutable sender batch, creates all
runtimes and TCP connections, constructs the readers, reserves their retention
windows, and waits until all sender and reader threads are ready. No record bytes
are sent before timing starts. The coordinator captures one start `Instant` and
sends it to every worker. Reader durations use that common start, including
release/wakeup skew; no reader starts its own later timer. Each reader records
its finish immediately after consuming all of its records and reaching clean EOF.
The aggregate elapsed time is the largest of those durations: the common start
through the last reader's completion. Each sender closes its write half after
sending its assigned count.

Startup uses channels rather than a fixed-count barrier. If setup or thread
creation fails, dropping the start senders releases waiting workers with errors
instead of leaving a barrier waiting forever. Channels are not used per record.

The measured read interval includes:

- Releasing/waking workers, TCP transport/backpressure, and waiting for socket input.
- Every `wait_to_read` call, including framing, stream ID and CRC validation,
  receive-buffer management, and batch splitting/freezing.
- Every `try_read_next` call and the construction of each owning `Record`.
- Record/byte/batch counters, an optimization barrier for each record, and either
  immediately dropping records or maintaining the requested retention window.
- EOF detection. No clocks, output, or payload copies are added per record.

Worker joins and reporting do not determine the finish timestamps. A reader's
remaining retained records are inspected and destroyed after its own timestamp
and after dropping its `RecordReader`. In concurrent runs that cleanup can overlap
other readers still transferring. Eviction of older records during a transfer
is measured. The two consumption modes are selected before the read loop so
immediate-drop mode has no retention branch or queue operations per record.

The encoder stays local to this benchmark to keep its prebuilt-byte producer
independent of `RecordWriter`. It follows the public little-endian wire contract and uses CRC-32C over
the finalized header and payload. It never calls private constructors or casts
native header memory. Each batch cycles through valid stream IDs; sequence
numbers advance within each stream in that batch. **The exact batch is replayed,
so IDs and sequence numbers repeat between batches and connections.** This intentionally removes
producer encoding/CRC computation from the timed workload. The generic reader
does not enforce sequence continuity; this fixture is unsuitable for benchmarking
a future service endpoint that does. Keep that distinction explicit rather than
loosening service validation to accept a benchmark.

## Results and their limits

Each successful transfer must reach clean EOF, pass the reader's validation, and
match both the expected record count and encoded byte count on every connection.
Aggregate totals are checked too. Any failure exits
with an error instead of reporting successful throughput. The benchmark does
not scan payloads a second time or verify application-level sequence rules.

Each measured run prints an aggregate `RESULT` line containing elapsed read seconds,
records/second, million records/minute, encoded MiB/second, amortized ns/record,
prepared batches, mean records/batch, and maximum sender elapsed seconds. With
multiple connections, preceding `CONNECTION` lines report each reader's assigned
count and results. Aggregate throughput divides the total by the last completion
time; it does not sum or average individual throughput rates. The sender
interval overlaps the reader interval and includes blocking on backpressure.
Do not subtract it from read time to infer reader CPU time. Amortized ns/record
is throughput-derived, not a measurement of individual record latency.

This is **socket-to-record throughput on one machine**, not isolated reader CPU
performance, physical network capacity, or transaction-log service throughput.
The sender, loopback stack, memory bandwidth, scheduler, CPU architecture and
power state can limit the result. CRC still touches every payload byte. Reusing
a batch creates a cache-friendly producer and does not model arbitrary event
generation. No disk, replication, durable flush, command handling, or remote
machine is involved. Multiple localhost senders also compete with the readers
for resources; this is not a receiver-only scaling result with remote producers.
A successful run proves no latency or throughput guarantee.

Use `--runs` for repeated observations; there is no automatic statistical
sampling or regression threshold. A custom benchmark main keeps a 100-million-
record transfer a single deliberate operation. Criterion can be added later
for focused microbenchmarks without changing this executable's timing contract.
In-memory decoding/extraction benchmarks, cross-machine clients, mixed payload
distributions, and automated baseline comparison remain future work.

## Maintenance and verification

When changing this executable, check argument handling, both consumption modes,
empty and maximum payloads, a non-divisible final sender batch, repeated runs,
and both ordinary and all-target test commands. For concurrency changes, also
check totals not divisible by the connection count, one record per connection,
warmup partitioning, per-connection counts, and that aggregate time equals the
last reader finish rather than the sum of reader times. Small explicit socket runs are
sufficient to verify the harness; never put the 100-million-record workload in
unit tests. Keep production APIs and dependency features free of benchmark-only
hooks. Networking support and anyhow for this executable are dev-dependencies.

Before claiming a performance change, compare the same optimized toolchain,
target, payload size, batch size, retention window, and warmup/run settings.
Record `rustc -vV`, CPU model, OS, build flags, and background load with the
configuration and output. Run measurements serially on an otherwise idle
machine; avoid running compilation or other benchmarks concurrently. Preserve
the command and raw results, for example:

```powershell
rustc -vV
Get-CimInstance Win32_Processor | Select-Object Name
cargo bench -p transaction-log-exports --bench record_reader --locked | Tee-Object -FilePath target/record-reader-socket.txt
```

The output file above is resolved by PowerShell from the workspace root and
lives under ignored `target/`. Keep durable observations with their conditions
in this README; do not substitute the 100-million-records/minute design target
for a measured result.

## Initial measurement: 2026-09-14

The default command above completed on an Intel Core i9-12900HK (14 cores,
20 logical processors), Windows 11 Pro 10.0.26200, Rust 1.98.0
(`88d9e12ae`, LLVM 22.1.8), targeting `x86_64-pc-windows-msvc`. It used Cargo's
optimized bench profile with no additional build flags supplied for this run,
one sender and one reader, default socket buffers, no CPU affinity, and no
records retained. This was a desktop session with uncontrolled background load;
no concurrent builds or other benchmarks were launched during measurement.

| Setting or result | Value |
| --- | --- |
| Measured records | 100,000,000 |
| Payload / encoded record bytes | 2,048 / 2,064 |
| Encoded bytes received | 206,400,000,000 |
| Sender batch records / bytes | 8,192 / 16,908,288 |
| Separate warmup records | 1,000,000 |
| Measured runs | 1 |
| Read interval | 150.250578 seconds |
| Records/second | 665,555 |
| Million records/minute | 39.933 |
| Encoded MiB/second | 1,310.067 |
| Amortized ns/record | 1,502.51 |
| Prepared reader batches | 3,362,004 |
| Mean records/batch | 29.74 |
| Sender interval | 150.250244 seconds |

The run passed every reader CRC check, reached clean EOF, and matched the exact
record and byte counts. The raw console output and environment details were
saved under `target/record-reader-socket.txt` and
`target/record-reader-socket-environment.txt`; those are local, ignored artifacts
that subsequent runs may replace. This table preserves the initial observation.

Before the full run, eight small measured transfers checked repeated runs,
2 KiB payloads with immediate drop and retention, empty and maximum payloads,
one-record sender batches, and partial final sender batches. A one-million-
record run with 2 KiB payloads and a 100,000-record warmup took 1.591220 seconds.
All small transfers matched their counts and reached clean EOF. The existing
30 unit tests and four documentation tests passed; all-target testing skipped
benchmark execution as intended. Invalid numeric limits, unknown/missing
arguments, help output, and explicit test-mode skipping were also checked.

This single-connection observation is not a statistical baseline or an eight-reader
result. Multiplying its rate by eight projects approximately 319 million
records/minute only under linear scaling. Use the explicit concurrent mode to
measure actual aggregate throughput.

## Eight-connection measurement: 2026-09-14

After implementing concurrent readers, this command completed on the same
i9-12900HK / Windows 11 / Rust 1.98.0 setup described above:

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8
```

There were eight independent reader threads/runtimes and eight sender threads,
with a shared immutable sender batch. Payloads were 2,048 bytes, batches were
8,192 records, retention was disabled, and a total one-million-record warmup
preceded one measured run. Each connection received 125,000 warmup records and
12,500,000 measured records. No extra build flags, CPU affinity, socket buffer
tuning, concurrent builds, or other benchmarks were introduced. Desktop
background load and CPU scheduling remained uncontrolled.

| Aggregate result | Value |
| --- | --- |
| Records / encoded bytes | 100,000,000 / 206,400,000,000 |
| Common start through last reader EOF | 23.135731 seconds |
| Records/second | 4,322,319 |
| Million records/minute | 259.339 |
| Encoded MiB/second | 8,507.982 |
| Amortized ns/record | 231.36 |
| Prepared reader batches | 3,318,515 |
| Mean records/batch | 30.13 |
| Maximum sender interval | 23.130112 seconds |

| Reader | Records | Seconds from common start through EOF |
| --- | --- | --- |
| 1 | 12,500,000 | 22.449432 |
| 2 | 12,500,000 | 22.051471 |
| 3 | 12,500,000 | 22.770917 |
| 4 | 12,500,000 | 22.660716 |
| 5 | 12,500,000 | 23.135731 |
| 6 | 12,500,000 | 22.646994 |
| 7 | 12,500,000 | 22.637847 |
| 8 | 12,500,000 | 21.643443 |

Every connection passed stream ID and CRC validation, matched its expected
12,500,000 records and 25,800,000,000 encoded bytes, and reached clean EOF.
Aggregate counts matched too. Output and environment details were saved locally
under `target/record-reader-eight-socket.txt` and
`target/record-reader-eight-environment.txt`.

Before the full run, small transfers checked eight-way uneven splitting of
100,003 records (three readers with 12,501 and five with 12,500), repeated runs,
uneven warmup totals, one record per connection, empty/maximum payloads,
retention, partial sender batches, and the single-connection mode. An independent
check of the reported per-reader results verified exact aggregate byte/count
totals and that aggregate time equalled the maximum reader time. A total
one-million-record eight-connection transfer took 0.233798 seconds. Invalid
connection counts and incompatible record/warmup totals were rejected. The
30 unit tests and four documentation tests passed, and all-target testing
continued to skip benchmark execution. Formatting and Clippy checks passed.

Compared with the earlier single-connection observation of 150.250578 seconds,
the concurrent run was approximately **6.49 times faster**, rather than eight.
These are individual desktop measurements, not a controlled statistical scaling
study. Concurrent mode now uses dedicated reader workers even for one connection;
the initial single-connection measurement ran its reader on the main thread.
The production reader and per-record read loop were unchanged. For closer
comparisons, repeat both `--connections 1` and `--connections 8` with this same
executable and the same workload and environment. No specific resource bottleneck
was established by this benchmark.

## Three-target measurement: 2026-09-16

After adding the independent writer and combined targets, all three commands ran
serially on an Intel Core i9-12900HK (14 cores / 20 logical processors), Windows
11 Pro 10.0.26200, Rust 1.98.0 (`88d9e12ae`, LLVM 22.1.8), targeting
`x86_64-pc-windows-msvc`. Cargo used its optimized bench profile, with no additional
Rust flags supplied. CPU affinity and socket buffers were unchanged. No other
benchmark or compilation was launched concurrently; desktop background activity
and scheduling remained uncontrolled.

```powershell
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8
cargo bench -p transaction-log-exports --bench record_writer --locked -- --connections 8
cargo bench -p transaction-log-exports --bench record_io --locked -- --connections 8
```

Each command transferred **100,000,000 records / 206,400,000,000 encoded bytes**
across eight connections after a separate one-million-record warmup. Payloads
were 2,048 bytes, batches were 8,192 records, retention was disabled, and there
was one measured run per target. Both writer targets used `serialize` with
eight-byte body writes. The reader target replayed its prebuilt fixture.

| Target | Aggregate seconds | Million records/minute | Encoded MiB/second |
| --- | ---: | ---: | ---: |
| Reader with prebuilt sender | 23.168342 | 258.974 | 8,496.006 |
| Writer with raw byte drain | 29.289744 | 204.850 | 6,720.386 |
| Combined writer and reader | 30.649856 | 195.759 | 6,422.163 |

Every connection received exactly 12,500,000 records' worth of bytes
(25,800,000,000). The two real-reader targets also validated and counted each
record. The raw byte drain checked byte count and clean EOF, not record contents.
Both writer targets made 1,526 buffer sends per connection (12,208 total),
including the final partial batch. Independent checks of the printed results
confirmed per-connection counts, aggregate totals and timing maxima.

These establish initial observations for each workload, not statistical regression
thresholds. The combined run includes writer CRC generation and reader CRC
validation. The difference between the writer and combined times does not isolate
reader CPU cost: their receivers use different buffering/I/O patterns and compete
with senders for the same hardware. Compare repeated runs of the same target to
assess a future change. These are throughput measurements, not command latency,
network capacity between machines, or durable-file throughput.

Raw outputs and environment details are in ignored local artifacts:
`target/record-reader-100m-20260916.txt`,
`target/record-writer-100m-20260916.txt`,
`target/record-io-100m-20260916.txt`, and
`target/record-io-benchmark-environment.txt`. The table and conditions above remain
the durable record if `target/` is cleaned.

Before these full transfers, short runs exercised both new targets and workloads
with empty/maximum payloads, uneven totals, one record per connection, repeated
runs, uneven warmup, partial batches, seven-byte body chunks and retained records.
Twenty-seven invalid configurations were rejected; help and explicit skip guards
were checked. One-million-record eight-connection runs also passed for both
serialization and copying on each target. The initial full measurement above did
not include copying. Workspace unit tests, documentation tests, all-target test
skipping, formatting, Clippy and Rustdoc checks passed.

Later completed suites and their machine/source metadata are preserved in the
[published observations](../../../benchmarks/README.md). Those same-source,
single-sample compiler comparisons varied substantially on this shared desktop
and did not establish a compiler regression. The root README presents those saved
results; the earlier single-run figures above remain historical observations.

# Transaction Log

A performance-focused Rust transaction log for event-sourced CQRS systems, building
toward durable ordered streams, historical reads and fixed-membership replication.

**Implemented:** binary record I/O, indexed file appends, storage metadata and
per-stream recovery. **Current executable:** a configuration-validation scaffold
that prints its version and exits. Clustering and application messages are mostly
scaffolding; client serving, replication and startup integration remain planned.

## Build and run

The workspace uses Rust edition 2024 and is tested with Rust 1.98.1. From the
repository root:

```sh
cargo run -- --cluster-mode single --storage-directory ./data
cargo run -- --help
cargo test --workspace --locked
```

The executable does not yet create storage or start a server. The
[service overview](services/transaction-log/README.md) describes its current behavior
and [configuration inputs](services/transaction-log/src/configuration/README.md).

## Implementation progress

| Area | Implemented | Planned |
| --- | --- | --- |
| Record I/O | Typed identities, CRC framing, retained immutable records, batched reader and synchronous writer modes. | Additional workload and latency measurements. |
| Storage and streams | Paths, fresh pairs, indexed ordered appends, flush/sync/finalization, metadata persistence and per-stream recovery with checkpoint advancement. | Startup-wide orchestration, live rotation and durability scheduling. |
| Configuration | CLI/environment loading, typed fixed-topology values and stored configuration APIs. | UUID establishment and comparison with persisted settings at startup. |
| Clustering and messages | Configuration models and module scaffolding; reusable message framing is available separately. | Version/identity exchanges, replication, coordination and application schemas. |
| Serving | Location and range models. | Client lifecycle, ingestion admission/queues/routing, historical queries, subscriptions and status endpoints. |
| Measurement | Independent and combined record benchmarks, saved artifacts, reporting and explicit-budget comparisons. | Disk/recovery/replication benchmarks, dedicated performance workers and CI artifact hosting. |

The [stream initializer](services/transaction-log/src/streams/stream_initializer/README.md)
recovers an individual stream and hands its active pair to the indexed writer.
Application startup does not call it yet. The [cluster design](CLUSTERING.md) records
planned lifecycle requirements and open questions.

## Workspace

| Component | Start here |
| --- | --- |
| [Record I/O library](services/transaction-log-exports/README.md) | Public record, reader and writer APIs with executable examples. |
| [Service](services/transaction-log/README.md) | Storage, streams, configuration and application scaffolding. |
| [Object pool](lib/object-pool/README.md) | Single-threaded reuse and reclamation. |
| [Message I/O](lib/message-io/README.md) | Bounded length-prefixed Serde/Postcard control messages. |
| [Benchmark suite](services/transaction-log-exports/benches/README.md) | Workloads, timing boundaries and measurement history. |
| [Benchmark automation](scripts/README.md) / [saved measurements](benchmarks/README.md) | Run, archive, report and compare observations. |

## Design priorities

Record ownership stays explicit, validity is established at decoding boundaries,
and synchronous appends build reusable batches. Applications choose when bytes are
sent, flushed and synchronized. Constructor-selected writer types keep serialization
and existing-record copying distinct. The module specifications explain the
engineering choices and their constraints in expandable notes.

## Benchmarks

The design target has been 100 million records per minute for record I/O. The
measurements below cover loopback transport and the implemented record layer;
they do not establish durable-storage or replicated-service throughput.

| Target | Timed path |
| --- | --- |
| `record_reader` | Prebuilt bytes to the production reader, including CRC validation. |
| `record_writer` | Production writer to a raw byte drain. |
| `record_io` | Production writer to the production reader. |

### 100-million-record results

<!-- benchmark-results:start -->
Recorded on 2026-09-16, with identical source and workload settings. Each row
is **one measured sample**, not a repeated-run median.

| Benchmark | Rust | Seconds | Million records/min | Encoded MiB/s |
| --- | --- | ---: | ---: | ---: |
| `record_reader` | 1.98.1 | 31.766 | 188.881 | 6,196.508 |
| `record_writer` | 1.98.1 | 33.785 | 177.594 | 5,826.231 |
| `record_io` | 1.98.1 | 55.704 | 107.712 | 3,533.634 |
| `record_reader` | 1.98.0 | 48.308 | 124.203 | 4,074.662 |
| `record_writer` | 1.98.0 | 79.890 | 75.103 | 2,463.854 |
| `record_io` | 1.98.0 | 40.944 | 146.541 | 4,807.498 |

Saved data: [Rust 1.98.1](benchmarks/2026-09-16-rust-1.98.1.json) and
[Rust 1.98.0](benchmarks/2026-09-16-rust-1.98.0.json), including every
connection, machine specifications, build settings and source fingerprint.

These samples vary substantially from the earlier observations in the
[measurement history](services/transaction-log-exports/benches/README.md#three-target-measurement-2026-09-16).
The mixed comparison does not establish a compiler regression or justify a
downgrade. This desktop has uncontrolled background activity, thermal state
and power limits; these observations are not an approved regression baseline.
<!-- benchmark-results:end -->

Each sample uses eight connections, 2 KiB payloads and 8,192-record batches after
an untimed warmup. Published writer samples use serialization; existing-record
copying is a separate supported workload.

<details>
<summary>Design and maintenance notes</summary>

Each measured sample transfers **100,000,000 records across eight connections**:
12,500,000 records per connection. Payloads are 2,048 bytes; the complete encoded
record is 2,064 bytes. That is **206.4 GB of encoded traffic per sample**, generated
progressively rather than allocated as a single fixture.

The configuration uses 8,192-record sender batches, a separate one-million-record
warmup, no retained-record window, `TCP_NODELAY`, default OS socket buffers and no
CPU affinity. Eight sender threads and eight receiver threads share one machine.
Writer input is synthetic; it is not a measurement of a particular application
serializer.

Machine and build context:

| Component | Specification |
| --- | --- |
| CPU | Intel Core i9-12900HK |
| Cores | 14 physical, 20 logical; 20 available to the benchmark process |
| RAM | 63.68 GiB reported physical memory |
| OS | Windows 11 Pro, build 10.0.26200 |
| Power plan | Balanced |
| Rust / LLVM | Rust 1.98.1 (`48a229cea`) and 1.98.0 (`88d9e12ae`) / LLVM 22.1.8 |
| Target | `x86_64-pc-windows-msvc` |
| Build | Optimized Cargo bench profile; no additional Rust flags |

The [benchmark specification and measurement history](services/transaction-log-exports/benches/README.md)
provides timing boundaries, per-connection validation and earlier observations.

### Interpreting the numbers

The reader benchmark excludes timed writer encoding and CRC generation. The
writer benchmark excludes reader parsing and validation. The combined benchmark
includes both sides. All three still include loopback transport, backpressure,
memory bandwidth and scheduling; they are not isolated CPU microbenchmarks.

Each transfer must reach clean EOF and match its encoded byte count. Targets with
a production reader also validate and count every record. Elapsed time uses a
common start through completion, not a sum of concurrent worker durations.
The writer targets include the last sender or receiver completion; the reader
target uses the last reader completion. No disk or replication work is included.

Desktop background activity and thermal state are uncontrolled. Treat these
results as reproducible workload observations, not service guarantees. Compare
repeated runs on the same target and configuration before attributing a difference
to a code change. Throughput-derived nanoseconds per record are not latency
percentiles, and subtracting concurrent benchmark times does not isolate the
cost of one component.

</details>

### Run and archive

Requires Python 3.11+, Cargo, rustc and Git. Start with a short harness check:

```sh
python scripts/benchmarks.py run --records 10003 --warmup-records 1001 --runs 2 --include-copy
```

The full `python scripts/benchmarks.py run` builds first and measures serially,
defaulting to three 100-million-record samples per target. It retains JSON, CSV,
Markdown and raw logs under ignored `benchmark-results/`. The
[automation guide](scripts/README.md) covers reporting, compatibility checks,
explicit regression budgets and preserving complete bundles. Ordinary tests skip
long transfers. There is no configured performance CI or external artifact service.

<a id="workspace-and-development"></a>

## Development and documentation

```sh
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo doc --workspace --no-deps --open
```

Crate overviews lead to module specifications for APIs, ownership, failure behavior
and maintained design rationale. README examples included in Rustdoc are checked
as documentation tests; the service's binary target needs the
[manual doctest procedure](services/transaction-log/src/streams/README.md#verification-and-performance).

<details>
<summary>Design and maintenance notes</summary>

Owned [storage fixtures](services/transaction-log/src/storage/README.md#shared-test-storage)
clean test files on normal completion or unwinding while retaining empty stream
bases for reuse. Platform-specific filesystem tests cover Windows and Unix rules.

[Editor recommendations](.vscode/extensions.json) include rust-analyzer, CodeLLDB
and Dependi. The **Debug Transaction Log** launch configuration builds and runs the
scaffold under the debugger; external-code stepping filters in
[settings](.vscode/settings.json) are best-effort. Repository-wide contribution and
agent guidance is maintained in [AGENTS.md](AGENTS.md).

</details>

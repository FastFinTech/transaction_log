# Transaction Log

A performance-focused Rust transaction log for event-sourced CQRS systems.
The project is building toward a replicated service that accepts ordered event
streams, persists them, and serves stream-specific reads. Throughput, predictable
latency, buffer ownership and the cost of each record operation drive the design.

**Status:** the record format, reader, writer and TCP benchmark suite are
implemented, alongside log-file identities, record-range location models, path
layout and stream-directory initialization. An indexed log writer now appends
ordered records and dense offsets to an owned file pair, with explicit flush,
sync and finalization. File recovery validates from a supplied trusted boundary,
repairs indexes and hands over partial or full files. The service executable is a
scaffold; startup recovery orchestration, queries, replication and client protocol
handling remain in development.

The service entry point (`services/transaction-log/src/main.rs`) loads
raw startup inputs from CLI arguments and environment variables. `--help`
prints generated option descriptions; missing required inputs produce a loading
error. After loading, it prints `Transaction Log v<version>` to standard output,
then exits. No storage access or setup is wired into startup.
Typed conversion validates the fixed three-node topology; server startup and log
recovery remain planned. Storage defaults to `/data` on Linux and `./data` on Windows;
`--storage-directory` or `TL_STORAGE_DIRECTORY` overrides it for local runs.
Cargo embeds the service's inherited workspace package version at compile time;
the executable does not read a manifest or runtime version setting. CI can assign
a release version before compilation; automated version assignment and version
handshakes are not yet implemented.

## Design priorities

- **Measure the hot path.** Independent reader and writer benchmarks support
  focused changes; a combined benchmark checks their effect on the complete
  record I/O path. Performance claims include workload and machine context.
- **Keep ownership explicit.** Read records retain shared ownership of their bytes
  across reader refills. Writers serialize directly into reusable batch storage.
- **Keep record creation synchronous.** Applications choose when to send a batch,
  flush destination buffers and synchronize file data.
- **Validate at boundaries.** The reader validates framing, stream IDs and CRC
  before exposing records. Accessors consume that established validity.
- **Use static dispatch.** Writer constructors select serialization or
  existing-record copying at compile time, preventing accidental API mixing
  without a runtime mode branch.

The design target has been 100 million records per minute for record I/O.
The measurements below cover loopback transport and the implemented record layer;
they do not establish that rate for durable storage or the planned cluster.

## Benchmarks

Performance is a primary deliverable of this project. The suite currently covers:

| Target | Producer | Receiver | Purpose |
| --- | --- | --- | --- |
| `record_reader` | Replayed prebuilt bytes | Production reader, with CRC validation | Evaluate reader changes independently of the writer. |
| `record_writer` | Production writer | Raw byte drain | Evaluate writer changes independently of the reader. |
| `record_io` | Production writer | Production reader, with CRC validation | Measure the combined path over loopback TCP. |

The writer targets support two workloads; the published writer measurements use
serialization:

- **Serialize:** a callback writes a 2 KiB payload in eight-byte chunks, then the
  writer finalizes its header and CRC.
- **Copy:** `write_record` appends existing validated records to the output buffer.
  Fixture creation and initial validation happen before timing; copying and
  sending are measured. The combined receiver still validates every received CRC.

**Benchmarks will be added as system implementation continues.** Planned coverage
includes stream-file append and replay, indexing and queries, durable flushes,
replication, recovery, and application-level latency under load. Those results
will be reported separately so a socket result cannot be mistaken for durable
transaction-log throughput.

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

### Run and archive the complete suite

Requires Rust/Cargo, Git and Python 3.11+; the runner uses only Python's standard
library. From the repository root:

```sh
python scripts/benchmarks.py run
```

This builds all three targets once, then runs them serially. Defaults are
100 million records per sample, eight connections, 2 KiB payloads and **three
measured samples per target**. The published quick comparisons used `--runs 1`.
To also measure existing-record copying:

```sh
python scripts/benchmarks.py run --include-copy
```

Start with a shorter harness check:

```sh
python scripts/benchmarks.py run --records 10003 --warmup-records 1001 --runs 2 --include-copy
```

Every invocation creates a new directory under `benchmark-results/`, containing:

| Artifact | Contents |
| --- | --- |
| `results.json` | Versioned machine, toolchain, source identity, configuration and per-sample/per-connection results. |
| `samples.csv` | Normalized samples for analysis and trend charts. |
| `summary.md` | Generated machine/workload context and median results table. |
| `*.log` | Full benchmark output and Cargo build diagnostics. |

Each benchmark also prints machine specifications when invoked directly with
`cargo bench`. The suite records Git revision, dirty-tree state, a source/config
fingerprint, compiler settings, timestamps and exact commands. It preserves
partial results when a case fails and refuses to overwrite an existing run.
The local results directory is Git-ignored and survives `cargo clean`; archive
it as a CI artifact or in durable storage to retain history across build agents.

Individual targets remain independently callable:

```sh
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8
cargo bench -p transaction-log-exports --bench record_writer --locked -- --connections 8
cargo bench -p transaction-log-exports --bench record_io --locked -- --connections 8
```

These commands default to one measured sample. Ordinary and all-target test runs
skip long benchmark workloads.

### Regression tracking and build integration

The runner can be called from a build or deployment pipeline, then its result
bundle uploaded even if the build fails. Tables can be regenerated without
rerunning the workload:

```sh
python scripts/benchmarks.py report path/to/results.json --output report.md
python scripts/benchmarks.py compare path/to/baseline/results.json path/to/current/results.json --max-regression-percent 5 --output comparison.md
```

Comparisons use median throughput for matching workloads and environments.
Machine, compiler, build-setting or workload mismatches are rejected by default;
available RAM is recorded but excluded from comparison identity. A slowdown beyond
the explicitly chosen budget returns a nonzero exit code. The 5% example is not an
established project threshold: a stable runner and sufficient samples are needed
before setting one.

Keep approved baselines rather than automatically promoting each new build.
There is no CI-provider-specific workflow or artifact-retention service configured
yet. [Automation documentation](scripts/README.md) describes the result schema,
failure handling, comparison rules and integration commands.

## Implementation progress

- [x] Rust workspace, async application entry point and editor/debugger configuration.
- [x] Binary record protocol, typed stream/sequence identifiers and CRC-32C framing.
- [x] Immutable record ownership and inexpensive header/payload access.
- [x] Async reader with batch validation and records that survive subsequent reads.
- [x] Synchronous record builder and compile-time writer modes.
- [x] Explicit async buffer output, destination flushing and optional file data synchronization.
- [x] Single-threaded object-pool utility with unused-object reclamation.
- [x] Reusable async Serde message I/O with bounded, length-prefixed Postcard frames; application messages and connection integration remain pending.
- [x] Storage provider with owned startup configuration, deterministic log/index/checkpoint paths and stream-directory initialization.
- [x] Typed, validated log-file numbers and identities, with arithmetic mapping from record IDs to file ranges.
- [x] Typed record start/end locations and a range model with lazy enumeration of bounded, postfix, whole-file and prefix reads.
- [x] Stream-owned file identities, record endpoints/ranges and checkpoint model, with validated Serde metadata and checkpoint persistence; the stream initializer is disabled pending adaptation to the current validators.
- [x] Indexed log writer with stream/sequence checks, log-before-index flushing, separate flushed/durable progress and full-file finalization.
- [x] File-only index writer with reusable buffering and separate buffer output, file flushing and data synchronization.
- [x] Provider-based recovery file acquisition, supplied-boundary validation, unified index/log repair and synchronized partial/full handover.
- [x] Independent reader/writer and combined loopback benchmarks.
- [x] Benchmark artifacts with machine specifications, report generation and regression comparison.
- [ ] Service connection setup, handshake and client lifecycle.
- [ ] Stream routing, ingestion queues, admission control and periodic progress/status messages.
- [ ] Client ingestion rejection/disconnection.
- [ ] Multi-file lifecycle coordination and stream-specific index queries.
- [ ] Per-stream recovery coordination through checkpoints, pair validation, later-file cleanup and checkpoint advancement; the initializer scaffold is currently disabled.
- [ ] Active-writer preparation, durable flush scheduling and startup-wide recovery orchestration.
- [ ] Replication and cluster coordination.
- [x] Raw and validated single/cluster configuration with fixed node slots and derived DNS hostnames; storage supports configuration read/write, while startup integration remains deferred.
- [ ] Storage, indexing, replication, recovery and latency benchmarks.
- [ ] Dedicated performance CI workers and durable benchmark-history publication.

The checklist distinguishes implemented I/O components from service behavior.
The indexed log writer coordinates an individual pair; the service's persistence
schedule, live file rotation and startup integration remain to be implemented.
Pair recovery and storage-level checkpoint publication are implemented. The
stream initializer remains disabled; its adaptation and writer preparation are deferred.

## Workspace and development

| Path | Responsibility |
| --- | --- |
| [`services/transaction-log`](services/transaction-log) | Service scaffold, storage models/paths, file acquisition, indexed appends and pair validation/repair. |
| [`services/transaction-log-exports`](services/transaction-log-exports) | Public record types, reader, writer and benchmarks. |
| [`lib/object-pool`](lib/object-pool) | Reusable single-threaded object pool. |
| [`lib/message-io`](lib/message-io) | Async extension traits for bounded length-prefixed Postcard messages. |
| [`scripts`](scripts/README.md) | Benchmark execution, reporting and comparison tooling. |
| [`benchmarks`](benchmarks/README.md) | Selected structured measurements published with the repository. |

The workspace uses Rust edition 2024 and is currently tested with Rust 1.98.1.

```sh
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo run -- --cluster-mode single --storage-directory ./data
cargo run -- --help
```

The application validates configuration, prints its embedded version and exits. Storage startup integration remains deferred.
For VS Code or Devin, install the extensions recommended in
[.vscode/extensions.json](.vscode/extensions.json), including rust-analyzer,
CodeLLDB and Dependi. The **Debug Transaction Log** launch configuration builds
and launches the app under the debugger. External-code stepping filters are best-effort;
their configuration is in [.vscode/settings.json](.vscode/settings.json).

## Documentation

This root README is the repository entry point: project purpose, implementation
status, performance evidence and how to build or evaluate the project.
Crate documentation is the API reference, with examples and ownership/error
contracts for library consumers. Module READMEs preserve detailed requirements,
design decisions, safety invariants and optimization evidence.

| Documentation | Scope |
| --- | --- |
| [Clustering and read replicas](CLUSTERING.md) | Planned permanent deployment modes, fixed membership/master, startup gating, replica-loss exit policy and Kubernetes deployment requirements; open replication/read contracts. |
| [Clustering](services/transaction-log/src/clustering/README.md) | Module boundaries and planned handshake/runtime responsibilities. |
| [Clustering configuration](services/transaction-log/src/clustering/configuration/README.md) | Deployment settings, established configuration with permanent UUID, fixed topology, node/domain parsing and Serde contracts. |
| [Raw clustering inputs](services/transaction-log/src/configuration/README.md) | Mode, local node name and cluster domain with environment, CLI and file-document attributes; startup loading and typed validation. |
| [Application messages](services/transaction-log/src/messages/README.md) | Collection point for application wire messages and their message-io/Postcard wire contract. |
| [Exports crate overview](services/transaction-log-exports/src/lib.rs) | Public types and API entry points. |
| [Record specification](services/transaction-log-exports/src/record/README.md) | Wire format, ownership, validation and reader integration. |
| [Writer specification](services/transaction-log-exports/src/record_writer/README.md) | Constructor modes, buffering, completion, cancellation and hot-path rationale. |
| [Storage](services/transaction-log/src/storage/README.md) | Root configuration, physical paths, file acquisition, directory initialization and configuration persistence. |
| [Stream locations](services/transaction-log/src/streams/location/README.md) | Logical file identities, sequence grouping, record boundaries and lazy range enumeration. |
| [Stream checkpoint](services/transaction-log/src/streams/README.md#stream-checkpoint-model) | Stream-owned checkpoint model, Serde representation and certification/publication contract; storage read/write are implemented, while initializer advancement is disabled. |
| [Stream operations](services/transaction-log/src/streams/README.md) | Indexed appends, validation, repair, partial/full handover and failure handling. |
| [Object pool](lib/object-pool/README.md) | Ownership, reclamation policy and usage. |
| [Message I/O](lib/message-io/README.md) | Framing, Serde extension APIs, size limits, ownership and partial-I/O/cancellation contracts. |
| [Benchmark specification](services/transaction-log-exports/benches/README.md) | Workloads, timing contracts and measurement history. |

Several module READMEs are included directly in Rustdoc and their Rust examples
are checked as documentation tests. Keep API details there and link from this page,
rather than maintaining a second copy in the root README. Changes to API contracts
and their rationale should update the relevant module documentation and tests.

Build the library documentation locally:

```sh
cargo doc -p transaction-log-exports --no-deps --open
```

# Published benchmark measurements

Selected completed suites behind the [repository performance table](../README.md#benchmarks).
Each JSON retains workload, machine/toolchain context and source identity. These
single-sample desktop observations are not service guarantees or approved regression
baselines.

## Measurements

| Saved suite | Workload |
| --- | --- |
| [Rust 1.98.1](2026-09-16-rust-1.98.1.json) | Reader, writer/serialize and combined/serialize; one 100-million-record sample each. |
| [Rust 1.98.0](2026-09-16-rust-1.98.0.json) | The same source fingerprint and workload settings on the same recorded machine. |

Both use eight connections, 2 KiB payloads, 8,192-record batches and a separate
one-million-record warmup. Copy workloads are supported but absent from these suites.

## Usage

Regenerate reports without rerunning measurements:

```sh
python scripts/benchmarks.py report benchmarks/2026-09-16-rust-1.98.1.json
python scripts/benchmarks.py report benchmarks/2026-09-16-rust-1.98.0.json
```

## Interpretation and provenance

The source was a dirty working tree, identified by its captured hash rather than
the Git revision alone. Samples ran serially on a shared Windows desktop without
controlled background load, affinity or thermal state. Mixed results do not
establish a compiler regression.

<details>
<summary>Design and maintenance notes</summary>

### Observations: 2026-09-16

Two completed suites are retained:

- [Rust 1.98.1](2026-09-16-rust-1.98.1.json).
- [Rust 1.98.0](2026-09-16-rust-1.98.0.json).

Each records one 100-million-record sample for the reader, writer/serialize and
combined/serialize workloads. Each sample uses eight connections, 2 KiB payloads
and 8,192-record batches. A separate one-million-record warmup precedes each case.
Source fingerprints and workload settings match across the two suites. Copying
is supported by the harness but is not part of these published suites.

The source was an uncommitted working tree based on the recorded Git revision.
Use the source SHA-256 and captured build configuration to identify it; the Git
revision alone does not represent the measured source. Measurements ran serially
on the recorded Windows desktop, without controlled affinity, thermal state or
background workload. The mixed results do not establish a compiler regression.
These are throughput observations, not service guarantees or approved CI
regression baselines. An earlier interrupted suite remains in local artifacts;
it is not represented here as a completed measurement suite.

Regenerate the table directly from the saved data:

```sh
python scripts/benchmarks.py report benchmarks/2026-09-16-rust-1.98.1.json
python scripts/benchmarks.py report benchmarks/2026-09-16-rust-1.98.0.json
```

Repeat its workloads on matching hardware/build settings:

```sh
python scripts/benchmarks.py run --runs 1
```

Select the desired installed toolchain for the command's process when making a
compiler comparison; no repository pin or global default change is required.
Keep all samples and the conditions of each run when comparing results.

The [benchmark specification](../services/transaction-log-exports/benches/README.md)
defines timing boundaries, validity checks and interpretation. The
[automation specification](../scripts/README.md) defines the result schema,
comparison compatibility, failure handling and exit codes. New measurements
should preserve those contracts or explicitly document why comparison is no
longer valid. Update the root table from saved results, never from estimated or
extrapolated rates. Workload additions should extend the suite and its contracts
as the service implementation progresses.

### Artifact retention

This directory holds selected, reviewed JSON result files behind the performance
tables in the [root README](../README.md). It is a durable repository record of
those observations, not a directory for every local or CI run. Never silently
replace an older reference with the latest build's numbers.

The [suite runner](../scripts/README.md) writes complete bundles under the
Git-ignored `benchmark-results/` directory, including JSON, CSV, generated Markdown,
build logs and raw benchmark logs. Preserve those complete bundles as build
artifacts or in durable storage. The JSON retained here contains normalized and
original metrics, per-connection results, exact commands, workload parameters,
machine/toolchain specifications and source identity. Its log filenames refer to
the original bundle; raw logs and compiled binaries are not committed here.

</details>

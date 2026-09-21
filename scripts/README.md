# Benchmark automation

Build, run, archive, report and compare the record I/O suite with Python 3.11+ and
its standard library. Cargo, rustc and Git must be on PATH. Commands resolve the
workspace from the script's location and can run from any working directory.

## Types and modules

| Command | Responsibility |
| --- | --- |
| [`benchmarks.py run`](benchmarks.py) | Build selected targets once, measure serially and retain a new result bundle. |
| `report` | Render saved JSON without building or measuring. |
| `compare` | Compare compatible completed suites against an explicit regression budget. |
| `self-test` | Exercise parsing, reporting and comparison without Rust builds or transfers. |

## Usage

```sh
python scripts/benchmarks.py run --records 10003 --warmup-records 1001 --runs 2 --include-copy
python scripts/benchmarks.py report benchmarks/2026-09-16-rust-1.98.1.json
```

The default full run uses three 100-million-record samples per target and eight
connections. `--help` lists options. Reporting reads existing data only; the runner
does not deploy, publish artifacts externally or edit the repository README.

<details>
<summary>Design and maintenance notes</summary>

### Run the suite

```sh
# Default: three 100-million-record samples per target, eight connections,
# 2 KiB payloads, 8,192-record batches, one-million-record warmup per target.
python scripts/benchmarks.py run

# Fast harness check, including the existing-record-copy workload:
python scripts/benchmarks.py run --records 10003 --warmup-records 1001 --runs 2 --include-copy

# Choose a fresh artifact directory, useful for a CI job:
python scripts/benchmarks.py run --output benchmark-results/build-123 --label perf-worker-01
```

All selected executables are built with `cargo bench --no-run --locked` before
any measurements. Cargo's JSON artifact messages identify executable paths;
the runner does not guess hashed filenames or invoke Cargo between measurements.
It then runs each target serially with explicit arguments and `--bench`.
`record_reader` uses its prebuilt sender; the writer and combined targets default
to serialization. `--include-copy` adds a copy case for each selected writer target.
The [benchmark README](../services/transaction-log-exports/benches/README.md) owns
workload and timing contracts. No production timing loop is replaced by this script.

The suite's default connection count is **eight** and sample count is **three**;
individual `cargo bench` targets still default to one connection and one sample.
Other flags include `--targets`, `--payload-bytes`, `--batch-records`,
`--write-chunk-bytes`, `--retain-records`, `--warmup-records` and `--runs`.
Retention requires omitting the raw-drain writer target. A zero warmup disables it.
`--timeout-seconds` defaults to 3,600 seconds for each build/case, including all
samples in that case; zero disables the timeout. Timeout is a failed run, not a
valid throughput observation. `--help` describes the full CLI.

</details>

## Behavior and guarantees

Every run gets a new bundle; an existing output directory is never overwritten.
JSON is authoritative and retains completed earlier cases if later work fails.
Failed, incomplete or still-running suites cannot become successful regression
baselines. Timeout defaults to 3,600 seconds per build/case; zero disables it.

<details>
<summary>Design and maintenance notes</summary>

### Result bundles and schema

The default directory is `benchmark-results/<UTC timestamp>-<commit>-<unique ID>`.
This is ignored by Git and survives `cargo clean`. An explicit `--output` must
name a new directory; a previous run is never overwritten. Archive entire bundles
as CI artifacts or in durable storage to retain history across ephemeral workers.
Git-ignored local storage alone is not an external history service.

| File | Purpose |
| --- | --- |
| `results.json` | Versioned structured source for reporting and comparisons. |
| `samples.csv` | One normalized row per measured sample, suitable for trend charts or ingestion. |
| `summary.md` | Generated machine/workload context and median results table. |
| `record_*-*.log` | Complete benchmark stdout/stderr, including machine metadata and per-connection results. |
| `build.log` | Cargo JSON build output and diagnostics. |

JSON schema version 1 contains:

- Suite start/end UTC timestamps, status, optional stable runner label and Python version.
- Git commit, dirty flag/status, and SHA-256 of Rust/Python/Cargo source/config files,
  including untracked additions. Documentation/results are excluded from that hash.
- Rust/compiler and Cargo versions, workspace profile settings, explicit relevant build environment
  variables, and hashes of workspace/user Cargo config files. It never dumps the
  full environment or credentials. Bench and inherited release-profile environment
  overrides are included. Ancestor/global configuration outside these
  locations is not exhaustively discovered; use a controlled runner configuration.
- Exact workload settings and executed argv arrays, build exit code, and raw log names.
- Per-case status and the benchmark's `ENVIRONMENT` object: CPU, cores, RAM, OS,
  architecture, available parallelism and power plan/governor where available.
- Normalized samples with elapsed seconds and throughput, original aggregate metrics,
  per-connection metrics, and median/min/max summaries across samples.

Metadata comes from the [shared Rust collector](../services/transaction-log-exports/benches/environment/README.md),
so direct `cargo bench` output also carries machine specifications. Unknown facts
remain null/absent. Available RAM is an observation, not installed DIMM capacity.
Hardware totals, affinity and container/cgroup resource limits are not equivalent;
available parallelism is included but is not a complete resource-limit inventory.

The runner validates every measured run and per-connection count, encoded byte
totals, batching where applicable, common-start timing maxima, workload identity,
and finite metrics. It rejects missing/duplicate/malformed output rather than
publishing a misleading result. It verifies the source/config hash again after
measurement and fails the suite if it changed. Toolchain and captured Cargo
configuration are checked again too. A dirty checkout is allowed, but
is never represented as the committed build alone.

`results.json` is checkpointed atomically before/after cases and finalized on
ordinary errors, timeouts and keyboard interruption. Successful earlier cases
remain available if a later one fails; the suite is marked failed/incomplete and
cannot serve as a successful regression baseline. A hard process/host termination
can leave a `running` checkpoint, which comparisons also reject. The CSV and
Markdown are convenience views; JSON is authoritative.

</details>

### Comparisons

Comparison requires matching workloads and, by default, matching build/machine
identity. The regression budget is explicit. Cross-environment comparison requires
an override and remains labelled; matching metadata does not establish matching
thermal or background conditions.

<details>
<summary>Design and maintenance notes</summary>

### Regenerate a table or compare builds

```sh
python scripts/benchmarks.py report benchmark-results/build-123/results.json
python scripts/benchmarks.py report benchmark-results/build-123/results.json --output report.md

python scripts/benchmarks.py compare path/to/baseline/results.json benchmark-results/build-123/results.json --max-regression-percent 5 --output comparison.md
```

`report` reads JSON only; it does not build or rerun benchmarks. Use its table for
reviewed README updates. Keep a selected reference JSON under `benchmarks/` when
publishing results in the repository; retain full run bundles separately.

`compare` compares median million-records/minute for exactly matching target/workload
sets and workload parameters. Different sample counts are allowed, but use enough
samples to understand variability. Code revisions are expected to differ; compiler,
build settings, machine specs and runner label must match by default. Volatile
available RAM is excluded from comparison identity. Missing CPU/memory/OS identity
also blocks automatic comparison. `--allow-environment-change` permits an explicit
cross-environment comparison and prominently labels it; workload mismatches remain
errors. Power-plan names and OS versions are compared as reported, so a changed
environment normally needs a newly reviewed baseline.

The regression budget is deliberately required, not silently selected by the tool.
A 5% example is not an established project performance policy. A drop greater than
the requested budget fails; improvements do not. CPU thermal state and background
load remain potential sources of variability even with matching metadata. CI
should use stable dedicated hardware, preserve baselines rather than automatically
promoting the latest build, and investigate repeated slowdowns before changing one.

Exit codes: **0** success/within budget; **1** build, execution, parsing, metadata,
configuration or comparison incompatibility; **2** a measured regression beyond
budget (`argparse` also uses 2 for malformed command syntax). Upload artifacts even
when the runner or comparison fails. The provider-specific CI workflow and artifact
retention policy are intentionally left to the hosting/build infrastructure.

</details>

## Performance

The script orchestrates and checks results outside the Rust timing loops. It
compares median throughput and reports spread across samples. Short harness checks
are not stable performance baselines; workload/timing contracts belong to the
[benchmark specification](../services/transaction-log-exports/benches/README.md).

<a id="verification"></a>

## Validation

```sh
python scripts/benchmarks.py self-test
```

<details>
<summary>Design and maintenance notes</summary>

Self-tests exercise report round trips, partial status, malformed/truncated data,
timing/count validation, regression thresholds and incompatible environments without
compiling Rust or transferring records. Small actual suites separately validate
execution, Cargo target discovery and argument forwarding, including no-overwrite
behavior, failed-run persistence and exit codes. Rust tests/Clippy check the metadata
collector, while ordinary/all-target runs confirm probes and transfers are skipped.

</details>

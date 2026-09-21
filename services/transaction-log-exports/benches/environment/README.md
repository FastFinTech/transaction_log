# Benchmark machine metadata

Report performance-relevant machine context before warmup and timed work. Every
benchmark emits one `ENVIRONMENT` JSON line; the suite runner retains it alongside
the workload, source and build identity.

## Types and modules

| Entry point | Responsibility |
| --- | --- |
| [`print()`](mod.rs) | Probe the host and emit a schema-version-1 metadata object. |
| [Suite runner](../../../../scripts/README.md) | Archive the object and apply comparison compatibility checks. |

## Usage

An explicitly requested benchmark emits metadata automatically. Its stdout contains:

```text
ENVIRONMENT { ... schema-version-1 machine fields ... }
```

The line is machine-readable JSON after the prefix. Help and ordinary/all-target
tests exit before probing; there is no production-service call site.

## Behavior and guarantees

Unknown values remain omitted or null. Probe failures do not stop a transfer, but
the runner rejects automatic comparisons lacking sufficient machine identity unless
explicitly overridden. No serial numbers, usernames, hostnames or full environment
dumps are collected.

<details>
<summary>Design and maintenance notes</summary>

**Platform probes.** Windows uses PowerShell/CIM for CPU model, physical/logical
counts, total/available RAM, OS version and active power plan. Linux reads procfs
and os-release, including the frequency governor when available. macOS uses sysctl
and sw_vers. Architecture, platform and Rust's available parallelism are always
added; affinity, containers and scheduling limits can make that parallelism differ
from hardware CPU counts.

Toolchain, explicit compiler settings and Git/source identity belong to the runner
that builds the executable. Querying whichever rustc later appears on PATH cannot
prove which compiler built a standalone binary. Available RAM is a changing
observation and is excluded from comparison identity.

</details>

## Performance

All probes run outside warmup and timing. Process launch and metadata collection
costs are not benchmarked or added to per-record work.

## Validation

Validation parses each target's emitted JSON and checks its retention in suite
reports. Help/test-mode invocations exercise the no-probe guard.

<details>
<summary>Design and maintenance notes</summary>

The Windows collector has been exercised locally. Linux/macOS probes are
best-effort implementations awaiting validation on those hosts. Missing-value and
compatibility checks belong to the runner's self-tests; host probes cannot invent
values simply to make comparisons pass.

</details>

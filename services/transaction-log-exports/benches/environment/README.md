# Benchmark machine metadata

`mod.rs` provides `print()`, called once by each benchmark after its explicit-run
guard and configuration checks, before warmup and timed work. It emits an
`ENVIRONMENT` line containing a JSON object with schema version 1. The suite
runner preserves that object in each case's log and structured results.

Windows uses PowerShell/CIM for CPU model, physical and logical CPU counts, total
and available RAM, OS version and active power plan. Linux reads procfs and
os-release and reports the CPU frequency governor when available. macOS uses
sysctl and sw_vers. Architecture, platform and Rust's available parallelism are
always added; available parallelism can differ from hardware CPU count under
affinity, containers or scheduling limits. Unknown facts are omitted or null,
never represented as invented zero values. Failed metadata probes do not stop
the transfer; the runner refuses automatic comparisons with insufficient machine
identity unless the caller explicitly overrides that restriction.

Only performance-relevant machine information is collected. There are no serial
numbers, usernames, hostnames or full environment-variable dumps. Toolchain,
explicit compiler settings and Git/source identity are captured by the suite
runner, which builds the executables itself. A standalone executable cannot prove
which compiler built it by querying whatever rustc happens to be on PATH later.

This module is shared by all three targets and has no dependencies in production
code. `serde_json` is a development dependency. All probes happen outside timing;
help and ordinary/all-target tests must exit before any probe. The Windows path
has been exercised locally. Linux/macOS paths are best-effort implementations
that still need validation on those hosts. Changes should be verified by parsing
each target's emitted JSON and checking the suite reports retain it. Keep missing
values visible, and exclude volatile available RAM from environment comparisons.

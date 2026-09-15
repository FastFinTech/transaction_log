# Transaction Log

A Rust workspace organized into:

- `lib/`: shared library crates, including `object-pool`.
- `services/transaction-log/`: the Transaction Log application.
- `services/transaction-log-exports/`: the application's public exports crate.

The application imports `transaction-log-exports` and uses Tokio for its async
entry point and anyhow for error handling.

The app's [log file provider](services/transaction-log/src/log_file_provider/README.md)
currently stores the configured root directory for stream files. Filesystem
operations and startup configuration are not implemented yet.

Run the application from the workspace root:

```sh
cargo run
```

Expected output:

```text
Hello world
```

## Records

The exports crate provides immutable records, typed identifiers, and an async
record reader. The [record module specification](services/transaction-log-exports/src/record/README.md)
documents the wire/file format, ownership and validation contracts, hot-path
decisions and evidence, testing guidance, and planned writing/service behavior.

Read that specification before changing records, their protocol, or their
reader/writer integration. It is also included in the module's generated Rust
documentation; keep the detailed requirements there.

The [record writer module](services/transaction-log-exports/src/record_writer/README.md)
provides `RecordWriter::write(id, write_body)` for writing opaque record bodies
through a callback directly into reusable storage, including header/CRC finalization
and rollback. Shared output coordination and socket output are planned separately.

The [object pool](lib/object-pool/README.md) provides synchronous ownership
transfer through an executor's exclusive mutable access, without internal locking.
It grows without a fixed limit and uses `maintenance_tick()` to reclaim the minimum
available count tracked throughout each configured group of ticks, keeping collection
capacity for refilling. The owner supplies the maintenance cadence. Writer integration
and the periodic timer remain planned.

## Reader performance

Run the opt-in TCP benchmark with 100 million records and 2 KiB payloads:

```sh
cargo bench -p transaction-log-exports --bench record_reader --locked
```

To divide that total across eight connections and eight reader threads:

```sh
cargo bench -p transaction-log-exports --bench record_reader --locked -- --connections 8
```

The [benchmark README](services/transaction-log-exports/benches/README.md)
documents short runs, configuration, timing boundaries, and result interpretation.
Routine test runs do not launch the long benchmark.

## Debugging in VS Code or Devin

Both editors use the shared `.vscode/` configuration. Open this workspace's root
folder and install the recommended **rust-analyzer** and **CodeLLDB** extensions
in each editor.

Set a breakpoint on the `println!` line in
`services/transaction-log/src/main.rs`, select **Debug Transaction Log** in
**Run and Debug**, and press **F5**. The launch configuration builds the app with
Cargo, discovers the executable automatically, and starts it in the integrated
terminal with Rust backtraces enabled.

The launch profile uses [CodeLLDB's Cargo support](https://github.com/vadimcn/codelldb/blob/master/MANUAL.md#cargo-support),
so it does not require a separate build task or a hardcoded executable path.

Workspace settings configure LLDB's Step Into filter to skip functions whose
names start with `std`, `core`, `alloc`, `tokio`, `anyhow`, `bytes`, `crc32c`, or `pin_project_lite`
crate paths (including paths beginning with `<`). Stepping also avoids functions
without debug information. Add new external crate names to `step-avoid-regexp`
in `.vscode/settings.json` as dependencies grow; use underscores for crate names
containing hyphens.

This is a best-effort stepping filter, not a complete "Just My Code" mode.
Explicit breakpoints still work in dependencies, and stepping out of your code
can still land in runtime or generated code. Restart the debug session after
changing these settings.

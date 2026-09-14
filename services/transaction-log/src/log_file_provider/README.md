# Log file provider

This application module is the starting point for managing stream log files
under a configured root directory. It lives in the Transaction Log app; it is
not part of the public record/reader exports crate. The module is compiled into
the app, but application startup does not construct a provider yet.

`mod.rs` exposes `LogFileProvider`, whose implementation lives in `provider.rs`.
This README is also included in the module's Rust documentation. Keep module
requirements here and local API contracts beside the implementation.

## Implemented contract

- `LogFileProvider::new(root_directory: PathBuf)` takes ownership of the path,
  so the provider does not borrow its caller's configuration.
- Construction stores the path as supplied. It does not normalize it, require
  an absolute path, check existence or permissions, create a directory, or open
  any files. No filesystem failure can occur during this constructor.
- `root_directory()` borrows the stored path as `&Path` without allocation.
- The provider currently holds no file handles, background tasks, or shared
  state. It performs no I/O when constructed or dropped.

## Design boundary and future work

The intended storage design has a log file and an index file per stream.
This scaffold does not decide naming conventions, directory layout, index
ownership, handle caching, concurrency, opening modes, or recovery behavior.
Those decisions belong to subsequent implementation work. Do not infer that
successful construction proves that the path exists or is suitable for storage.

File operations and their error types are not implemented. A future operation
that accesses the filesystem must report its failures at the appropriate API
boundary. The constructor currently needs neither an async runtime nor a
`Result`; storing a path alone does not justify either.

There is no record-processing hot path in this module yet and no performance
claim for it. Preserve the application's throughput requirements when designing
file access, especially handle reuse and work performed per record. This module
does not change the record format or the reader's validation responsibilities.
Read the [record specification](../../../transaction-log-exports/src/record/README.md)
before integrating record I/O.

## Verification and maintenance

Compile the app and run formatting and Clippy checks when changing this scaffold.
There are no filesystem behaviors to test yet; a unit test that merely stores
and returns a `PathBuf` would add little value. As file behavior is introduced,
add same-file tests for its actual contracts and failure cases, and update this
README to distinguish implemented behavior from planned work.

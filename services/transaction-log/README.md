# Transaction Log service

The application crate combines storage and stream operations with a minimal
executable scaffold. Record persistence and per-stream recovery are implemented;
the executable currently validates configuration, prints its version and exits.
It does not yet open storage, serve clients or run replication.

## Types and modules

| Module | Responsibility |
| --- | --- |
| [`configuration`] | Raw CLI/environment inputs and optional caller-supplied document mapping. |
| [`storage`] | Directory layout, file acquisition, discovery, removal and metadata persistence. |
| [`streams`] | Typed locations, ordered paired appends, recovery and checkpoint advancement. |
| [`clustering`] | Implemented configuration values within a scaffold for future session coordination. |
| [`messages`] | Scaffold for application gossip/control schemas and exchanges. |

## Usage

From the workspace root:

```sh
cargo run -- --cluster-mode single --storage-directory ./data
cargo run -- --help
```

The storage default is `./data` on Windows and `/data` elsewhere; the option above
only loads a path today. Help is generated from configuration fields. Missing or
invalid required settings fail before successful version output.

## Behavior and guarantees

Startup parses process arguments/environment, converts the clustering inputs into
validated values and prints `Transaction Log v<version>`. Application-wide file
source selection, stored-setting comparison, UUID assignment and startup recovery
integration remain planned.

<details>
<summary>Design and maintenance notes</summary>

**Version ownership.** Cargo embeds the inherited workspace package version at
compile time through `CARGO_PKG_VERSION`. The executable does not read a manifest
or runtime version setting. CI can assign a release version before compilation;
automated assignment and version handshakes are not implemented.

**Implemented components versus running service.** Storage can persist established
configuration and checkpoints, while the stream initializer recovers one stream,
cleans later pairs, publishes recovered progress and returns an active pair. Its
result can become an indexed writer without I/O. These APIs are usable independently
of the executable scaffold. Scheduling, live rotation, historical queries, client
admission, networking and replication still require application integration.

</details>

## Performance

No service, durable-storage or recovery throughput has been measured. The
[record benchmarks](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/benches/README.md)
exercise reusable record I/O over loopback, not this service lifecycle.

## Validation

```sh
cargo test -p transaction-log --locked
cargo test -p transaction-log --release --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

The [streams documentation-test guidance](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/README.md#verification-and-performance)
explains checking examples because Cargo skips doctests for this binary target.

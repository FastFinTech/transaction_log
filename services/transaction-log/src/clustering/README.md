# Clustering

This application module owns cluster configuration and will contain connection
handshakes and runtime cluster coordination. The [root clustering design](../../../../CLUSTERING.md)
records deployment and lifecycle decisions. Configuration is implemented; startup
integration, networking, handshakes, replication and command readiness remain
planned. The service executable does not yet start a cluster.

## Module organization

`mod.rs` declares the public `configuration` module and includes this overview in
Rustdoc. All configuration types live under `clustering::configuration`, including
`ClusteringConfiguration`, `ClusterDefinition`, `ClusterMembership`, `Member`, `MemberId`, `Hostname`,
their validation errors and the fixed minimum replica count. The
[configuration specification](configuration/README.md) owns their parsing,
validation, ownership, comparison and testing contracts. They are not re-exported
at the clustering root.

Unvalidated input strings live separately under
[`configuration::clustering`](../configuration/clustering/README.md).
Field attributes declare environment, CLI and file-document mappings. Conversion
into the validated types and startup integration remain planned.

Future handshake and runtime modules belong alongside `configuration`, rather
than inside it. Static configuration establishes valid names, distinct IDs and
hostnames, and a local identity present in the configured membership. It does not
establish peer authentication, agreement between machines, exclusive execution,
connection readiness or replication progress.

## Planned connection responsibilities

Handshake design must check persisted cluster identity, agreement on the shared
master/replica definition, and a connecting member's configured identity. Local
IDs intentionally differ between machines; replica order carries no meaning for
agreement. Active IDs must be unique, including the master's own local ID. A
connection to a configured member's hostname must verify the expected peer ID,
since distinct hostnames can lead to the same destination. Claimed identity alone
does not establish authenticated ownership of that identity.

Shared-definition validation and order-independent agreement checks are implemented
in `configuration`; the handshake itself remains planned. Wire messages,
credentials, connection ownership and duplicate-session handling
remain to be specified before implementing handshakes. Runtime owners must enforce
full-cluster startup readiness and the master's replica-loss exit policy; the
configuration module does no I/O or live-state management.

The reusable [message I/O crate](../../../../lib/message-io/README.md) supplies
bounded length-prefixed Postcard serialization through async read/write extension
traits. Application message schemas and service integration remain planned;
application-version agreement belongs before clustering rather than in the codec.

## Verification

Follow the [configuration validation guidance](configuration/README.md#validation-and-future-integration)
when changing its types. From the workspace root, run:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

Maintain separate module specifications and same-file behavior tests when adding
handshakes or runtime coordination. No cluster performance claims are established.

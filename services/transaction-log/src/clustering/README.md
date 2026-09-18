# Clustering

This application module owns validated cluster configuration and will contain
handshakes and runtime coordination. `mod.rs` declares `configuration` and includes
this overview in Rustdoc. The [root design](../../../../CLUSTERING.md) records the
agreed lifecycle. Networking, replication and readiness remain planned.

## Module boundaries

[`configuration`](configuration/README.md) owns `ClusteringConfiguration`,
`EstablishedClusteringConfiguration`, `ClusterMembership`, `NodeName`, `Hostname`
and their validation errors. Established configuration pairs deployment settings
with an internally supplied UUID in either mode. Storage saves/restores that type
directly; UUID establishment and startup integration remain deferred.
There are exactly three slots: master, replica-1
and replica-2. One cluster domain derives all node hostnames. Shared agreement is
UUID and domain equality once established; local node slots intentionally differ. Membership stores only
the local slot and domain; node hostnames are formatted when needed.

Unvalidated inputs and `conf` source attributes live under
[`configuration`](../configuration/README.md).
Startup loads and converts them, prints the version and exits. It neither accesses
storage nor starts a cluster.
Runtime modules
will belong beside validated `configuration`, rather than inside it.

## Planned session contract

All three members must connect and resynchronize before command admission. Losing
either required replica ends the operating session; reconnects and replacements
are startup-only activities. Recovery is coordinated across the cluster and
command handlers. There is no election, promotion or degraded service mode.

Handshakes must verify application version first, then persisted cluster identity,
shared domain and expected fixed node slot. Duplicate active slots must be
rejected, including the master slot. DNS can accidentally route different names
to one process, so derived names alone do not establish peer identity. Claimed
identity also does not establish authenticated ownership or storage continuity.

Runtime owners must enforce startup readiness, stop command admission on failure
and invalidate handler connections. Durable acknowledgement, in-flight outcomes,
storage reconciliation, authentication and session recovery remain unspecified.
Static configuration establishes none of these guarantees.

The [message I/O crate](../../../../lib/message-io/README.md) provides bounded
length-prefixed Postcard messages for intermittent control traffic. Application
message schemas and connection integration remain planned; version agreement
belongs before clustering rather than in the codec.

## Verification

Follow the [configuration specification](configuration/README.md) when changing
its types. Run:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

Add module specifications and same-file behavior tests for future runtime modules.
No cluster performance measurements or availability guarantees are established.

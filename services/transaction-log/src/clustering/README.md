# Clustering

Scaffolding for fixed-membership cluster startup and runtime coordination. Validated
configuration models are implemented; networking, handshakes, replication and
readiness are not. The executable currently validates inputs, prints its version
and exits.

## Types and modules

| Module | Responsibility |
| --- | --- |
| [`configuration`] | Validated deployment settings, node/domain values and established configuration with a supplied UUID. |

[Raw configuration](crate::configuration) owns loading, and [storage](crate::storage)
can persist the established value. UUID assignment and startup integration remain
planned. The [cluster design](https://github.com/FastFinTech/transaction_log/blob/main/CLUSTERING.md)
records the agreed lifecycle and open questions.

## Behavior and guarantees

Only the configuration contracts are implemented. The planned runtime uses one
permanent master and exactly two required replicas, with no election or degraded
service mode.

<details>
<summary>Design and maintenance notes</summary>

**Planned session contract.** All three members connect and resynchronize before
command admission. Losing either replica ends the operating session; reconnects
and replacements occur during startup. Recovery must coordinate with command
handlers, stop admission on failure and invalidate handler connections.

Handshakes check application version before cluster negotiation, then the persisted
UUID, shared domain and expected fixed slot. Duplicate active slots, including the
master, are rejected. Local slots intentionally differ, so shared agreement compares
UUID/domain rather than whole membership values. Different DNS names can resolve
to the same process; neither a derived address nor claimed identity proves
authenticated ownership or storage continuity.

Durable acknowledgement, in-flight outcomes, storage reconciliation, authentication
and session recovery remain unspecified. The reusable
[message-io crate](https://github.com/FastFinTech/transaction_log/blob/main/lib/message-io/README.md)
is intended for intermittent gossip/control messages. Application schemas and
connection integration remain planned; the codec performs no version agreement.
Runtime components will own these behaviors separately from the static values.

</details>

## Validation

```powershell
cargo test -p transaction-log --locked clustering
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

Configuration tests establish parsing, value and metadata contracts. There are no
implemented cluster exchanges, availability guarantees or runtime measurements to
validate yet.

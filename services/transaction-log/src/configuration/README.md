# Configuration inputs

`raw_application_configuration.rs` defines `RawApplicationConfiguration`, the
entry point's loader. It flattens clustering CLI/environment options and adds
`--storage-directory` / `TL_STORAGE_DIRECTORY`. Storage defaults to `/data` on
non-Windows systems (the intended Linux container volume mount) and `./data` on
Windows. Override it for native local debugging; every local cluster process needs
a separate directory. The file key is `storage_directory`, beside `mode` and the
optional nested `cluster` object. File reading remains caller work.

This module owns all raw application configuration inputs. Each type has its own source file; `mod.rs` declares and re-exports `RawApplicationConfiguration`, `RawClusteringConfiguration` and `RawClusterConfiguration`. Wire message types live in the separate [messages module](../messages/README.md).

Raw inputs are distinct from validated subsystem values. The `conf` derive macro
handles source mapping, presence/type errors and CLI help without custom loading
machinery. Domain validity belongs to conversion into each subsystem's typed
configuration. Loading raw strings does not establish a valid deployment.

The entry point loads application inputs from CLI arguments and environment
variables, including generated help, and converts them into validated clustering
configuration and prints the application version. Storage startup integration remains deferred.
Application-wide source selection, file reading and cluster startup
are not implemented. Environment-only and
CLI-only loading need no file. Configuration
uses owned strings at startup and makes no hot-path performance claim.

Run `cargo test -p transaction-log --locked`, `cargo fmt --all --check` and
`cargo clippy -p transaction-log --all-targets --locked -- -D warnings` from the
workspace root when changing inputs or their source mappings.

## Raw clustering inputs

`configuration` defines `RawClusteringConfiguration` in
`raw_clustering_configuration.rs`, with the grouped `RawClusterConfiguration` in
`raw_cluster_configuration.rs`. `mod.rs` exports both and includes this specification
in Rustdoc. [`conf`](https://docs.rs/conf/latest/conf/) field attributes describe
environment variables, CLI flags and file-document keys. Fields are private with
read-only getters. There is no custom loader or raw error hierarchy.

| Field | Environment variable | CLI flag | Type |
| --- | --- | --- | --- |
| `mode` | `TL_CLUSTER_MODE` | `--cluster-mode` | Required string |
| `cluster.my_node_name` | `TL_CLUSTER_MY_NODE_NAME` | `--cluster-my-node-name` | Required string within the optional group |
| `cluster.cluster_domain` | `TL_CLUSTER_DOMAIN` | `--cluster-domain` | Required string within the optional group |

Single mode needs only `TL_CLUSTER_MODE=single`. Cluster inputs look like:

```text
TL_CLUSTER_MODE=cluster
TL_CLUSTER_MY_NODE_NAME=replica-1
TL_CLUSTER_DOMAIN=payments.example.com
```

The topology is always one master and exactly two replicas.
`--cluster-mode` is permanent once the database's storage is initialized;
single storage cannot become clustered, and clustered storage cannot become single.
This is the agreed lifecycle requirement, not a guarantee of raw loading.
Comparison with stored configuration and storage setup remain unimplemented.

`--cluster-my-node-name` is permanent once the node's storage is initialized;
changing configuration or restarting must not reassign that storage to another
node slot. Future startup must reject disagreement with stored configuration;
the raw types and static conversion cannot enforce that persisted-state rule.

Node names and domain syntax are validated by the
[typed conversion](../clustering/configuration/README.md),
not raw loading. `cluster: Option<RawClusterConfiguration>` groups both settings.
Neither supplied means `None`; either supplied activates the group and the parser
requires both. A present file object, including an empty object, also activates
the group. Inputs can complete a group across sources according to precedence.
Raw strings retain whitespace, empty values and malformed input. The crate rejects
missing mode and incomplete groups; conversion checks allowed mode/node values,
mode/group consistency and DNS syntax. There is no replica count, master pair
or replica list. No field has a default: deployment mode must be explicit.

Raw loading is intentionally independent of mode semantics. For example,
`mode=cluster` without the group loads successfully but fails typed conversion;
`mode=single` with a complete group loads successfully but fails conversion.
An incomplete group fails loading regardless of mode. `ClusterMembership::new`
then combines the parsed node and domain without further validation. Formatted
node hostname lengths are not checked by membership or the raw configuration.

There is no UUID input. The
[`EstablishedClusteringConfiguration`](../clustering/configuration/README.md#established-configuration) combines
validated configuration with a UUID in either mode. First-load UUID assignment
and subsequent storage checks belong to later startup work, not this raw model.

`#[conf(flatten)]` preserves flat CLI/environment names while file documents use
a nested object:

```json
{
  "mode": "cluster",
  "cluster": {
    "my_node_name": "replica-1",
    "cluster_domain": "payments.example.com"
  }
}
```

Single-mode documents omit `cluster`. File keys are the field names.
`#[conf(serde)]` permits a supplied Serde deserializer
through `conf_builder().doc(name, deserializer)`. File format/path selection and
reading bytes remain caller responsibilities; no file is required or loaded by
startup. Precedence is CLI over environment over the supplied document.

Startup loads these options through `RawApplicationConfiguration` and
`conf::Conf::parse` for process arguments and environment variables,
then `ClusteringConfiguration::try_from`. `--help` uses field doc comments and exits
successfully before required-field checks. Loading errors exit with failure;
domain errors propagate through the entry point. The executable prints its
embedded application version and exits. It does not access storage, compare
persisted configuration, assign UUIDs, initialize directories or connect a cluster.
File-source selection and runtime clustering remain deferred.
Owned strings are configuration work, with no
record hot-path or performance claim.

### Design assessment

The current raw types match the agreed minimal design: explicit single/cluster
mode and one optional group containing local node name and cluster domain.
Declarative loading owns source mappings and presence requirements; typed
conversion owns semantic validation. No custom raw errors, discovery settings,
configurable membership or application setup behavior are needed here.
Keep persistence and startup decisions outside this module. No raw-model API
change is currently required.

Same-file tests use explicit inputs rather than mutating process-global environment.
They cover environment/CLI/document mappings, source precedence, required mode,
absent/complete/partial groups in each source, groups completed across sources,
empty strings and preservation of unvalidated input. Run:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

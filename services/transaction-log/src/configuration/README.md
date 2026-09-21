# Configuration inputs

Load raw service options from CLI arguments and environment variables, with optional
caller-supplied documents. Startup converts clustering inputs into validated values,
prints the application version and exits. Storage and cluster startup integration
remain planned.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`RawApplicationConfiguration`] | Combine the storage directory with flattened clustering options. |
| [`RawClusteringConfiguration`] | Require a deployment mode and collect the optional cluster group. |
| [`RawClusterConfiguration`] | Hold the local node name and shared domain before semantic validation. |

[Typed clustering configuration](crate::clustering::configuration) validates mode,
node slots and DNS syntax. The raw types expose private fields through read-only
getters and preserve unvalidated strings.

## Usage

```sh
cargo run -- --cluster-mode single --storage-directory ./data
cargo run -- --help
```

Loading these options does not create storage. Each local cluster process will
need its own directory when storage integration is implemented.

<details>
<summary>Design and maintenance notes</summary>

**Explicit inputs without process-global changes.** The same loader accepts supplied
arguments and environment pairs, useful for embedding and tests:

```rust
use conf::Conf;
use transaction_log::{
    configuration::RawApplicationConfiguration,
    clustering::configuration::ClusteringConfiguration,
};

let raw = RawApplicationConfiguration::try_parse_from(
    ["transaction-log", "--cluster-mode", "single"],
    [] as [(&str, &str); 0],
)?;
let configuration = ClusteringConfiguration::try_from(raw.clustering().clone())?;
assert_eq!(configuration, ClusteringConfiguration::Single);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The declarative `conf` loader owns source mapping, presence/type errors and generated
help. Subsystem conversion owns semantic validation, avoiding a custom raw loader
or duplicate error hierarchy.

</details>

## Behavior and guarantees

### Source mappings

| File field | CLI flag | Environment variable | Requirement/default |
| --- | --- | --- | --- |
| `storage_directory` | `--storage-directory` | `TL_STORAGE_DIRECTORY` | `./data` on Windows; `/data` on other platforms. |
| `mode` | `--cluster-mode` | `TL_CLUSTER_MODE` | Required string. |
| `cluster.my_node_name` | `--cluster-my-node-name` | `TL_CLUSTER_MY_NODE_NAME` | Required when the cluster group is present. |
| `cluster.cluster_domain` | `--cluster-domain` | `TL_CLUSTER_DOMAIN` | Required when the cluster group is present. |

CLI values override environment values, which override a supplied document.
Clustering fields have no defaults. File selection and reading remain caller work;
the executable currently loads only process arguments and environment variables.

<details>
<summary>Design and maintenance notes</summary>

**Grouped file input.** Flat CLI/environment names map to a nested document:

```json
{
  "storage_directory": "node-data",
  "mode": "cluster",
  "cluster": {
    "my_node_name": "replica-1",
    "cluster_domain": "payments.example.com"
  }
}
```

Single-mode documents omit `cluster`. The `conf(serde)` attributes permit a Serde
deserializer through `conf_builder().doc(name, deserializer)`; they do not choose
a file format/path or read bytes. A file is unnecessary for CLI-only or
environment-only loading. The non-Windows storage default targets the intended
Linux container volume mount and can be overridden for native debugging.

</details>

### Raw clustering inputs

Supplying neither cluster field leaves the group absent. Supplying either field,
or a file object even when empty, activates it and requires both fields. Sources
can complete the group together according to precedence. Raw strings retain
whitespace, empty values and malformed syntax.

Loading and semantic conversion are separate: `cluster` mode without a group can
load but fails conversion; `single` with a complete group also fails conversion.
An incomplete group fails loading regardless of mode.

<details>
<summary>Design and maintenance notes</summary>

**Static values versus persisted identity.** Typed conversion checks allowed mode
and node names, mode/group consistency and domain syntax. Membership then combines
validated values without another check; it does not validate the length of a
formatted `node.domain` result. The topology is one master and exactly two replicas,
with no configurable member list or replica count.

Mode and local node slot are intended to remain permanent for initialized storage.
Comparing new inputs with stored settings belongs to future startup work, so raw
loading and static conversion cannot enforce that rule. There is no UUID input.
`EstablishedClusteringConfiguration` combines a supplied UUID with validated values;
first-load assignment and later storage agreement remain lifecycle work.

</details>

### Executable behavior

`conf::Conf::parse` handles process inputs and generated `--help`. Help exits
successfully before required-field checks. Loading errors fail; typed conversion
errors propagate through the entry point. Successful conversion prints the embedded
version and exits, without storage access, UUID assignment or cluster connections.

## Performance

Loading and conversion own strings during startup. They do not run per record,
and no configuration-throughput measurements are available.

## Validation

```powershell
cargo test -p transaction-log --locked configuration
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

<details>
<summary>Design and maintenance notes</summary>

Tests use explicit inputs rather than mutating the process environment. They cover
CLI/environment/document mappings, precedence, storage defaults, required mode,
absent/complete/partial groups in each source, groups completed across sources and
preservation of empty or malformed raw strings. Typed configuration tests separately
establish domain validity and distinguish it from successful raw loading.

</details>

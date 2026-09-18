# Raw clustering inputs

`configuration::clustering` defines `RawClusteringConfiguration` in
`raw_clustering_configuration.rs`; `mod.rs` exports it and includes this specification
in Rustdoc. The raw type uses [`conf`](https://docs.rs/conf/latest/conf/) field
attributes to describe environment variables, CLI flags and file-document keys.
Its fields are private with read-only getters. There is no custom loader,
constructor, conversion implementation or application error hierarchy.

| Field | Environment variable | CLI flag | Type |
| --- | --- | --- | --- |
| `mode` | `TL_CLUSTER_MODE` | `--cluster-mode` | Required string |
| `local_member_id` | `TL_CLUSTER_LOCAL_MEMBER_ID` | `--cluster-local-member-id` | Optional string |
| `master` | `TL_CLUSTER_MASTER` | `--cluster-master` | Optional string |
| `replicas` | `TL_CLUSTER_REPLICAS` | `--cluster-replicas` | Optional string |

File-document keys are the field names. `#[conf(serde)]` enables the crate's
document support, so callers can supply a Serde deserializer through
`RawClusteringConfiguration::conf_builder().doc(name, deserializer)`. File format,
path selection and reading bytes remain caller responsibilities; files are not
required. The service entry point calls `conf::Conf::parse` to read process arguments
and environment variables. The crate prints generated CLI help for `--help` and
exits successfully, or reports loading errors and exits with failure. Field doc
comments supply option descriptions. `try_parse_from` accepts explicit inputs
without exiting and is used in tests. File-document loading is not wired into startup.

Example environment inputs:

```text
TL_CLUSTER_MODE=clustered
TL_CLUSTER_LOCAL_MEMBER_ID=replica-01
TL_CLUSTER_MASTER=master-01=db-master.example.com
TL_CLUSTER_REPLICAS=replica-01=db-replica-01.example.com,replica-02=db-replica-02.example.com
```

Master is intended as one `member_id=hostname` pair; replicas as a comma-separated
list of pairs. Local identity is only a member ID. These remain opaque strings
here, including whitespace, empty strings and malformed entries. There is no
mode default; the crate rejects a missing mode. Other fields are optional because
singleton needs no membership. Conditional requirements for clustered mode,
allowed mode values, pair parsing, normalization, name validity, uniqueness and
local membership belong to a future `TryFrom` into the
[validated configuration](../../clustering/configuration/README.md).

The crate's source precedence is CLI over environment over the supplied document.
Raw loading establishes only presence and input types, not a valid cluster.
The executable loads raw inputs but does not yet convert them or start a cluster.
Domain conversion and application-wide source selection are deferred.
Owned strings are allocated at configuration time; no record hot-path
work or performance claim is introduced.

Same-file tests use explicit environment inputs instead of mutating process-global
state. They cover each environment/CLI mapping, document keys, source precedence,
missing mode, optional fields, empty strings and preservation of unvalidated input.
Run from the workspace root:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

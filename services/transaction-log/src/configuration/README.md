# Configuration inputs

This module owns raw application configuration inputs. `mod.rs` declares
`clustering`; its [specification](clustering/README.md) covers the annotated raw
clustering type, its source mappings and loading contracts.

Raw inputs are distinct from validated subsystem values. The `conf` derive macro
handles source mapping, presence/type errors and CLI help without custom loading
machinery. Domain validity belongs to later conversion into each subsystem's typed
configuration. Loading raw strings does not establish a valid deployment.

The entry point loads raw clustering inputs from CLI arguments and environment
variables, including generated help. Application-wide source selection, file reading,
domain conversion and cluster startup are not implemented. Environment-only and
CLI-only loading need no file. Configuration
uses owned strings at startup and makes no hot-path performance claim.

Run `cargo test -p transaction-log --locked`, `cargo fmt --all --check` and
`cargo clippy -p transaction-log --all-targets --locked -- -D warnings` from the
workspace root when changing inputs or their source mappings.

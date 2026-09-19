# Validated clustering configuration

`clustering::configuration` owns deployment values and established configuration,
distinct from the
[raw inputs](../../configuration/README.md). Startup converts raw inputs
with `ClusteringConfiguration::try_from` before reporting the application version.
The executable then exits. Reading stored configuration, comparing deployment
settings, assigning UUIDs and initializing storage are not wired into startup.
The [clustering overview](../README.md) and [lifecycle design](../../../../../CLUSTERING.md)
own the planned runtime behavior; configuration does not enforce it.

## Module coverage and public contracts

- `clustering_configuration.rs`: `Single` or `Cluster(ClusterMembership)`, composition
  of validated values and conversion from raw fields, with same-file tests.
- `established_clustering_configuration.rs`: deployment configuration paired with
  its permanent UUID in either mode, derived Serde and same-file tests.
- `clustering_configuration_error.rs`: one error type for mode, node name,
  conditional fields and concrete hostname errors.
- `node_name.rs`: the fixed `NodeName` slots and their parsing/tests.
- `cluster_membership.rs`: local slot and shared domain, infallible construction,
  derived Serde, role lookup and hostname formatting, with same-file tests.
- `hostname.rs`, `hostname_error.rs`, `hostname_error_kind.rs`: DNS-name value,
  parsing/tests, retained original input and typed validation reasons.
- `mod.rs`: thin declarations, exports and this specification's Rustdoc inclusion.

## Fixed topology and conversion

Only `master`, `replica-1` and `replica-2` are valid node names. `NodeName::parse`
trims surrounding whitespace, is case-sensitive and preserves rejected input in
`InvalidNodeName`. The enum prevents invalid slots after parsing. There are no
arbitrary member names, configurable replica count, hostname pairs or member lists.

The local node name is permanent once its storage is initialized. Neither restart
nor a changed `--cluster-my-node-name` may reassign that storage to a different
slot. Future startup must compare configuration with persisted node identity and
reject changes; static conversion alone cannot enforce this persisted-state rule.
Persisted-state comparison and startup storage integration remain planned.

Deployment mode is also permanent for initialized storage: `--cluster-mode` cannot
switch that storage between single and cluster. Future startup must reject mode
changes against persisted metadata; startup comparison remains planned.

`ClusterMembership` stores only `my_node_name: NodeName` and
`cluster_domain: Hostname`. The shared configuration is just the domain; there is
no separate definition object or cached hostname array. Borrowing getters expose
these private fields. `is_master()` derives the role. `hostname(NodeName)` formats
an owned `node.domain` string for any fixed slot, without DNS resolution or
revalidating the stored domain. A root dot is preserved.

`ClusterMembership::new(NodeName, Hostname)` takes ownership of already validated
field values without additional checks. There is no membership `TryFrom` or
custom deserializer. Membership does not impose a separate cluster-domain length
limit: a domain may satisfy the standalone hostname limit while a prefixed node
hostname exceeds it. `hostname` formats a string without checking that result.

Raw conversion accepts trimmed, lowercase `single` or `cluster`, with no aliases.
Single mode forbids the raw cluster group, including a group of empty strings.
Cluster requires the group. The raw parser already requires both `my_node_name`
and `cluster_domain` whenever the group is activated. Conversion order is mode;
group presence/absence; node parsing; domain parsing; membership construction.
Errors preserve original mode/node input or wrap the concrete `HostnameError`.

Shared agreement is ordinary `cluster_domain()` equality. Domain casing and
surrounding whitespace normalize; a root dot remains significant because it
changes resolver behavior. Local slots intentionally differ across machines, so
handshakes must compare domains rather than whole memberships. No list
sorting, duplicate checks or list-mismatch error machinery is needed. Runtime must
still reject duplicate active slots and verify the expected peer at each derived
hostname; different DNS names can point to the same process.

## Established configuration

`EstablishedClusteringConfiguration` pairs two private, read-only fields:
`cluster_id: uuid::Uuid` and `clustering_configuration: ClusteringConfiguration`.
Both singleton and clustered deployments have a UUID. The deployment mode is
represented only inside configuration; cluster configuration also contains the
shared domain and this node's permanent slot. There is no duplicate mode or slot.

The type belongs to clustering because it represents established application
state, independently of where it is persisted. `new` assembles an externally
supplied UUID and validated configuration; it does not generate UUIDs, establish
agreement, initialize storage or prove that establishment has happened. These
lifecycle operations remain deferred. The planned master generates the cluster
UUID during first establishment and shares it with both replicas; subsequent
loads retain the UUID. It is not a user-supplied deployment setting. No particular
UUID version is required by this value model.

Storage directly reads and writes this model, and its atomic write operation can
replace existing metadata. The clustering lifecycle caller owns write-once
establishment and preservation of the permanent identity; storage does not enforce
that application policy. Lifecycle enforcement remains planned.

Peers must agree on UUID and domain while occupying different local slots.
Whole-value equality includes the slot, so it is not the cluster handshake's
agreement check. Fresh storage has no established configuration; deciding whether
it can join an initialized cluster belongs to establishment, not Serde or storage.

Serde is derived with `deny_unknown_fields`. UUIDs are strings in human-readable
formats using the `uuid` crate's representation; nested configuration uses its
existing field parsers. Missing, duplicate and unknown struct fields are rejected.
Singleton JSON is, for example:

```json
{"cluster_id":"12345678-9abc-def0-1234-56789abcdef0","clustering_configuration":"single"}
```

Clustered JSON is:

```json
{"cluster_id":"12345678-9abc-def0-1234-56789abcdef0","clustering_configuration":{"cluster":{"my_node_name":"master","cluster_domain":"cluster.example"}}}
```

The [storage provider](../../storage/README.md#provider-metadata-operations) saves/restores this type
directly using Serde in `clustering.json`, with no intermediate persistence model.
Storage owns filesystem operations and I/O/serialization errors; clustering owns
the type. Startup does not yet invoke those operations. Cloning configuration
copies its domain string; these values are not per-record hot-path state.
Same-file tests cover independent JSON fixtures in both modes and all slots,
normalization, UUID/slot distinctions and malformed metadata.

## Hostname syntax, ownership and rationale

### Serialization

The configuration types implement format-independent Serde traits, including
the established model described above. UUID assignment and storage read/write
operations remain separate from these value types.
JSON clustering configuration is `"single"` or
`{"cluster":{"my_node_name":"master","cluster_domain":"cluster.example"}}`.
`NodeName` and `Hostname` serialize as strings; deserialization calls their
existing parsers, preserving trimming, case handling and syntax checks.
`ClusteringConfiguration` and `ClusterMembership` derive Serde. Membership loading
uses each field type's existing validation. Missing, duplicate and unknown
membership fields are rejected through the derive and `deny_unknown_fields`.
Derived node hostname lengths are not checked by this model. Serde loading validates
static values only; it cannot enforce storage permanence or peer agreement.

Serialization through owned string conversions clones the hostname value;
these startup operations are not part of record processing.

### DNS validation

`Hostname::parse(String)` trims with `str::trim`, validates and stores lowercase
ASCII. Private fields and a read-only `name()` getter preserve validity; equality
and hashing compare stored text. It permits one optional trailing root dot and
preserves it: `db.example.` is explicitly absolute, while `db.example` can be
affected by resolver search rules. Multiple trailing dots are rejected.

Names contain one or more nonempty labels, each 1–63 ASCII bytes, using letters,
digits and hyphens with alphanumeric boundaries. Single-label names and labels
starting with digits are allowed. The maximum is 253 bytes excluding the optional
root dot (254 stored bytes including it). `MAX_NAME_LENGTH` and `MAX_LABEL_LENGTH`
are authoritative. Reject Unicode, underscores, wildcards, interior whitespace,
URLs, ports and paths. There is no IDNA conversion; ASCII `xn--` labels receive
ordinary syntax checks rather than IDNA validation. IP literals recognized by
`std::net::IpAddr` are rejected, including IPv4 with an optional root dot.

Checks return the first failure: empty trimmed input, overall length, IP literal,
then each label in order for emptiness, length, characters and boundaries.
`HostnameError::name()` retains original padding and case; `kind()` is typed.
Label indexes are zero-based and exclude the root label; character byte offsets
refer to the trimmed input before lowercasing. For example, `"  DB._host  "`
reports byte offset 3 while retaining the original input.

Label syntax follows [RFC 1123 section 2.1](https://www.rfc-editor.org/rfc/rfc1123.html#section-2.1);
lengths account for DNS label length bytes and the root label in the 255-byte wire
limit from [RFC 1035 section 2.3.4](https://www.rfc-editor.org/rfc/rfc1035.html#section-2.3.4).
This validates hostnames, not arbitrary DNS record names. Rust has no standard
validated DNS hostname type; `IpAddr` handles numeric address recognition.
Parsing establishes neither existence, reachability nor authentication.

Hostname parsing retains an untrimmed input's allocation and lowercases in place;
trimming on success copies the trimmed text. Failures retain the original input.
Membership owns only its domain string; raw conversion clones the borrowed raw
node/domain strings, and cloning membership clones the domain. Formatting a node
hostname allocates a new string, rather than caching three addresses for occasional use.
These are configuration operations, not per-record work. There are no locks,
workers, DNS queries, unsafe code or performance measurements here.

## Planned lifecycle and validation

All three permanent members must connect and resynchronize at startup before
command-handler admission. Once serving, losing either replica ends the cluster
session; no live joins or replacement connections are admitted. Coordinated
restart/resynchronization must cascade to command handlers. Cluster identity,
storage identities, mode permanence, recovery and durable acknowledgement rules
remain runtime/persistence responsibilities, not guarantees of these value objects.
Fixed ports are planned but numbers have not been selected.

Same-file tests cover all slots and invalid alternatives, normalization, root-dot
agreement, formatted hostnames, the standalone hostname length boundary with and
without a root dot, invalid domain syntax, mode/conditional-field errors and role
selection. Membership decoding tests retain field validation and malformed-object
coverage without reinstating cross-field length checks.
Hostname tests cover 63/64-byte labels, 253/254-byte full names, all ASCII classes,
Unicode, IP literals, malformed labels, error offsets and precedence.

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

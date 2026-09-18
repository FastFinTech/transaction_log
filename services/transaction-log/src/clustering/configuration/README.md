# Clustering configuration

`clustering::configuration` implements the in-memory deployment configuration and
its validation. The [clustering overview](../README.md) describes module boundaries,
and the [root clustering design](../../../../../CLUSTERING.md) describes
the planned lifecycle. Networking, initialization metadata, recovery, replication
and process-exit policy are not implemented here. The executable does not yet
consume this configuration.

## Types and ownership

- `clustering_configuration.rs`: `ClusteringConfiguration` explicitly selects
  `Singleton` or `Clustered(ClusterMembership)`.
- `cluster_definition.rs`: shared master/replica definition, canonical replica
  ordering, agreement checks and the authoritative `MINIMUM_REPLICA_COUNT` (two).
- `cluster_definition_error.rs`: static shared-definition validation failures.
- `cluster_definition_mismatch.rs`: typed differences between valid definitions.
- `cluster_membership.rs`: local identity paired with a validated shared definition,
  local membership validation and role lookup.
- `clustering_configuration_error.rs`: local identity validation failures.
- `member_id.rs` and `member_id_error.rs`: an owned, opaque member name and its
  raw-name validation error.
- `member_id_error_kind.rs`: typed reasons for name validation failures.
- `hostname.rs`: parsing and immutable storage of a configured DNS hostname.
- `hostname_error.rs` and `hostname_error_kind.rs`: original rejected hostname
  input and typed validation reasons.
- `member.rs`: a logical member identity paired with a configured hostname.
- `mod.rs`: declarations, re-exports and this specification's Rustdoc inclusion.

## Member identity and parsing

`MemberId` is an immutable value identifying a logical member within configured
cluster membership. Its private name is exposed by the read-only `name()` getter.
Equality and hashing use the stored name. Parsing a name establishes only its
syntax; it does not prove membership, authenticate a peer or identify a cluster.

`MemberId::parse(String)` trims surrounding whitespace with `str::trim` and requires
the result to contain 3–64 ASCII characters. Allowed characters are
`A..Z`, `a..z`, `0..9`, hyphen (`-`) and underscore (`_`); the first and last
characters must be letters or digits. For example, `replica-01` and `Replica_A`
are valid. Interior whitespace, control characters, Unicode, dots, path separators
and other punctuation are rejected. Repeated interior hyphens/underscores are valid.

The three-character minimum encourages meaningful names rather than single-letter
identities. The 64-byte maximum bounds identity size while accommodating
descriptive names.
ASCII avoids visually ambiguous Unicode variants and makes byte and character
lengths identical for valid names. Alphanumeric boundaries keep names readable;
these are application identity rules, not a DNS-name contract. Names identify
logical configured members, not socket addresses, cluster IDs or storage paths.
Case is preserved and significant. The trimmed version is stored; there is no
other normalization. Surrounding Unicode whitespace is trimmed as well, while
Unicode within the resulting name is rejected. Membership comparisons use these
trimmed identities, so `" master "` and `"master"` identify the same member.
`"Master"` and `"master"` remain different identities. Configuration duplicate
checks operate on parsed names, so differently padded inputs cannot create
distinct replicas.

| Input | Result |
| --- | --- |
| `"  replica-01  "` | Stored name `"replica-01"`. |
| `"Replica_A"` | Stored unchanged, including case. |
| `"ab"` or whitespace only | `TooShort`. |
| 65 ASCII letters, after trimming | `TooLong`. |
| `"replica.01"` or `"replica 01"` | `InvalidCharacter`. |
| `"-replica"` or `"replica_"` | `InvalidBoundary`. |

`MemberId::MIN_NAME_LENGTH` and `MAX_NAME_LENGTH` are the authoritative limits.
Validation checks minimum length, maximum length, allowed characters, then
boundaries, returning the first failure. Empty or whitespace-only input therefore
returns `TooShort`. `MemberIdError::name()` preserves the original untrimmed
input, and `kind()` returns a `MemberIdErrorKind`. Invalid-character errors report
the zero-based byte offset in the trimmed name, rather than in the original input.
For non-ASCII UTF-8 this is the start of the first invalid character. For example,
`"  ab c  "` retains that input in the error but reports byte offset 2.
Length is measured in trimmed UTF-8 bytes before ASCII validation: a Unicode name
can fail a length check before its characters are inspected.

Parsing takes ownership of the supplied `String`. It retains that allocation
when no trimming is needed; successful parsing with surrounding whitespace copies
the trimmed name into a new string. On failure the error owns the original input.
There is no unchecked public constructor, setter or mutable name access, so
consumers can rely on the established name validity without revalidation.

## Hostname parsing

`Hostname` is the configured network name, separate from `MemberId`, which remains
only a logical member name. `Hostname::parse(String)` owns the input, trims with
`str::trim`, validates it and stores lowercase ASCII. `name()` borrows that stored
string. Private fields and read-only access preserve validity. Equality and
hashing compare the stored form, so case and surrounding whitespace do not
distinguish hostnames. This differs intentionally from case-sensitive member IDs.

The hostname rules are:

- One or more dot-separated labels, each containing 1–63 ASCII bytes.
- Labels contain only letters, digits and hyphens, with a letter or digit at
  each end. Digits may begin labels. Single-label names such as `master` are valid.
- At most 253 bytes excluding one optional final root dot. A name with that dot
  can therefore contain 254 stored bytes. The constants `MAX_NAME_LENGTH` and
  `MAX_LABEL_LENGTH` on `Hostname` are the authoritative limits.
- Preserve the optional final dot. An explicitly absolute name such as
  `master.db.` remains distinct from `master.db`, which can be subject to resolver
  search rules. Do not strip the dot during connection setup.
- Reject Unicode labels, underscores, wildcards, interior whitespace, URL syntax,
  embedded ports and path separators. No IDNA conversion is performed; ASCII
  `xn--` labels pass the same syntax checks without validating their IDNA content.
- Reject inputs recognized as IP literals by `std::net::IpAddr`, including an
  IPv4 literal with an optional final dot. This type is for configured DNS names.

The label syntax follows the hostname rules in
[RFC 1123 section 2.1](https://www.rfc-editor.org/rfc/rfc1123.html#section-2.1).
The label and overall limits reflect the DNS encoding limits in
[RFC 1035 section 2.3.4](https://www.rfc-editor.org/rfc/rfc1035.html#section-2.3.4):
length bytes and the terminal root label count toward the 255-byte wire limit,
giving a maximum of 253 text bytes without the final dot. This is an application
hostname contract, not a parser for arbitrary DNS record names.

| Input | Result |
| --- | --- |
| `"  MASTER.DB  "` | Stored `"master.db"`. |
| `"master-0.db.svc.cluster.local"` | Stored unchanged. |
| `"MASTER.DB."` | Stored `"master.db."`, retaining the root dot. |
| Empty or whitespace only | `Empty`. |
| `"master..db"` | `EmptyLabel { label_index: 1 }`. |
| `"db.master_0"` | `InvalidCharacter { byte_index: 9 }`. |
| `"db.-master"` | `InvalidBoundary { label_index: 1 }`. |
| `"127.0.0.1"` or `"::1"` | `IpLiteral`. |

Validation returns the first failure: empty trimmed input, overall length, IP
literal, then labels in input order. Each label is checked for emptiness, length,
allowed characters and boundaries. `HostnameError::name()` owns the original
untrimmed input and case; `kind()` reports the typed reason. Label indexes are
zero-based and exclude the optional root dot. Character byte offsets refer to
the trimmed input before lowercasing. No error reports an offset into normalized
or original padded text instead.

Parsing performs no DNS lookup or I/O and establishes neither existence nor
reachability. It also does not authenticate a member. Rust's standard library has
no validated hostname value type; `IpAddr` and `SocketAddr` describe numeric
addresses, while `ToSocketAddrs` performs resolution. The small application value
therefore stores configured hostname syntax and delegates IP-literal recognition
to the standard parser. Future connection code owns resolution and retry policy.
See [std::net](https://doc.rust-lang.org/std/net/index.html) and
[ToSocketAddrs](https://doc.rust-lang.org/std/net/trait.ToSocketAddrs.html).

Parsing retains the supplied allocation when no trimming is needed and lowercases
it in place. Trimming on success creates a new string. Failures retain the original
input. These costs occur during configuration, not per record; no performance
claim is made.

Fixed-port endpoint wiring remains planned. No port number is selected here,
and no connection discovery API is introduced.

## Member composition

`Member::new(id: MemberId, hostname: Hostname)` takes ownership of two validated
values and returns `Member` infallibly. Its fields are private, with borrowing
`id()` and `hostname()` getters and no mutable access. It performs no parsing,
allocation, DNS lookup or I/O. Member identity and network location stay separate:
changing a configured hostname does not change the logical member ID.

`Member` contains no role, port, cluster ID or runtime state. Role belongs to
membership, while fixed ports and connection setup remain application concerns.
Derived equality compares both ID and hostname, so two configuration values with
the same ID but different hostnames are unequal as complete members. Membership
validation must compare `member.id()` instead of whole members to prevent an
address change from bypassing identity conflict checks. Cloning a member clones
its owned identity and hostname strings.

## Shared definition and local membership

`ClusterDefinition` owns the permanent master `Member` and replica `Member`s.
It contains no local identity, cluster ID or runtime state. Every machine should
use the same shared definition plus its own local ID.

`ClusterDefinition::validate(master: Member, replicas: Vec<Member>)` establishes:

1. At least two replicas (excluding the master).
2. No replica has the master's ID.
3. No replica repeats an earlier replica ID.
4. Hostnames are unique across the master and replicas.

Count is checked first. Each replica is then checked in supplied order for master
ID, duplicate ID and duplicate hostname, returning the first error. Parsed names
are not validated again. No maximum replica count is imposed.
`ClusterDefinitionError` retains the rejected count or conflicting parsed identity;
`DuplicateHostname` retains the hostname and both member IDs. Conflicting values
are cloned only when needed to return an owned error.

Unique hostnames prevent distinct members targeting the same configured name and
fixed port. Comparisons use already-trimmed, lowercase `Hostname` values, retaining
root-dot differences. No DNS lookup or alias/address uniqueness check occurs;
different configured names may still resolve to the same destination.

After successful validation, replicas are sorted in place by the case-sensitive
member ID name. The supplied order carries no meaning and is not retained.
Canonical order makes derived definition
equality independent of input order and mismatch reporting deterministic. Member
strings are moved rather than cloned; sorting does not allocate another vector.
`master_member()` borrows the master and `replica_members()` borrows a read-only
slice in canonical ID order. `contains_member(&MemberId)` checks the master and
replica IDs, independently of their hostnames.

`ClusteringConfiguration::clustered(local: MemberId, definition: ClusterDefinition)`
takes ownership of a validated shared definition, then checks that the local ID
belongs to it. An unlisted ID returns `ClusteringConfigurationError::UnknownLocalMember`,
moving the unmatched ID into the error. This returns `Clustered(ClusterMembership)`;
`Singleton` contains no definition or local identity.

`ClusterMembership` contains only `local_member_id` and `definition`, exposed by
borrowing getters. Its internal validation does not repeat definition validation.
`is_master()` derives the role from the local ID and definition's master ID; false
means the local identity is one of the configured replicas. No separate local
hostname or role is stored that could disagree with the definition. Comparing
whole memberships includes the local ID; handshake agreement must compare their
shared definitions instead, since local IDs intentionally differ.

Private fields, read-only access and validated construction preserve all these
contracts. Cloning definitions/memberships clones their owned strings and vector;
no shared state, locks or background work are introduced. None of these values
establishes authentication, exclusive execution, connection readiness, durable
replication or command admission.

## Definition agreement

`expected.verify_matches(&actual)` returns `Ok(())` when validated shared definitions
agree, independently of replica input order. It compares the full master and
replica ID/hostname information using parsed values. No hash, serialization, wire
protocol, DNS lookup or live connection checks are involved.

`ClusterDefinitionMismatch` returns the first difference with expected/actual
orientation:

| Difference | Retained context |
| --- | --- |
| Different master ID | `Master { expected, actual }`. |
| Different hostname for a master or replica ID | `Hostname { member_id, expected, actual }`. |
| An expected replica is absent | `MissingReplica { member_id }`. |
| An additional actual replica is present | `UnexpectedReplica { member_id }`. |

Comparison checks master ID, master hostname, then expected replicas in canonical
ID order for missing IDs or changed hostnames, then actual replicas for additional
IDs in canonical order. A replacement replica therefore reports the missing
expected ID before the unexpected one. This is a first-mismatch diagnostic, not
an accumulated diff; reversing expected/actual reverses missing/extra orientation.
Error values own cloned context; successful comparisons allocate no error data.
Derived `PartialEq`/`Eq` have the same agreement semantics because validated
replica lists are canonicalized. Member IDs remain case-sensitive and hostname
root dots remain significant.

## Boundaries and design rationale

This is static configuration validation, not proof that members are connected,
authenticated, recovered or current. It does not establish exclusive execution,
persist a cluster ID, or enforce the permanent mode against existing storage.
Those require startup and connection owners with the relevant state.

Configured hostnames with fixed service ports are the agreed direction. Hostname
syntax and member composition are implemented; port numbers, resolution,
serialization and configuration-file loading remain deferred. A cluster ID will be generated at initialization,
so it is not a caller-selected field in this configuration. The fixed replica
minimum is application policy rather than a configurable quorum setting.

Definition validation performs simple pairwise duplicate checks without a temporary
set, followed by sorting. Agreement checks also search the small replica lists
directly. Duplicate checks and agreement are quadratic in replica count; these
are configuration/handshake operations for a small fixed cluster. No per-record work, locks,
workers or I/O are introduced. No performance improvement is claimed or measured.

## Validation and future integration

Same-file definition tests cover the minimum count, canonical order and equality
under reordered input, unique IDs/hostnames, validation precedence and all mismatch
variants with retained expected/actual context. They cover normalized hostname
agreement, root-dot differences, case-sensitive IDs, missing/extra replicas and
canonical first-mismatch ordering. Membership tests cover each local role, the same
shared definition across local identities, trimmed IDs and unlisted/wrong-case IDs.
Member-name tests cover
empty/short input, the literal 2/3 and 64/65-byte boundaries, surrounding whitespace
trimming, all ASCII bytes, Unicode and lookalike rejection, invalid-character
offsets, alphanumeric boundaries and preservation of names and case. Membership
definition tests also cover duplicate replicas and master conflicts after trimming,
conflicting IDs with different hostnames, and hostname conflicts normalized from
different casing and surrounding whitespace.
Hostname tests use literal length boundaries (63/64 label bytes and 253/254 name
bytes), including absolute names with a final dot, and cover normalization,
single-label names, empty labels, ASCII/Unicode rejection, IP literals,
invalid-character offsets, label boundaries and first-error ordering.
Run from the workspace root:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
```

Future startup must compare this mode and membership with persisted initialization
state before opening service connections. Runtime replica eligibility, full-cluster
readiness and command completion remain separate contracts; static validation must
not be presented as enforcing those behaviors.

# Application messages

Scaffolding for cluster gossip/control messages, including future version and
configuration agreement. No message schemas or connection exchanges are implemented,
and the service does not yet depend on or invoke message-io.

## Behavior and guarantees

The planned transport is the reusable
[message-io crate](https://github.com/FastFinTech/transaction_log/blob/main/lib/message-io/README.md).
Application schemas and exchange ordering remain to be defined. Raw configuration
loading and validated clustering values belong to their existing modules.

<details>
<summary>Design and maintenance notes</summary>

**Planned wire contract.** Message-io encodes Serde values with Postcard, preceded
by a four-byte little-endian body length. Its existing contract owns the inclusive
1 MiB body limit, zero-byte bodies, owned read results and rejection of oversized,
malformed or trailing data. Application types reuse that framing and its constants.

`write_message` and `read_message` carry no automatic type identifier or schema
version. The exchange selects the expected type, and Postcard's positional encoding
requires schema agreement. Independent literal wire fixtures can detect accidental
schema changes that matching encoders/decoders would miss.

Byte acceptance by the destination is separate from flushing, durability and remote
acknowledgement. Connection owners inherit the codec's partial-I/O, error and
cancellation rules; interrupted streams cannot safely replay a message blindly.
Deserialization alone establishes neither valid application configuration nor
authenticated identity or cluster readiness.

**Planned exchanges.** Application-version agreement precedes clustering negotiation.
Its initial message and exact-version check remain unimplemented. Cluster identity,
authentication, scheduling, timeouts and readiness belong to connection owners.
These are intermittent control messages; they do not define bulk record replication
or automatically serialize raw configuration. No message throughput is measured.

</details>

## Validation

`cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings`
checks this scaffold's documentation. There are no message-specific behavior tests
yet. Future schema checks need literal wire fixtures, domain rejection and schema
boundaries; connection tests will establish exchange ordering and failure behavior.

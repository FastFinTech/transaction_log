# Application messages

`messages` is the application-level collection point for wire message types,
including configuration and version-agreement messages. `mod.rs` includes this specification in
Rustdoc; add individual message types in separate files with thin re-exports here.
No message schemas or connection exchanges are implemented yet. Raw configuration
loading and validated clustering values remain in their existing modules.

## Wire serialization

Messages will use the reusable [message-io crate](../../../../lib/message-io/README.md):
Serde `Serialize`/`Deserialize` message values encoded with Postcard, preceded by a
four-byte **little-endian `u32` body-length prefix**. The length excludes the prefix.
The encoded body is limited to 1 MiB, inclusive; zero-byte bodies are supported.
The crate's `protocol.rs` owns the authoritative constants. Do not add a second
framer or duplicate limits in message types.

Send with `write_message(&message).await` and receive with
`read_message::<MessageType>().await`. Reading returns an owned value and rejects
oversized frames, malformed Postcard values and trailing body bytes. The wire
contains no automatic message-type identifier or schema version: the application
exchange must select the expected type. Postcard is positional, so schemas must
agree; intentional encoding changes need independent literal fixtures.

Writing success means byte acceptance by the destination, not flushing,
durability or remote acknowledgement. Connection owners must follow message-io's
partial-I/O, cancellation and error contracts, including discarding interrupted
streams rather than blindly replaying messages. Application validation belongs
at the receiving boundary; Serde deserialization alone does not establish valid
configuration, authenticated identity or cluster readiness.

## Scope and future work

Application-version agreement is intended to precede clustering negotiation.
Its initial message and exact-version checks remain to be implemented. Cluster
identity checks, connection scheduling, authentication, timeouts and readiness
are separate application responsibilities. This module does not serialize raw
configuration automatically or implement bulk record replication.

These messages are intermittent control traffic, not a record hot path.
Message-io is designed for non-hot-path use and makes no high-performance claim.
There are no message-specific measurements yet, and the service does not yet
depend on or invoke message-io.

## Validation

Keep behavior tests in each message's source file. Test independently specified
wire bytes, domain-validation failures and schema boundaries rather than only
encoder/decoder round trips. Exchange tests will belong beside their connection
owner and should cover version mismatch, malformed/truncated input and errors.
Follow the message-io specification when changing shared framing contracts.

From the workspace root:

```sh
cargo test -p transaction-log --locked
cargo fmt --all --check
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

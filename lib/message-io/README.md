# Message I/O

Send and receive owned Serde messages over Tokio byte streams using bounded,
length-prefixed Postcard frames. The caller chooses the message type and controls
when output is flushed.

In the transaction-log service, this library is intended for exchanging cluster
gossip and control messages between nodes.

## Types and modules

| Type | Responsibility |
| --- | --- |
| [`MessageReadExt`] | Add `read_message::<T>().await` to an `AsyncRead + Unpin` source. |
| [`MessageWriteExt`] | Add `write_message(&value).await` to an `AsyncWrite + Unpin` destination. |
| [`MessageReadError`] | Distinguish clean EOF, transport failures, and rejected message frames. |
| [`MessageWriteError`] | Distinguish encoding failures before I/O from destination errors after possible partial output. |

## Usage

Import the extension traits, write a message, and read it back as the expected
type. Writes accept `Serialize + ?Sized` values; reads return `DeserializeOwned`
values that can outlive the reader.

```rust
use message_io::{MessageReadExt, MessageWriteExt};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Greeting { text: String }

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut wire = Vec::new();
wire.write_message(&Greeting { text: "hello".into() }).await?;

let mut reader = wire.as_slice();
let greeting = reader.read_message::<Greeting>().await?;
assert_eq!(greeting.text, "hello");
# Ok(())
# }
```

Each operation exclusively borrows its source or destination. A successful write
means that the destination accepted the frame; buffered destinations may need an
explicit flush before the message is available to their consumer.

<details>
<summary>Design and maintenance notes</summary>

**Using buffered streams.** A request/response exchange should flush its outgoing
request before waiting for the peer's response. Otherwise, the request may remain
in a local buffer while both peers wait. Flushing stays explicit so callers can
also batch several messages before sending them. This in-memory example shows
the same separation between accepting a message and flushing its bytes:

```rust
use message_io::{MessageReadExt, MessageWriteExt};
use tokio::io::{AsyncWriteExt, BufWriter};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut writer = BufWriter::with_capacity(64, Vec::new());
writer.write_message("hello").await?;
assert!(writer.get_ref().is_empty()); // The frame is still in BufWriter.

writer.flush().await?;
let mut reader = writer.get_ref().as_slice();
assert_eq!(reader.read_message::<String>().await?, "hello");
# Ok(())
# }
```

Keep using the same buffered reader across messages: it may have prefetched bytes
from the next frame. Message decoding consumes only the requested frame, while
any read-ahead remains owned by that reader.

**Owned results.** Each read collects a body in a temporary buffer that is dropped
before return. `DeserializeOwned` prevents the returned value from borrowing that
storage; borrowed strings and slices remain valid inputs for writing. The read
restriction is enforced at compile time:

```compile_fail
use message_io::MessageReadExt;
use serde::Deserialize;

#[derive(Deserialize)]
struct Borrowed<'a> { value: &'a str }

async fn receive() {
    let mut reader = &[0u8; 4][..];
    let _ = reader.read_message::<Borrowed<'_>>().await;
}
```

The exclusive borrow keeps each operation's progress with its caller. The traits
use concrete futures without extra `Send` or `Sync` bounds; ordinary auto traits
govern executor use. Internal locks, runtime borrow checks, queues, workers, and
boxed futures are unnecessary for this ownership model.

</details>

## Behavior and guarantees

### Wire contract

| Part | Encoding or limit |
| --- | --- |
| Length prefix | Four-byte little-endian `u32`, counting body bytes only; [`LENGTH_PREFIX_LEN`] is 4. |
| Body | Exactly one Postcard value, with no trailing bytes. |
| Body limit | 1 MiB inclusive in both directions, defined by [`MAX_MESSAGE_BODY_LEN`]. |

Zero-byte bodies are valid when the message type encodes no bytes, such as unit
values and empty structs. Oversized advertised lengths are rejected before body
allocation or reading. Partial reads and writes are handled internally.

The frame contains no message-type identifier or schema version. Peers must
agree on compatible message schemas and the expected type before decoding.
Application deserializers establish domain validity; decoding alone does not
establish authenticated identity or other application guarantees.

### Completion and failures

`write_message` serializes the complete bounded frame before starting I/O.
`MessageWriteError::TooLarge` and `MessageWriteError::Encode` therefore leave the
destination untouched and reusable. Success means every frame byte was accepted
by `AsyncWrite`. Flushing is a separate caller operation; neither acceptance nor
flushing establishes durable storage or remote acknowledgement.

`MessageReadError::EndOfStream` means clean EOF before any byte of the next
prefix. EOF inside the prefix or body is an I/O error retaining `UnexpectedEof`.
Transport and Postcard failures preserve their concrete source errors.

After other read errors, write I/O errors, panics, or cancellation of polled
operations, **discard the affected source or destination**. It may be mid-frame;
retrying on the same stream is unsafe. The traits retain no progress or poison
flag to enforce this rule. Encoding errors are the reusable-destination exception
above, and clean EOF ends the message stream.

Dropping an unpolled future performs no encoding or I/O. Cancelling a polled
future does not guarantee that underlying I/O stopped or accepted nothing.

<details>
<summary>Design and maintenance notes</summary>

**Framing and schema agreement.** The 1 MiB body limit is a conservative initial
choice for control messages. The body uses Postcard directly; framing adds only
the length prefix.

Postcard serialization is positional, so a stable wire format does not make
arbitrary message-schema changes compatible. Peers must establish schema
agreement before exchanging messages.
The Postcard [wire specification](https://postcard.jamesmunns.com/wire-format.html)
and [API documentation](https://docs.rs/postcard/latest/postcard/) describe the
encoding.

**Read boundaries.** The reader first attempts one prefix byte so it can
separate clean EOF from a truncated prefix. It then uses exact reads for the
remaining prefix and advertised body. `take_from_bytes` decodes one value and
exposes any remainder, allowing rejection of trailing bytes without consuming
the next frame. Oversize errors retain the advertised length; trailing-byte
errors retain the unconsumed count. These checks establish framing, while the
chosen deserializer establishes the message's domain rules.

**Bounded encoding before output.** The writer starts with an initialized prefix
and serializes directly into the same vector through the private `BoundedFrame`
Postcard storage adapter. The adapter checks the body bound before each append;
the writer backfills the prefix only after successful encoding. This prevents
an encoding error from publishing an incomplete frame.

A separate overflow flag distinguishes a real size violation from other encoder
errors. The writer checks it even if a custom serializer swallows the adapter's
error, so such a serializer cannot turn an oversized attempt into successful
output. The adapter changes storage behavior while leaving serialization to
Postcard.

**Progress and cancellation.** Tokio's exact-read/all-write operations handle
short transfers and retain their cursor while the same future is pending.
Resuming that future continues its progress. Dropping it loses the operation's
local state, and a new call starts at the stream's current position. An accepted
or consumed prefix may stop anywhere inside a frame, so restarting cannot safely
recover its boundary. A destination that accepts zero bytes produces an I/O
`WriteZero` error.

Even a complete frame write does not prove that a peer handled the message.
Replay and duplicate handling require an application protocol with the relevant
acknowledgement and identity information.

</details>

## Performance

Each operation uses a temporary vector whose capacity is released after the
call. Reads allocate and initialize the advertised body; deserializers may also
allocate owned message fields. Writes grow one vector holding the prefix and
body, then pass the completed frame to the destination.

This design targets intermittent control messages. **No message-I/O benchmarks
have been run**, and there is no throughput or zero-copy claim.

<details>
<summary>Design and maintenance notes</summary>

Checking the bound as bytes are appended avoids an unbounded `to_allocvec`
followed by a late length check. Growing on demand also avoids reserving the full
1 MiB for every small message. Vector capacity may exceed initialized length
during normal growth: the limit bounds encoded body bytes, not total allocated
memory or work performed by arbitrary custom Serde implementations.

Temporary storage keeps buffer lifetimes within each operation and lets the
traits work directly on the caller's stream. Reusing scratch buffers across
calls would require an owner for that storage. This remains deferred until
caller requirements or measurements justify changing the ownership model.

</details>

## Validation

From the repository root:

```sh
cargo test -p message-io --locked
cargo test -p message-io --release --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo rustdoc -p message-io --locked -- -D warnings
```

The test commands check the read/write behavior and all three documentation
examples, including the compile-fail ownership example inside the usage notes.

<details>
<summary>Design and maintenance notes</summary>

Independent literal fixtures check the wire format without relying on agreement
between the encoder and decoder: matching mistakes could otherwise pass a
round-trip test. Cases at numeric boundaries, zero-byte bodies and the exact size
limit establish the accepted encoding range. Malformed values, trailing bytes,
and clean versus truncated EOF distinguish framing and decoding failures.

Fragmented input and consecutive frames check that each read consumes exactly
one message. Owned-result tests establish that a decoded value survives its
reader. Buffered read-ahead and explicit-flush tests distinguish message progress
from movement of bytes through the underlying stream.

Failure tests inspect actual consumed or accepted bytes because an interrupted
operation may already have made partial progress. They cover I/O errors,
cancellation, and transport, serializer and deserializer panics. Encoding-failure
cases verify that the destination remains untouched; unpolled-future cases verify
that no encoding or I/O occurs. Wake/resume tests check that continuing the same
future neither re-encodes a message nor replays bytes.

Pinned sources and destinations exercise the extension traits' borrowing
contract. Duplex tests use small transport buffers to force backpressure across
message boundaries, exposing progress bugs that an always-ready stream could
hide.

</details>

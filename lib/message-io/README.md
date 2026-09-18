# Message I/O

Reusable async Serde message I/O through Postcard and Tokio byte sources and
destinations. Cluster-specific messages, connection setup, authentication,
application-version agreement and replication policy belong to the application.
The service does not yet use this crate for handshakes.

## API and ownership

Import `MessageReadExt` and `MessageWriteExt` to add `read_message::<T>().await`
and `write_message(&value).await` to `AsyncRead + Unpin` and `AsyncWrite + Unpin`.
Both operations exclusively borrow the byte source/destination; there are no
locks, runtime borrow checks, workers, queues or boxed futures. No additional
`Send`/`Sync` bounds are imposed. Ordinary auto traits govern executor use.

```rust
use message_io::{MessageReadExt, MessageWriteExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Hello { member: String }

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut wire = Vec::new();
wire.write_message(&Hello { member: "replica-01".into() }).await?;
let mut reader = wire.as_slice();
let hello = reader.read_message::<Hello>().await?;
assert_eq!(hello.member, "replica-01");
# Ok(())
# }
```

Reading requires `DeserializeOwned`: the returned message cannot borrow the
temporary receive buffer, which is dropped before return. It can outlive the
reader. Writing accepts `Serialize + ?Sized`, including borrowed strings/slices.
The caller selects the expected type; the frame contains no type identifier.
Application deserializers must enforce domain validity. This crate does not
make arbitrary Serde-derived values valid or authenticate their contents.

Borrowing from the temporary message buffer is prohibited at compile time:

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

## Wire contract

Each frame is a four-byte **little-endian `u32` body length**, followed by one
Postcard value. The length excludes the prefix. `LENGTH_PREFIX_LEN` and
`MAX_MESSAGE_BODY_LEN` in `protocol.rs` are authoritative. Bodies are limited to
1 MiB, inclusive, a conservative initial control-message limit rather than a
performance claim. The limit is fixed for both directions. Zero-byte bodies are
allowed because Postcard unit values and empty structs may encode no bytes.

Readers reject an oversized advertised length before allocating or reading its
body. They collect exactly the advertised bytes, decode one value with
`take_from_bytes` and reject any remainder. They do not consume the next frame.
Partial reads/writes are handled through Tokio's exact-read/all-write operations.
No COBS, checksum, compression or version negotiation is added. Record framing
remains a separate contract and no bulk-replication encoding is selected here.

Postcard serialization is positional, so application message schemas must be
compatible before this codec is used. App-version agreement is intentionally
outside this crate. Wire-format stability does not permit arbitrary schema
changes. Postcard's [wire specification](https://postcard.jamesmunns.com/wire-format.html)
and [API documentation](https://docs.rs/postcard/latest/postcard/) describe the
encoding; fixed literal protocol fixtures must accompany intentional changes.

## Completion, errors and cancellation

`MessageReadError::EndOfStream` means clean EOF before any next-prefix byte.
EOF inside a prefix or body returns `Io` retaining `UnexpectedEof`. Other concrete
transport errors are retained as sources, along with concrete Postcard decode
errors. Oversize and trailing-byte errors retain the rejected length/count.

Writers serialize the entire bounded frame before any I/O. `TooLarge` or `Encode`
therefore leaves the destination untouched and reusable. Success means only
that `AsyncWrite` accepted every frame byte, possibly into destination buffers.
Flushing is an explicit caller operation and is not a durable synchronization
or remote acknowledgement. `Io` retains the concrete destination error, including
`WriteZero` when the destination stops accepting bytes.

A read error or a write I/O error can leave a partial frame consumed/accepted.
After errors, panics or cancellation of polled operations, the connection owner
must discard the source/destination rather than retry on the same stream. The
extension traits retain no progress or poison flag and cannot enforce this rule.
Encoding errors are the safe reuse exception described above; clean EOF ends
the message stream. Dropping an unpolled future performs no encoding or I/O.
Dropping a future does not guarantee that underlying I/O stopped or accepted
nothing. A complete frame write also does not prove that a peer handled it, so
application-level replay/duplicate rules remain application responsibilities.

## Implementation and costs

This README covers `src/lib.rs` (thin exports and Rustdoc inclusion), `protocol.rs`
(constants), `message_reader.rs` and `message_writer.rs` (extension traits and
same-file tests), `message_read_error.rs` and `message_write_error.rs` (typed
errors), and `bounded_frame.rs` (private Postcard storage adapter).

Each operation uses its own temporary vector; capacity is not retained between
calls. Reads allocate/initialize the advertised body and owned deserializers may
allocate message fields. Writes grow one prefix/body vector and serialize directly
into it, then backfill the initialized prefix. The Postcard flavor checks the
bound before every append and reports overflow separately from encoder failures.
This avoids an unbounded `to_allocvec` followed by a late length check and avoids
reserving the full 1 MiB for every small message. A frame-size bound is not a bound
on allocations or work performed by arbitrary custom Serde implementations.
Vector capacity may exceed initialized length due to ordinary growth; the limit
applies to encoded body bytes, not an exact total-memory budget.

No unsafe code is used. This simple API targets control messages and makes no
throughput or zero-copy claim. Persistent scratch buffers and record batching
are deferred until ownership/use requirements or measurements justify them.

## Validation

Tests belong alongside the read/write behavior. Cover independent encoded fixtures,
fragmentation, consecutive messages, owned results, zero-byte bodies, exact size
limits, malformed encoding, trailing bytes, clean/truncated EOF, encoding failures
before I/O, partial I/O errors and cancellation/unpolled behavior. Keep ordinary
tests separate from long benchmarks; there are no message-I/O benchmarks yet.

From the workspace root:

```sh
cargo test -p message-io --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo doc -p message-io --no-deps --locked
```

The example is included in Rustdoc and checked as a documentation test.

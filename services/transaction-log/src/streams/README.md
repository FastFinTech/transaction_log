# Stream operations

This application module implements `IndexWriter`, buffered dense-index file
output, and `IndexedLogWriter`, the append lifecycle of one stream's log file
and its paired index. The pair writer composes the index writer and the exports
crate's `RecordWriter` rather than implementing either encoding/output layer.
`IndexedLogValidator` supplies the recovery boundary: it validates from a supplied
trusted endpoint, repairs index suffixes, supports explicit invalid-log-tail
removal, and hands over either a partial writer or a completed full file.

The owner supplies an empty or validated pair, buffers ordered records, commands
flushing and synchronization, and explicitly finalizes a full file. Live
subscriptions, historical requests, scheduling, checkpoint publication and
multi-file startup/rotation coordination remain future work. File acquisition
for validation uses the storage provider's named log/index opening methods.

## Where to start

| Component | Responsibility |
| --- | --- |
| [Indexed log writer](indexed_log_writer/README.md) | Append ordered records to a log/index pair; control flushing, synchronization, progress and finalization. Start here for normal output. |
| [Indexed log validator](indexed_log_validator/README.md) | Validate an existing pair from a supplied trusted boundary, repair its index, explicitly truncate invalid log tails, then hand over a partial writer or completed full file. Start here for recovery. |
| [Index writer](index_writer/README.md) | Encode offsets into a reusable buffer and send, flush or synchronize an owned file. Shared by the pair writer and validator. |

Each folder keeps its component, supporting types and detailed specification
together; tests remain in the source file of the behavior they exercise.
The component `mod.rs` files contain declarations, README inclusion and re-exports.
This top-level `mod.rs` preserves the public imports, including
`streams::IndexWriter`, `streams::IndexedLogWriter` and
`streams::IndexedLogValidator`; the component modules remain private.

The index writer is a sibling because both normal appending and index repair use
it. Recovery-only findings, errors and completion metadata live with the
validator. The validator hands ownership to the pair
writer after establishing and synchronizing the append boundary.

## Shared contracts

The [storage specification](../storage/README.md) owns file identities, the
100,000-record grouping, permanent paths, dense index layout and location models.
The [record specification](../../../transaction-log-exports/src/record/README.md)
owns record framing and CRC validity. The
[record writer specification](../../../transaction-log-exports/src/record_writer/README.md)
owns reusable record buffering and destination output semantics. Reuse those
contracts and constants; this module introduces no alternative record encoding.

Log files contain consecutive unchanged record encodings. Index files contain
one little-endian `u64` exclusive log end per record, with no header or initial
zero entry. A full pair holds 100,000 records and 100,000 index entries (800,000
index bytes). Active and finalized pairs use the same format and permanent paths.
No component here supplies file rotation, alternate encodings or path policy.

Complete log output and the log destination's flush before exposing corresponding
index bytes. Buffering, flushing and durable synchronization are distinct progress
boundaries; synchronizing the pair is not an atomic transaction across two files.
The pair writer coordinates normal output, and the validator applies the same
ordering during repair and handover.

An owner excludes concurrent writes and supplies trusted prefixes explicitly.
Failure, panic or cancellation during I/O cannot authorize reuse or publication
of a partly processed pair. Component READMEs describe the exact failure states,
recovery preconditions and handover guarantees. Scheduling, checkpoint publication
and directory-entry durability remain owner responsibilities.

## Historical reads and live delivery

Live subscriptions receive records independently of these storage components.
Historical queries open separate read-only handles to the same log and index,
with independent cursors and compatible sharing. Bytes remaining in application
buffers are not available through those handles. With normal cached local-file I/O, completed
writes can be read through the OS cache before durable synchronization.

The request owner chooses whether historical availability requires the flushed or
durable boundary. The pair writer reports both and imposes neither serving policy.
The stream-wide published prefix must also account for preceding files and startup
readiness. A request fixes its end before copying; it must not chase a growing EOF.
Concurrent index readers must handle a partially written final eight-byte entry
and must not treat raw index length as an independently verified publication marker.
No payload cache or data-serving method belongs on these components.

## Verification and performance

Component READMEs describe their same-file tests and edge cases. Preserve those
contracts when reorganizing code, and retain independent expected encodings,
failure/cancellation coverage and the compile-fail constructor example.
Each component README is included in its module's Rustdoc so its examples remain
documentation tests.

Run `cargo test -p transaction-log --locked`, the release tests, workspace Clippy,
formatting and Rustdoc checks. The application is currently a binary crate:
ordinary Cargo tests do not run its documentation examples. Check those with
`rustdoc --test` against a temporary library build of `src/main.rs`, including
the application's dependency artifacts, until a library target is introduced.
Keep temporary outputs under the ignored `target/` directory.

Long throughput benchmarks remain opt-in. These components have no disk or
recovery performance measurements yet. Future benchmarks should distinguish
buffered appends, cached file output, durable synchronization and recovery.

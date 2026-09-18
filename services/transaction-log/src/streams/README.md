# Stream operations

This application module implements `IndexWriter`, buffered dense-index file
output, and `IndexedLogWriter`, the append lifecycle of one stream's log file
and its paired index. The pair writer composes the index writer and the exports
crate's `RecordWriter` rather than implementing either encoding/output layer.
`IndexedLogValidator` supplies the recovery boundary: it validates from a supplied
trusted endpoint, repairs index suffixes, supports explicit invalid-log-tail
removal, and hands over either a partial writer or a completed full file.
`StreamCheckpoint` represents the certified local log/index boundary as a typed,
Serde-enabled value; it does not itself load or publish checkpoint files.

The owner supplies an empty or validated pair, buffers ordered records, commands
flushing and synchronization, and explicitly finalizes a full file. Live
subscriptions, historical requests, scheduling, checkpoint publication and
multi-file startup/rotation coordination remain future work. File acquisition
for validation uses the storage provider's named log/index opening methods.

## Where to start

| Component | Responsibility |
| --- | --- |
| [Stream locations](location/README.md) | Logical file IDs, sequence-to-file grouping, record endpoints and lazy range enumeration. |
| [Stream checkpoint](#stream-checkpoint-model) | Typed checkpoint boundary, Serde representation and certification/publication requirements. Persistence remains deferred. |
| [Indexed log writer](indexed_log_writer/README.md) | Append ordered records to a log/index pair; control flushing, synchronization, progress and finalization. Start here for normal output. |
| [Indexed log validator](indexed_log_validator/README.md) | Validate an existing pair from a supplied trusted boundary, repair its index, explicitly truncate invalid log tails, then hand over a partial writer or completed full file. Start here for recovery. |
| [Index writer](index_writer/README.md) | Encode offsets into a reusable buffer and send, flush or synchronize an owned file. Shared by the pair writer and validator. |

`stream_checkpoint.rs` directly owns the checkpoint model and its same-file tests.
The writer and validator folders keep their components, supporting types and detailed specifications
together; tests remain in the source file of the behavior they exercise.
The component `mod.rs` files contain declarations, README inclusion and re-exports.
This top-level `mod.rs` preserves the public imports, including
`streams::IndexWriter`, `streams::IndexedLogWriter` and
`streams::IndexedLogValidator` and `streams::StreamCheckpoint`. Location types
are available through both `streams::location` and top-level streams re-exports;
the writer/validator component modules remain private.

The index writer is a sibling because both normal appending and index repair use
it. Recovery-only findings, errors and completion metadata live with the
validator. The validator hands ownership to the pair
writer after establishing and synchronizing the append boundary.

## Shared contracts

The [location specification](location/README.md) owns logical file identities,
the 100,000-record grouping, endpoints and ranges. The
[storage specification](../storage/README.md) owns permanent paths; the
[index writer specification](index_writer/README.md#dense-index-layout) owns the
shared dense index encoding.
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
Checkpoints certify both records and their exact index entries; the owner must
establish both, synchronize log then index, and only then persist the checkpoint.
Recovery checks the supplied boundary and trusts the earlier certified contents.
Failure, panic or cancellation during I/O cannot authorize reuse or publication
of a partly processed pair. Component READMEs describe the exact failure states,
recovery preconditions and handover guarantees. Scheduling, checkpoint publication
and directory-entry durability remain owner responsibilities.

## Historical reads and live delivery

Live subscriptions receive records independently of these stream components.
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

## Stream checkpoint model

`StreamCheckpoint` stores one read-only `end: RecordEndLocation`, exposed through
a `getset` copy getter. The endpoint keeps the last included record and its byte
position together:

- `checkpoint.end().record_id()` identifies the last complete record included in
  the checkpoint. Its identity already includes the stream ID.
- `checkpoint.end().position()` is the byte offset from the beginning of that
  record's log file to immediately after the complete encoded record, including its CRC.
  The boundary is exclusive: following bytes have not been checkpointed by this
  snapshot. It is not the position of the last byte, an index-file position, or
  a cumulative byte count across multiple files.

`checkpoint.end().log_file_id()` derives the file identity through the endpoint,
so the model does not store redundant stream/file identifiers that could disagree.
Endpoint access and file lookup belong to `RecordEndLocation`; the checkpoint
does not duplicate those methods. The separate checkpoint type expresses the
recovery boundary's meaning, which a general record end location does not carry.
At sequence 99,999 the endpoint still belongs to file zero; sequence 100,000
belongs to file one and has a position within that new file. The helper never
increments the sequence and remains usable at `u64::MAX`. An endpoint may precede
the physical end of a file containing newer, uncheckpointed records.

Use `Option<StreamCheckpoint>` for an absent checkpoint, including an empty stream;
do not reserve sequence zero as a sentinel. Positions use `u64` because log files
can exceed 4 GiB. The model is a small owned `Copy` value without allocation,
mutable accessors or filesystem access. Its native layout does not define its
serialized representation.

`StreamCheckpoint::new(end)` is an infallible data constructor. It does not
claim to validate the relationship between the record and its supplied position,
and it does not add a partial numeric check in place of inspecting actual storage.
The future checkpoint owner must establish the complete contract when producing
or loading a trusted recovery checkpoint.

A checkpoint published for recovery certifies a contiguous prefix of valid records
AND correct index entries on the local replica. It cannot advance until framing,
CRC, stream identity, sequence continuity and the corresponding exact record end
offsets are established. Synchronize the covered log data, then the index data,
before durably publishing the checkpoint. It may lag this durable pair, but must
never lead either file. Constructing the model establishes none of those facts.
Checkpoint loading, persistence, advancement and startup integration remain future
work. `IndexedLogValidator` accepts the explicitly supplied trusted endpoint; it
does not load or choose the checkpoint itself. It checks prefix presence and the
final certified index entry's agreement with that endpoint, without revalidating
earlier records or index entries. It validates and repairs the suffix. A missing
trusted index prefix or mismatching endpoint requires the caller to supply an
earlier trustworthy boundary for a separate attempt.

```rust
use transaction_log::streams::RecordEndLocation;
use transaction_log::streams::StreamCheckpoint;
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(99_999));
let end = RecordEndLocation::new(id, 6_553_500_000);
let checkpoint = StreamCheckpoint::new(end);
assert_eq!(checkpoint.end().record_id(), id);
assert_eq!(checkpoint.end().position(), 6_553_500_000);
assert_eq!(checkpoint.end().log_file_id().file_number().get(), 0);

let bytes = serde_json::to_vec_pretty(&checkpoint)?;
assert_eq!(serde_json::from_slice::<StreamCheckpoint>(&bytes)?, checkpoint);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`StorageProvider::checkpoint_file_path(stream_id)` names this stream's checkpoint
at `{root}/streams/{stream_id:04}/checkpoint.json`. The path depends only on the
stream, so advancing across log-file ranges does not change the checkpoint's
location. Initialization creates its parent directory but no checkpoint file.
The future loader must check that the decoded checkpoint's stream ID matches the
requested stream, as well as establishing its agreement with storage.

### Planned historical-read availability

Historical requests require a published contiguous prefix of validated, fully
indexed records. A ready recovery checkpoint can establish a durable serving
boundary. For the active file, `IndexedLogWriter` also reports a flushed boundary:
independent read-only handles can read completed writes through the OS cache
before durable synchronization. Whether to expose that newer prefix or require
durability remains the serving owner's policy; the writer reports both boundaries.
On startup, loading checkpoint metadata alone does not make a stream ready;
required indexes must also be available and consistent, with rebuilding where
necessary. Raw index length is not a substitute for this readiness contract.

Provide a per-stream endpoint reporting the **last queryable sequence number**
so historical clients can discover this limit before requesting data. Its value
comes from the serving owner's ready, published endpoint. It may lag the last
received or appended record; those newer records are not the advertised historical
limit. Represent absence explicitly when no queryable prefix exists, rather
than using sequence zero as a sentinel. A stream still recovering must not advertise
an unready checkpoint as available history.

The reported limit is a snapshot of the serving replica's availability. Each
historical request still checks its range against that replica's ready prefix;
an earlier status response does not establish readiness on another replica or
guarantee that older files remain retained. Requests beyond the limit must not
be reported as successful shortened ranges. Whether they return unavailable or
explicitly wait, along with the endpoint's name and wire response format, remains
future protocol design. These are development notes; no endpoint or historical
request handling is implemented yet.

### JSON representation

Checkpoint metadata uses human-readable JSON. It is small and updated outside
the per-record hot path, so a compact binary encoding is not necessary. Serde
derives describe the fields, and `serde_json` provides the JSON codec:

```json
{
  "end": {
    "record_id": {
      "stream_id": 42,
      "sequence_number": 99999
    },
    "position": 6553500000
  }
}
```

Use `serde_json::to_vec_pretty(&checkpoint)` to prepare bytes for asynchronous
file output, and `serde_json::from_slice::<StreamCheckpoint>(&bytes)` to decode
them. The same traits also support `to_writer_pretty` and `from_reader` for
synchronous `std::io` destinations/sources. These codecs do not flush, sync or
atomically publish a checkpoint file. Those operations belong to the future
persistence owner, which must publish only after covered log and index data are durable.

Fields are required; missing, duplicate or unknown object fields are errors,
including inside the endpoint and its nested record ID. There are no default-zero
substitutions.
`StreamId` deserialization uses its existing checked conversion, so JSON cannot
introduce an ID outside `0..=4095`. Sequence numbers and positions decode as `u64`:
negative, fractional and overflowing values are rejected. JSON integer text
preserves the full `u64` range through `serde_json`; tooling that rewrites these
files must preserve integers rather than round them through floating point.

These checks establish typed metadata only. A syntactically valid checkpoint
can still point to missing, mismatched or non-durable records; file-level recovery
must establish those facts before trusting it. Deserialization does not add
filesystem I/O or partial record/offset validation to this model.

The serialization traits are format-independent and reuse the identifier types'
validation. There are no per-format custom deserializers or separate JSON DTOs.
JSON is the selected metadata encoding; no binary checkpoint codec or persisted
file envelope has been introduced. Metadata serialization does not change the
binary record protocol, identifier layouts, getters or hot-path validation.

### Checkpoint validation

`stream_checkpoint.rs` tests preservation of the supplied typed endpoint at sequence
zero, file rotation, a position above 4 GiB and sequence exhaustion. These tests
exercise the metadata model, not validation or durability of real stored records.
JSON fixtures check pretty output and I/O round trips, exact maximum sequence
values, large offsets, missing/duplicate/unknown fields at all three object levels,
invalid numeric values, truncation and trailing garbage. Expected JSON is literal, so serialization and
deserialization cannot silently agree on an unintended field shape.

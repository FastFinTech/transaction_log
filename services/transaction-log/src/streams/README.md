# Stream operations

Recover a stream's persisted log/index files, publish its recovery checkpoint and
continue ordered appends through an owned active pair. The module also supplies
the typed locations and progress boundaries shared by writing, recovery and future
reads. Per-stream recovery and paired writing are implemented; application startup
integration, live rotation and query serving remain planned.

## Types and modules

| Type or module | Responsibility |
| --- | --- |
| [`location`] | File identities, sequence-to-file grouping, typed record endpoints and lazy read ranges. |
| [`StreamCheckpoint`] | A stored recovery boundary; the [checkpoint contract](#stream-checkpoint-model) below owns its certification and metadata semantics. |
| [`StreamInitializer`] / [`InitializedStream`] | Recover one stream, clean discarded files, publish progress and hand over its active pair. Start here for stream recovery. |
| [`IndexedLogWriter`] / [`AppendOutcome`] | Append ordered records and control buffered, flushed and synchronized progress for one pair. Start here for normal output. |
| [`IndexedLogValidator`] / [`ValidatedFilePair`] | Recover one existing pair and return its endpoint, plus append-positioned handles when partial. |
| [`IndexWriter`] | Buffer and output dense index offsets, shared by normal appends and index repair. |
| [`validation`] / [`ValidationFile`] | Focused log/index recovery and shared file capabilities for inspection, repair and synchronization. |

The component specifications explain [paired writing](self::indexed_log_writer),
[initialization](self::stream_initializer), [pair recovery](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/validation/indexed_log_validator/README.md)
and [index output](self::index_writer). Their public types and errors are
re-exported from `streams`; location and validation types are also available
through their public modules.

## Usage

Recover one stream and consume its active pair as a writer:

```no_run
use tokio::fs::File;
use transaction_log::{
    storage::StorageProvider,
    streams::{IndexedLogWriter, StreamInitializer},
};
use transaction_log_exports::StreamId;

async fn recover_writer(
    stream_id: StreamId,
    storage: &StorageProvider,
) -> anyhow::Result<IndexedLogWriter<File>> {
    let initialized = StreamInitializer::initialize(stream_id, storage).await?;
    Ok(IndexedLogWriter::from(initialized))
}
```

The caller supplies exclusive recovery access and an established storage hierarchy.
Initialization positions the active files for appending; conversion moves them
into the writer without I/O and preserves recovered progress. The caller then
chooses when to append, flush, synchronize and finalize a full file.

## Behavior and guarantees

### Shared contracts

A log holds consecutive unchanged record encodings. Its dense index stores one
little-endian `u64` exclusive log end per record, with no header or initial zero
entry. An ordinary full pair contains 100,000 records and 800,000 index bytes.
Active and finalized pairs use the same format and permanent paths.

| Contract | Authoritative specification |
| --- | --- |
| File grouping, identities, endpoints and ranges | [Stream locations](self::location) |
| Record framing, encoded limits and CRC validity | [Record format](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/src/record/README.md#wire-and-file-contract) |
| Record buffering and output completion | [Record writer](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log-exports/src/record_writer/README.md) |
| Dense index encoding | [Index layout](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/index_writer/README.md#dense-index-layout) |
| Paths, acquisition and metadata publication | [Storage](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md) |
| Shared recovery file operations | [Validation capabilities](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/validation/README.md#shared-file-capabilities) |

Log output and destination flushing precede the corresponding index output.
Buffered, flushed and synchronized progress are distinct: only synchronization
establishes file-data durability. Pair synchronization is not an atomic transaction
across two files. Checkpoint publication follows validation of the covered records
and exact index entries, then synchronization of the log and index in that order.

Owners exclude conflicting access and supply trusted prefixes explicitly.
Recovery checks its boundary and trusts the earlier certified contents. Failure,
panic or cancellation during I/O cannot authorize publication of an incomplete
pair; component contracts determine the required recovery and reuse restrictions.

<details>
<summary>Design and maintenance notes</summary>

**Ownership follows the lifecycle.** `IndexedLogWriter` composes `RecordWriter`
and `IndexWriter`, sharing their encodings and output contracts. The index writer
is a sibling of paired writing and validation because both append and repair need
it. Recovery findings and errors stay with the validation components.

Focused validators preserve the supplied concrete file type through `ValidationFile`;
index repair adds `AsyncWrite`, while log recovery only reads, truncates and
synchronizes. The combined validator acquires files through storage and establishes
append positions. The initializer selects the contiguous stream prefix, performs
cleanup and checkpoint publication, and prepares an active pair. Its caller
constructs the writer, preserving the separation between recovery and live policy.

The writer returns an `AppendOutcome` after each paired buffered append, including
whether that record filled the file. It tracks file-local buffered, flushed and
durable endpoints while the initializer also needs stream-wide recovered progress.
An empty active successor can therefore have no local endpoint even when a
checkpoint covers preceding complete files.

Scheduling, runtime checkpoint updates and live rotation remain future work.
No alternate record format or storage-path policy is introduced by these stream
operations. Directory-entry durability follows the storage provider's platform
contract and the recovery owner's established hierarchy.

</details>

### Stream checkpoint model

`StreamCheckpoint` stores a read-only `end: RecordEndLocation`. Its `end()` getter
identifies the last included record and the exclusive byte position immediately
after that record's CRC in its own log file. The position can precede physical EOF
when newer uncheckpointed records follow it.

`StreamCheckpoint::new(end)` stores supplied metadata without I/O or validation.
A checkpoint published for recovery certifies a contiguous local prefix of valid
records **and correct index entries**. It may lag the synchronized pair but must
never lead either file. Constructing or decoding the value does not establish
that certification.

Use `Option<StreamCheckpoint>` for absence, including an empty stream; sequence
zero is a valid first record. Storage reads and publishes checkpoints, and the
initializer checks stream identity and advances progress after recovery and cleanup.

<details>
<summary>Design and maintenance notes</summary>

**One endpoint owns identity and position.** The checkpoint exposes its record ID,
position and derived file identity through `RecordEndLocation`, avoiding duplicate
stream/file fields and duplicate accessors. The position is a `LogFilePosition`,
not an index address, last-byte position or cumulative byte count across files.
It retains the full `u64` width because log files can exceed 4 GiB; `.get()` extracts
the raw offset for I/O or encoding.

Sequence 99,999 belongs to file zero; sequence 100,000 belongs to file one with a
position in that new file. Deriving the file identity never increments the sequence.
The checkpoint is a small owned `Copy` value with no allocation, mutable accessors
or filesystem access, and its native layout does not define its serialization.

The distinct checkpoint type gives a general record end its recovery meaning.
An infallible constructor deliberately avoids a partial numeric check that could
appear to certify real records. The producing recovery owner establishes framing,
CRC, stream identity, sequence continuity, exact index ends and synchronization
before publication. On reuse, pair recovery checks prefix presence and the final
certified index entry without rescanning earlier contents. A missing or mismatched
trusted index requires a separately supplied earlier trustworthy boundary for a
new attempt; there is no implicit fallback.

This example constructs and round-trips metadata only:

```rust
use transaction_log::streams::{LogFilePosition, RecordEndLocation, StreamCheckpoint};
use transaction_log_exports::{RecordId, SequenceNumber, StreamId};

let id = RecordId::new(StreamId::new(42)?, SequenceNumber::new(99_999));
let end = RecordEndLocation::new(id, LogFilePosition::new(6_553_500_000));
let checkpoint = StreamCheckpoint::new(end);
assert_eq!(checkpoint.end().record_id(), id);
assert_eq!(checkpoint.end().position(), LogFilePosition::new(6_553_500_000));
assert_eq!(checkpoint.end().log_file_id().file_number().get(), 0);

let bytes = serde_json::to_vec_pretty(&checkpoint)?;
assert_eq!(serde_json::from_slice::<StreamCheckpoint>(&bytes)?, checkpoint);
# Ok::<(), Box<dyn std::error::Error>>(())
```

**Metadata representation.** Checkpoints use readable JSON because they are small
and updated outside the per-record hot path. Serde derives reuse the endpoint and
identifier checks without format-specific deserializers or separate JSON objects.
`LogFilePosition` is transparent to Serde, retaining a numeric position field:

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

All fields are required. Missing, duplicate or unknown fields fail at the checkpoint,
endpoint and record-ID levels, with no default-zero substitutions. Stream IDs use
the checked `0..=4095` domain. Sequences and positions decode as `u64`, rejecting
negative, fractional and overflowing numbers. `serde_json` preserves the full
integer range; tooling that rewrites this metadata must not round through floating
point. These checks establish typed metadata, not the existence or durability of
the represented records.

`to_vec_pretty` and `from_slice` work with asynchronously acquired bytes; the same
traits support synchronous `to_writer_pretty` and `from_reader`. The codecs perform
no flushing, synchronization or atomic publication. No binary checkpoint codec or
persisted envelope is implemented, and metadata serialization does not change the
binary record protocol or hot-path validation.

**Persistence belongs to storage.** `checkpoint_file_path(stream_id)` names
`{root}/streams/{stream_id:04}/checkpoint.json`. Advancing across log files does not
change that location. Provider construction creates the stream directory but no
checkpoint file. The initializer verifies the decoded stream ID against the
requested stream, then coordinates recovery and publication under the
[storage checkpoint contract](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#stream-checkpoints).
Loading metadata alone does not make the stream ready.

</details>

### Historical reads and live delivery

Historical queries, live subscriptions and queryable-progress reporting are
planned. The writer already exposes flushed and durable progress; choosing which
boundary is available to readers belongs to the serving owner.

<details>
<summary>Design and maintenance notes</summary>

**A published prefix defines availability.** Historical requests need a contiguous
prefix of validated, fully indexed records. Recovery can establish a durable
boundary, while a writer's flushed boundary may permit newer data through normal
cached local-file I/O before durable synchronization. Application-buffered bytes
are not visible through separate read handles. The serving policy chooses between
these boundaries; the writer imposes neither.

Historical queries use independent read-only log/index handles with their own
cursors and compatible sharing. Live delivery is independent of these file
components. The published prefix accounts for preceding files and startup readiness;
a loaded checkpoint alone is insufficient when required indexes are unavailable
or inconsistent. Raw index length is not a publication marker: a concurrent reader
can encounter a partially written final eight-byte entry.

Each request fixes its end before copying instead of chasing a growing EOF. The
planned per-stream status endpoint reports the **last queryable sequence number**,
which may lag received or appended data. Absence is explicit when no queryable
prefix exists; a recovering stream cannot advertise an unready checkpoint.

That status is a snapshot of one replica. Every request still checks its range
against that replica's ready prefix; an earlier status response proves neither
another replica's readiness nor continued retention of old files. Requests beyond
the limit cannot be reported as successful shortened ranges. Whether they fail as
unavailable or explicitly wait, and the endpoint name and wire response, remain
future protocol decisions. No payload cache or serving API is implemented on the
stream components.

</details>

## Performance

Paired writing uses reusable record/index buffers and cached expected-record state;
recovery processes one file pair at a time. The
[writer's deferred optimization notes](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/streams/indexed_log_writer/README.md#available-optimizations-deferred)
separate possible changes from the implemented append contract.

There are no disk-output or recovery throughput measurements for these components.
The record-layer loopback benchmarks do not establish durable stream performance.
Buffered appends, cached file output, synchronization and recovery need distinct
measurements. Long throughput benchmarks remain opt-in.

<a id="verification-and-performance"></a>

## Validation

From the workspace root:

```powershell
cargo test -p transaction-log --locked streams::
cargo test -p transaction-log --release --locked streams::
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
cargo rustdoc -p transaction-log --bin transaction-log --locked -- -D warnings
```

Component specifications describe the tests for writing, recovery and locations.
Owned [storage fixtures](https://github.com/FastFinTech/transaction_log/blob/main/services/transaction-log/src/storage/README.md#shared-test-storage)
clean each test's files on scope exit while retaining empty stream directories.

<details>
<summary>Design and maintenance notes</summary>

**Documentation checks in a binary crate.** These READMEs are included in Rustdoc,
but ordinary Cargo tests skip documentation examples for the service binary.
Their executable and compile-fail examples are checked with `rustdoc --test` against
a temporary library build of `src/main.rs`, supplying the application's current
dependency artifacts. The build needs `CARGO_PKG_VERSION` for the entry point's
version string and the edition selected by the workspace. Temporary library and
documentation-test outputs stay under ignored `target/`. This checks the same
module code until a library target is introduced.

Independent expected encodings expose mistakes that a matching encoder/decoder
could share. Focused suites cover partial progress, errors, cancellation and
synchronization; composition tests observe actual files, checkpoints and returned
cursors. Compile-fail examples cover type and constructor restrictions. These
checks establish contracts, not power-loss behavior or performance results.

</details>

### Checkpoint validation

Checkpoint tests distinguish metadata correctness from validation of real storage.

<details>
<summary>Design and maintenance notes</summary>

The model preserves the supplied typed endpoint at sequence zero, file rotation,
positions above 4 GiB and the full sequence range. Literal JSON fixtures verify the
readable field shape, pretty output, I/O round trips and exact large integer values;
serialization and deserialization cannot silently agree on a different format.

Malformed cases cover missing, duplicate and unknown fields at all three object
levels, invalid numeric types and values, truncated input and trailing garbage.
These tests establish the model and codec contracts. Provider and initializer
suites separately exercise persistence, trusted-boundary checks and publication
ordering against files.

</details>

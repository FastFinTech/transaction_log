# Stream initializer

`StreamInitializer` is the planned startup recovery coordinator for **one stream**.
Its source and tests are currently commented out, and `streams` does not declare
or re-export the module. The disabled scaffold uses a removed validator API; it
must be adapted to the current [pair validator](../validation/indexed_log_validator/README.md)
before being re-enabled. No initializer or startup integration is currently running.
The requirements below describe the intended recovery lifecycle, not a callable API.

## Planned recovery phases

Construction selects one stream and borrows its provider without I/O. The planned
`initialize(&mut self)` runs these private operations in order, propagating errors.
Its eventual return is `Result<IndexedLogWriter<File>>`:

1. `load_checkpoint`: load the stream's last checkpoint or establish absence.
2. `discover_log_files`: find the maximum log and set up inclusive file enumeration.
3. `validate_and_repair`: validate logs, remove corrupt tails and repair indexes.
4. `remove_later_files`: delete pairs beyond the recovered contiguous prefix.
5. `advance_checkpoint`: synchronize covered data and publish the checkpoint.
6. `prepare_active_writer`: return the retained writer or create a fresh pair.

Recovery state, concrete error contracts and writer handover will be settled when
the scaffold is adapted. Incremental checkpoint publication after each completed
file is not implemented.

### Checkpoint loading

`load_checkpoint` calls the provider's async `read_checkpoint`. A missing path
means no checkpoint and creates nothing. Malformed JSON, oversized metadata and
other I/O failures propagate with their concrete `StorageError` and path intact.
The initializer rejects a checkpoint belonging to another stream, reporting the
path and both stream IDs. It replaces its retained checkpoint only after a
successful read and identity check; failure preserves previous state and aborts
`initialize` before file discovery. Cancellation likewise cannot accept a partial
result. Loading does not inspect log/index contents or certify readiness.

### File discovery and enumeration

After checkpoint loading, discovery asks the provider for the maximum existing
log. It rejects a maximum below the checkpoint's file, or absent despite an
existing checkpoint. Otherwise it retains private first/maximum file IDs:
the checkpoint's own file (not its successor), or file zero without a
checkpoint, through the maximum inclusive. Consumers use `first.iter_to(maximum)`
to enumerate lazily through the shared `LogFileId` API; there is no custom
initializer iterator type. With no checkpoint and no logs, both bounds are absent.
Successful discovery replaces the previous bounds;
errors or cancellation do not accept partially discovered bounds.

The shared iterator uses constant state and performs no I/O. IDs between the bounds may
name missing files; validation will detect those gaps later. The iterator stops
before asking the final ID for a successor, including at `LogFileNumber::MAX`,
and remains exhausted on subsequent calls. Empty maximum logs count. Discovery
does not inspect checkpoint/file agreement, repair indexes or delete later files.

### Single-file validation and repair

Single-file recovery delegates to `IndexedLogValidator::validate`, which returns
`Result<ValidatedFilePair, IndexedLogValidationError>`. The caller supplies an ID
from this stream. It supplies the checkpoint's
trusted endpoint only when the requested file is the checkpoint's own file;
other files are validated from the beginning, with concrete errors preserved.

The validator scans and truncates the log, synchronizes it, then replaces and
synchronizes the index suffix. Complete results contain an endpoint; partial
results also contain both files positioned for appending. No separate report or
repair call is required. It does not advance checkpoints or construct a writer.
Missing files and operational failures propagate as errors; the range loop
distinguishes a missing authoritative log from other failures.

The validator's exclusive-ownership and incomplete-I/O contracts apply. Recovery
failure or cancellation may leave a repaired index or truncated tail, but never
returns a successful pair; these changes are not an atomic pair operation.
Dropping/reopening alone does not quiesce outstanding
Tokio file operations; the future coordinator must respect that lifecycle.

### Recovery range loop

The loop consumes `first.iter_to(maximum)` and calls single-file recovery for
each ID. `ValidatedFilePair::Complete` supplies a synchronized endpoint with both
handles already closed. `Partial` results, including empty or truncated pairs,
stop the loop and retain their synchronized handles for writer handover. A partial
pair's accepted endpoint extends `validated_end`; an empty pair preserves the
previous file's endpoint. This endpoint is synchronized validation progress, not a
published checkpoint.

Only `IndexedLogValidationError::Storage(StorageError::Io)` with `NotFound` at the requested
log path is a gap. A gap in the checkpoint's own file propagates as an error;
other gaps stop recovery. Index acquisition failures, inconsistent trusted
prefixes, read failures and completion/sync failures propagate without authorizing
discard. No files means no recovery I/O or endpoint.

Successful recovery records `first_file_to_remove`: the missing file itself
(allowing removal of an orphan index), or the successor of a partial file when
later files exist. The cleanup range ends at the original discovered maximum.
No successor is requested for a partial maximum file. The subsequent cleanup
operation removes that range before checkpoint advancement; the loop itself
does not delete future pairs or publish service readiness.

Only the accepted endpoint, retained partial files and cleanup boundary are
published to the initializer after a successful loop. Failure/cancellation may
leave earlier repairs or synchronized full pairs on disk, but do not publish new
loop state or a checkpoint. This is not a rollback guarantee. The caller must
quiesce outstanding file operations before retrying; no automatic retry or startup
integration is implemented. No per-file validator collection is retained.

### Later-file cleanup and checkpoint publication

Cleanup enumerates the marked range inclusively and removes each log before its
index. Missing files are accepted, including a gap with only an orphan index.
It does not remove directories. The marker is cleared only after complete
success; failure or cancellation may leave partial deletion and prevents the
normal initialization sequence from advancing its checkpoint.

Advancement rejects an uncleared cleanup marker. The retained partial files are
already synchronized and positioned; constructing and retaining their writer is a
separate handover step. Full pairs were synchronized and closed in the range loop.
The provider
also synchronizes pair-directory entries and their ancestors through its root on
Unix before publication, covering newly created repair indexes.

A nonempty `validated_end` becomes the supplied `StreamCheckpoint` for provider
publication. Only successful publication updates the initializer's checkpoint.
An empty stream writes no checkpoint; an empty pair after a full file preserves
the full file's checkpoint boundary. Provider publication uses synchronized
staging JSON and rename replacement, with Unix ancestor-directory synchronization.
Windows directory durability and durability of the configured root's own parent
are outside that guarantee. See [storage](../../storage/README.md#stream-checkpoints).

Synchronization/publication failures return errors, possibly with a synchronized
writer retained or metadata already renamed. They do not certify app readiness.
The caller must reread metadata after an uncertain publication; no rollback,
automatic retry or cluster coordination is implemented.

## Intended responsibility

The initializer will load the stream's last checkpoint, validate subsequent log
contents, repair indexes, advance the checkpoint and hand ownership of the active
pair to an `IndexedLogWriter`. It does not initialize all streams or schedule
normal appends, flushes, replication or file rotation during service operation.
An eventual caller will coordinate initializers for multiple streams.

Reuse the [validator](../validation/indexed_log_validator/README.md) for pair recovery and
the [writer](../indexed_log_writer/README.md) for append ownership. The
[stream specification](../README.md#stream-checkpoint-model) owns checkpoint
certification; [storage](../../storage/README.md) owns physical paths and file
operations. No alternative codec or path layout belongs here.

## Requirements for the implementation step

Recovery must preserve a contiguous stream prefix and exclude concurrent writes.
A loaded checkpoint must belong to the requested stream. It certifies both log
records and their matching index entries; advancement requires synchronization
of log data, then index data, before checkpoint publication. Absence of records
is represented by an absent checkpoint, rather than a sequence-zero sentinel.
Operational failures must not authorize truncation or publish readiness.

### Agreed local recovery policy

Keep only the contiguous valid log prefix:

- On a reported corrupt log tail, truncate the current file at the last accepted
  record boundary, preserving every valid preceding record. The validator handles
  truncation and log synchronization before rebuilding and synchronizing the index.
- On a missing log file, delete any later log files beyond that gap.
- On an incomplete log file, including one made incomplete by tail truncation,
  preserve its valid prefix and delete any later log files.

Discarded future log files' paired indexes must also be removed; their offsets
cannot describe retained records. A missing derived index is repairable and is
not itself a log gap. An operational I/O failure is not evidence that a file is
missing or its contents are corrupt.

This deliberately discards disconnected local suffixes rather than attempting
to salvage records across a gap. Cluster coordination will subsequently reload
the required data. Local initialization does not perform that synchronization
or establish cluster readiness; those remain separate lifecycle responsibilities.

Adapting the disabled scaffold, fresh-pair creation and returning the retained
active writer remain to be implemented. Automatic retry after incomplete I/O and cluster synchronization
remain separate lifecycle work. No workers, locks or concurrency policy have
been selected.

This is startup work rather than a per-record hot path. There are no initializer
performance measurements or optimization claims.

## Source and validation

`stream_initializer.rs` contains the commented-out scaffold and its tests;
`mod.rs` is also outside the compiled module tree. Iterator tests live with
`LogFileId` in the [location module](../location/README.md), and active pair recovery
tests live with `IndexedLogValidator`.
When re-enabling the initializer, adapt and run its same-file tests for empty discovery, inclusive checkpoint/file-zero
starts, missing intermediate IDs, contradictory maxima, large lazy ranges and
provider failures, as well as absence without side effects, a literal
checkpoint boundary, wrong-stream rejection and concrete storage errors.
Single-file tests cover independent expected index bytes, missing/bad/extra index
suffixes, corrupt tails with zero or more valid preceding records, trusted-prefix
preservation, later-file boundary selection, missing logs and an unavailable
trusted index. They verify retained log bytes and no checkpoint publication.
Range-loop tests cover full-to-partial recovery, empty partial pairs, corrupt-tail
truncation, gap-at-zero and gap-after-full handling, a full final file, missing
checkpoint-covered logs, operational failures and retention of later files for
the subsequent cleanup step. Cleanup/publication tests cover orphan indexes,
preservation of the recovered pair, cleanup failures blocking advancement, empty
streams, sequence zero, checkpoint replacement failure and restart-visible
metadata. Provider tests cover replacement, interrupted staging reuse, partial
pair deletion and concrete filesystem collisions.
Provider tests cover bounded reads, malformed JSON and filesystem failures.
Check with service tests, formatting, Clippy and Rustdoc. The disabled initializer
tests do not run as part of today's service suite. When recovery is added,
keep tests beside its implementation and cover checkpoints, file boundaries,
index repair, gaps, corrupt input and interrupted recovery using independent
fixtures and observable filesystem results.

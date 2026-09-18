# Stream initializer

`StreamInitializer` is the planned startup recovery coordinator for **one stream**.
It is re-exported as `streams::StreamInitializer`. The type and async method
skeletons exist, but recovery is not implemented. There is no constructor,
recovery state or app startup integration. The private placeholder field prevents
construction before those contracts are defined.

## Method skeletons

`initialize(&mut self)` calls five private async methods in order, propagating
errors with `?` and returning `Result<IndexedLogWriter<File>>`:

1. `load_checkpoint`: load the stream's last checkpoint or establish absence.
2. `discover_log_files`: discover consecutive files from the recovery boundary.
3. `validate_and_repair`: validate log contents, remove corrupt tails and future
   files beyond the contiguous prefix, and repair index suffixes.
4. `advance_checkpoint`: synchronize covered data and publish the checkpoint.
5. `prepare_active_writer`: hand over a partial pair or create a fresh pair.

Every step contains `todo!()`; calling `initialize` would panic at step one.
The intermediate methods provisionally return `anyhow::Result<()>`. Recovery
state, concrete error contracts and per-file sequencing will be settled during
implementation. In particular, the skeleton does not yet implement incremental
checkpoint publication after each completed file.

## Intended responsibility

The initializer will load the stream's last checkpoint, validate subsequent log
contents, repair indexes, advance the checkpoint and hand ownership of the active
pair to an `IndexedLogWriter`. It does not initialize all streams or schedule
normal appends, flushes, replication or file rotation during service operation.
An eventual caller will coordinate initializers for multiple streams.

Reuse the [validator](../indexed_log_validator/README.md) for pair recovery and
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

### Agreed recovery policy (not implemented)

Keep only the contiguous valid log prefix:

- On a reported corrupt log tail, truncate the current file at the last accepted
  record boundary, preserving every valid preceding record. Reuse the validator's
  explicit tail-truncation operation and its index-repair contract.
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

Checkpoint persistence, discovery of consecutive files, fresh-pair creation,
failure/cancellation handling and the constructor/state API remain to be
designed and implemented. The recovery policy above is agreed, but the scaffold
still performs no deletion or truncation. No workers, locks or concurrency
policy have been selected.

This is startup work rather than a per-record hot path. There are no initializer
performance measurements or optimization claims.

## Source and validation

`stream_initializer.rs` owns the type; `mod.rs` includes this specification and
re-exports it. There is no observable behavior to unit-test yet. Check the scaffold
with service compilation, formatting, Clippy and Rustdoc. When recovery is added,
keep tests beside its implementation and cover checkpoints, file boundaries,
index repair, gaps, corrupt input and interrupted recovery using independent
fixtures and observable filesystem results.

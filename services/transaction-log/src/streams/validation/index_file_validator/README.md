# Index file validator

`IndexFileValidator` checks a caller-trusted index entry and replaces everything
following it with offsets supplied by log validation. Its associated `validate`
function owns one dense index file throughout recovery and returns the file after
successful synchronization. The unit struct holds no state or constructor. Public
callers import `IndexFileValidator`, `IndexFileValidationError` and `ValidationFile` through
`transaction_log::streams` or `transaction_log::streams::validation`.

[`IndexedLogValidator`](../indexed_log_validator/README.md) composes this
operation with log validation for one stored pair. Startup integration remains a
separate step.

Each index entry is exactly eight little-endian bytes containing one record's
absolute exclusive end in its paired log file. There is no header or sentinel.
Entry zero describes the first record assigned to the log file. This is the same
format produced by [`IndexWriter`](../../index_writer/README.md).

## Source map

- `index_file_validator.rs`: the public operation, private trusted-entry lookup
  and same-file tests.
- `index_file_validation_error.rs`: count, trusted-entry and operational errors.
- `mod.rs`: declarations, re-exports and this README's Rustdoc inclusion.

The parent [validation module](../README.md) owns the shared `ValidationFile` trait
and test file; neither belongs exclusively to index recovery.

## API and recovery sequence

`IndexFileValidator::validate<F: ValidationFile + AsyncWrite>(file, last_trusted, suffix_ends)`
takes an already-open file, an optional `RecordEndLocation` and a borrowed `&[u64]`.
Awaiting it returns `Result<F, IndexFileValidationError>`, preserving the input's
concrete file type. Tokio files implement the bound directly; ordinary calls infer
`F` without an explicit type argument. Controlled test files use the same operation.

`last_trusted` is the endpoint certified by the stream checkpoint, which is
published only after validation and synchronization of the covered log and index.
It is the same original trusted endpoint supplied to log validation, not the log
validator's newly discovered final endpoint. `None` establishes an empty prefix.

The operation has one sequence:

1. Check that the trusted entry count plus the suffix count fits in one file,
   before any I/O.
2. If a trusted endpoint exists, use its record ID to locate its dense entry and
   require the stored value to equal its byte position. Earlier entries remain
   checkpoint-certified. Without a trusted endpoint, no index bytes or metadata
   need to be read.
3. Truncate to the trusted prefix, seek there and write every supplied offset
   using `IndexWriter`. Existing suffix values are never read or compared.
4. Complete buffer output, flush the file, synchronize it and return ownership.
   Empty and already-correct indexes also go through synchronization.

`suffix_ends` contains exactly one absolute exclusive log end for every consecutive
record accepted by log validation after the trusted endpoint. The caller establishes
record validity and the meaning of those offsets; this component checks their
count. An empty slice removes all entries and partial bytes after the trusted
prefix. With no trusted prefix it leaves an empty file.

The private `read_index_at` helper reads the file length only when looking up a
trusted entry. It returns `None` if the complete entry is absent. Seek/read failures
remain I/O errors. An absent or unequal value returns `TrustedIndexMismatch` without
changing the file. Neither the length nor the trusted count persists after the call.

A caller can pass the returned file into a new call with the same checkpoint and
offsets. The resulting bytes are identical; the suffix is replaced and synchronized
again. The boundary is supplied explicitly on every call rather than advancing
implicitly. Callers establish the file cursor explicitly before later reading or
appending; the API does not promise an append position.

## File capabilities

The shared [file-capability contract](../README.md#shared-file-capabilities) supplies
reading, seeking, length operations and synchronization through `ValidationFile`.
Index validation adds `AsyncWrite` because it replaces suffix bytes. It returns the
same concrete file type after successful output and synchronization. Trait
capabilities do not establish trusted records or index contents.

## Why replace the whole suffix

The log is authoritative, and recovery already discovers every needed offset.
Unconditional replacement removes index-tail reading, comparison, matching-prefix
tracking and a separate repair-plan lifecycle. It also discards stale complete
entries and partial trailing bytes through the same operation.

A file holds at most 100,000 entries, or 800,000 encoded index bytes. Rewriting
correct entries and synchronizing each processed file is the deliberate cost of
this simpler startup operation. There are no recovery-throughput measurements yet.
The borrowed offsets are encoded into `IndexWriter`'s buffer; this component does
not allocate an input buffer for old index bytes or a separate owned repair plan.

## Ownership, completion and failure

Validation runs during startup, when no other code accesses the
file. The caller provides exclusive access; the component does not lock the file
or check for external changes. The handle must support seeking, truncation, writing
and synchronization, plus reading when a trusted entry is supplied. Earlier writes
must have finished flushing. File opening, paths and recovery coordination remain
caller responsibilities.

The validation future owns the file, returning it only after all work succeeds.
There is no persistent validator state or usability flag. Errors, panic or
cancellation drop ownership; they cannot expose a successfully validated file.
Excessive counts and missing/mismatched trusted entries leave bytes unchanged.
Errors preserve the concrete file or `IndexWriter` failure. Dropping an unpolled
future drops the owned handle without starting I/O.

Replacement is not atomic: failure can leave a truncated or partially written
suffix. Underlying file I/O may still finish after cancellation; the owner must
quiesce outstanding operations before another recovery attempt. Recovery can
rebuild the suffix from the log using the unchanged trusted checkpoint. A failed
replacement must not authorize checkpoint advancement or publication of a ready
stream. Synchronizing the index alone is not an atomic log/index commit and does not
synchronize the parent directory or publish a checkpoint.

`IndexedLogValidator` marshals log and index validation for one stored pair,
supplying the log validator's `suffix_ends()` with the same original trusted
endpoint. It returns synchronized results without publishing a checkpoint. A
future startup owner will coordinate the stream and publish its checkpoint only
after the covered pairs succeed. Neither focused validator calls the other, opens
storage paths, chooses checkpoints or starts application services.

## Testing

Tests live beside the implementation. They cover every
incomplete trusted-prefix length, mismatched trusted entries, original I/O errors,
missing/partial/correct/wrong/excessive suffixes, trusted-prefix preservation,
repeated calls with different suffix lengths, empty tails and whole-file replacement.
Independent byte fixtures verify little-endian encoding and full-width offsets.

Boundary tests include empty, nearly full and full trusted prefixes, the exact
100,000-entry limit, and rejection before mutation followed by reopening for a
corrected call. A write-only handle proves whole-file replacement does not read old
entries. A read-only handle fails even for already matching values and returns no
successful file.

The parent module's [shared test file](../README.md#shared-test-file) supplies
`TestFile` and explicit `FileProbe` controls for short writes, pauses and failures.
Ordinary filesystem tests pass a Tokio file directly. Tests cover every
incomplete byte prefix of two suffix entries, including the entry boundary, plus
truncation, flushing and synchronization. They verify concrete errors, retained
prefix/suffix bytes and that no successful file is returned after failure. Tests
quiesce abandoned handles before inspecting bytes after partial writes, following
the shared helper's cleanup contract.

Empty, matching, trusted and rebuilt indexes must wait for synchronization before
success. Synchronization failures return no file even after replacement completed.
Cancellation coverage includes unpolled futures, pending trusted-entry inspection
and each repair boundary. Pauses are at API boundaries, not inside kernel operations.
These tests do not simulate power loss, exhaust every OS failure or measure recovery
performance.

Run:

```powershell
cargo test -p transaction-log index_file_validator --locked
cargo clippy -p transaction-log --all-targets --locked -- -D warnings
cargo doc -p transaction-log --no-deps --locked
```

# Repository guidance

Keep agent guidance in this repository-wide file. Keep module-specific
requirements and design rationale in module-level `README.md` files, rather
than creating module-level `AGENTS.md` files.

Every module must have a thorough, maintained `README.md`. It must give a future
contributor or agent enough context to understand and safely change the module
without relying on prior conversations. Document its purpose and scope, public
contracts, requirements and invariants, design decisions and their reasons,
hot-path concerns and optimization evidence where applicable, testing guidance,
and the distinction between implemented and planned behavior. For modules sharing
a directory, explain their distinct responsibilities through the type/module
table and relevant behavior sections.

Add the README when creating a module. When changing an existing module, update
its README and fill material documentation gaps as part of the same work. A
placeholder or a list of APIs alone does not satisfy this requirement. Keep the
documentation precise and useful rather than adding length for its own sake.

## Working with the owner

When the owner authorizes work, confirm the concrete scope in a brief commentary
before editing: identify the requested change and its boundary. Authorization
such as "go ahead" applies to the specific agreed step, not every related idea
discussed earlier.
Extended discussion can explore an entire lifecycle while authorizing only one
small implementation step. Do not infer broader permission from the discussion's
length, detail, apparent agreement or accumulated design decisions. Those decisions
provide context for the authorized step; they do not authorize implementing later
steps. In particular, agreement about eventual startup behavior does not authorize
wiring it into startup when the current request concerns only a model or API.
If the scope is materially ambiguous, ask one focused question
before implementing the ambiguous part; otherwise state the understood scope
and proceed without requesting approval again. Adding a model or storage API
does not authorize startup wiring, setup, UUID assignment, configuration comparison
or application policy. Treat those as separate steps unless explicitly included.
If additional behavior seems necessary, explain the dependency and resolve its
scope with the owner rather than silently expanding the implementation.

Treat sequence-number exhaustion and the shortened terminal log-file range as
operationally unreachable (roughly hundreds of thousands of years away at the
intended write rate). Do not add branches, error variants, fallback behavior or
tests solely to accommodate the terminal file or `SequenceNumber::MAX` unless the
owner explicitly requests it. Existing checked successor operations may panic at
exhaustion; that is the intended policy.

Develop substantial features in small, coherent steps that the owner can read
and criticize. Implement the agreed step without bundling speculative later
architecture into it. Discussion and brainstorming are not instructions to
implement every option considered; honor explicit requests to review without
editing. Once implementation is requested, complete the agreed work without
reopening settled routine choices. For a requested guided walkthrough, explain
one manageable part at a time and wait for the owner to continue.

When the owner edits an implementation, read the current version and preserve
the intended simplification. Fix the actual defect rather than restoring an
earlier agent design by habit. Remove discarded machinery when the design changes;
do not keep it for hypothetical future use.

## Reading module specifications

Start with the root [README.md](README.md) for the workspace overview. Before
changing a module, read its README and the specifications of any related modules
whose contracts the change touches, including the relevant collapsed notes.
Collapsing is presentation only; the requirements remain authoritative. Folder
boundaries do not limit a shared contract: a reader or writer must follow the
specification for its record format.

The [record specification](services/transaction-log-exports/src/record/README.md)
owns record values and the shared wire/file format. Read it when changing records
or either I/O direction. The sibling
[reader specification](services/transaction-log-exports/src/record_reader/README.md)
owns input, batching, cancellation and failure behavior; the
[writer specification](services/transaction-log-exports/src/record_writer/README.md)
owns construction and output completion.

## Code organization and readability

- Keep general reusable utilities under `lib/`, and service applications and their
  exports crates under `services/`. Application policy belongs in the application,
  rather than being imposed by a reusable library.
- Group associated types in module folders, normally with one type per file and
  a thin `mod.rs` containing declarations, documentation and re-exports. Preserve
  documented exceptions, including the writer's mode markers beside its struct.
- Prefer direct, readable control flow. Extract helpers for a meaningful contract
  or reuse; moving the same complexity into several helpers is not simplification.
  Use names that distinguish payload length, encoded record length and batch size.
- Give shared constants and logic one authoritative home. Search existing
  protocol helpers before adding codecs, offsets, limits or checksum operations.
  Remove redundant wrappers instead of maintaining parallel implementations.

## API, ownership and responsibility boundaries

- Use typed identifiers for distinct domain concepts. Keep value objects' fields
  private and expose read-only accessors, following the existing `getset`
  conventions where appropriate. Give constructors and helpers only the visibility
  their callers need; trusted construction should not become an unchecked public API.
- Put validation at the boundary that has enough information to enforce the full
  contract. Preserve established validity through ownership and API restrictions,
  so trusted hot-path getters need not revalidate. Avoid public constructors that
  do only partial validation while appearing to establish complete validity.
  Raw-value validity and stateful rules such as sequence continuity are different
  responsibilities; the latter need the layer that owns the relevant stream state.
- Prefer concrete callbacks and generic static dispatch to boxed callbacks, queued
  trait objects or custom serializer traits when those abstractions add no needed
  capability. Use constructor-selected types when mutually exclusive APIs should
  be impossible to mix; optional capability traits can expose operations supported
  only by suitable destinations. Keep the type machinery as small as the contract.
- Establish the actual owner and execution context before designing concurrency.
  Prefer exclusive ownership and `&mut self` for single-owner components. Async
  does not by itself require shared state, locks, runtime borrow checking or
  `Send`/`Sync` bounds. Let ordinary auto traits apply unless the contract requires
  more. Introduce queues, workers, timers and pools only at the layer that needs them.
- Keep record payloads opaque to the record I/O layer. Event types, transaction
  structure, serialization choices, connection handshakes, scheduling, backpressure
  and replication policy belong to their owning application layers. Reusable I/O
  can accept an already-initialized owned destination instead of opening it itself.
- Make completion guarantees explicit. Buffering bytes, destination acceptance,
  destination flushing, durable synchronization and remote acknowledgement are
  distinct events. Account for partial progress, errors, panics and cancellation;
  never assume a failed or cancelled write accepted no bytes or can be safely
  replayed. Preserve concrete callback/I/O errors and use typed library errors,
  following the existing `thiserror` conventions.

## Hot-path engineering and safety

Consider work per field, per record and per batch separately. Reuse capacity and
amortize allocation, splitting, reference counting and I/O where ownership and
latency requirements allow. Avoid mandatory per-record allocations, copies,
buffer handoffs, locks or repeated validation without a demonstrated need.
An intentional copy can still be the right ownership tradeoff: distinguish
payload copying, allocation and handle cloning, and describe their actual costs
rather than claiming end-to-end zero-copy behavior.

Do not infer machine cost from Rust expression count. Safe fixed-width decoding
and simple abstractions can compile to ordinary loads. Inspect optimized assembly
when relevant and measure before claiming an improvement; `repr(C)`, padding,
alignment or a cached raw pointer is not automatically faster. Keep native Rust
layout separate from the explicit wire/file encoding.

Keep unsafe implementation details narrow and document the proof at each use:
bounds, initialization, alignment, aliasing, lifetime and allocation stability.
Reserved capacity is not initialized length. Raw pointers into a buffer require
a proof that storage cannot reallocate or otherwise become invalid while used;
pinning an owning struct alone does not provide that guarantee. Preserve those
proofs and publication/rollback invariants when changing buffer operations.

## Maintaining documentation

Target the root README at professionals evaluating the repository: purpose,
implementation progress, measured performance and build/run instructions. Keep
crate documentation focused on consumer APIs, examples and ownership/error
contracts. Link these layers to module specifications; do not include the entire
root README in Rustdoc or duplicate detailed contracts across them.

Use module READMEs as the maintained source for requirements and their reasons:
ownership and validation boundaries, format contracts, hot-path concerns,
optimization choices, performance evidence, and intentionally deferred work.
Distinguish implemented behavior from planned behavior and measured results from
targets or assumptions.

We are building the initial version. Describe the current agreed contract;
remove obsolete comments and APIs from superseded, unreleased designs rather
than inventing legacy compatibility requirements. Preserve an alternative's
rationale only when it helps explain the current choice.

Follow intentional user-requested design changes and update the affected module
READMEs, API contracts, and tests together. Preserve requirements and rationale
during unrelated refactors. When a decision changes, explain the replacement
rather than silently deleting its reasoning or treating current code alone as
the specification.

Keep API documentation, preconditions, and local safety proofs beside the Rust
code. They complement the broader rationale in module READMEs. Link to the
relevant specification from repository-level documentation instead of copying
its details into multiple summaries or into this file.

Some module READMEs are included in generated Rust documentation. Maintain their
examples as compilable documentation tests, including examples inside collapsed
notes. When moving or reorganizing documentation, verify doctest discovery and
check generated Rustdoc links, tables and disclosure markup.

### Module README structure

Write module and reusable-library READMEs for two reading depths: a concise guide
for users, with expandable engineering explanations for maintainers. Begin with
a short summary of what the module does. Where useful, explain its role in the
wider application, distinguishing actual use from intended use.

Use these section names and ordering consistently:

| Section | Content |
| --- | --- |
| Types and modules | Linked types or modules with short descriptions of their responsibilities. |
| Usage | A small, representative example. |
| Behavior and guarantees | Contracts callers need to use the module correctly. |
| Performance | Relevant costs and measured evidence, clearly distinguished. |
| Validation | Commands for checking the module, with explanations of its testing strategy. |

Omit sections that have no useful content. Choose descriptive subsections within
this common structure. The root README retains its evaluator-oriented structure.

Put deeper explanations beneath the relevant section in a collapsed `<details>`
block labelled `Design and maintenance notes` using `<summary>`. Keep section
headings and essential usage contracts visible. Add notes only where there is
something useful to explain; do not create empty blocks or filler.

Design and maintenance notes explain how the implementation works, why its design
was chosen, and which constraints future changes must preserve. Useful material
includes ownership reasoning, algorithms, failure handling, tradeoffs, performance
decisions and examples of more involved usage. Validation notes explain what the
tests establish and why those cases matter.

Keep agent working instructions in this repository-wide file. Module READMEs
describe the module's contracts, invariants and engineering rationale; do not add
agent-instruction sections or maintenance checklists that repeat general editing,
test-placement, benchmarking or documentation-workflow rules.

Avoid documentation about the document itself, routine source-file or dependency
inventories, and lists of unrelated responsibilities. Mention a source file or
dependency when it helps explain an engineering decision. Discuss usage in other
modules when it illuminates a design choice or demonstrates an important
interaction. Brief application context may belong in the opening summary;
detailed integration explanations belong with the relevant behavior or example.

## Validation

Use the affected module README's validation guidance and run checks appropriate
to the change. Document new public APIs. Keep tests in the same source file as
the behavior they test, and preserve independent protocol fixtures and expected
values. Recheck relevant performance evidence when changing hot-path code;
record compiler, target, and measurement conditions with new claims.

Test observable contracts and meaningful edge cases, not just matching encoder
and decoder implementations. Use independent encoded fixtures/expected values
to catch shared mistakes. Where relevant, cover numeric limits, malformed or
fragmented input, retained ownership, allocation reuse, rollback, partial I/O,
errors, panics and cancellation. Use compile-fail documentation examples for
compile-time API restrictions. Never violate unsafe preconditions to test that
malformed input is rejected; exercise the validating boundary instead.

## Benchmarking and performance claims

Use [scripts/benchmarks.py](scripts/benchmarks.py) to run, retain, report and compare
measurements. Read the [automation guide](scripts/README.md) and the
[benchmark contracts](services/transaction-log-exports/benches/README.md) for
commands, timing boundaries and artifact formats. Keep detailed procedures and
individual measurements there, rather than copying them into this file.

- Keep long benchmarks opt-in and separate from ordinary tests. Check harness
  changes with small loads before full transfers. Match the run scope to the
  question: a requested quick comparison should not grow into a repeated full
  suite. Select the relevant independent reader/writer or combined workload.
- Run measurements serially, with builds finished beforehand. Do not start
  competing builds, tests or benchmarks during measurement. After an interrupted
  run, check for surviving benchmark workers and stop that run before retrying.
  Keep source and build configuration unchanged during each suite.
- Treat this personal machine as a shared, variable measurement environment.
  Background work, thermal state, power limits and scheduling can affect results
  even when the user is hands-off. Matching machine metadata does not prove
  matching operating conditions. A single run cannot establish a code or compiler
  regression; use repeated comparable runs and report the median and spread when
  making a performance claim. Reliable regression gates need stable conditions.
- Compare the same workload and settings; explicitly identify any intentional
  difference. Compiler comparisons should use the same source and scoped
  toolchain selection, without changing the default or repository pin merely
  because a noisy measurement was slower. Do not disable validation or change
  measured work to improve a number without identifying a distinct workload.
- Retain raw logs, structured results, workload settings, source identity and
  machine/toolchain specifications, including slower samples and failed runs.
  Generate published tables from completed, validated results and link the saved
  data. Label partial/interrupted runs as such; never describe a planned artifact
  as already available or silently replace a baseline with a better-looking run.

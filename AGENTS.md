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
a directory, make the README's coverage of each module explicit.

Add the README when creating a module. When changing an existing module, update
its README and fill material documentation gaps as part of the same work. A
placeholder or a list of APIs alone does not satisfy this requirement. Keep the
documentation precise and useful rather than adding length for its own sake.

## Reading module specifications

Start with the root [README.md](README.md) for the workspace overview. Before
changing a module, read its README and the specifications of any related modules
whose contracts the change touches. Folder boundaries do not limit a shared
contract: a reader or writer must follow the specification for its record format.

The current [record specification](services/transaction-log-exports/src/record/README.md)
covers record values, the protocol, and reader/writer integration. Read it when
changing any of those areas, including the sibling `record_reader.rs` file.

## Maintaining documentation

Use module READMEs as the maintained source for requirements and their reasons:
ownership and validation boundaries, format contracts, hot-path concerns,
optimization choices, performance evidence, and intentionally deferred work.
Distinguish implemented behavior from planned behavior and measured results from
targets or assumptions.

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
examples as compilable documentation tests and preserve that connection when
moving or reorganizing documentation.

## Validation

Use the affected module README's validation guidance and run checks appropriate
to the change. Document new public APIs. Keep tests in the same source file as
the behavior they test, and preserve independent protocol fixtures and expected
values. Recheck relevant performance evidence when changing hot-path code;
record compiler, target, and measurement conditions with new claims.

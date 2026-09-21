## ADDED Requirements

### Requirement: Rust structure parser and codec execution
The production mmCIF syntax parser and supported Foldcomp decoder SHALL be
implemented and maintained as Rust modules in this repository, without replacing
them with external parser/codec backends, linking the Gemmi/Foldcomp C++ adapters
or invoking Python or a CLI decoder. The existing public providers, schemas, options and text-format
feature boundary SHALL remain compatible.

#### Scenario: Production structure decoding
- **WHEN** a consumer builds and executes the structure and Foldcomp providers after migration
- **THEN** CIF parsing and FCZ decoding SHALL use repository-owned Rust implementations
- **AND** the two provider packages SHALL NOT compile their former vendored C++ or expose the former codec/parser FFI.

#### Scenario: Foldcomp without text parsing
- **WHEN** a consumer builds Foldcomp without the text-formats feature
- **THEN** shared structural functionality SHALL remain available without activating the unused CIF parser.

### Requirement: Raw CIF compatibility
The Rust CIF backend SHALL preserve the current supported syntax, raw identifiers,
block and atom ordering, required category metadata and quoted-value provenance.
It SHALL retain contextual failures for malformed or unsupported input and enforce
existing resource limits.

#### Scenario: Missing tokens and author identifiers
- **WHEN** an mmCIF input includes unquoted missing tokens, quoted literal dots/question marks and a nonnumeric author sequence ID
- **THEN** missing values and literal strings SHALL remain distinguishable
- **AND** author and label identifiers SHALL retain their complete original values.

#### Scenario: Metadata and multiple blocks
- **WHEN** metadata occurs after atom rows or a source contains several data blocks
- **THEN** atom/residue output and block identity SHALL match the frozen baseline
- **AND** completed normalized entries SHALL be released as execution advances.

#### Scenario: Malformed syntax
- **WHEN** a loop, quoted value or required coordinate field is malformed
- **THEN** the operation SHALL fail with source/block context without returning silently truncated success or panicking.

### Requirement: Complete compatible Rust FCZ reconstruction
The Rust Foldcomp decoder SHALL preserve supported FCZ interpretation, all-atom
reconstruction, anchor correction, side chains, identity/order, OXT and B factors.
Its output SHALL feed the existing shared residue and geometry implementation.

#### Scenario: Multi-anchor structure
- **WHEN** a supported FCZ payload contains several anchors and residues with side chains
- **THEN** all reconstructed atoms, names, ordering and metadata SHALL match the frozen reference contract
- **AND** coordinate and derived-angle differences SHALL remain within established numerical bounds.

#### Scenario: Unknown residues and terminal atoms
- **WHEN** an input uses a supported unknown-residue code or terminal OXT
- **THEN** backbone-only unknown-residue behavior, terminal atom placement and atom counts SHALL match the baseline.

### Requirement: Bounded Rust decoding of untrusted bytes
Replacement parsing/decoding SHALL use checked lengths, indices and arithmetic,
validate supported layouts, reject non-finite invalid fields, and enforce input
and reconstructed-output limits before unbounded allocation. Invalid inputs SHALL
produce recoverable errors without relying on undefined upstream behavior.

#### Scenario: Understated atom count
- **WHEN** FCZ residue codes imply more atoms than the configured maximum despite a smaller header atom count
- **THEN** decoding SHALL fail before allocating the full reconstructed output.

#### Scenario: Invalid sections
- **WHEN** an input has truncated fields, inconsistent section sizes or invalid anchor indices
- **THEN** parsing SHALL terminate with a contextual error and SHALL NOT panic or read outside the supplied bytes.

### Requirement: Preserved selection and execution semantics
The port SHALL preserve database selectors, zero/K decode behavior, metadata
identity, fresh execution state and neighbor-preserving query behavior.

#### Scenario: Selected subset with irrelevant corruption
- **WHEN** K distinct valid payloads are selected and an unselected payload is corrupt
- **THEN** exactly K payloads SHALL decode and the unselected corruption SHALL NOT change the result.

#### Scenario: Empty selector
- **WHEN** an empty Foldcomp selector is supplied
- **THEN** execution SHALL return a typed empty table with zero payload decodes.

#### Scenario: Filtered residue query
- **WHEN** a query filters a residue's neighbors out of the output or projects only one angle
- **THEN** the retained angle and its null status SHALL match the complete unfiltered computation.

### Requirement: Evidence before backend replacement
Each production backend switch SHALL be supported by a versioned differential
corpus, existing regressions, bounded malformed-input testing and representative
performance/platform evidence. Numerical tolerances SHALL be frozen before
cutover and SHALL NOT be widened merely to pass the new implementation.

#### Scenario: Numerical acceptance
- **WHEN** decoded FCZ output is evaluated
- **THEN** the comparison SHALL use decoded arrays from the same encoded input
- **AND** it SHALL check identifiers, row counts, null masks and worst-case numerical errors rather than substituting a precompression RMSD allowance.

#### Scenario: Distribution acceptance
- **WHEN** a formats revision is handed to polars-bio
- **THEN** supported-platform wheel tests SHALL run against installed artifacts outside the checkout
- **AND** runtime artifacts SHALL use the Rust backends while retaining applicable code and fixture provenance notices.

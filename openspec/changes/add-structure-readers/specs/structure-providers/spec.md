## ADDED Requirements

### Requirement: Complete atom and residue format providers
The formats workspace SHALL supply PDB/mmCIF and supported local Foldcomp providers with fixed atom or residue output schemas selected by native read options. Residue identity, coordinate selection, connectivity and geometry SHALL be computed inside native provider execution, using the shared public contract referenced by design.md.

#### Scenario: Residue provider without Python
- **WHEN** a Rust DataFusion caller registers a PDB/mmCIF provider with residue output
- **THEN** it SHALL receive complete residue identity, N/CA/C/O coordinates and all six defined backbone quantities without Python or a functions-repository dependency.

#### Scenario: Common codec output
- **WHEN** a Foldcomp provider is configured for residue output
- **THEN** it SHALL use the same native residue builder and schema as text structure providers, with descriptors computed from reconstructed coordinates.

### Requirement: Modular shared native structural implementation
The structure crate SHALL own normalized identity/entry types, schemas/options, pure Float64 geometry, residue assembly and atom/residue batch construction. The Foldcomp crate SHALL reuse that implementation through a one-way dependency. Text parser/native Gemmi support SHALL be feature-gated independently of shared structural modules.

#### Scenario: Foldcomp-only consumer
- **WHEN** a consumer builds the Foldcomp crate without text format support
- **THEN** shared entry/residue functionality SHALL compile without requiring the unused PDB/mmCIF parser backend.

#### Scenario: Pure geometry test
- **WHEN** numerical kernels are tested with explicit point coordinates
- **THEN** their tests SHALL not need to parse files or construct Arrow tables to establish angle signs, ranges and undefined geometry.

### Requirement: Source identity and missing-value preservation
Providers SHALL preserve both author and label identifiers when available, nonnumeric author sequence IDs, original atom ordinals, source/model/chain/segment boundaries, alternate sites and nullable optional metadata. Missing required/non-finite coordinates SHALL fail with source context rather than being replaced by defaults.

#### Scenario: Nonnumeric author identifier and absent occupancy
- **WHEN** a valid mmCIF row has auth_seq_id X1, a distinct label sequence ID and missing occupancy
- **THEN** both identifiers SHALL remain distinct and occupancy SHALL be null.

#### Scenario: Boundary with reused chain label
- **WHEN** a PDB chain label is reused after TER or in another model
- **THEN** site identities SHALL remain distinct and residue geometry SHALL NOT cross that boundary.

### Requirement: Neighbor-preserving native scan execution
Native providers SHALL assign whole entries to workers, preserve grouped residue/neighbor context across internal chunks and output batches, and compute neighbor-dependent values before unsafe row predicates or limits. Projection SHALL retain internal data dependencies even when those columns are absent from output.

#### Scenario: Middle-residue phi projection
- **WHEN** a query requests only a middle residue's phi and filters its neighbors out of output
- **THEN** phi SHALL match the fully computed original entry's phi.

#### Scenario: Tiny batches and count-only output
- **WHEN** a residue scan uses small batches or an empty column projection
- **THEN** its descriptor values/null masks and level-specific row counts SHALL match a full scan with ordinary batches and columns.

### Requirement: Bounded entry lifetime and independent executions
Provider schema discovery SHALL avoid collection-wide coordinate decoding. Each execution SHALL have independent state and process entries under the configured partition/size budgets, releasing parser/codec/entry resources on completion, cancellation or error. Any per-entry document buffering SHALL be documented rather than described as constant-memory parsing.

#### Scenario: Repeated concurrent scans
- **WHEN** independent executions read one source with different output options
- **THEN** they SHALL not share mutable cursors or overwrite each other's options/results.

#### Scenario: Collection progression
- **WHEN** an execution advances from one completed structure to the next
- **THEN** it SHALL release decoded data for completed entries instead of retaining the whole decoded collection.

### Requirement: Checked Foldcomp subset selection before decoding
The Foldcomp provider SHALL validate supported sidecars/record bounds, distinguish names/keys/titles/ordinals, and schedule only selected distinct payloads before codec invocation. Omitted selectors SHALL mean all entries and empty selectors SHALL mean zero. Missing or ambiguous selected names SHALL error; numeric-key selection MAY omit lookup names when the supported index suffices.

#### Scenario: Two-entry subset
- **WHEN** two valid distinct names are selected from a larger database
- **THEN** exactly those two payloads SHALL decode, with metadata lookup work measured separately.

#### Scenario: Empty selection
- **WHEN** an empty selector is supplied
- **THEN** the provider SHALL return a typed empty result with zero payload decodes

#### Scenario: Invalid selected range
- **WHEN** a selected payload range is invalid
- **THEN** the provider SHALL fail before passing that range to the native codec.

### Requirement: Independent native corpus and versioned consumer handoff
The formats repository SHALL own the pinned independent oracle generator and canonical small input/output corpus for atom, residue and codec behavior. Normal correctness tests SHALL run offline against committed expected tables, and polars-bio fixture copies SHALL carry the corpus revision and content hashes.

#### Scenario: Full parity check
- **WHEN** native output is compared with a golden table
- **THEN** complete key multisets, row counts, types and null masks SHALL be checked before applying established numerical tolerances.

#### Scenario: Exported integration subset
- **WHEN** a corpus subset is supplied to polars-bio
- **THEN** its source revision/input/output hashes SHALL be verifiable and wheel tests SHALL not require a sibling formats checkout or live downloads.

### Requirement: Consumable native artifacts and release compatibility
Native parser/codec dependencies SHALL pass the supported-platform feasibility checks and be included with required build inputs/notices in distribution artifacts. New format crates SHALL use compatible shared-format/DataFusion/Arrow versions and expose the provider interfaces needed by polars-bio without a functions release.

#### Scenario: Python integration handoff
- **WHEN** the native providers pass release gates
- **THEN** a compatible formats version/tag/commit and corpus revision SHALL be available for polars-bio's bindings and wheel validation.

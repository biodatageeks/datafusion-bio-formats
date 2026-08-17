## ADDED Requirements

### Requirement: Gated GRG On-Disk Compatibility

The system SHALL enable the GRG provider only after supported on-disk versions,
required graph sections, and independently implemented parsing rules are
recorded in a compatibility matrix.

#### Scenario: Listed GRG version
- **WHEN** a local GRG declares a version in the compatibility matrix
- **AND** all required sections are present
- **THEN** the provider may open it as an immutable graph.

#### Scenario: Unknown GRG version
- **WHEN** a GRG declares an unlisted version or required feature flag
- **THEN** opening fails with an unsupported-version error
- **AND** the provider does not guess section layouts.

#### Scenario: Compatibility feature disabled
- **WHEN** the GRG crate has not passed its compatibility and licensing gates
- **THEN** it is not enabled in default release artifacts.

### Requirement: Local Immutable GRG Scope

The initial GRG provider SHALL open read-only local graph files and SHALL reject
remote object-store locations rather than implicitly downloading an unbounded
graph.

#### Scenario: Local GRG
- **WHEN** a caller supplies a supported local file
- **THEN** the provider opens it read-only
- **AND** may use memory mapping or bounded local random access.

#### Scenario: Remote GRG
- **WHEN** a caller supplies an HTTP, S3, GCS, Azure, or other object-store
  location
- **THEN** opening fails with a documented local-only limitation
- **AND** no full-object download is started.

### Requirement: GRG Mutation View

The system SHALL expose one output row per GRG mutation, not one row per unique
position, with stable mutation identity, position, available reference and
alternate alleles, and nested selected-sample presence.

#### Scenario: Distinct mutations at one position
- **WHEN** multiple GRG mutations share a genomic position
- **THEN** each mutation is emitted as a distinct row
- **AND** their mutation identifiers are preserved.

#### Scenario: Typed mutation identifier
- **WHEN** a mutation has a stable numeric graph identifier
- **THEN** it is exposed as `mutation_id: UInt64`
- **AND** the common `id` presentation is stable and reversible.

#### Scenario: Missing allele annotation
- **WHEN** a supported graph does not store a reference or alternate string for
  a mutation
- **THEN** the corresponding allele column is null
- **AND** the provider does not infer a base from another mutation.

### Requirement: GRG Contig And Coordinate Semantics

The system SHALL use a graph-stored contig when available or an explicit
`contig_name` read option when the graph represents a contig externally.

#### Scenario: Stored contig
- **WHEN** mutation metadata contains a contig
- **THEN** `chrom` preserves that value.

#### Scenario: Configured contig
- **WHEN** the graph has positions but no contig field
- **AND** `contig_name` is supplied
- **THEN** every mutation row uses the configured contig.

#### Scenario: No contig identity
- **WHEN** neither stored nor configured contig is available
- **THEN** `chrom` is null
- **AND** chromosome predicates cannot be reported as exact except for
  null-aware expressions explicitly supported by the provider.

#### Scenario: Position conversion
- **WHEN** a mutation position is emitted
- **THEN** `start` and `end` follow the shared site-only coordinate contract.

### Requirement: GRG Haplotype Sample Identity

The system SHALL expose GRG sample nodes in stable source order using stored
sample labels or deterministic node-based names.

#### Scenario: Stored sample labels
- **WHEN** the graph stores unique labels for haplotype sample nodes
- **THEN** those labels define selectable sample names.

#### Scenario: Unlabeled sample nodes
- **WHEN** sample nodes have no stored labels
- **THEN** the provider creates deterministic names containing their stable node
  identifiers
- **AND** metadata marks the names as synthetic.

#### Scenario: Selected haplotype order
- **WHEN** a caller selects haplotypes in a custom order
- **THEN** genotype values and sample metadata follow request order.

### Requirement: GRG Haplotype Presence Mode

The system SHALL provide a haplotype mode where `GT: List<UInt8>` is `1` when a
selected haplotype inherits the row mutation, `0` when it does not, and null
when graph missingness makes presence unknown.

#### Scenario: Mutation descendant haplotype
- **WHEN** graph inheritance assigns the mutation to a selected haplotype
- **THEN** its `GT` value is `1`.

#### Scenario: Non-descendant haplotype
- **WHEN** graph inheritance determines that a selected non-missing haplotype
  does not carry the mutation
- **THEN** its `GT` value is `0`.

#### Scenario: Missing haplotype state
- **WHEN** the GRG missing-data representation makes the mutation state unknown
  for a selected haplotype
- **THEN** its `GT` value is null.

### Requirement: GRG Fixed-Ploidy Individual Mode

The system SHALL provide an individual mode that groups consecutive haplotype
sample nodes by an explicit positive ploidy and emits the count of mutation
copies from zero through that ploidy.

#### Scenario: Diploid grouping
- **WHEN** individual mode uses `ploidy = 2`
- **THEN** each consecutive pair of selected source haplotypes produces one
  individual value from `0` through `2`.

#### Scenario: Arbitrary fixed ploidy
- **WHEN** individual mode uses a valid ploidy `P`
- **THEN** each complete consecutive group of `P` haplotypes produces one value
  from `0` through `P`.

#### Scenario: Incomplete final group
- **WHEN** the source or selected grouping is not divisible by configured
  ploidy
- **THEN** planning fails with an incomplete-group error
- **AND** no haplotype is silently dropped.

#### Scenario: Missing member haplotype
- **WHEN** any haplotype in an individual group has unknown mutation state
- **THEN** the individual's `GT` value is null.

#### Scenario: Selection boundary
- **WHEN** individual mode is active
- **THEN** callers select individuals or complete declared haplotype groups
- **AND** cannot create an individual from unrelated partial groups.

### Requirement: GRG Mutation Catalog Pushdown

The system SHALL evaluate supported mutation-ID, position, allele, and available
contig predicates against lightweight mutation metadata before genotype graph
traversal.

#### Scenario: Mutation ID selection
- **WHEN** equality or `IN` selects stable mutation IDs
- **THEN** only exact matching mutations are scheduled for traversal
- **AND** pushdown is reported as `Exact`.

#### Scenario: Position interval
- **WHEN** a supported position interval is supplied
- **THEN** exact matching mutation catalog entries are selected before sample
  traversal.

#### Scenario: Configured contig mismatch
- **WHEN** `contig_name` is configured
- **AND** an exact chromosome predicate excludes that contig
- **THEN** the provider returns an empty plan without traversing the graph.

### Requirement: GRG Projection And Sample Pushdown

The system SHALL skip genotype traversal for metadata-only scans and restrict
genotype computation to selected mutation and sample nodes.

#### Scenario: Metadata-only mutation query
- **WHEN** `genotypes` is not projected
- **THEN** mutation metadata is emitted without descendant-sample traversal.

#### Scenario: Sparse sample selection
- **WHEN** only a subset of haplotypes or individuals is selected
- **THEN** the provider computes presence only for graph paths needed to resolve
  that subset where the graph representation permits.

#### Scenario: Empty sample selection
- **WHEN** the explicit sample set is empty
- **THEN** no sample-presence traversal is performed.

### Requirement: Downward Graph Traversal Strategy

The system SHALL use the graph direction and indexes intended for
mutation-to-descendant-sample queries and SHALL avoid constructing optional
upward edges unless a documented selected algorithm requires them.

#### Scenario: Selected sparse mutation
- **WHEN** a mutation reaches only a small descendant sample set
- **THEN** traversal work is proportional to its visited subgraph rather than
  all mutation/sample pairs.

#### Scenario: Upward edges absent
- **WHEN** the mutation view can be evaluated through existing downward edges
- **THEN** provider opening does not build a complete upward-edge index.

#### Scenario: Shared subgraph
- **WHEN** traversal encounters graph nodes shared by multiple paths
- **THEN** visited-state handling prevents duplicate sample counts
- **AND** respects the GRG inheritance model.

### Requirement: GRG Mutation Partitioning

The system SHALL partition selected mutation catalog ranges across no more than
the DataFusion target partition count while sharing immutable graph storage.

#### Scenario: Parallel mutation ranges
- **WHEN** selected mutations and target partitions permit parallelism
- **THEN** each mutation is owned by one physical partition
- **AND** the immutable graph can be read concurrently without mutation.

#### Scenario: Parallel output order
- **WHEN** multiple GRG partitions execute
- **THEN** every selected mutation is emitted once
- **AND** global mutation order is not promised.

#### Scenario: One partition
- **WHEN** the effective partition count is one
- **THEN** mutation catalog order is preserved.

### Requirement: GRG Graph Integrity

The system SHALL validate on-disk section bounds, node and edge identifiers,
sample-node declarations, mutation references, graph invariants required by the
supported format version, and configured allocation limits.

#### Scenario: Out-of-bounds edge
- **WHEN** an edge references a node outside the declared node range
- **THEN** opening or traversal fails with graph section and node context.

#### Scenario: Invalid mutation node
- **WHEN** a mutation references an invalid graph node or malformed allele
  payload
- **THEN** the provider returns a corruption error
- **AND** does not emit the mutation as universally absent.

#### Scenario: Declared graph exceeds limits
- **WHEN** file-declared node, edge, mutation, or string counts exceed configured
  limits or checked arithmetic
- **THEN** opening fails before the corresponding allocation.

### Requirement: GRG Differential Conformance

The system SHALL validate supported graph semantics against independently
generated expected matrices and an external compatible GRG implementation.

#### Scenario: Synthetic topology fixture
- **WHEN** a synthetic graph includes shared ancestry, recurrent positions,
  missingness, and multiple selected sample subsets
- **THEN** haplotype and individual outputs match hand-derived expected values.

#### Scenario: External oracle fixture
- **WHEN** the same compatible fixture is queried through this provider and
  external Python `grgl`
- **THEN** mutation metadata and presence values agree after documented ID and
  coordinate normalization.

### Requirement: Independent GRG Licensing Boundary

The system SHALL implement GRG runtime support without linking, vendoring, or
copying GPL `grgl` code into default project artifacts unless a separate
licensing decision explicitly changes that boundary.

#### Scenario: Normal workspace build
- **WHEN** the GRG feature is built after approval
- **THEN** the runtime dependency graph contains no GPL `grgl` library.

#### Scenario: Optional external oracle
- **WHEN** Python `grgl` is used for differential tests
- **THEN** it executes as a separate optional test process
- **AND** its absence does not prevent normal Rust builds.

### Requirement: Read-Only Mutation-View Scope

The system SHALL limit the initial GRG capability to reading the mutation view
and SHALL NOT require graph writing, graph simplification, ancestry queries, or
association algorithms.

#### Scenario: Provider registration
- **WHEN** approved GRG support is enabled
- **THEN** a compatible local graph can be registered as a DataFusion mutation
  table
- **AND** no graph mutation API is exposed.

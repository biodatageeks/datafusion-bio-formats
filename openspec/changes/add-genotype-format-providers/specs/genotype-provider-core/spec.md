## ADDED Requirements

### Requirement: Variant-Major Genotype Batch Contract

The system SHALL expose genotype sources as one Arrow row per source variant or
graph mutation, with selected sample values stored in children of a nested
`genotypes` struct rather than by default emitting one row per variant/sample
pair.

#### Scenario: Multi-sample variant
- **WHEN** a source variant contains calls for multiple selected samples
- **THEN** the provider emits one variant row
- **AND** each projected genotype child contains values in selected-sample order.

#### Scenario: Empty explicit sample selection
- **WHEN** the caller explicitly selects no samples
- **THEN** the provider emits the selected variant metadata rows
- **AND** it does not decode sample genotype values.

### Requirement: Stable Sample Metadata

The system SHALL record the final ordered selected sample names in Arrow field
metadata using a documented key that is stable across batches and physical
partitions.

#### Scenario: Reordered sample request
- **WHEN** a caller requests samples in an order different from source order
- **THEN** every genotype child uses the request order
- **AND** field metadata records that same order under the shared genotype
  sample-names key.

#### Scenario: Duplicate requested names
- **WHEN** the requested sample list repeats a sample name
- **THEN** the provider retains the first occurrence
- **AND** subsequent occurrences do not create duplicate genotype values.

#### Scenario: Unknown sample with strict policy
- **WHEN** a requested sample is absent
- **AND** `missing_sample_policy` is not explicitly set to `ignore`
- **THEN** planning fails with an error naming the absent sample.

#### Scenario: Unknown sample with ignore policy
- **WHEN** a requested sample is absent
- **AND** `missing_sample_policy = ignore`
- **THEN** the absent sample is omitted
- **AND** metadata records only the final samples that are present.

### Requirement: Coordinate System Contract

The system SHALL support the project's one-based closed and zero-based
half-open coordinate modes without changing source variant identity.

#### Scenario: One-based site-only position
- **WHEN** a site-only source position is `P`
- **AND** one-based coordinates are requested
- **THEN** `start = P`
- **AND** `end = P`.

#### Scenario: Zero-based site-only position
- **WHEN** a site-only source position is `P`
- **AND** zero-based coordinates are requested
- **THEN** `start = P - 1`
- **AND** `end = P`.

#### Scenario: Invalid source position
- **WHEN** a source declares a one-based position less than one
- **THEN** the provider returns a contextual format error
- **AND** it does not underflow during coordinate conversion.

### Requirement: Format-Native Allele Semantics

The system SHALL expose allele columns according to the source format and SHALL
NOT infer biological reference/alternate orientation where the source does not
define it.

#### Scenario: Ordered non-reference alleles
- **WHEN** PLINK 1 or BGEN is scanned
- **THEN** the provider exposes its native ordered alleles
- **AND** it does not silently rename them to `ref` and `alt`.

#### Scenario: Counted allele metadata
- **WHEN** an output field contains a genotype or dosage relative to an allele
- **THEN** Arrow metadata identifies the counted allele or allele-index order.

### Requirement: Projected Genotype Fields

The system SHALL allow a caller to select format-supported genotype children
before decoding and SHALL preserve the format's lossless output mode.

#### Scenario: Subset of genotype fields
- **WHEN** a caller requests only one supported genotype child
- **THEN** unrequested children are absent from the projected schema
- **AND** their values are not converted into Arrow arrays.

#### Scenario: Unsupported genotype field
- **WHEN** a caller requests a field unavailable in the source or output mode
- **THEN** planning fails with an unsupported-field error
- **AND** the error lists the available fields.

#### Scenario: Missing genotype value
- **WHEN** a source marks a selected sample genotype as missing
- **THEN** the corresponding outer sample value is null
- **AND** an empty inner list is not substituted for missingness.

### Requirement: Projection Before Payload I/O

The system SHALL resolve projection before genotype payload reads and SHALL read
hidden metadata dependencies only when required for filtering.

#### Scenario: Metadata-only projection
- **WHEN** a query does not project `genotypes`
- **AND** its filters do not depend on genotype values
- **THEN** genotype payload bytes are not read when companion metadata can
  answer the query.

#### Scenario: Unprojected filter column
- **WHEN** an exact or residual filter requires a metadata column that is not
  projected
- **THEN** the provider reads that column as a hidden dependency
- **AND** removes it from final output.

#### Scenario: Empty projection count
- **WHEN** an unfiltered count has no projected columns
- **AND** a trusted header or companion contains the exact row count
- **THEN** the provider may answer without reading variant payload records.

### Requirement: Companion File Resolution

The system SHALL resolve explicit companion locations before conventional
same-dataset suffixes and SHALL validate that all companions describe a
consistent fileset.

#### Scenario: Explicit companion
- **WHEN** the caller supplies an explicit companion location
- **THEN** that location is used without probing conventional alternatives.

#### Scenario: Conventional companion
- **WHEN** no explicit location is supplied
- **THEN** the provider probes only documented format-specific companion names
- **AND** uses the first valid consistent companion.

#### Scenario: Missing companion
- **WHEN** a required companion cannot be resolved
- **THEN** the error lists sanitized attempted locations
- **AND** credentials and signed query parameters are not included.

#### Scenario: Inconsistent fileset
- **WHEN** companion sample or variant counts disagree with the primary file
- **THEN** planning fails before genotype payload decoding.

### Requirement: Object-Store Companion Semantics

The system SHALL use the configured OpenDAL storage context for primary and
companion objects except where a format capability explicitly documents a
local-only restriction.

#### Scenario: Remote conventional companion
- **WHEN** a remote primary object has a conventional companion in the same
  storage namespace
- **THEN** discovery checks that remote namespace
- **AND** does not assume a local filesystem path.

#### Scenario: Remote SQLite companion
- **WHEN** a selected remote index requires local random-access APIs
- **THEN** the provider uses a bounded identity-aware local cache
- **AND** validates object identity before reusing a cached copy.

#### Scenario: Concurrent cache opens
- **WHEN** concurrent scans open the same unchanged remote companion
- **THEN** at most one complete download is required
- **AND** every scan observes an atomically completed cache entry.

### Requirement: Genomic And Identifier Predicate Planning

The system SHALL plan supported conjunctions over chromosome, coordinates, and
format-supported identifiers before genotype payload decoding.

#### Scenario: Unsatisfiable predicate
- **WHEN** supported predicate analysis proves the selection is empty
- **THEN** the provider returns a valid empty execution plan
- **AND** no primary genotype payload is read.

#### Scenario: Exact metadata predicate
- **WHEN** a metadata catalog evaluates the complete supported expression
- **THEN** pushdown is reported as `Exact`
- **AND** DataFusion does not need a residual for that expression.

#### Scenario: Index candidate predicate
- **WHEN** an index yields a superset that requires record validation
- **THEN** pushdown is reported as `Inexact`
- **AND** record-level filtering removes false-positive candidates.

#### Scenario: Unsupported nested genotype predicate
- **WHEN** a filter depends on a nested genotype value unsupported by the
  provider
- **THEN** pushdown is reported as `Unsupported`
- **AND** the expression remains a DataFusion residual.

### Requirement: Correct Limit Pushdown

The system SHALL apply a pushed limit only at a point that cannot remove rows
needed after exact, inexact, or residual filter evaluation.

#### Scenario: Limit after exact filtering
- **WHEN** every predicate is evaluated exactly by the provider
- **THEN** the provider may stop after emitting the requested number of
  matching rows.

#### Scenario: Inexact candidates
- **WHEN** an index returns inexact candidates
- **THEN** the provider validates candidates before counting them toward the
  limit.

#### Scenario: Unsupported residual
- **WHEN** DataFusion retains an unsupported residual filter
- **THEN** the scan does not push the limit below that residual unless
  DataFusion proves the transformation safe.

### Requirement: Range And Partition Planning

The system SHALL assign selected physical source ranges to no more than the
DataFusion target partition count using format-appropriate byte estimates.

#### Scenario: Fewer work units than target partitions
- **WHEN** selected independent work units are fewer than target partitions
- **THEN** the effective partition count does not exceed the work-unit count.

#### Scenario: Overlapping selected ranges
- **WHEN** an index returns overlapping ranges for the same physical records
- **THEN** the planner merges or de-duplicates them before ownership assignment
- **AND** output rows are not duplicated.

#### Scenario: Parallel output
- **WHEN** a scan has multiple physical partitions
- **THEN** all selected rows are emitted exactly once
- **AND** no global source-order guarantee is made without an explicit sort.

#### Scenario: Single-partition output
- **WHEN** a scan has one physical partition
- **THEN** source record order is preserved for emitted rows.

### Requirement: Scoped Decoder Concurrency

The system SHALL use DataFusion physical partitions as the outer concurrency
mechanism and SHALL NOT create an additional unbounded decoder thread pool.

#### Scenario: Parallel DataFusion scan
- **WHEN** target partitions are greater than one
- **THEN** each partition uses bounded explicit inner decoder concurrency
- **AND** unrelated scans are not affected through process-global settings.

### Requirement: Coalesced Sparse Range I/O

The system SHALL coalesce adjacent selected ranges only within configured gap
and maximum-request thresholds.

#### Scenario: Nearby ranges
- **WHEN** selected ranges are separated by no more than the coalescing gap
- **AND** their combined request fits the maximum range size
- **THEN** they may be fetched in one request.

#### Scenario: Distant sparse ranges
- **WHEN** selected ranges span a large unselected interval
- **THEN** the provider does not fetch the entire interval merely to reduce
  request count.

### Requirement: Bounded Streaming Output

The system SHALL stream RecordBatches bounded by the DataFusion session batch
size and a configurable soft genotype-byte budget.

#### Scenario: Row count bound
- **WHEN** a partition produces more rows than `SessionConfig::batch_size()`
- **THEN** it emits multiple batches
- **AND** no batch exceeds the configured row count.

#### Scenario: Wide cohort bound
- **WHEN** estimated genotype values would exceed the soft byte budget before
  reaching the row limit
- **THEN** the current batch is emitted early.

#### Scenario: Single oversized valid row
- **WHEN** one valid row alone exceeds the soft byte budget but not a hard
  allocation limit
- **THEN** that row may be emitted as a one-row batch.

#### Scenario: Malicious declared dimensions
- **WHEN** file-declared dimensions exceed configured hard limits or overflow
  checked arithmetic
- **THEN** decoding fails before attempting the allocation.

### Requirement: Genotype Scan Metrics

The system SHALL expose metrics that distinguish metadata pruning, physical
I/O, decompression, genotype decoding, and emitted rows.

#### Scenario: Projection skips genotype payload
- **WHEN** a metadata-only query completes
- **THEN** metrics report zero genotype payload bytes when the companion
  contains all requested data
- **AND** report the number of payload records skipped.

#### Scenario: Sparse samples in compressed blocks
- **WHEN** a format requires whole-block decompression but only a sample subset
  is selected
- **THEN** metrics separately report decompressed bytes and decoded sample
  values.

#### Scenario: Partition dependency records
- **WHEN** a format partition reads internal dependency records
- **THEN** metrics distinguish dependency reads from emitted variant rows.

### Requirement: Contextual And Non-Lossy Errors

The system SHALL distinguish malformed input, unsupported valid features,
missing companions, inconsistent filesets, and configured resource limits.

#### Scenario: Corrupt genotype payload
- **WHEN** a genotype payload is truncated or violates format invariants
- **THEN** the scan returns a contextual error
- **AND** does not convert corruption to missing genotype values.

#### Scenario: Valid unsupported feature
- **WHEN** a file uses a valid feature outside the implemented support matrix
- **THEN** the provider returns an unsupported-feature error identifying that
  feature.

#### Scenario: Streaming error after prior batches
- **WHEN** corruption is discovered after valid batches have been emitted
- **THEN** the stream terminates with the error
- **AND** the error is not suppressed as end-of-stream.

### Requirement: Independent Implementation Licensing Boundary

The system SHALL keep incompatibly licensed reference implementations outside
default runtime artifacts unless a separate documented licensing review
approves their use.

#### Scenario: Differential testing with external oracle
- **WHEN** LGPL or GPL tooling is used to verify generated fixtures
- **THEN** it runs as an optional external test process
- **AND** default Rust artifacts do not link or vendor that tooling.

#### Scenario: Production decoder source
- **WHEN** PGEN or GRG production decoding is implemented
- **THEN** the implementation is based on public format documentation and
  independently authored code
- **AND** its dependency license review is recorded.

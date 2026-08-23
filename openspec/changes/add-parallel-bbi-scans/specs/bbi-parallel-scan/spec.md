## ADDED Requirements

### Requirement: Partitioned whole-file BBI scans

The BigWig and BigBed providers SHALL expose source partitions derived from the
configured DataFusion target partition count and the file's embedded primary
cir-tree layout.

#### Scenario: Whole-file scan requests parallelism

- **WHEN** an unfiltered BigWig or BigBed scan requests `N` target partitions
- **THEN** the physical BBI source reports up to `N` non-empty partitions
- **AND** each execution partition reads only its assigned regions

#### Scenario: One chromosome dominates encoded work

- **WHEN** a whole-file scan contains one region whose encoded block work
  dominates the other selected regions
- **THEN** the planner MAY split that region at block-informed coordinate
  boundaries
- **AND** assignments are balanced by estimated on-disk data bytes rather than by
  region count alone

#### Scenario: Fewer independently readable components than requested partitions

- **WHEN** a scan requests more partitions than there are coordinate-overlap
  connected primary-data block components
- **THEN** the source reports fewer than the requested partitions
- **AND** no cut is placed through a component solely to reach the requested
  count

#### Scenario: Layout traversal reaches its safety limit

- **WHEN** a valid BBI file exceeds the configured complete-index traversal
  safety limit
- **THEN** provider construction succeeds
- **AND** the scan falls back to one source partition

#### Scenario: Narrow filtered scan

- **WHEN** a scan explicitly selects one genomic region
- **THEN** the provider uses one source partition instead of opening multiple
  independent readers for the narrow lookup

#### Scenario: Residual coordinate predicate

- **WHEN** a coordinate-column predicate cannot be converted into an explicit
  indexed region
- **THEN** the predicate does not disable source parallelism for the remaining
  whole-file scan

### Requirement: Partition-invariant BBI content

Parallel BBI scans MUST emit the same records and original coordinates as the
equivalent one-partition scan, independent of cross-partition ordering.

#### Scenario: Record overlaps an internal shard boundary

- **WHEN** independent shard queries both encounter a record overlapping their
  shared coordinate boundary
- **THEN** exactly one shard emits that record according to start-coordinate
  ownership
- **AND** its start and end coordinates equal the one-partition result

#### Scenario: Empty selection

- **WHEN** pushed-down genomic filters select no valid BBI regions
- **THEN** the scan completes with zero rows and a valid physical execution plan

### Requirement: Bounded streaming projections

Partitioned BBI execution MUST preserve projection pushdown and bounded streaming
batch construction.

#### Scenario: Empty projection count

- **WHEN** DataFusion requests an empty BBI projection for `count(*)`
- **THEN** batches represent logical row counts without allocating value arrays
- **AND** batch row counts remain bounded independently of chromosome size so
  consumers can cancel or satisfy a limit without decoding the entire region

#### Scenario: Decoded projection

- **WHEN** DataFusion requests one or more BBI columns
- **THEN** every emitted record batch contains only the projected fields
- **AND** no execution partition buffers an entire chromosome

### Requirement: Partition diagnostics

The physical BBI plan SHALL expose enough metadata to verify partition planning
without reading result batches.

#### Scenario: Inspect physical plan

- **WHEN** a caller inspects a BigWig or BigBed execution plan
- **THEN** the plan reports its source partition count
- **AND** it reports the estimated data bytes assigned to each partition

## ADDED Requirements

### Requirement: Target-Partition Controlled VCF Zarr Scans

The system SHALL expose VCF Zarr scans through DataFusion physical partitions controlled by the session target partition count.

#### Scenario: Single target partition
- **WHEN** a VCF Zarr table is scanned with `target_partitions = 1`
- **THEN** the execution plan exposes one physical partition
- **AND** scan results match existing single-partition behavior.

#### Scenario: Multiple target partitions
- **WHEN** a VCF Zarr table is scanned with `target_partitions > 1`
- **AND** the selected data spans multiple Zarr variant chunks
- **THEN** the execution plan exposes multiple physical partitions
- **AND** the effective partition count does not exceed `target_partitions`.

### Requirement: Chunk-Aligned VCF Zarr Partitioning

The system SHALL preserve selected Zarr variant chunk boundaries when partitioning VCF Zarr scans.

#### Scenario: Selected chunks fewer than target partitions
- **WHEN** a scan selects fewer Zarr variant chunks than the configured target partition count
- **THEN** the effective partition count is capped by the selected chunk count
- **AND** no selected chunk is split across multiple execution partitions.

#### Scenario: Selected chunks exceed target partitions
- **WHEN** a scan selects more Zarr variant chunks than the configured target partition count
- **THEN** selected chunks are grouped into at most the target partition count
- **AND** each selected chunk is assigned to only one execution partition.

#### Scenario: Sparse row selections within a chunk
- **WHEN** pruning selects sparse rows within the same Zarr variant chunk
- **THEN** the exact selected row ranges are preserved
- **AND** the chunk's selected rows remain assigned to one execution partition.

### Requirement: Parallel Output Ordering Documentation

The system SHALL document that VCF Zarr scans with multiple effective physical partitions do not guarantee global variant row order.

#### Scenario: Parallel scan order
- **WHEN** a VCF Zarr scan executes with more than one effective physical partition
- **THEN** emitted rows are complete and correct
- **AND** callers are not promised original store order unless they add an explicit sort.

### Requirement: Pruning And Projection Compatibility

The system SHALL preserve existing VCF Zarr projection pruning, genomic predicate pruning, sample selection, and logical schema behavior when scans use multiple physical partitions.

#### Scenario: Projection pruning with parallel partitions
- **WHEN** a parallel VCF Zarr scan projects a subset of logical columns
- **THEN** unneeded raw VCF Zarr arrays are not read
- **AND** output schema matches the requested projection.

#### Scenario: Genomic predicate pruning with parallel partitions
- **WHEN** a parallel VCF Zarr scan uses genomic predicates on `chrom`, `start`, or `end`
- **THEN** row pruning is applied before partition planning
- **AND** the results match the equivalent single-partition scan.

#### Scenario: Empty pruned selection
- **WHEN** genomic pruning selects no VCF Zarr rows
- **THEN** the execution plan remains valid
- **AND** collecting the query returns an empty result with the projected schema.

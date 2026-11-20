## ADDED Requirements

### Requirement: Byte-Weighted Region Splitting

The partition balancer SHALL accept, for each region size estimate, an optional
list of per-window compressed-byte weights. When a reference sequence must be
split across partitions and at least two weighted windows lie in the
splittable range, the balancer SHALL place each cut at the end of the window
where the cumulative weight first reaches the partition's proportional share
of the range. When fewer than two weighted windows lie in the range, the
balancer SHALL use the splitting behaviour that existed before weights were
introduced.

#### Scenario: Uneven coverage is split by bytes
- **WHEN** one reference sequence carries weights where a quarter of its
  windows hold three quarters of the bytes
- **AND** it is balanced across four partitions
- **THEN** each partition's summed weight is within one window's weight of a
  quarter of the total.

#### Scenario: No weights keeps existing behaviour
- **WHEN** every estimate has an empty weight list
- **THEN** the partition assignments are identical to those produced before
  weights were introduced.

#### Scenario: Too few weighted windows
- **WHEN** a reference sequence has exactly one weighted window in the
  splittable range
- **THEN** the balancer splits it with the pre-existing non-empty-bin or
  base-pair-proportional rule.

#### Scenario: Structural guarantees are unchanged
- **WHEN** a reference sequence is split using weights
- **THEN** its sub-regions are contiguous and non-overlapping
- **AND** the last sub-region is open-ended
- **AND** unmapped-tail regions and the partition-count cap behave as before.

### Requirement: BAM Index Byte Weights

Indexed BAM scans SHALL derive per-window compressed-byte weights from the BAI
or CSI index and supply them to the partition balancer. The weights SHALL be
taken from the index's position-to-file-offset mapping, made non-decreasing so
that unset or out-of-order entries inherit the preceding offset, and SHALL sum
to the reference sequence's estimated byte size.

#### Scenario: BAI index
- **WHEN** a coordinate-sorted BAM with a BAI index is planned for a scan
- **THEN** each reference sequence with data has one weight per 16 kb window
  that holds a non-zero share of its compressed bytes.

#### Scenario: CSI index
- **WHEN** the same file is planned with a CSI index
- **THEN** weights are derived from the CSI offsets and the scan is planned
  without error.

#### Scenario: Index without usable offsets
- **WHEN** the index yields fewer than two weighted windows for a reference
  sequence
- **THEN** planning succeeds and that reference sequence is split with the
  pre-existing rule.

### Requirement: Partitioning Preserves Scan Results

Changing how regions are split SHALL NOT change query results. For any
`target_partitions`, an indexed BAM scan MUST return the same set of rows.

#### Scenario: Same rows at every partition count
- **WHEN** a BAM with uneven coverage is fully scanned with
  `target_partitions` of 1, 2, 8 and 16
- **THEN** every scan returns the same rows when compared as a sorted set.

#### Scenario: Other indexed formats unaffected
- **WHEN** a CRAM, VCF, BCF, GFF, GTF or pairs file is planned
- **THEN** its partition assignments are identical to those produced before
  this change.

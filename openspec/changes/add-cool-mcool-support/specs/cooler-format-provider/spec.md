# cooler-format-provider Specification (delta)

## ADDED Requirements

### Requirement: Cooler data-collection resolution

The provider SHALL resolve Cooler data collections from a root `.cool` collection, an `.mcool` resolution, or an explicit cooler group URI, and SHALL reject ambiguous or conflicting selections with actionable errors.

#### Scenario: Select an mcool resolution

- **WHEN** a caller opens `contacts.mcool` with resolution `10000`
- **THEN** the provider selects `/resolutions/10000`

#### Scenario: Ambiguous resolution

- **WHEN** a caller opens a multi-resolution `.mcool` without a resolution or
  group URI
- **THEN** the provider errors and lists the available resolutions

#### Scenario: Conflicting addressing

- **WHEN** a cooler group URI and resolution argument select different groups
- **THEN** the provider rejects the conflicting request

### Requirement: Cooler pixel schemas

The provider SHALL expose either joined genomic pixel rows or raw COO rows, SHALL optionally expose per-axis balancing weights, and SHALL preserve the complete valid range of stored coordinates and counts.

#### Scenario: Joined pixel scan

- **WHEN** a caller scans with bin joining enabled
- **THEN** each pixel contains both chromosome/start/end triplets and `count`

#### Scenario: Raw COO scan

- **WHEN** a caller scans with bin joining disabled
- **THEN** each pixel contains `bin1_id`, `bin2_id`, and `count` without
  coordinate conversion

#### Scenario: Wide numeric values

- **WHEN** bin coordinates exceed `i32::MAX` or `count` uses Int64, UInt32,
  UInt64, or Float64 storage
- **THEN** the Arrow schema and values preserve the stored range without
  signed or floating-point narrowing

#### Scenario: Optional balancing weights

- **WHEN** weights are requested from a collection containing `bins/weight`
- **THEN** `weight1` and `weight2` are aligned with each pixel's bins

### Requirement: Metadata-only collection discovery

The provider SHALL list all data collections and their structural attributes without reading pixel datasets, preserving signed, unsigned, or floating sum metadata exactly.

#### Scenario: List an mcool file

- **WHEN** collection discovery runs on an `.mcool`
- **THEN** one metadata record is returned per stored resolution without a
  pixel scan

#### Scenario: Preserve exact sum class

- **WHEN** independent collections use integer or floating `sum` attributes
- **THEN** each sum is returned as its original Int64, UInt64, or Float64 class
  without conversion through f64

### Requirement: Cooler projection pushdown

The provider SHALL read and materialize only the datasets required by the projected fields.

#### Scenario: Project one axis and count

- **WHEN** a scan projects only first-axis coordinates and `count`
- **THEN** the second-axis ID dataset is neither indexed nor read and second-axis coordinate materialization is skipped

#### Scenario: Project one bin metadata field

- **WHEN** a scan projects a chromosome, start, end, or weight field
- **THEN** only the bin metadata arrays required to materialize that field are
  loaded, except for additional arrays required by predicate pruning

#### Scenario: Empty projection

- **WHEN** a row-count query supplies an empty projection
- **THEN** the provider returns the correct row count without indexing or
  decoding pixel columns

### Requirement: Cooler genomic predicate pruning

The provider SHALL map supported first-axis chromosome and coordinate filters through Cooler indexes to pixel row ranges and SHALL report those filters as inexact.

#### Scenario: First-axis range filter

- **WHEN** a filter constrains `chrom1` and its start/end coordinates
- **THEN** only the corresponding `bin1` pixel row ranges are scanned and the
  consumer can reapply the filter for exact results

#### Scenario: Unsupported filter

- **WHEN** a predicate cannot be mapped safely to a first-axis row range
- **THEN** the provider leaves the scan range unpruned

### Requirement: Bounded and partitioned execution

The provider SHALL stream bounded Arrow batches and SHALL partition the row space into disjoint ranges aligned to `bin1` boundaries when multiple target partitions are requested.

#### Scenario: Partition equivalence

- **WHEN** the same collection is scanned with one and multiple partitions
- **THEN** both scans produce identical row sets

### Requirement: Safe optimized HDF5 chunk decoding

The provider MAY decode supported HDF5 chunks directly, but SHALL select the libhdf5 compatibility path before execution whenever a column's layout, filters, masks, byte order, or validation probe is unsupported.

#### Scenario: Supported shuffle-deflate chunks

- **WHEN** every chunk uses the validated shuffle-plus-deflate pipeline with
  no skipped filter masks
- **THEN** the provider may use direct chunk decoding and returns the same
  values as libhdf5

#### Scenario: Unsupported per-chunk mask

- **WHEN** any indexed chunk reports a skipped filter in its mask
- **THEN** the whole column uses libhdf5 rather than failing after partial
  direct execution

### Requirement: Validated joined references

The provider SHALL validate parallel pixel-array shapes, bin-array shapes,
chromosome references, and decoded pixel bin references before using them for
scan sizing or joined array indexing.

#### Scenario: Malformed parallel pixel arrays

- **WHEN** `pixels/bin1_id`, `pixels/bin2_id`, and `pixels/count` are not all
  one-dimensional arrays of identical length
- **THEN** provider construction returns a contextual invalid-file error

#### Scenario: Malformed bin reference

- **WHEN** `bins/chrom` or `pixels/bin1_id`/`pixels/bin2_id` contains a negative or out-of-range reference
- **THEN** the joined scan returns a contextual invalid-file error instead of panicking

### Requirement: Local-file scope

The provider SHALL accept local seekable Cooler files and SHALL reject remote object-store URLs with a clear not-supported error.

#### Scenario: Remote URL

- **WHEN** a caller supplies an `s3://`, `gs://`, or HTTP URL
- **THEN** provider construction fails with a local-file-only message

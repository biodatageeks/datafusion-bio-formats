## ADDED Requirements

### Requirement: Cold-Start Optimized Fjall Configuration
The system SHALL configure fjall keyspace options at DB creation time to minimize cold-start annotation latency for single-sample VCF annotation workloads where bloom filter, index, and data blocks must be loaded from disk on first access.

The cold-start configuration MUST:
- Pin all bloom filter blocks in cache across all LSM levels via `filter_block_pinning_policy(PinningPolicy::all(true))`
- Pin all index blocks in cache across all LSM levels via `index_block_pinning_policy(PinningPolicy::all(true))`
- Enable `expect_point_read_hits` to skip bloom filter construction on the last level, since 95-98% of WGS/WES variant lookups are expected to match known variants
- Set bloom filter false positive rate to 0.01% on all levels for tight novel-variant rejection on upper levels
- Enable data block hash index with ratio 0.75 for O(1) within-block key lookups on cached blocks
- Use 8 KiB data block size to reduce I/O operations for sorted VCF access patterns
- Disable SST-level data block compression since position entry values are already zstd-compressed with a trained dictionary

#### Scenario: Cold-start DB open with pinned blocks
- **WHEN** a fjall VEP cache is opened for the first time in a process
- **THEN** all bloom filter blocks for levels L0 through L5 are loaded into the block cache (~37 MB)
- **AND** all index blocks for all levels are loaded into the block cache (~90 MB)
- **AND** total pinned block loading completes in under 100 ms on NVMe SSD

#### Scenario: Novel variant rejection with pinned bloom filters
- **WHEN** a VCF variant does not exist in the fjall database
- **THEN** the bloom filter rejects the lookup using only pinned in-memory filter blocks
- **AND** no data block disk reads are performed
- **AND** the rejection completes in under 500 nanoseconds

#### Scenario: Known variant lookup with hash index
- **WHEN** a VCF variant exists in the fjall database and its data block is already in the block cache
- **THEN** the within-block key lookup uses the hash index instead of binary search
- **AND** the lookup completes in under 2 microseconds

#### Scenario: Sorted VCF benefits from 8 KiB blocks
- **WHEN** a positionally-sorted VCF is annotated against the fjall database
- **THEN** consecutive lookups for nearby genomic positions frequently share the same 8 KiB data block
- **AND** the number of distinct data block I/O operations is approximately halved compared to 4 KiB blocks

### Requirement: Configurable Annotation Session Parameters
The system SHALL expose fjall tuning parameters via the `bio.annotation` DataFusion session configuration namespace, allowing users to adjust cache behavior for different workload profiles.

The configurable parameters MUST include:
- `bio.annotation.cache_size_mb` (default 512) -- fjall block cache size in MB
- `bio.annotation.expect_hits` (default true) -- whether to skip last-level bloom filters
- `bio.annotation.zstd_level` (default 3) -- compression level for cache writes
- `bio.annotation.dict_size_kb` (default 112) -- zstd dictionary size for cache writes

#### Scenario: Default configuration for standard WGS/WES annotation
- **WHEN** no `bio.annotation` overrides are set in the session
- **THEN** the system uses 512 MB block cache, `expect_hits=true`, zstd level 3, 112 KB dictionary
- **AND** this configuration is optimized for cold-start annotation of 4-5M variant WGS samples

#### Scenario: Override for rare-variant filtering pipeline
- **WHEN** a user sets `SET bio.annotation.expect_hits = false`
- **THEN** the system builds bloom filters on all levels including the last level
- **AND** novel variant rejection is effective on all LSM levels at the cost of ~340 MB additional bloom filter memory

#### Scenario: Cache size override for memory-constrained environments
- **WHEN** a user sets `SET bio.annotation.cache_size_mb = 256`
- **THEN** the fjall block cache is limited to 256 MB
- **AND** annotation still functions correctly with potentially higher cold-start latency due to more cache evictions

### Requirement: Sorted Bulk Ingestion via start_ingestion()
The system SHALL provide a sorted bulk ingestion mode that uses fjall's `Keyspace::start_ingestion()` API to write pre-sorted position entries directly to SST files, bypassing the memtable, journal, and compaction pipeline.

The sorted ingestion MUST:
- Process chromosomes in canonical order (1-22, X, Y, MT) with positions ascending within each chromosome
- Write key-value pairs in strict lexicographic key order via `Ingestion::write()`
- Train a zstd dictionary from a sample of the source data before beginning sorted ingest
- Compress each position entry value using the trained dictionary before writing
- Merge entries for duplicate positions (same `(chrom, start, end)`) by combining their allele tables
- Call `Ingestion::finish()` after all entries are written
- Run post-ingest compaction to consolidate SST files

#### Scenario: Sorted ingest from variation Parquet cache
- **WHEN** the build tool ingests a VEP variation Parquet file (1.17B rows) using sorted ingestion
- **THEN** the ingestion completes 3-6x faster than the current batch-insert approach
- **AND** the resulting fjall database is functionally identical (same position entries, same annotation values)

#### Scenario: Post-ingest compaction
- **WHEN** sorted ingestion completes and compaction runs
- **THEN** the data keyspace is consolidated into minimal SST files per level
- **AND** subsequent point lookups require at most one bloom filter check per level

#### Scenario: Zstd dictionary training before sorted ingest
- **WHEN** sorted ingestion begins
- **THEN** the system first reads a 10,000-row sample from the source table
- **AND** trains a zstd dictionary from serialized position entries
- **AND** stores the dictionary in the `meta` keyspace for decompression at query time

### Requirement: Position-Keyed Lookup with Extended Coordinate Probes
The system SHALL perform point lookups against the fjall KV store using position-keyed entries, with extended coordinate probes to handle VEP-style coordinate normalization differences between VCF input and cache entries.

The lookup execution MUST:
- Encode each VCF variant's `(chrom, start, end)` as an 18-byte position key
- Probe the primary normalized coordinate first
- When extended probes are enabled, additionally probe: insertion-style `start>end` coordinates, prefix-trimmed shifted coordinates for deletions, and tandem repeat window coordinates
- Match alleles within each position entry using the configured match mode (Exact, ExactOrColocated, or ExactOrRelaxed)
- Return null annotation columns for variants with no matching position or allele
- Reuse a single zstd decompressor instance across all lookups in a stream partition

#### Scenario: SNV lookup (single probe)
- **WHEN** a VCF contains an SNV at position 1000
- **THEN** the system probes key `(chrom, 1000, 1000)` in the fjall store
- **AND** matches the alt allele against the position entry's allele table

#### Scenario: Deletion lookup with extended probes
- **WHEN** a VCF contains a deletion REF=TTA ALT=T at position 1000
- **AND** the VEP cache stores this deletion with prefix-trimmed coordinates at position 1001
- **THEN** the system probes both `(chrom, 1000, 1002)` and `(chrom, 1001, 1002)` among other coordinate variants
- **AND** finds the match at the shifted position

#### Scenario: Novel variant with no matching position
- **WHEN** a VCF variant's position does not exist in the fjall store
- **THEN** all coordinate probes return no entry (bloom filter rejection on each)
- **AND** the output row contains the VCF columns with null annotation columns

### Requirement: Feature-Gated KV Cache Dependencies
The fjall KV cache integration MUST be gated behind an optional cargo feature to avoid impacting builds that do not need KV-backed lookups.

The feature gate MUST:
- Use feature name `kv-cache` in `bio-function-vep/Cargo.toml`
- Guard all fjall-dependent code with `#[cfg(feature = "kv-cache")]`
- Include `fjall`, `arrow-ipc`, `zstd`, and `ahash` as optional dependencies under the feature
- Be enabled by default in the crate's `[features]` section

#### Scenario: Build with KV cache (default)
- **WHEN** `cargo build --package datafusion-bio-function-vep` is run
- **THEN** the build includes fjall KV cache support since `kv-cache` is a default feature
- **AND** `VepKvStore`, `KvLookupExec`, and `KvCacheTableProvider` are available

#### Scenario: Build without KV cache
- **WHEN** `cargo build --package datafusion-bio-function-vep --no-default-features` is run
- **THEN** the build succeeds without fjall dependencies
- **AND** only Parquet-based `lookup_variants()` is available

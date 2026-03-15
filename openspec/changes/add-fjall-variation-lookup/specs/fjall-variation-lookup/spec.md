## ADDED Requirements

### Requirement: Cold-Start Optimized Fjall Configuration
The system SHALL configure fjall keyspace options at DB creation time to minimize cold-start annotation latency for single-sample VCF annotation workloads where bloom filter, index, and data blocks must be loaded from disk on first access.

The cold-start configuration MUST:
- Disable partitioned metadata for the default cold-start DB profile via `filter_block_partitioning_policy(PinningPolicy::all(false))` and `index_block_partitioning_policy(PinningPolicy::all(false))`, because stock fjall 3.x only keeps the top-level structures resident for partitioned metadata
- Pin all bloom filter blocks in cache across all LSM levels via `filter_block_pinning_policy(PinningPolicy::all(true))`
- Pin all index blocks in cache across all LSM levels via `index_block_pinning_policy(PinningPolicy::all(true))`
- Enable `expect_point_read_hits` to skip bloom filter construction on the last level, since 95-98% of WGS/WES variant lookups are expected to match known variants
- Set bloom filter false positive rate to 0.01% on all levels for tight novel-variant rejection on upper levels
- Enable data block hash index with ratio 0.75 for O(1) within-block key lookups on cached blocks
- Use 8 KiB data block size to reduce I/O operations for sorted VCF access patterns
- Set restart interval to maximize hash index effectiveness (benchmark sweep: 4, 8, 16; lower intervals create more restart points = more hash buckets for Decision 3's hash index)
- Configure SST target size to 256 MiB to limit L6 file count (~289 SSTs vs ~1,156 at default 64 MiB), and set `max_cached_files(512)` for file descriptor headroom
- Disable SST-level data block compression since position entry values are already zstd-compressed with a trained dictionary

#### Scenario: Cold-start DB open with pinned blocks
- **WHEN** a fjall VEP cache is opened for the first time in a process
- **AND** metadata partitioning is disabled for the cold-start profile
- **AND** SST target size is 256 MiB (limiting L6 to ~289 SSTs)
- **AND** restart interval is tuned per benchmark (default recommendation: 4)
- **THEN** all bloom filter blocks for levels L0 through L5 are loaded into the block cache (~190 MB)
- **AND** all index blocks for all levels are loaded into the block cache (~155 MB)
- **AND** total pinned block loading completes in under 200 ms on NVMe SSD

#### Scenario: Partitioned metadata is not treated as fully pinned
- **WHEN** a fjall database is created with partitioned index or filter blocks enabled
- **THEN** the implementation MUST NOT claim that all lower-level metadata is pinned solely via `PinningPolicy::all(true)`
- **AND** the cold-start optimized profile MUST either disable metadata partitioning or document the extra demand-loaded partition reads explicitly

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
- `bio.annotation.lookup_backend` (default `auto`) -- one of `parquet`, `fjall`, `hybrid`, or `auto`
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

#### Scenario: Force parquet lookup backend
- **WHEN** a user sets `SET bio.annotation.lookup_backend = 'parquet'`
- **THEN** known-variant lookup uses the existing parquet/SQL interval-join path
- **AND** no fjall database is opened for that query

#### Scenario: Hybrid lookup backend
- **WHEN** a user sets `SET bio.annotation.lookup_backend = 'hybrid'`
- **THEN** the system probes the fjall cache first
- **AND** unresolved or unsupported cases are routed to the parquet interval-join path
- **AND** the final annotation output is identical to the parquet correctness baseline

### Requirement: Sorted Bulk Ingestion via start_ingestion()
The system SHALL provide a sorted bulk ingestion mode that uses fjall's `Keyspace::start_ingestion()` API to write pre-sorted position entries without routing them through the normal memtable/journal write path, while explicitly accounting for stock fjall 3.x compaction behavior.

The sorted ingestion MUST:
- Process chromosomes in canonical order (1-22, X, Y, MT) with positions ascending within each chromosome
- Write key-value pairs in strict lexicographic key order via `Ingestion::write()`
- Train a zstd dictionary from a sample of the source data before beginning sorted ingest
- Compress each position entry value using the trained dictionary before writing
- Merge entries for duplicate positions (same `(chrom, start, end)`) by combining their allele tables
- Call `Ingestion::finish()` after all entries are written
- Treat the ingested output as requiring a follow-up compaction step for the final read-optimized shape, because stock fjall 3.x does not expose a public "direct final-level load with target-aware compaction" path
- Avoid assuming that `Keyspace::major_compact()` honors the configured leveled strategy's table target size

#### Scenario: Sorted ingest from variation Parquet cache
- **WHEN** the build tool ingests a VEP variation Parquet file (1.17B rows) using sorted ingestion
- **THEN** the ingestion completes 3-6x faster than the current batch-insert approach
- **AND** the resulting fjall database is functionally identical (same position entries, same annotation values)

#### Scenario: Post-ingest compaction respects stock fjall limitations
- **WHEN** sorted ingestion completes and the implementation performs a follow-up compaction step
- **THEN** it MUST account for the fact that stock fjall 3.0.x `major_compact()` uses a 64 MB target size
- **AND** it MUST NOT assume that the public API preserves a larger custom table-size policy during that step

#### Scenario: Zstd dictionary training before sorted ingest
- **WHEN** sorted ingestion begins
- **THEN** the system first reads a 10,000-row sample from the source table
- **AND** trains a zstd dictionary from serialized position entries
- **AND** stores the dictionary in the `meta` keyspace for decompression at query time

### Requirement: Position-Keyed Lookup with Extended Coordinate Probes
The system SHALL perform point lookups against the fjall KV store using position-keyed entries, with extended coordinate probes to handle VEP-style coordinate normalization differences between VCF input and cache entries.

The lookup execution MUST:
- Encode the primary probe key as `[2B chrom][8B start]`, with end-coordinate discrimination performed inside the decoded position entry value
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

### Requirement: Pluggable KV Backend
The system SHALL support multiple KV backend implementations behind a common `VepKvBackend` trait, allowing different storage engines to be used for position-keyed variant lookups without changing the lookup execution logic.

The pluggable backend MUST:
- Define a `VepKvBackend` trait with `get()`, `contains()`, and `metadata()` methods
- Provide a default `FjallBackend` implementation (pure Rust, all platforms)
- Support an optional `LmdbBackend` implementation gated behind the `lmdb-backend` feature flag
- Allow `KvLookupExec` and `VepKvStore` to operate on `Arc<dyn VepKvBackend>` without backend-specific logic

#### Scenario: Default fjall backend
- **WHEN** the KV cache is opened without specifying a backend
- **THEN** the system uses the `FjallBackend` implementation
- **AND** all existing cold-start tuning options (pinning, bloom filters, hash index) are applied

#### Scenario: LMDB backend via feature flag
- **WHEN** the crate is built with `--features lmdb-backend`
- **AND** the user configures the LMDB backend
- **THEN** the system uses the `LmdbBackend` implementation with mmap-based access
- **AND** cold-start latency is near-zero (no block loading required)

#### Scenario: Backend-agnostic lookup execution
- **WHEN** `KvLookupExec` performs variant lookups
- **THEN** it operates through the `VepKvBackend` trait interface
- **AND** produces identical annotation results regardless of which backend is active

### Requirement: Value Layout Calibration
The system SHALL treat fjall key-value separation as a measured storage-layout option rather than a default, selecting it only when serialized `PositionEntry` sizes justify the extra point-read indirection.

The value-layout decision MUST:
- Default to inline values (`with_kv_separation(None)`) for the cold-start lookup-optimized database profile
- Measure compressed `PositionEntry` size distribution before enabling key-value separation
- Benchmark at least one thresholded separated layout (for example 1 KiB or 2 KiB threshold) against the inline baseline before changing the default

#### Scenario: Inline values remain default for compact entries
- **WHEN** the measured compressed `PositionEntry` distribution stays below the documented enablement threshold
- **THEN** the implementation keeps values inline in the main fjall table
- **AND** point lookups avoid an additional blob-file hop

#### Scenario: Large entries can opt into key-value separation
- **WHEN** benchmark results show materially better build/compaction behavior for large `PositionEntry` values
- **THEN** the implementation MAY expose a separated-value build mode
- **AND** it MUST document the read-latency trade-off relative to inline values

### Requirement: Chromosome Warmup/Prefetch
The system SHALL prefetch data for upcoming chromosomes to overlap I/O with computation, reducing cold-start latency when processing sorted VCF input.

The warmup mechanism MUST:
- Provide a `warmup(chromosomes: &[ChromCode])` method on the `VepKvBackend` trait
- For fjall: issue `posix_fadvise(POSIX_FADV_WILLNEED)` on relevant SST file ranges
- For LMDB: issue `madvise(MADV_WILLNEED)` on chromosome key range pages
- Be called at the start of each chromosome batch in `KvLookupExec`

#### Scenario: Sequential chromosome warmup
- **WHEN** `KvLookupExec` begins processing variants on chromosome N
- **THEN** the system issues a warmup call for chromosome N+1's data regions
- **AND** the warmup I/O overlaps with computation on chromosome N

#### Scenario: Panel-specific warmup (only target chromosomes)
- **WHEN** a targeted panel VCF contains variants on only a subset of chromosomes
- **THEN** the system warms up only the data regions for those chromosomes
- **AND** no unnecessary I/O is performed for chromosomes not present in the input

### Requirement: Pipelined I/O Look-Ahead
The system SHALL look ahead at upcoming VCF variant keys and prefetch their data blocks to overlap I/O with computation within a chromosome.

The look-ahead mechanism MUST:
- Maintain a look-ahead buffer of N keys (default N=64) in `KvLookupExec`
- Predict data block locations from pinned index blocks for look-ahead keys
- Issue async prefetch reads for predicted blocks while processing the current batch
- Work with both fjall (`fadvise` on predicted data block offsets) and LMDB (`madvise` on predicted leaf pages)

#### Scenario: Look-ahead within sorted VCF batch
- **WHEN** `KvLookupExec` processes a batch of sorted VCF variants
- **THEN** the system looks ahead at the next 64 variant keys beyond the current batch
- **AND** issues prefetch I/O for data blocks that will be needed for those keys

#### Scenario: Cross-block prefetch benefit
- **WHEN** consecutive VCF variants span multiple data blocks
- **THEN** the look-ahead prefetch ensures the next data block is already being loaded from disk
- **AND** the effective I/O wait time per block transition is reduced

### Requirement: Observability Metrics via `VEP_KV_PROFILE`
The system SHALL provide structured observability metrics for KV lookup performance, controlled by the `VEP_KV_PROFILE` environment variable.

The observability system MUST:
- When `VEP_KV_PROFILE=1`, emit a per-run JSON summary to stderr containing: `total_variants`, `kv_hits`, `kv_misses`, `position_hit_allele_miss`, `extended_probe_hits`, `total_get_calls`, `bytes_decompressed`, `elapsed_lookup_ns`, `elapsed_decompress_ns`, `elapsed_allele_match_ns`
- When `VEP_KV_PROFILE=verbose`, additionally emit per-chromosome metrics: `first_lookup_latency_us`, `warmup_elapsed_us`
- At startup (when profiling is enabled), emit: `backend_type`, `cache_size_mb`, `expect_hits`, `db_path`, `db_size_bytes`
- Use atomic counters for all metrics (~1-2 ns per increment when enabled)
- Impose zero overhead when `VEP_KV_PROFILE` is unset (single `static AtomicBool` load on the hot path)

#### Scenario: Per-run profiling output
- **WHEN** `VEP_KV_PROFILE=1` is set in the environment
- **AND** a 5M-variant VCF is annotated against the fjall cache
- **THEN** a JSON object is emitted to stderr after annotation completes
- **AND** the JSON contains hit/miss counts, latency breakdowns, and byte counters
- **AND** the overhead from profiling is under 10 ms for the full run

#### Scenario: Verbose per-chromosome profiling
- **WHEN** `VEP_KV_PROFILE=verbose` is set
- **THEN** the output includes per-chromosome entries with first-lookup latency and warmup timing
- **AND** the per-run summary is still emitted

#### Scenario: No profiling overhead when disabled
- **WHEN** `VEP_KV_PROFILE` is not set in the environment
- **THEN** the profiling code path is skipped via a single atomic load
- **AND** annotation performance is identical to a build without profiling support

### Requirement: Projection-Aware Value Deserialization
The system SHALL support column projection in `PositionEntry::decode()` to avoid deserializing unused columns when only a subset of the 78 annotation columns is requested.

The projection-aware deserialization MUST:
- Accept a column projection bitmask parameter in `PositionEntry::decode()`
- Always perform full zstd decompression (the entire value is one compressed blob)
- Skip parsing and Arrow array construction for non-projected columns after decompression
- Produce identical results for projected columns as full deserialization

#### Scenario: Narrow projection (2 of 78 columns)
- **WHEN** a query requests only `consequence_type` and `impact` from the annotation cache
- **THEN** `PositionEntry::decode()` decompresses the full value but only parses and allocates the 2 requested column arrays
- **AND** deserialization time is reduced compared to full 78-column decode

### Requirement: Per-Chromosome Parallel Partitions
The system SHALL support multi-partition execution in `KvLookupExec` to exploit I/O parallelism on NVMe storage.

The parallel partition support MUST:
- Report N output partitions in `KvLookupExec` (one per chromosome in the input VCF) when the upstream plan partitions by chromosome
- Share a single `Arc<dyn VepKvBackend>` across all partitions
- Allow each partition to independently call `warmup()` and maintain its own look-ahead buffer
- Fall back to single-partition mode when the upstream input is not partitioned by chromosome

#### Scenario: Multi-chromosome I/O parallelism
- **WHEN** a sorted VCF with variants on 22 autosomes is annotated
- **AND** the upstream plan partitions by chromosome
- **THEN** `KvLookupExec` executes up to N concurrent partitions
- **AND** each partition issues independent warmup and data block reads
- **AND** NVMe I/O utilization is higher than single-partition execution

# Implementation Tasks

## 0. Phase 0: Backend Abstraction & LMDB Prototype

### 0.1 Define `VepKvBackend` trait
- [ ] 0.1.1 Define `VepKvBackend` trait in `kv_cache/mod.rs` with `get()`, `contains()`, `metadata()` methods
- [ ] 0.1.2 Add `warmup(chromosomes: &[ChromCode])` method to the trait (default no-op)

### 0.2 Refactor `VepKvStore` to use trait object
- [ ] 0.2.1 Change `VepKvStore` to operate on `Arc<dyn VepKvBackend>` instead of directly on fjall types
- [ ] 0.2.2 Update `KvLookupExec` to use `Arc<dyn VepKvBackend>`

### 0.3 Extract `FjallBackend`
- [ ] 0.3.1 Extract current fjall code into `FjallBackend` struct implementing `VepKvBackend`
- [ ] 0.3.2 Verify existing tests pass with refactored code

### 0.4 Add `lmdb-backend` feature flag
- [ ] 0.4.1 Add `heed` as optional dependency under `lmdb-backend` feature in `Cargo.toml`
- [ ] 0.4.2 Gate LMDB-specific code with `#[cfg(feature = "lmdb-backend")]`

### 0.5 Implement `LmdbBackend`
- [ ] 0.5.1 Implement `LmdbBackend` with `MDB_APPEND` bulk loading
- [ ] 0.5.2 Implement `warmup()` via `madvise(MADV_WILLNEED)` on chromosome key range pages

### 0.6 Comparative benchmarks
- [ ] 0.6.1 Add benchmark: fjall vs LMDB for 1M-entry subset (point lookups)
- [ ] 0.6.2 Benchmark cold-start open time: fjall (pinned) vs LMDB (mmap)

## 1. Phase 1: Fjall Cold-Start Configuration Tuning

### 1.1 Update KeyspaceCreateOptions for read-optimized DB creation
- [ ] 1.1.0 Disable partitioned metadata for the cold-start DB profile via `filter_block_partitioning_policy(PinningPolicy::all(false))` and `index_block_partitioning_policy(PinningPolicy::all(false))`
- [ ] 1.1.1 In `kv_store.rs` `VepKvStore::create()`, set `filter_block_pinning_policy(PinningPolicy::all(true))`
- [ ] 1.1.2 Set `index_block_pinning_policy(PinningPolicy::all(true))`
- [ ] 1.1.3 Set `expect_point_read_hits(true)` to skip last-level bloom filters
- [ ] 1.1.4 Set `filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(BloomConstructionPolicy::FalsePositiveRate(0.0001))))` for tight FPR on all levels
- [ ] 1.1.5 Set `data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))` for in-block hash index
- [ ] 1.1.6 Set `data_block_size_policy(BlockSizePolicy::all(8 * 1024))` for fewer I/O ops with sorted access
- [ ] 1.1.6a Benchmark `data_block_restart_interval_policy` sweep: **4 vs 8 vs 16** on the 10-byte key layout. Lower intervals create more restart points = more hash buckets for Decision 3's hash index (interval=4 → ~24 hash buckets → ~5 keys/bucket; interval=16 → ~6 buckets → ~21 keys/bucket). Keep the best-performing interval as the default.
- [ ] 1.1.7 Verify existing unit tests pass with new configuration
- [ ] 1.1.8 Add test measuring cold-start open time with pinned blocks

### 1.2 Update session configuration (`config.rs`)
- [ ] 1.2.1 Add `expect_hits: bool, default = true` to `AnnotationConfig`
- [ ] 1.2.1a Add `lookup_backend: parquet|fjall|hybrid|auto` to `AnnotationConfig`
- [ ] 1.2.2 Wire `bio.annotation.cache_size_mb` through to `VepKvStore::open_with_cache_size()` (fix current mismatch where default is 1024 MB in config but 256 MB in `open()`)
- [ ] 1.2.3 Wire `bio.annotation.expect_hits` into `KeyspaceCreateOptions` at DB creation time
- [ ] 1.2.4 Update default `cache_size_mb` from 1024 to 512 (cold-start working set is ~640 MB total)
- [ ] 1.2.5 Wire `bio.annotation.lookup_backend` so the existing parquet `LookupProvider` remains available as `parquet` and `hybrid` fallback

### 1.3 Update VepKvStore::open() to use session-aware defaults
- [ ] 1.3.1 Change `VepKvStore::open()` default cache from 256 MB to 512 MB
- [ ] 1.3.2 Ensure `KvCacheTableProvider::open()` reads cache size from session config when available

### 1.4 Position-Only Key Encoding
- [ ] 1.4.1 Implement 10-byte key encoding `[2B chrom][8B start]` in `key_encoding.rs`
- [ ] 1.4.2 Update `PositionEntry` to store end coordinate(s) in value
- [ ] 1.4.3 Update extended coordinate probes to use position-only lookups
- [ ] 1.4.4 Migrate existing loader to emit 10-byte keys
- [ ] 1.4.5 Verify all three allele match modes work with position-only keys

### 1.5 SST Target Size & File Handle Limits
- [ ] 1.5.1 Set `Leveled::default().with_table_target_size(256 * 1024 * 1024)` in compaction strategy to limit L6 SST count to ~289 (vs ~1,156 at default 64 MiB)
- [ ] 1.5.2 Set `Database::builder(path).max_cached_files(Some(512))` for file descriptor headroom above ~300 SSTs
- [ ] 1.5.3 Validate that `max_cached_files(512)` > total SST count after build on a representative DB

### 1.6 Chromosome Warmup/Prefetch (Backend-Specific)
- [ ] 1.6.1 Add `warmup(chromosomes: &[ChromCode])` method to `VepKvBackend` trait
- [ ] 1.6.2 Implement fjall warmup: use `prefix(chrom_code_bytes)` iterator in background task to load data blocks into fjall's block cache directly (portable, pure Rust)
- [ ] 1.6.3 Implement LMDB warmup: `madvise(WILLNEED)` on chromosome key range pages
- [ ] 1.6.4 Call warmup at start of each chromosome batch in `KvLookupExec`

### 1.7 Pipelined I/O Look-Ahead
- [ ] 1.7.1 Add look-ahead buffer (N=64 keys) to `KvLookupExec`
- [ ] 1.7.2 Predict data block locations from index for look-ahead keys
- [ ] 1.7.3 Issue async prefetch for predicted blocks
- [ ] 1.7.4 Benchmark with/without look-ahead on 5M-variant WGS

### 1.8 Observability Metrics (`VEP_KV_PROFILE`)
- [ ] 1.8.1 Define `KvProfileMetrics` struct with atomic counters for: `total_variants`, `kv_hits`, `kv_misses`, `position_hit_allele_miss`, `extended_probe_hits`, `total_get_calls`, `bytes_decompressed`, `elapsed_lookup_ns`, `elapsed_decompress_ns`, `elapsed_allele_match_ns`
- [ ] 1.8.2 Add per-chromosome metrics: `first_lookup_latency_us`, `warmup_elapsed_us`
- [ ] 1.8.3 Add startup info emission: `backend_type`, `cache_size_mb`, `expect_hits`, `db_path`, `db_size_bytes`
- [ ] 1.8.4 Implement `VEP_KV_PROFILE=1` per-run JSON summary to stderr
- [ ] 1.8.5 Implement `VEP_KV_PROFILE=verbose` per-chromosome breakdown
- [ ] 1.8.6 Gate all metric collection behind `static AtomicBool` for zero overhead when disabled
- [ ] 1.8.7 Add test verifying profiling output format and zero-overhead when disabled

### 1.9 Projection-Aware Value Deserialization
- [ ] 1.9.1 Add column projection bitmask parameter to `PositionEntry::decode()`
- [ ] 1.9.2 After zstd decompression, skip parsing/allocation for non-projected columns in the column-major buffer
- [ ] 1.9.3 Wire projection from `KvLookupExec`'s required columns into `PositionEntry::decode()`
- [ ] 1.9.4 Add test verifying projected decode produces identical results to full decode for projected columns
- [ ] 1.9.5 Benchmark: 2-column projection vs full 78-column decode on 1M entries

### 1.10 Per-Chromosome Parallel Partitions
- [ ] 1.10.1 Update `KvLookupExec` to report N output partitions (one per chromosome in input VCF) when upstream plan is chromosome-partitioned
- [ ] 1.10.2 Each partition independently calls `warmup()` for its chromosome
- [ ] 1.10.3 Each partition maintains its own look-ahead buffer
- [ ] 1.10.4 All partitions share `Arc<dyn VepKvBackend>`
- [ ] 1.10.5 Fall back to single-partition mode when upstream is not chromosome-partitioned
- [ ] 1.10.6 Benchmark: multi-partition vs single-partition on NVMe with 5M-variant WGS

### 1.11 Early-Exit Allele Matching
- [ ] 1.11.1 In `Exact` match mode, check allele table at known offset before full column deserialization
- [ ] 1.11.2 Skip remaining column deserialization when no allele matches
- [ ] 1.11.3 Add test verifying early-exit produces identical results to full decode path

## 2. Phase 2: Sorted Bulk Ingestion

### 2.1 Implement sorted ingest via `start_ingestion()`
- [ ] 2.1.1 Add `build_sorted()` method to `CacheLoader` using `Keyspace::start_ingestion()` API
- [ ] 2.1.2 Ensure input data is globally sorted by key order: process chromosomes sequentially (1-22, X, Y, MT), positions ascending within each chromosome
- [ ] 2.1.3 Within each chromosome, group rows by `(chrom, start, end)` and serialize position entries in key order
- [ ] 2.1.4 Write each `(key, compressed_value)` pair via `Ingestion::write()` in sorted order
- [ ] 2.1.5 Call `Ingestion::finish()` after all entries are written
- [ ] 2.1.6 Handle the zstd dictionary training phase (sample 10K rows before sorted ingest, same as current)
- [ ] 2.1.7 Merge existing entries for duplicate positions (same logic as current `merge_position_entries()`)
- [ ] 2.1.8 Document and test the stock fjall 3.0.x behavior that ingested tables still require a follow-up compaction step for the final steady-state layout

### 2.2 Post-ingest compaction
- [ ] 2.2.1 Evaluate whether `Keyspace::major_compact()` is acceptable for this workload given its stock 64 MB target size; if not, keep compaction strategy target-aware and document the limitation
- [ ] 2.2.2 Persist metadata and dictionary after compaction
- [ ] 2.2.3 Measure and log compaction time

### 2.3 Integration test for sorted ingest
- [ ] 2.3.1 Build fjall DB using `build_sorted()` from test variation data
- [ ] 2.3.2 Verify all position entries are present and decompressible
- [ ] 2.3.3 Compare DB contents with `batch_insert_raw()` baseline to ensure equivalence
- [ ] 2.3.4 Measure ingest time improvement vs current approach

## 3. Phase 3: Benchmarking and Validation

### 3.1 Cold-start benchmark
- [ ] 3.1.1 Create benchmark measuring: DB open time, first-query latency, 5M-variant annotation throughput
- [ ] 3.1.2 Compare default fjall config vs tuned config on same DB
- [ ] 3.1.3 Measure memory usage (pinned blocks + block cache) via `/proc/self/status` or `jemalloc`
- [ ] 3.1.4 Test on both NVMe SSD and SATA SSD to validate I/O reduction claims
- [ ] 3.1.5 Benchmark metadata partitioning on vs off to confirm that the chosen pinning strategy actually removes lower-level metadata reads
- [ ] 3.1.6 Benchmark hash-ratio sweep (`0.5`, `0.75`, `1.0`) and restart interval sweep (`4`, `8`, `16`)

### 3.2 Build time benchmark
- [ ] 3.2.1 Compare `batch_insert_raw()` vs `start_ingestion()` for full 1.17B-entry variation cache
- [ ] 3.2.2 Measure DB size (should be identical since encoding is unchanged)
- [ ] 3.2.3 Measure post-compaction time
- [ ] 3.2.4 Benchmark inline values vs KV separation thresholds (for example 1 KiB and 2 KiB) using real serialized `PositionEntry` size histograms
- [ ] 3.2.5 Measure the effect of any `major_compact()` step separately from ingestion so its 64 MB stock target does not get hidden inside the total
- [ ] 3.2.6 Benchmark zstd dictionary sizes: 112 KB (default), 256 KB, 512 KB — measure compression ratio and decompression throughput on representative VEP variation data

### 3.3 Correctness validation
- [ ] 3.3.1 Verify annotation output matches between default and tuned fjall configs (bit-exact)
- [ ] 3.3.2 Verify all three allele match modes (Exact, ExactOrColocated, ExactOrRelaxed) produce same results
- [ ] 3.3.3 Verify extended coordinate probes still work with 8 KiB block size
- [ ] 3.3.4 Run existing `kv_cache` test suite with tuned configuration
- [ ] 3.3.5 Verify `lookup_backend=parquet|fjall|hybrid|auto` all produce identical annotation results on the same fixtures

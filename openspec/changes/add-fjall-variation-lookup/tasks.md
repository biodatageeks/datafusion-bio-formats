# Implementation Tasks

## 1. Phase 1: Fjall Cold-Start Configuration Tuning

### 1.1 Update KeyspaceCreateOptions for read-optimized DB creation
- [ ] 1.1.1 In `kv_store.rs` `VepKvStore::create()`, set `filter_block_pinning_policy(PinningPolicy::all(true))`
- [ ] 1.1.2 Set `index_block_pinning_policy(PinningPolicy::all(true))`
- [ ] 1.1.3 Set `expect_point_read_hits(true)` to skip last-level bloom filters
- [ ] 1.1.4 Set `filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(BloomConstructionPolicy::FalsePositiveRate(0.0001))))` for tight FPR on all levels
- [ ] 1.1.5 Set `data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))` for in-block hash index
- [ ] 1.1.6 Set `data_block_size_policy(BlockSizePolicy::all(8 * 1024))` for fewer I/O ops with sorted access
- [ ] 1.1.7 Verify existing unit tests pass with new configuration
- [ ] 1.1.8 Add test measuring cold-start open time with pinned blocks

### 1.2 Update session configuration (`config.rs`)
- [ ] 1.2.1 Add `expect_hits: bool, default = true` to `AnnotationConfig`
- [ ] 1.2.2 Wire `bio.annotation.cache_size_mb` through to `VepKvStore::open_with_cache_size()` (fix current mismatch where default is 1024 MB in config but 256 MB in `open()`)
- [ ] 1.2.3 Wire `bio.annotation.expect_hits` into `KeyspaceCreateOptions` at DB creation time
- [ ] 1.2.4 Update default `cache_size_mb` from 1024 to 512 (cold-start working set is ~640 MB total)

### 1.3 Update VepKvStore::open() to use session-aware defaults
- [ ] 1.3.1 Change `VepKvStore::open()` default cache from 256 MB to 512 MB
- [ ] 1.3.2 Ensure `KvCacheTableProvider::open()` reads cache size from session config when available

## 2. Phase 2: Sorted Bulk Ingestion

### 2.1 Implement sorted ingest via `start_ingestion()`
- [ ] 2.1.1 Add `build_sorted()` method to `CacheLoader` using `Keyspace::start_ingestion()` API
- [ ] 2.1.2 Ensure input data is globally sorted by key order: process chromosomes sequentially (1-22, X, Y, MT), positions ascending within each chromosome
- [ ] 2.1.3 Within each chromosome, group rows by `(chrom, start, end)` and serialize position entries in key order
- [ ] 2.1.4 Write each `(key, compressed_value)` pair via `Ingestion::write()` in sorted order
- [ ] 2.1.5 Call `Ingestion::finish()` after all entries are written
- [ ] 2.1.6 Handle the zstd dictionary training phase (sample 10K rows before sorted ingest, same as current)
- [ ] 2.1.7 Merge existing entries for duplicate positions (same logic as current `merge_position_entries()`)

### 2.2 Post-ingest compaction
- [ ] 2.2.1 After `finish()`, run major compaction on the data keyspace to consolidate SST files
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

### 3.2 Build time benchmark
- [ ] 3.2.1 Compare `batch_insert_raw()` vs `start_ingestion()` for full 1.17B-entry variation cache
- [ ] 3.2.2 Measure DB size (should be identical since encoding is unchanged)
- [ ] 3.2.3 Measure post-compaction time

### 3.3 Correctness validation
- [ ] 3.3.1 Verify annotation output matches between default and tuned fjall configs (bit-exact)
- [ ] 3.3.2 Verify all three allele match modes (Exact, ExactOrColocated, ExactOrRelaxed) produce same results
- [ ] 3.3.3 Verify extended coordinate probes still work with 8 KiB block size
- [ ] 3.3.4 Run existing `kv_cache` test suite with tuned configuration

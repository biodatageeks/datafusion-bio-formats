# Fjall Variation Lookup -- Design

## Context

The Ensembl VEP variation cache contains 1.17 billion known variant entries across 78 columns in a 30 GB Parquet file. An existing fjall-backed KV cache in `bio-function-vep` (`kv_cache/` module) provides position-keyed point lookups with zstd dictionary compression and multi-mode allele matching. The current implementation uses fjall's default configuration, which is designed for general-purpose mixed read/write workloads.

The primary workload is **cold-start single-sample annotation**: open the fjall DB, annotate one VCF (4-5M variants in ~30 seconds), close. There is no warm cache to amortize across samples. Every block read is potentially a first-time disk read.

### Stakeholders

- VEP annotation pipeline users (faster cold-start variant lookups)
- `bio-function-vep` crate maintainers
- polars-bio downstream consumers

## Goals / Non-Goals

**Goals:**
- Minimize cold-start annotation latency for single-sample VCF annotation
- Pin bloom filter and index blocks to eliminate first-access disk reads
- Use sorted bulk ingestion (`start_ingestion()`) to reduce build time 3-10x
- Make fjall tuning parameters configurable via `bio.annotation` session config
- Preserve the existing position-keyed architecture and allele matching logic

**Non-Goals:**
- Changing the key encoding (position-keyed with 18-byte keys is proven)
- Changing the value encoding (column-major with zstd dict compression is effective)
- Per-chromosome keyspaces (single keyspace with chrom-code prefix is sufficient)
- Warm-cache optimization (cold-start dominates the single-sample workload)
- Replacing `annotate_variants()` (consequence prediction still uses COITree)

---

## Existing Architecture (Unchanged)

The current `kv_cache/` module in `bio-function-vep` implements:

### Key Encoding (`key_encoding.rs`)
`[2B chrom_code BE][8B start BE][8B end BE]` = 18 bytes per position. Single `data` keyspace plus `meta` keyspace. Lexicographic byte ordering matches genomic ordering.

### Value Encoding (`position_entry.rs`)
One entry per genomic position containing all alleles at that position. Column-major binary layout with per-column null bitmaps, allele table, and zstd dictionary compression. Schema stored as Arrow IPC in metadata.

### Lookup Execution (`cache_exec.rs`)
`KvLookupExec` streams VCF batches and probes the KV store per-position with:
- Three match modes: Exact, ExactOrColocated, ExactOrRelaxed
- Extended coordinate probes for indel normalization (insertion-style, prefix-trimmed, tandem repeat shifts)
- Reusable zstd decompressor across all lookups in a stream
- Performance profiling via `VEP_KV_PROFILE` env var

### Ingest Pipeline (`loader.rs`)
`CacheLoader` streams from any `TableProvider`, trains a zstd dictionary from a 10K-row sample, and writes position entries with parallel partition processing.

---

## Cold-Start Performance Model

For each VCF variant lookup, fjall must:

1. **Check bloom filter** -- needs filter block loaded (first time = disk read)
2. **Read index block** -- locate data block (first time = disk read)
3. **Read data block** -- fetch key-value pair (first time = disk read)

With sorted VCF input, consecutive lookups hit nearby keys (same or adjacent data blocks). The effective access pattern is:

| Component | Size (300M positions) | Default pinning | Proposed pinning | Cold-start load time (NVMe) |
|---|---|---|---|---|
| Bloom filters (L0-L5, with `expect_point_read_hits`) | ~37 MB | L0 only | **All levels** | ~12 ms |
| Index blocks (all levels) | ~90 MB | L0+L1 | **All levels** | ~30 ms |
| **Pinned total** | **~127 MB** | ~10 MB | **~127 MB** | **~42 ms** |

A 5M-variant WGS with ~95% hit rate touches ~170K distinct data blocks (~170 MB of data I/O). On NVMe SSD at 3 GB/s: ~57 ms. This is the irreducible I/O floor for data blocks.

**Total cold-start annotation time (5M variants):**
- Current (default fjall config): ~15-30 seconds (filter/index cache misses dominate)
- Proposed (pinned + tuned): ~5-10 seconds (only data block I/O remains)

---

## Decisions

### Decision 1: Pin all bloom filter and index blocks

**What:** Set `filter_block_pinning_policy(PinningPolicy::all(true))` and `index_block_pinning_policy(PinningPolicy::all(true))`.

**Why:** With default pinning (L0 filter only, L0+L1 index), the first lookup that reaches L2+ triggers disk reads for filter and index blocks. In leveled compaction with 7 levels, 99%+ of data lives at L5-L6. Every novel variant check and every first-access known variant pays ~50-100 us per unpinned level.

**Trade-off:** +127 MB memory at DB open time vs default ~10 MB. Loads in ~42 ms on NVMe. Acceptable for a 30-second annotation run.

### Decision 2: Enable `expect_point_read_hits`

**What:** Set `expect_point_read_hits(true)` -- skips building bloom filters on the last level.

**Why:** For standard WGS/WES annotation against the full VEP cache, 95-98% of input variants exist in dbSNP/gnomAD. The 2-5% misses that reach the last level without a bloom filter do one extra data block read (~50 us) instead of being rejected by a bloom check. Total penalty: ~5K extra disk reads x 50 us = ~250 ms over a full 5M-variant annotation.

**Savings:** ~340 MB of bloom filter memory (last level holds ~90% of all filter data). Pinned bloom filters drop from ~375 MB to ~37 MB.

**When to disable:** Rare-variant filtering pipelines (50%+ miss rate) should set `bio.annotation.expect_hits = false`.

### Decision 3: Enable data block hash index

**What:** Set `data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))`.

**Why:** Within a cached 8 KiB data block, fjall defaults to binary search (~7 comparisons for ~60 keys per block). A hash index converts this to O(1). For sorted VCF input, consecutive lookups frequently hit the same cached block -- the hash index turns each subsequent in-block lookup from ~50 ns (binary search) to ~10 ns (hash probe).

**Trade-off:** ~75% overhead on in-memory block metadata. Only affects cached blocks, so bounded by block cache size, not DB size.

### Decision 4: Increase data block size to 8 KiB

**What:** Set `data_block_size_policy(BlockSizePolicy::all(8 * 1024))`.

**Why:** For sorted VCF access, larger blocks reduce the number of I/O operations by packing more keys per read. With 300M position keys:

| Block size | Blocks | Distinct I/O ops (5M hits) | Keys per block |
|---|---|---|---|
| 4 KiB (default) | ~10M | ~170K | ~30 |
| 8 KiB (proposed) | ~5M | ~85K | ~60 |

Halves I/O operations with minimal waste since VCF access is sequential. Also improves OS read-ahead effectiveness.

**Trade-off:** Slightly worse for random access (reads more data per block than needed). Acceptable since VCF is always position-sorted.

### Decision 5: Use `start_ingestion()` for sorted bulk load

**What:** Replace `batch_insert_raw()` with fjall's `Keyspace::start_ingestion()` API for pre-sorted data.

**Why:** The current loader writes through the full LSM path (memtable -> flush -> compaction), generating massive L0 files and expensive compaction cascades for 300M entries. Since variation cache data is sorted by `(chrom, position)` and keys are big-endian, data is already in lexicographic order. `start_ingestion()` writes sorted SSTs directly to the last level, bypassing memtable, journaling, and compaction.

**Expected speedup:** 3-10x faster ingest (30 min -> 5-10 min for 1.17B entries).

**Caveat:** Requires globally sorted input. The loader must process data in key order -- either sort partition output or process one chromosome at a time (naturally sorted within each chromosome).

### Decision 6: Tighter bloom filter FPR (0.01% all levels)

**What:** Set `filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(BloomConstructionPolicy::FalsePositiveRate(0.0001))))`.

**Current default:** L0: FPR 0.01%, L1+: 10 bits/key (~0.8% FPR).

**Why:** With `expect_point_read_hits=true`, bloom filters only exist on L0-L5. These catch novel variants before they reach the last level. Tighter FPR (0.01% vs 0.8%) means fewer false positives reaching L6: ~30 per 300K misses (0.01%) vs ~2400 (0.8%).

**Memory cost:** ~14 bits/key vs 10 bits/key = ~52 MB vs 37 MB for L0-L5. +15 MB is negligible.

### Decision 7: Block cache sized for cold-start working set

**What:** Default 512 MB block cache (configurable via `bio.annotation.cache_size_mb`).

**Why:** The cold-start working set is: pinned filter+index blocks (~127 MB) + active data blocks (~2-5 MB per batch) + OS read-ahead buffer. Total ~135 MB active. 512 MB provides headroom for chromosome transitions and concurrent partition processing without over-allocating. Going to 1-2 GB wastes memory that will never fill during a single-sample run.

---

## Fjall Configuration Summary

### Read-Optimized Keyspace Options (applied at DB creation)

```rust
KeyspaceCreateOptions::default()
    // Bloom filters: tight FPR, skip last level (95%+ hits expected)
    .filter_policy(FilterPolicy::all(
        FilterPolicyEntry::Bloom(
            BloomConstructionPolicy::FalsePositiveRate(0.0001)
        )
    ))
    .expect_point_read_hits(true)
    // Pin ALL filter + index blocks for cold-start performance
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .index_block_pinning_policy(PinningPolicy::all(true))
    // In-block hash index for O(1) within-block lookups
    .data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))
    // 8 KiB blocks: fewer I/O ops for sorted VCF access
    .data_block_size_policy(BlockSizePolicy::all(8 * 1024))
    // Disable SST compression (values already zstd-compressed)
    .data_block_compression_policy(CompressionPolicy::disabled())
```

### Write-Optimized Overrides (for ingest only)

```rust
    // Bulk load: skip journal, defer compaction
    .manual_journal_persist(true)
    .compaction_strategy(Arc::new(
        Leveled::default().with_l0_threshold(16)
    ))
```

### Database-Level Settings

```rust
Database::builder(path)
    .cache_size(512 * 1024 * 1024)  // 512 MB default
    .open()
```

### Session Configuration (`bio.annotation` namespace)

| Key | Default | Description |
|---|---|---|
| `bio.annotation.cache_size_mb` | 512 | fjall block cache size for KV reads |
| `bio.annotation.expect_hits` | true | Skip last-level bloom filters (set false for rare-variant pipelines) |
| `bio.annotation.zstd_level` | 3 | Compression level for cache writes |
| `bio.annotation.dict_size_kb` | 112 | Zstd dictionary size for cache writes |

---

## Performance Expectations (Cold-Start, Single Sample)

### Summary: 5M Variant WGS Annotation

| Metric | Current (default fjall) | Proposed (tuned) | Improvement |
|---|---|---|---|
| DB open + filter/index load | ~1-2s (lazy, cache misses) | ~42 ms (pinned, preloaded) | **25-50x** |
| Novel variant rejection | ~50-100 us (filter cache miss) | ~100-200 ns (pinned bloom) | **250-1000x** |
| Known variant lookup (first access) | ~100-200 us (3 disk reads) | ~50-100 us (1 disk read, filter+index cached) | **2x** |
| Known variant lookup (cached block) | ~1-3 us (binary search) | ~0.5-1 us (hash probe) | **2-3x** |
| 5M WGS annotation (end-to-end) | ~15-30s | ~5-10s | **2-4x** |
| DB build (1.17B entries) | ~30 min (batch insert) | ~5-10 min (`start_ingestion()`) | **3-6x** |
| Memory at query time | ~266 MB (256 cache + ~10 pinned) | ~640 MB (512 cache + ~127 pinned) | +374 MB |

### Detailed Breakdown: Where Time Is Saved

Workload profile for 5M-variant WGS: ~95% known variants (4.75M hits), ~5% novel variants (250K misses), ~170K distinct data blocks touched.

#### 1. DB Open + Filter/Index Loading (~1-2s saved)

**Current:** Fjall lazily loads filter/index blocks on first access. With 7 LSM levels and 99%+ of data at L5-L6, the first lookup reaching each level triggers a disk read for its filter block + index block. These cache misses accumulate across the first few thousand lookups.

**Tuned:** Pinning policies preload all ~127 MB of filter+index blocks at open time. On NVMe at 3 GB/s that is ~42 ms upfront, then zero filter/index cache misses for the entire run.

#### 2. Novel Variant Rejection -- the dominant win (~15-25s saved)

**Current:** Each of the ~250K novel variant lookups must load filter blocks from disk per-level before the bloom filter can reject it. Even though the bloom filter eventually rejects the lookup, the disk I/O to load the filter block dominates: ~50-100 us per lookup per unpinned level. With most data at L5-L6, this means 4-5 unpinned levels per lookup.

**Tuned:** Pinned bloom filters are already in RAM. A miss is just a hash computation + memory lookup per level (~100-200 ns total). At 0.01% FPR, virtually no false positives leak through to trigger unnecessary data block reads.

**Aggregate:** ~250K misses x ~80 us saved = **~20 seconds saved**. This is the single largest improvement and explains why pinning filter+index blocks is the most impactful change.

#### 3. Known Variant First-Access Data Block I/O (~4-5s saved)

**Current:** Each first-access known variant performs 3 disk reads: filter block + index block + data block. With ~170K distinct 4 KiB data blocks, that is ~170K x 3 = ~510K disk reads.

**Tuned:** Filter + index are pinned, so only 1 disk read remains (the data block itself). The 8 KiB block size packs ~60 keys/block vs ~30 at 4 KiB, halving the number of distinct blocks to ~85K. Total: ~85K disk reads instead of ~510K.

**Aggregate:** ~85K fewer disk reads x ~50 us = **~4 seconds saved**

#### 4. In-Block Lookups on Cached Blocks (~2-4s saved)

**Current:** Binary search within cached 4 KiB blocks (~30 keys, ~7 comparisons per lookup, ~1-3 us).

**Tuned:** Hash index (`ratio=0.75`) gives O(1) in-block lookup (~0.5-1 us). Larger 8 KiB blocks mean more consecutive VCF positions share the same cached block, so a higher fraction of the 4.75M known-variant lookups benefit from the hash index on already-cached blocks.

**Aggregate:** modest per-lookup improvement but across millions of cached hits: **~2-4 seconds saved**

#### 5. `expect_point_read_hits=true` (saves ~340 MB RAM, costs +250 ms)

Skips building bloom filters on the last level, which holds ~90% of all filter data. The 2-5% of lookups that miss and reach L6 without a bloom filter do one extra data block read (~50 us each). Penalty: ~5K extra reads x 50 us = **+250 ms** -- negligible vs the ~340 MB memory saved.

#### End-to-End Aggregate

| Component | Current | Tuned | Saving |
|---|---|---|---|
| DB open + block loading | ~1-2s | ~42 ms | ~1-2s |
| Novel variant rejection (250K) | ~15-25s | ~50 ms | **~15-25s** |
| Data block I/O (first access) | ~8-10s | ~4-5s | ~4-5s |
| In-block lookups (cached) | ~5-8s | ~2-4s | ~2-4s |
| `expect_hits` penalty | 0 | +250 ms | -250 ms |
| **Total (end-to-end)** | **~15-30s** | **~5-10s** | **2-4x** |

The **dominant win** is pinning filter+index blocks. Without pinning, every novel variant rejection pays ~50-100 us in disk I/O per unpinned level. With 250K novel variants across multiple levels, that alone accounts for 15-25 seconds of the current runtime. Once pinned, those become nanosecond-scale RAM lookups.

### Build Time (One-Time Cost)

`start_ingestion()` for sorted bulk load: **~30 min to ~5-10 min** (3-6x). This only matters when rebuilding the cache from a new Ensembl release, not per-annotation run.

---

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|---|---|---|
| Pinned blocks increase memory by ~374 MB | OOM on low-memory machines | Configurable via `bio.annotation.cache_size_mb`; can disable pinning with custom `KeyspaceCreateOptions` |
| `expect_point_read_hits=true` misses ~2-5% of novel variants at last level | +250 ms for 5M-variant WGS | Configurable via `bio.annotation.expect_hits`; penalty is < 1% of total annotation time |
| `start_ingestion()` requires globally sorted input | Must sort or process per-chromosome | Variation cache is already sorted by `(chrom, position)`; process chromosomes sequentially |
| 8 KiB blocks waste more on random access | Negligible -- VCF is always sorted | Could be made configurable if needed |
| Hash ratio 0.75 increases in-memory block size | Bounded by block cache capacity | Only affects cached blocks; no disk space overhead |
| Fjall 3.x API changes | Update needed for new major versions | Pin to exact version; existing DB format is forward-compatible within fjall 3.x |

## Open Questions

- Whether to add an explicit warmup step (`VepKvStore::warmup(&[chromosomes])`) that pre-touches filter/index blocks for target chromosomes before the main lookup loop
- Whether `start_ingestion()` can handle the current multi-partition streaming loader or requires a single-threaded sorted writer
- Optimal `RestartIntervalPolicy` for 8 KiB blocks with 18-byte keys (default 16 may be too high)
- Whether to expose `data_block_size_policy` as a session config or keep it as a build-time constant

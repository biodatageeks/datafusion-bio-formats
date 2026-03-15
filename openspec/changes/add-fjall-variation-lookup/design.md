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
- Changing the value encoding (column-major with zstd dict compression is effective)
- Per-chromosome keyspaces (single keyspace with chrom-code prefix is sufficient)
- Warm-cache optimization (cold-start dominates the single-sample workload)
- Replacing `annotate_variants()` (consequence prediction still uses COITree)

---

## Alternatives Considered

| Alternative | 5M Lookups | Cold-Start | Memory | Portability | Verdict |
|---|---|---|---|---|---|
| fjall (tuned) | 10-15s | ~115ms pinned | ~512 MB | **Pure Rust, all platforms + WASM** | Default backend |
| LMDB/heed | 2-5s | ~0ms (mmap) | ~0 user-space | C lib, Linux/macOS/Windows, no WASM | **Opt-in via feature flag** |
| libmdbx | 2-5s | ~0ms (mmap) | ~0 user-space | C lib, same as LMDB | Consider as LMDB alternative |
| RocksDB | 3-8s | 0.5-2s | ~500-800 MB | C++, heavy build, painful cross-compile | Skip |
| Custom flat file | 1-3s | ~0ms | ~0 | Pure Rust possible | Fastest but maintenance burden |
| redb/sled | 4-15s | 100-500ms | 200-500 MB | Pure Rust | Not competitive |

Why mmap wins for this workload: cold-start (no block loading), sequential access (OS read-ahead), 95% hits (bloom filters less valuable), read-only (no write overhead). But fjall's pure-Rust portability is a strategic advantage for polars-bio distribution (Python wheels, cross-compilation, WASM).

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

## Current Parquet SQL Path

Today `lookup_variants()` in `datafusion-bio-functions` synthesizes SQL equivalent to:

```sql
SELECT a.*, b.requested_columns...
FROM `cache` AS b, `vcf` AS a
WHERE a.chrom = b.chrom
  AND CAST(a.end AS INTEGER) >= CAST(b.start AS INTEGER)
  AND CAST(a.start AS INTEGER) <= CAST(b.end AS INTEGER)
  AND match_allele(a.ref, a.alt, b.allele_string)
```

Observed implications from the current implementation:

- The parquet path is still a generic SQL join produced by [`lookup_provider.rs`](/Users/mwiewior/CLionProjects/datafusion-bio-functions/datafusion/bio-function-vep/src/lookup_provider.rs), so build/probe orientation is delegated to the optimizer instead of being explicit at the API boundary.
- `IntervalJoinExec` is useful for wide interval joins, but this workload is mostly exact point probes. The cache side still streams the requested Parquet columns even when the final answer is a single row at a single position.
- `match_allele()` runs after the overlap candidate set is produced, so allele normalization and `allele_string` splitting are repeated row-by-row for every candidate match.
- `prune_nulls` is carried through the table-function surface today but is not applied in the provider scan path, so it does not mitigate Parquet I/O or output-shaping cost.

Performance implication: the parquet backend remains dominated by streaming and post-filter CPU even when the workload is overwhelmingly exact hits, which is why the KV backend should be selectable explicitly instead of trying to squeeze more out of the current SQL formulation alone.

---

## Fjall 3.0.2 Reality Check

Reviewing the stock `fjall-3.0.2` and `lsm-tree-3.0.2` sources changes a few assumptions in the current spec:

- **Metadata pinning is coupled to partitioning.** Later levels default to partitioned index/filter blocks. In that layout fjall eagerly keeps only the top-level structures resident; lower partitions are still demand-loaded. `PinningPolicy::all(true)` alone does not achieve "all metadata pinned" unless metadata partitioning is disabled for the cold-start profile.
- **`start_ingestion()` is faster, but it does not create the final read-optimized shape by itself.** It bypasses memtables and journaling, but the ingested tables are still registered as a new run and depend on follow-up compaction to reach steady state.
- **`major_compact()` is not strategy-aware in stock fjall 3.0.2.** The public helper compacts with a hardcoded 64 MB target, so it can silently undo a larger table-size choice if the implementation assumes it honors the configured leveled strategy.
- **`HashRatioPolicy` is a bucket-per-item multiplier.** `0.75` means roughly 0.75 hash buckets per item in a data block, not "75% memory overhead".
- **KV separation is available, but it is not free.** `with_kv_separation()` can reduce compaction/write amplification for large values, but point reads become two-hop reads (index table + blob file), which is often the wrong trade-off for lookup-heavy position entries unless the value-size distribution is large enough.

These are implementation constraints, not reasons to abandon fjall. They do mean the OpenSpec should steer the implementation toward knobs that the stock 3.0.x API actually honors.

---

## Cold-Start Performance Model

For each VCF variant lookup, fjall must:

1. **Check bloom filter** -- needs filter block loaded (first time = disk read)
2. **Read index block** -- locate data block (first time = disk read)
3. **Read data block** -- fetch key-value pair (first time = disk read)

With sorted VCF input, consecutive lookups hit nearby keys (same or adjacent data blocks). The effective access pattern below assumes the cold-start profile disables metadata partitioning so filter/index pinning applies to the full metadata blocks:

| Component | Size (1.15B positions, 10B keys) | Default pinning | Proposed pinning | Cold-start load time (NVMe) |
|---|---|---|---|---|
| Bloom filters (L0-L5, with `expect_point_read_hits`) | ~190 MB | L0 only | **All levels** | ~63 ms |
| Index blocks (all levels) | ~155 MB | L0+L1 | **All levels** | ~52 ms |
| **Pinned total** | **~345 MB** | ~10 MB | **~345 MB** | **~115 ms** |

**Note:** The VEP variation cache contains ~1.15B unique genomic positions (1.17B rows at ~1.02 alleles/position, empirically measured on chr22). This is ~4x higher than earlier estimates of 300M, which significantly impacts pinned block sizes, on-disk DB size (~74 GB with 10B keys), and data block I/O.

A 5M-variant WGS with ~95% hit rate touches ~3.7M distinct 8 KiB data blocks (~29 GB of data I/O). With sorted VCF access, this is quasi-sequential — OS read-ahead on NVMe yields effective throughput near 3 GB/s: ~10s. This is the irreducible I/O floor for data blocks, and the primary target for prefetch/look-ahead optimizations (Decisions 10-11).

**Total cold-start annotation time (5M variants):**
- Current (default fjall config): ~15-30 seconds (filter/index cache misses dominate)
- Proposed (pinned + tuned): ~10-15 seconds (data block I/O dominates)
- Proposed (pinned + tuned + prefetch): ~7-12 seconds (overlapped I/O)

---

## Decisions

### Decision 1: Disable metadata partitioning before pinning

**What:** For the cold-start single-sample profile, set:

```rust
.filter_block_partitioning_policy(PinningPolicy::all(false))
.index_block_partitioning_policy(PinningPolicy::all(false))
.filter_block_pinning_policy(PinningPolicy::all(true))
.index_block_pinning_policy(PinningPolicy::all(true))
```

**Why:** In stock fjall/lsm-tree 3.0.2, partitioned index and filter layouts keep only the top-level structures resident. Lower metadata partitions still load on demand. So the current spec's "pin all filter/index blocks" claim is only true if metadata partitioning is disabled for the DB profile we want to optimize.

**Trade-off:** Unpartitioned metadata blocks are larger and slightly less flexible for mixed workloads, but they make cold-start behavior predictable and remove extra random reads for lower-level metadata partitions. For this read-mostly, one-shot annotation workload, that is the better trade.

### Decision 2: Enable `expect_point_read_hits`

**What:** Set `expect_point_read_hits(true)` -- skips building bloom filters on the last level.

**Why:** For standard WGS/WES annotation against the full VEP cache, 95-98% of input variants exist in dbSNP/gnomAD. The 2-5% misses that reach the last level without a bloom filter do one extra data block read (~50 us) instead of being rejected by a bloom check. Total penalty: ~5K extra disk reads x 50 us = ~250 ms over a full 5M-variant annotation.

**Savings:** ~1.7 GB of bloom filter memory (last level holds ~90% of all filter data at 1.15B positions). Pinned bloom filters drop from ~1.9 GB to ~190 MB.

**Penalty analysis:** All lookups (hits and misses) traverse L0-L5 before reaching L6, since ~90% of data lives at L6. Without L6 bloom, the 250K novel variant misses each do one extra data block read at L6. However, since VCF is sorted and 95% of lookups are hits, most data blocks at L6 are already fetched for neighboring known variants — the marginal cost of miss lookups reading already-cached blocks is negligible. Only "novel-only" blocks (no nearby known variants) incur real I/O penalty: estimated ~5K extra reads × 50 us = **~250 ms**.

**When to disable:** Rare-variant filtering pipelines (50%+ miss rate) should set `bio.annotation.expect_hits = false`. At 1.15B positions, this costs ~1.7 GB additional memory for the L6 bloom filter.

### Decision 3: Enable data block hash index

**What:** Set `data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))`.

**Why:** Within a cached 8 KiB data block, fjall defaults to binary search. The embedded hash index converts repeated point probes in the same block to an O(1) bucket lookup. With sorted VCF input, consecutive variants often reuse the same data block, so this avoids repeated binary-index walks.

**Important fjall detail:** `HashRatioPolicy` is a **bucket-per-item multiplier**. With ~126 items per 8 KiB block, `0.75` produces about 94 bucket bytes, typically well under 1 byte per KV. It is not a RocksDB-style "75% metadata overhead" knob.

**Trade-off:** The index only helps when the same data block is already in memory, so it should be kept in the 0.5-1.0 range and benchmarked rather than cranked blindly.

### Decision 4: Increase data block size to 8 KiB

**What:** Set `data_block_size_policy(BlockSizePolicy::all(8 * 1024))`.

**Why:** For sorted VCF access, larger blocks reduce the number of I/O operations by packing more keys per read. With ~1.09B position keys (10B encoding):

| Block size | Blocks | Keys per block | Block span (bp) |
|---|---|---|---|
| 4 KiB (default) | ~17.3M | ~63 | ~173 bp |
| 8 KiB (proposed) | ~8.7M | ~126 | ~347 bp |

Halves I/O operations and increases the chance that consecutive VCF variants (~600 bp apart in WGS) share a data block. Also improves OS read-ahead effectiveness for the quasi-sequential access pattern.

**Trade-off:** Slightly worse for random access (reads more data per block than needed). Acceptable since VCF is always position-sorted.

### Decision 5: Use `start_ingestion()` for sorted bulk load

**What:** Replace `batch_insert_raw()` with fjall's `Keyspace::start_ingestion()` API for pre-sorted data.

**Why:** The current loader writes through the full LSM path (memtable -> flush -> compaction), generating expensive write amplification for 1.15B entries. Since variation cache data is sorted by `(chrom, position)` and keys are big-endian, it is a good fit for fjall's ingestion API.

**Important fjall detail:** Stock fjall 3.0.2 ingestion bypasses memtable/journal, but it still creates a new run that then depends on compaction to reach the final read shape. It is still the right direction, but the spec must not overstate it as "direct write to the final level".

**Expected speedup:** 3-6x faster initial build step versus the current batch-insert path. Final steady-state layout still depends on the post-ingest compaction path.

**Caveat:** Requires globally sorted input. The loader must process data in key order -- either sort partition output or process one chromosome at a time (naturally sorted within each chromosome).

### Decision 6: Tighter bloom filter FPR (0.01% all levels)

**What:** Set `filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(BloomConstructionPolicy::FalsePositiveRate(0.0001))))`.

**Current default:** L0: FPR 0.01%, L1+: 10 bits/key (~0.8% FPR).

**Why:** With `expect_point_read_hits=true`, bloom filters only exist on L0-L5 (~10% of 1.09B entries). These catch false positives at upper levels before reaching L6. Tighter FPR (0.01% vs 0.8%) means fewer false positives reaching L6: ~10 per 100K (0.01%) vs ~800 (0.8%).

**Memory cost:** ~14 bits/key vs 10 bits/key = ~190 MB vs 136 MB for L0-L5. +54 MB is negligible given ~345 MB total pinned.

### Decision 7: Block cache sized for cold-start working set

**What:** Default 512 MB block cache (configurable via `bio.annotation.cache_size_mb`).

**Why:** With metadata partitioning disabled and all filter/index blocks pinned, the cache must absorb the whole metadata working set up front. The remaining space is primarily for short-lived data blocks. The 5M-variant WGS workload touches millions of distinct data blocks in a mostly sequential sweep, so OS readahead matters more than a very large user-space cache.

**Trade-off:** 512 MB is a pragmatic default, not a law. If metadata grows beyond estimates after real DB builds, the implementation should increase the default rather than silently re-enable metadata partitioning.

---

### Decision 8: Pluggable Backend via `VepKvBackend` Trait

**What:** Define a common trait for KV backend implementations:

```rust
pub trait VepKvBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn contains(&self, key: &[u8]) -> Result<bool>;
    fn metadata(&self, key: &str) -> Result<Option<Vec<u8>>>;
}
```

Both `FjallBackend` and `LmdbBackend` implement this. `KvLookupExec` and `VepKvStore` operate on `Arc<dyn VepKvBackend>`. Default: fjall. LMDB available via feature flag `lmdb-backend`.

**Why:** Deep research shows LMDB/heed (mmap-based B+ tree) could be 2-3x faster than tuned fjall for cold-start single-sample annotation. A pluggable backend allows benchmarking both without committing to either.

**Portability consideration:** Fjall is pure Rust — zero C/FFI deps, compiles on every Rust target (Linux, macOS, Windows, WASM), trivial cross-compilation. LMDB/heed requires a C compiler and `liblmdb` — works in CI but complicates Python wheel builds (need `liblmdb-dev`), cross-compilation (need C cross-toolchain), and rules out WASM. This is why fjall stays as the default and LMDB is feature-gated — users who need maximum cold-start performance on Linux/macOS can opt in, while the default pure-Rust path works everywhere.

### Decision 9: 10-Byte Position-Only Keys

**What:** Reduce key encoding from `[2B chrom][8B start][8B end]` (18 bytes) to `[2B chrom][8B start]` (10 bytes).

**Why:**
- 95% of variants are SNVs (start==end) — end is redundant in key
- End coordinate already stored in position entry value
- Saves ~9.7 GB key storage across 1.15B entries (20.6 GB → 10.9 GB)
- Smaller index blocks → better cache utilization
- Extended coordinate probes simplified: probe position-only, resolve end from value
- VariantKey (tecnickcom) validates position-only indexing at scale

**Trade-off:** Multiple entries at same position with different end coords must be stored as a list in the value (already the case — position entries contain all alleles at a position).

### Decision 10: Backend-Specific Chromosome Warmup/Prefetch

**What:** Before annotating a chromosome, warm up the corresponding data regions using a backend-appropriate strategy. Overlaps I/O with computation on the previous chromosome.

**Fjall backend:** Use fjall's `prefix(chrom_code_bytes)` or `range(chrom_start..chrom_end)` iterator in a background task. Each `.next()` loads the next data block into fjall's block cache directly. This is preferable to `fadvise` because:
- Pure Rust, works on all platforms including WASM
- Loads directly into fjall's block cache (vs relying on OS page cache alignment with fjall's block boundaries)
- No need to reverse-engineer SST file byte ranges — fjall doesn't expose SST internal layouts, so computing offsets for `fadvise` would require tracking internal file positions

**LMDB backend:** Use `madvise(MADV_WILLNEED)` on chromosome key range pages. This is native and zero-cost for mmap — the OS kernel handles readahead directly on the mapped region.

**Why:**
- Estimated saving: ~3s on cold-start WGS
- Backend-specific strategies use each engine's natural I/O path
- Low complexity, no API changes beyond existing `warmup()` trait method

### Decision 11: Pipelined I/O Look-Ahead

**What:** Look ahead at the next N VCF variants' keys (e.g., N=64), compute expected data block locations from pinned index blocks, and issue async prefetch reads while processing current batch.

**Why:**
- Estimated saving: ~2-3s by overlapping computation with I/O
- For fjall: use index blocks to predict data block offsets, issue `fadvise`
- For LMDB: `madvise(WILLNEED)` on predicted leaf pages
- Medium complexity — requires look-ahead buffer in `KvLookupExec`

### Decision 12: Add explicit lookup backend selection and hybrid fallback

**What:** Introduce `bio.annotation.lookup_backend = parquet | fjall | hybrid | auto`.

- `parquet`: preserve the current SQL/`IntervalJoinExec` path
- `fjall`: force KV lookup
- `hybrid`: probe KV first, then route unresolved or unsupported cases to the parquet interval-join path
- `auto`: select `fjall` for high-hit known-variant lookup workloads, otherwise keep parquet

**Why:** The parquet path is still the correctness baseline and is already integrated with SQL planning. A hybrid mode lets us take the fast path for the 95-98% exact hits while preserving coverage for misses, debugging, and any representation edge cases we do not want to encode into the KV path on day one.

**Performance impact:** If KV resolves 95-98% of rows, the expensive parquet fallback is reduced to the remaining 2-5% instead of scanning the entire requested cache columns for every input row.

### Decision 13: Keep KV separation off by default; gate it on measured value sizes

**What:** Do not enable `with_kv_separation()` in the default cold-start profile. Benchmark it behind an opt-in creation flag using thresholds such as 1 KiB and 2 KiB on compressed `PositionEntry` values.

**Why:** KV separation reduces compaction/write amplification for large values, but every successful point lookup can become a two-hop read (table + blob file). For lookup-heavy annotation, that is usually a net loss unless the serialized position entries are substantially larger than the current dictionary-compressed payloads.

**Performance impact:** For sub-1 KiB compressed values, inline values should keep point lookups roughly 5-20% faster by avoiding blob-file indirection. If value sizes are materially larger, KV separation may still be worthwhile for build/compaction throughput and on-disk write amplification, so it should remain a benchmarked option rather than a default.

### Decision 14: Benchmark restart interval (4 vs 8 vs 16) to maximize hash index effectiveness

**What:** Sweep `data_block_restart_interval_policy` across **4, 8, and 16** for the fixed-width 10-byte genomic key format. The default is 16.

**Why:** The hash index (Decision 3, `hash_ratio=0.75`) uses restart points as hash buckets. The restart interval directly controls how many restart points exist per data block, which determines hash index effectiveness:

| Restart Interval | Restart Points (126 keys/block) | Hash Buckets (×0.75) | Keys/Bucket |
|------------------|---------------------------------|----------------------|-------------|
| 16 (default)     | ~8                              | ~6                   | ~21 — barely useful |
| 8                | ~16                             | ~12                  | ~10 — moderate |
| **4 (recommended)** | **~32**                      | **~24**              | **~5 — near-O(1)** |

Going *up* (e.g. to 32) would reduce restart points to ~4, producing ~3 hash buckets — worse than binary search and effectively making the hash index decorative. The correct direction is *down* to create more restart points and thus more hash buckets.

The trade-off from lower restart intervals is slightly more restart-point overhead per block (each restart point stores a full key instead of a delta-compressed suffix). With 10-byte keys, interval=4 adds ~24 extra full keys vs delta-compressed keys per block — roughly 24 × 10B = 240 bytes extra, ~3% of an 8 KiB block. This is negligible compared to the hash index improvement.

**Performance impact:** With interval=4 + hash_ratio=0.75, cached-block lookups approach true O(1) (~5 keys/bucket). Without this fix, Decision 3 (hash index) provides minimal benefit. Estimated ~1-2s improvement on 5M WGS from faster in-block resolution across millions of cached-block hits.

### Decision 15: Projection-Aware Value Deserialization

**What:** `PositionEntry::decode()` accepts a column projection bitmask. After zstd decompression (unavoidable since the entire value is one compressed blob), skip non-projected columns in the column-major buffer — each column is at a known offset in the serialized layout.

**Why:** KV entries store all 78 columns in a single compressed blob. When a query requests only 2 columns (e.g., `consequence_type`, `impact`), the current path fully deserializes all 78 columns after decompression. Parquet benefits from projection pushdown (30 GB → 8 GB I/O for 2 columns); the KV path has no equivalent. Projection-aware deserialization cannot skip decompression, but it can skip parsing/allocating the ~76 unused columns.

**Performance impact:** ~0.5-2s saved on 5M WGS when requesting few columns. The saving comes from avoiding Arrow array construction for unused columns, not from I/O reduction.

### Decision 16: Observability Metrics via `VEP_KV_PROFILE`

**What:** Define a `KvProfileMetrics` struct with atomic counters, emitted as JSON to stderr when `VEP_KV_PROFILE=1` (per-run summary) or `VEP_KV_PROFILE=verbose` (per-chromosome breakdown).

**Per-run metrics (`VEP_KV_PROFILE=1`):**
- `total_variants`, `kv_hits`, `kv_misses`, `position_hit_allele_miss`
- `extended_probe_hits`, `total_get_calls`
- `bytes_decompressed`, `elapsed_lookup_ns`, `elapsed_decompress_ns`, `elapsed_allele_match_ns`

**Per-chromosome metrics (`VEP_KV_PROFILE=verbose`):**
- `first_lookup_latency_us`, `warmup_elapsed_us`

**Startup info (always when profiling enabled):**
- `backend_type`, `cache_size_mb`, `expect_hits`, `db_path`, `db_size_bytes`

**Why:** The spec mentions `VEP_KV_PROFILE` in the existing `cache_exec.rs` description but doesn't define what metrics are captured or the output format. Explicit metric definitions enable consistent benchmarking across backend changes, regression detection, and user-facing diagnostics.

**Overhead:** Atomic counter increments (~1-2 ns each) when profiling is enabled. Zero overhead when `VEP_KV_PROFILE` is unset — the flag check is a single `static AtomicBool` load.

### Decision 17: Per-Chromosome Parallel Partitions in `KvLookupExec`

**What:** `KvLookupExec` reports N output partitions (one per chromosome present in the input VCF). Each partition independently calls `warmup()` for its chromosome and maintains its own look-ahead buffer. All partitions share a single `Arc<dyn VepKvBackend>`.

**Why:** Fjall is thread-safe for concurrent reads. A single-partition `KvLookupExec` serializes all I/O through one thread, under-utilizing NVMe parallelism. With 8-12 active partitions on NVMe, the I/O subsystem can service multiple concurrent data block reads.

**Performance impact:** ~1.5-2x I/O parallelism on NVMe. The gain is bounded by the storage device's queue depth and diminishes on SATA SSDs or spinning disks.

**Trade-off:** Requires the upstream input plan to partition by chromosome (standard for sorted VCF). If the input is single-partition, `KvLookupExec` falls back to single-partition mode.

### Decision 18: Early-Exit Allele Matching

**What:** For `Exact` match mode, check the allele table (at a known offset in the decompressed `PositionEntry`) before deserializing remaining columns. If no allele matches, skip the ~78 column deserialization entirely.

**Why:** In the common case where a position exists in the cache but the specific allele does not match (e.g., tri-allelic site where only 2 of 3 alleles are in the cache), the full column deserialization is wasted work. The allele table is a compact structure at a fixed offset — checking it first is cheap.

**Performance impact:** ~0.1-0.3s saved. Modest per-lookup saving but compounds across millions of `position_hit_allele_miss` cases.

### Decision 19: LEFT JOIN Note for Parquet Fallback

**What:** Document that the current parquet path uses an implicit INNER JOIN — variants not found in the cache are dropped from the output. The KV path and hybrid mode already preserve all input rows (returning null annotation columns for misses).

**Correction needed:** When `lookup_backend=parquet`, the parquet path should be updated to use LEFT JOIN semantics for correctness parity with the KV path. This is a `bio-function-ranges` concern (LEFT `IntervalJoinExec`), not this spec. Filed here as a cross-reference.

### Decision 20: Benchmark Zstd Dictionary Size

**What:** Benchmark zstd dictionary sizes of 112 KB (current default), 256 KB, and 512 KB for the VEP variation cache.

**Why:** The default 112 KB dictionary was chosen generically. With 78 columns of repetitive bioinformatics strings (consequence types, gene symbols, transcript IDs), a larger dictionary may capture more recurring patterns and improve compression ratio by 10-15%.

**Performance impact:** ~0.3-0.5s saved from reduced decompression time on 5M WGS (smaller compressed values → less data to decompress). Minimal memory overhead — the dictionary is loaded once.

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
    // Disable partitioned metadata so pinning actually keeps full metadata resident
    .filter_block_partitioning_policy(PinningPolicy::all(false))
    .index_block_partitioning_policy(PinningPolicy::all(false))
    // Pin ALL filter + index blocks for cold-start performance
    .filter_block_pinning_policy(PinningPolicy::all(true))
    .index_block_pinning_policy(PinningPolicy::all(true))
    // In-block hash index for O(1) within-block lookups
    .data_block_hash_ratio_policy(HashRatioPolicy::all(0.75))
    // 8 KiB blocks: fewer I/O ops for sorted VCF access
    .data_block_size_policy(BlockSizePolicy::all(8 * 1024))
    // Lower restart interval to maximize hash index bucket count
    // 126 keys/block ÷ 4 = ~32 restart points × 0.75 = ~24 hash buckets → ~5 keys/bucket
    .data_block_restart_interval_policy(RestartIntervalPolicy::all(4))
    // Disable SST compression (values already zstd-compressed)
    .data_block_compression_policy(CompressionPolicy::disabled())
```

**Note on index block compression:** Index blocks are kept uncompressed because they are pinned in memory and accessed on every `get()`. LZ4 decompression (~20ns/call) on every lookup would outweigh the ~75 MB savings from compressing ~155 MB of pinned index data. For a workload doing millions of point lookups per run, uncompressed pinned index blocks are the correct trade-off.

### Write-Optimized Overrides (for ingest only)

```rust
    // Bulk load: skip journal, defer compaction
    .manual_journal_persist(true)
    .compaction_strategy(Arc::new(
        Leveled::default().with_l0_threshold(16)
    ))
```

### Leveled Compaction Settings

```rust
    .compaction_strategy(Arc::new(
        Leveled::default()
            .with_l0_threshold(16)
            .with_table_target_size(256 * 1024 * 1024)  // 256 MiB SSTs → ~289 SSTs at L6
    ))
```

**Why 256 MiB SSTs:** With default 64 MiB SSTs, a ~74 GB DB produces ~1,156 SSTs at L6. macOS limits open file descriptors, and fjall's `max_cached_files` default (~150) would cause file descriptor exhaustion. At 256 MiB, L6 has ~289 SSTs — well within the `max_cached_files(512)` headroom below. Fewer SSTs also means fewer filter/index blocks to pin, slightly improving cold-start load time.

### Database-Level Settings

```rust
Database::builder(path)
    .cache_size(512 * 1024 * 1024)  // 512 MB default (~345 MB pinned + ~167 MB data blocks)
    .max_cached_files(Some(512))    // Safe headroom above ~300 SSTs with 256 MiB target
    .open()
```

### Session Configuration (`bio.annotation` namespace)

| Key | Default | Description |
|---|---|---|
| `bio.annotation.cache_size_mb` | 512 | fjall block cache size for KV reads |
| `bio.annotation.expect_hits` | true | Skip last-level bloom filters (set false for rare-variant pipelines) |
| `bio.annotation.lookup_backend` | `auto` | Choose `parquet`, `fjall`, `hybrid`, or `auto` lookup path |
| `bio.annotation.zstd_level` | 3 | Compression level for cache writes |
| `bio.annotation.dict_size_kb` | 112 | Zstd dictionary size for cache writes |

---

## Performance Expectations (Cold-Start, Single Sample)

### Summary: 5M Variant WGS Annotation

**Basis:** 1.15B positions (empirically measured: 1.17B rows / 1.02 alleles per position), 10B keys (~1.09B unique starts), ~55 B/entry compressed (zstd dict, 3.8x ratio). Full DB on disk: ~74 GB.

| Metric | Current (default fjall) | Proposed (tuned) | Improvement |
|---|---|---|---|
| DB open + filter/index load | ~1-2s (lazy, cache misses) | ~115 ms (pinned, preloaded) | **10-17x** |
| Novel variant rejection | ~50-100 us (filter cache miss) | ~100-200 ns (pinned bloom) | **250-1000x** |
| Known variant lookup (first access) | ~100-200 us (3 disk reads) | ~50-100 us (1 disk read, filter+index cached) | **2x** |
| Known variant lookup (cached block) | ~1-3 us (binary search) | ~0.5-1 us (hash probe) | **2-3x** |
| 5M WGS annotation (end-to-end) | ~15-30s | ~10-15s (tuned) / ~7-12s (+ prefetch) | **1.5-3x** |
| DB build (1.17B entries) | ~30 min (batch insert) | ~5-10 min (`start_ingestion()`) | **3-6x** |
| Memory at query time | ~266 MB (256 cache + ~10 pinned) | ~512 MB (512 cache incl. ~345 pinned) | +246 MB |

### Backend Comparison: fjall vs LMDB/heed

| Component | fjall (current) | fjall (tuned) | LMDB/heed (estimated) |
|---|---|---|---|
| DB open | ~1-2s | ~115ms | ~0ms (mmap) |
| Novel variant rejection (250K) | ~15-25s | ~50ms | ~75ms (B+ tree, no bloom) |
| Data block I/O (~3.7M blocks) | ~15-20s | ~8-12s | ~5-8s (mmap + OS read-ahead) |
| In-block lookups (cached) | ~5-8s | ~2-4s | ~1-2s (mmap page cache) |
| With prefetch + look-ahead | — | ~7-12s | ~3-6s |
| **Total (end-to-end)** | **~15-30s** | **~10-15s / ~7-12s** | **~5-8s / ~3-6s** |
| Memory | ~512 MB | ~512 MB | ~0 user-space (OS page cache) |

### Detailed Breakdown: Where Time Is Saved

Workload profile for 5M-variant WGS: ~95% known variants (4.75M hits), ~5% novel variants (250K misses), ~3.7M distinct 8 KiB data blocks touched (~29 GB). DB contains ~8.7M total blocks across ~74 GB (10B keys, 1.09B unique starts).

#### 1. DB Open + Filter/Index Loading (~1-2s saved)

**Current:** Fjall lazily loads filter/index blocks on first access. With 7 LSM levels and 99%+ of data at L5-L6, the first lookup reaching each level triggers a disk read for its filter block + index block. These cache misses accumulate across the first few thousand lookups.

**Tuned:** Pinning policies preload all ~345 MB of filter+index blocks at open time. On NVMe at 3 GB/s that is ~115 ms upfront, then zero filter/index cache misses for the entire run.

#### 2. Novel Variant Rejection (~15-25s saved)

**Current:** Each of the ~250K novel variant lookups must load filter blocks from disk per-level before the bloom filter can reject it. Even though the bloom filter eventually rejects the lookup, the disk I/O to load the filter block dominates: ~50-100 us per lookup per unpinned level. With most data at L5-L6, this means 4-5 unpinned levels per lookup.

**Tuned:** Pinned bloom filters are already in RAM. A miss is just a hash computation + memory lookup per level (~100-200 ns total). At 0.01% FPR, virtually no false positives leak through to trigger unnecessary data block reads.

**Aggregate:** ~250K misses x ~80 us saved = **~20 seconds saved**. This remains the single largest improvement — pinning eliminates the per-level disk I/O that dominates novel variant rejection.

#### 3. Data Block I/O -- the new dominant cost (~5-8s saved)

**Current:** Each first-access known variant performs 3 disk reads: filter block + index block + data block. With ~3.7M distinct blocks touched, that is ~3.7M x 3 = ~11M disk reads.

**Tuned:** Filter + index are pinned, so only 1 disk read remains per block. The 8 KiB block size packs ~126 keys/block (10B keys), and with ~1 WGS variant per ~600 bp vs ~347 bp block span, ~42% of all 8.7M blocks are touched (~3.7M blocks, ~29 GB). With sorted VCF access this is quasi-sequential — OS read-ahead on NVMe yields ~3 GB/s effective throughput: **~8-12s**.

**With prefetch (Decisions 10-11):** Chromosome warmup + look-ahead can overlap ~30-40% of I/O with computation, reducing effective data block time to **~5-8s**.

**Note:** At 1.15B positions, data block I/O replaces novel variant rejection as the dominant cost once bloom/index blocks are pinned. This is why Decisions 10-11 (prefetch) are critical.

#### 4. In-Block Lookups on Cached Blocks (~2-4s saved)

**Current:** Binary search within cached 4 KiB blocks (~63 keys with 10B keys, ~6 comparisons per lookup, ~1-3 us).

**Tuned:** Hash index (`ratio=0.75`) gives O(1) in-block lookup (~0.5-1 us). Larger 8 KiB blocks (~126 keys) mean more consecutive VCF positions share the same cached block, so a higher fraction of the 4.75M known-variant lookups benefit from the hash index on already-cached blocks.

**Aggregate:** modest per-lookup improvement but across millions of cached hits: **~2-4 seconds saved**

#### 5. `expect_point_read_hits=true` (saves ~1.7 GB RAM, costs +250 ms)

Skips building bloom filters on the last level, which holds ~90% of all filter data (~1.7 GB at 1.15B positions). The 2-5% of lookups that miss and reach L6 without a bloom filter must read a data block. However, since VCF is sorted and 95% of lookups are hits, most L6 data blocks are already fetched for neighboring known variants. Only blocks containing exclusively novel variants incur real extra I/O: estimated ~5K extra reads x 50 us = **+250 ms** — negligible vs the ~1.7 GB memory saved.

#### End-to-End Aggregate

| Component | Current | Tuned | Tuned + Prefetch | Saving |
|---|---|---|---|---|
| DB open + block loading | ~1-2s | ~115 ms | ~115 ms | ~1-2s |
| Novel variant rejection (250K) | ~15-25s | ~50 ms | ~50 ms | **~15-25s** |
| Data block I/O (~3.7M blocks) | ~15-20s | ~8-12s | ~5-8s | **~7-15s** |
| In-block lookups (cached) | ~5-8s | ~2-4s | ~2-4s | ~2-4s |
| `expect_hits` penalty | 0 | +250 ms | +250 ms | -250 ms |
| **Total (end-to-end)** | **~15-30s** | **~10-15s** | **~7-12s** | **1.5-3x** |

After pinning, **data block I/O becomes the dominant cost**. The ~3.7M distinct blocks (~29 GB) accessed during a 5M-variant WGS annotation are the irreducible I/O floor. Prefetch/look-ahead (Decisions 10-11) can overlap ~30-40% of this with computation, making them critical for the final performance target. LMDB/heed's mmap approach is particularly well-suited here — the OS manages page cache readahead natively.

### Build Time (One-Time Cost)

`start_ingestion()` for sorted bulk load: **~30 min to ~5-10 min** (3-6x) for the initial load step. Because stock fjall 3.0.2 still depends on subsequent compaction to reach the final steady-state layout, end-to-end build time must include that phase explicitly.

---

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|---|---|---|
| Pinned blocks consume ~345 MB of block cache | Less room for data block caching; OOM on low-memory machines | Configurable via `bio.annotation.cache_size_mb`; can disable pinning with custom `KeyspaceCreateOptions`. Data block caching is low-value for sequential WGS anyway. |
| `expect_point_read_hits=true` drops L6 bloom filter (~1.7 GB) | +250 ms for 5M-variant WGS (novel variants share data blocks with hits) | Configurable via `bio.annotation.expect_hits`; without it, need ~2.1 GB total for bloom filters |
| DB on disk is ~74 GB (10B keys) / ~84 GB (18B keys) | 2.5x the 30 GB Parquet source | Acceptable for indexed point-lookup access pattern; position-only keys save ~10 GB |
| Metadata partitioning makes "pin all metadata" ineffective | Hidden cold-start I/O remains | Disable filter/index partitioning in the cold-start profile before pinning |
| `start_ingestion()` requires globally sorted input | Must sort or process per-chromosome | Variation cache is already sorted by `(chrom, position)`; process chromosomes sequentially |
| `major_compact()` in stock fjall 3.0.2 hardcodes 64 MB target size | Can silently undo tuned table-size choices | Treat it as a measured, conditional step; do not assume it honors the configured leveled strategy |
| 8 KiB blocks waste more on random access | Negligible -- VCF is always sorted | Could be made configurable if needed |
| Hash ratio 0.75 increases in-memory block size | Bounded by block cache capacity | Only affects cached blocks; no disk space overhead |
| KV separation can add a second point-read hop | Slower point lookups for medium/small values | Keep it off by default and gate it on measured compressed value sizes |
| Fjall 3.x API changes | Update needed for new major versions | Pin to exact version; existing DB format is forward-compatible within fjall 3.x |

## Open Questions

- Whether `start_ingestion()` can handle the current multi-partition streaming loader or requires a single-threaded sorted writer
- Optimal `RestartIntervalPolicy` for 8 KiB blocks with 10-byte keys — benchmark sweep is 4/8/16 (see Decision 14; direction is *down* from default 16 to create more hash buckets)
- Whether to expose `data_block_size_policy` as a session config or keep it as a build-time constant
- Optimal look-ahead buffer size (N=64 is a starting point; may need tuning per storage medium)
- Whether value-size histograms justify enabling `with_kv_separation()` for any build profile
- Whether LMDB `MDB_APPEND` bulk loading can match or exceed fjall's `start_ingestion()` throughput

### Resolved Questions

- **Warmup step:** Yes — Decision 10 adds `warmup(chromosomes)` method to the backend trait (chromosome prefetch via `fadvise`/`madvise`)
- **Key encoding change:** Yes — Decision 9 reduces keys from 18 to 10 bytes (position-only, end stored in value)

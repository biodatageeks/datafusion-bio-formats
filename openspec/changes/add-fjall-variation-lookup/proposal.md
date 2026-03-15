# Add Fjall-Backed Fast Variant Lookup

## Why

The current parquet-backed `lookup_variants()` path expands to a generic SQL interval join plus a row-wise `match_allele(ref, alt, allele_string)` post-filter. That means planner-dependent build/probe orientation, full-column streaming from Parquet, and repeated allele normalization even when ~95-98% of input variants in a typical WGS/WES analysis are exact cache hits that should be resolved via O(1) point lookup. This is a point-lookup problem masquerading as a range-join.

An existing fjall-backed KV cache in `bio-function-vep` (`kv_cache/` module) already solves this with position-keyed entries, zstd dictionary compression, and allele matching -- but uses fjall's default configuration, and the current OpenSpec overstates a few stock fjall 3.x behaviors (notably metadata pinning, `start_ingestion()`, and `major_compact()`). The proposal needs to be tightened before implementation.

This proposal focuses on **fjall tuning and ingest optimizations** for the existing position-keyed architecture, targeting the primary workload: open DB, annotate one VCF sample (4-5M variants), close.

## What Changes

### Fjall Configuration Tuning (cold-start optimized)
- **Coordinate metadata partitioning with pinning** -- disable partitioned filter/index blocks for the cold-start DB profile so fjall 3.x can actually keep the full metadata resident instead of only the top-level structures
- **Pin all bloom filter and index blocks** in cache -- eliminates the most expensive cold-start disk reads once partitioning is disabled
- **Enable `expect_point_read_hits`** -- skip last-level bloom filters since 95-98% of lookups are hits, saving ~1.7 GB memory
- **Enable data block hash index** (`hash_ratio = 0.75`) -- roughly 0.75 buckets per item, giving a compact in-block accelerator for repeated point reads
- **Increase data block size to 8 KiB** -- fewer I/O operations for sorted VCF access pattern
- **Benchmark restart interval** (`4` vs `8` vs `16`) -- lower restart intervals create more restart points and thus more hash buckets (Decision 3), improving in-block hash index effectiveness from ~21 keys/bucket (interval=16) down to ~5 keys/bucket (interval=4)
- **Tighter bloom filter FPR** (0.01% on all levels) -- better novel-variant rejection on upper levels
- **Session-configurable block cache** (default 512 MB) -- sized for cold-start working set

### Ingest Pipeline Optimization
- **Use `Keyspace::start_ingestion()`** for sorted bulk load -- bypasses memtable/journal and is still materially faster than batch insert, but stock fjall 3.x still relies on compaction for the final read shape
- **Use target-aware post-ingest compaction** -- do not blindly rely on `major_compact()`, because fjall 3.0.x hardcodes a 64 MB target there regardless of the configured strategy
- **Zstd dictionary training** from sample data (already implemented)

### Backend Abstraction & Encoding Optimizations
- **Pluggable backend trait** (`VepKvBackend`) enabling fjall and LMDB/heed backends -- fjall stays default (pure Rust, all platforms), LMDB opt-in via `lmdb-backend` feature flag for maximum cold-start performance
- **Explicit lookup backend selection** (`parquet`, `fjall`, `hybrid`, `auto`) -- preserve the existing parquet SQL path and allow correctness-preserving fallback for misses or unsupported cases
- **10-byte position-only keys** reducing key storage by ~44% (18B → 10B) -- end coordinate stored in value since 95% of variants are SNVs
- **Keep KV separation off by default** -- only enable fjall value-log mode if measured compressed `PositionEntry` sizes are large enough that lower write amplification outweighs the extra point-read hop
- **Chromosome warmup/prefetch** for overlapped I/O -- backend-specific: fjall uses `prefix()` iterator to load into block cache directly; LMDB uses `madvise` on mapped pages
- **Pipelined I/O look-ahead** for data block prefetch -- look ahead at next 64 keys, prefetch predicted data blocks
- **SST target size tuning** (`table_target_size = 256 MiB`) -- reduces L6 SST count from ~1,156 to ~289, avoiding file descriptor exhaustion and improving cold-start metadata loading
- **Projection-aware value deserialization** -- skip unused column parsing after decompression when only a subset of 78 columns is requested (~0.5-2s saved)
- **Observability metrics** (`VEP_KV_PROFILE`) -- structured JSON output of hit/miss rates, latency breakdown, and per-chromosome warmup timing via atomic counters (~zero overhead when disabled)
- **Per-chromosome parallel partitions** in `KvLookupExec` -- multiple partitions share `Arc<dyn VepKvBackend>` for I/O parallelism on NVMe (~1.5-2x)
- **Early-exit allele matching** -- check allele table before full column deserialization in Exact match mode

### Crate Location
The fjall KV cache lives in `bio-function-vep` (not `bio-format-ensembl-cache`) since it depends on VEP-specific allele matching logic and is a materialized view optimized for the lookup operation, not a file format provider.

## Impact

- Affected specs: **NEW** `fjall-variation-lookup` capability
- Affected code:
  - `datafusion/bio-function-vep/src/kv_cache/kv_store.rs` -- updated `KeyspaceCreateOptions` for cold-start tuning
  - `datafusion/bio-function-vep/src/kv_cache/loader.rs` -- use `start_ingestion()` for sorted bulk load
  - `datafusion/bio-function-vep/src/kv_cache/cache_exec.rs` -- unchanged (already implements extended probes + allele matching)
  - `datafusion/bio-function-vep/src/config.rs` -- add cache tuning options to `AnnotationConfig`
  - `datafusion/bio-function-vep/src/lookup_provider.rs` -- retained as parquet fallback path for `lookup_backend=parquet|hybrid|auto`
  - `datafusion/bio-function-vep/Cargo.toml` -- fjall 3.x dependency (already present)
  - `datafusion/bio-function-vep/src/kv_cache/mod.rs` -- `VepKvBackend` trait definition
  - `datafusion/bio-function-vep/src/kv_cache/fjall_backend.rs` -- extracted fjall backend implementing trait
  - `datafusion/bio-function-vep/src/kv_cache/lmdb_backend.rs` -- LMDB/heed backend (feature-gated under `lmdb-backend`)
  - `datafusion/bio-function-vep/src/kv_cache/key_encoding.rs` -- 10-byte position-only key encoding
- `heed` added as optional dependency under `lmdb-backend` feature
- No breaking changes -- existing KV cache API is preserved; tuning is applied at DB creation time
- **Relationships:**
  - Complements `add-vep-annotation`: optimizes the `lookup_variants()` KV backend
  - Complements `add-vortex-cache-format`: Vortex optimizes bulk scans; fjall optimizes point lookups
  - Both are acceleration layers behind the same variation schema contract

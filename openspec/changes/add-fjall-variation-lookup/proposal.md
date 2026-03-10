# Add Fjall-Backed Fast Variant Lookup

## Why

The current `lookup_variants()` implementation streams the entire 30 GB variation cache (1.17B rows, 78 columns) through `IntervalJoinExec`, scanning every row group even when ~95-98% of input variants in a typical WGS/WES analysis already exist in the cache and could be resolved via O(1) point lookup. This is a point-lookup problem masquerading as a range-join. An existing fjall-backed KV cache in `bio-function-vep` (`kv_cache/` module) already solves this with position-keyed entries, zstd dictionary compression, and allele matching -- but uses fjall's default configuration, leaving significant performance on the table, especially for cold-start single-sample annotation.

This proposal focuses on **fjall tuning and ingest optimizations** for the existing position-keyed architecture, targeting the primary workload: open DB, annotate one VCF sample (4-5M variants), close.

## What Changes

### Fjall Configuration Tuning (cold-start optimized)
- **Pin all bloom filter and index blocks** in cache -- eliminates the most expensive cold-start disk reads
- **Enable `expect_point_read_hits`** -- skip last-level bloom filters since 95-98% of lookups are hits, saving ~340 MB memory
- **Enable data block hash index** (`hash_ratio = 0.75`) -- O(1) within-block lookups vs O(log N) binary search
- **Increase data block size to 8 KiB** -- fewer I/O operations for sorted VCF access pattern
- **Tighter bloom filter FPR** (0.01% on all levels) -- better novel-variant rejection on upper levels
- **Session-configurable block cache** (default 512 MB) -- sized for cold-start working set

### Ingest Pipeline Optimization
- **Use `Keyspace::start_ingestion()`** for sorted bulk load -- bypasses memtable/journal/compaction, 3-10x faster
- **Post-ingest major compaction** -- consolidates SST files for minimal bloom filter checks
- **Zstd dictionary training** from sample data (already implemented)

### Crate Location
The fjall KV cache lives in `bio-function-vep` (not `bio-format-ensembl-cache`) since it depends on VEP-specific allele matching logic and is a materialized view optimized for the lookup operation, not a file format provider.

## Impact

- Affected specs: **NEW** `fjall-variation-lookup` capability
- Affected code:
  - `datafusion/bio-function-vep/src/kv_cache/kv_store.rs` -- updated `KeyspaceCreateOptions` for cold-start tuning
  - `datafusion/bio-function-vep/src/kv_cache/loader.rs` -- use `start_ingestion()` for sorted bulk load
  - `datafusion/bio-function-vep/src/kv_cache/cache_exec.rs` -- unchanged (already implements extended probes + allele matching)
  - `datafusion/bio-function-vep/src/config.rs` -- add cache tuning options to `AnnotationConfig`
  - `datafusion/bio-function-vep/Cargo.toml` -- fjall 3.x dependency (already present)
- No breaking changes -- existing KV cache API is preserved; tuning is applied at DB creation time
- **Relationships:**
  - Complements `add-vep-annotation`: optimizes the `lookup_variants()` KV backend
  - Complements `add-vortex-cache-format`: Vortex optimizes bulk scans; fjall optimizes point lookups
  - Both are acceleration layers behind the same variation schema contract

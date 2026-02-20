# datafusion-bio-format-ensembl-cache

DataFusion `TableProvider` implementations for raw Ensembl VEP cache directories.

## Supported entities

- `variation`
- `transcript`
- `regulatory_feature`
- `motif_feature`

## Notes

- Reads `.gz` files directly (no manual `gunzip` required).
- Uses streaming execution and emits batches incrementally.
- Supports file-level parallel reading (partitioned by source files).
- Supports native Storable (`pst0`) decoding for transcript/regulatory cache files.
- Supports configurable coordinate output:
  - `coordinate_system_zero_based = false` (default): 1-based closed `[start, end]`
  - `coordinate_system_zero_based = true`: 0-based half-open `[start, end)`
- Writes coordinate mode to Arrow schema metadata as
  `bio.coordinate_system_zero_based`.
- Supports fixture/test payload shortcuts for unit fixtures:
  - `storable`: `JSON:{...}`
  - `sereal`: `SRL1{...}`

## Parallel Scanning

- By default, parallelism follows DataFusion session `target_partitions`.
- You can override per table via `EnsemblCacheOptions::target_partitions`.
- Effective partition count is capped by discovered source file count.
- Gzip files are not split internally; parallelism is across files.

## Example: Entity-Specific Provider

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_bio_format_ensembl_cache::{EnsemblCacheOptions, VariationTableProvider};
use std::sync::Arc;

# async fn example() -> datafusion::common::Result<()> {
let ctx = SessionContext::new();
let mut options = EnsemblCacheOptions::new("/path/to/cache");
options.coordinate_system_zero_based = true;
options.target_partitions = Some(8);
let table = VariationTableProvider::new(options)?;
ctx.register_table("vep_variation", Arc::new(table))?;
# Ok(())
# }
```

## Example: Generic Provider Factory

Use `EnsemblCacheTableProvider::for_entity(...)` when you want one entrypoint
and switch by entity type (`variation`, `transcript`, etc.) yourself.

```rust,no_run
use datafusion::prelude::SessionContext;
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind,
};

fn parse_cache_type(cache_type: &str) -> Option<EnsemblEntityKind> {
    match cache_type.to_ascii_lowercase().as_str() {
        "variation" => Some(EnsemblEntityKind::Variation),
        "transcript" => Some(EnsemblEntityKind::Transcript),
        "regulatory_feature" => Some(EnsemblEntityKind::RegulatoryFeature),
        "motif_feature" => Some(EnsemblEntityKind::MotifFeature),
        _ => None,
    }
}

# async fn example() -> datafusion::common::Result<()> {
let cache_type = "variation";
let kind = parse_cache_type(cache_type).expect("unsupported cache type");

let table = EnsemblCacheTableProvider::for_entity(
    kind,
    EnsemblCacheOptions::new("/path/to/cache"),
)?;

let ctx = SessionContext::new();
ctx.register_table("vep_cache", table)?;
# Ok(())
# }
```

## Context
datafusion-bio-formats owns DataFusion table providers and physical execution plans used directly by polars-bio and datafusion-bio-functions. It must upgrade before downstream repositories can consume DataFusion 53.1.0.

## Goals / Non-Goals
- Goals: compile and test all format crates against DataFusion 53.1.0; preserve projection pushdown, predicate pushdown, write execution, VCF UDFs, VCF Zarr pruning, and cloud/local storage behavior.
- Non-Goals: introduce new format support, change public table provider constructors, change file format schemas, or change coordinate semantics.

## Decisions
- Keep the repository's current Rust toolchain because `rust-toolchain.toml` already uses `1.91.0`, which satisfies DataFusion 53.1.0's Rust 1.88 minimum.
- Let DataFusion 53.1.0 choose the Arrow/Parquet 58.3.0 line through Cargo resolution.
- Use DataFusion's default feature set minus `compression`; DataFusion 53's `compression` feature pulls `liblzma-sys`, which conflicts with `xz2/lzma-sys` from the forked `noodles-cram` dependency.
- Update custom `ExecutionPlan` implementations to store `Arc<PlanProperties>` rather than `PlanProperties`.
- Move plan statistics implementations from `statistics` to `partition_statistics`.

## Risks
- Physical plan display and statistics changes can alter `EXPLAIN` output.
- Arrow 58 can expose stricter type or builder behavior in VCF and FASTQ serializers.
- DataFusion-level compressed CSV/JSON scan support from the `compression` feature is not enabled; bio-format compression support remains owned by the format-specific readers.
- Object store dependencies can be pulled to newer transitive versions by DataFusion 53.

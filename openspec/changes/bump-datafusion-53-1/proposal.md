# Change: Bump DataFusion to 53.1.0

## Why
DataFusion 53.1.0 is the target query engine version for the bioinformatics stack. datafusion-bio-formats must move first because downstream crates consume its DataFusion table provider and execution plan types.

## What Changes
- Update workspace DataFusion dependencies from 50.3.0 to 53.1.0.
- Disable DataFusion's default `compression` feature to avoid a native `links = "lzma"` conflict with the forked CRAM dependency stack.
- Update code for DataFusion 53 execution plan API changes, including `Arc<PlanProperties>` and `partition_statistics`.
- Preserve table provider behavior for all supported formats: BAM, BED, CRAM, FASTA, FASTQ, GFF, GTF, pairs, VCF, VCF Zarr, and Ensembl cache.
- Keep Rust toolchain on the repository's current `rust-toolchain.toml` value, which already satisfies DataFusion 53.1.0's Rust 1.88 minimum.

## Impact
- Affected specs: dependencies
- Affected code: root `Cargo.toml`, `Cargo.lock`, format crate physical execution plans, table providers, VCF UDFs, CRAM dependency resolution, examples, and tests.

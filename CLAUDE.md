<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust workspace that provides bioinformatics file format support for Apache DataFusion. The project implements DataFusion table providers for various biological file formats including FASTQ, VCF, BAM, BED, GFF, and FASTA files. Each format is implemented as a separate crate in the `datafusion/` directory.

## Common Development Commands

### Build and Test
- `cargo build` - Build all workspace crates
- `cargo test` - Run all tests across the workspace
- `cargo fmt --all -- --check` - Check code formatting (used in CI)
- `cargo fmt --all` - Format all code
- `cargo clippy` - Run clippy linter

### Running Examples
Each format has example files in `datafusion/bio-format-{format}/examples/`:
- `cargo run --example test_reader --package datafusion-bio-format-fastq`
- `cargo run --example test_reader --package datafusion-bio-format-vcf`
- `cargo run --example performance_test --package datafusion-bio-format-fastq`

### Testing Individual Crates
- `cargo test --package datafusion-bio-format-fastq`
- `cargo test --package datafusion-bio-format-vcf`
- `cargo test --package datafusion-bio-format-core`

### Running Benchmarks
- `cargo build --release --package datafusion-bio-benchmarks-runner` - Build benchmark runner
- `./target/release/benchmark-runner benchmarks/configs/gff.yml` - Run GFF benchmarks
- `./target/release/benchmark-runner benchmarks/configs/gff.yml --output-dir my_results` - Run with custom output directory
- See `benchmarks/README.md` for full documentation on the benchmark framework

## Architecture

### Workspace Structure
- **bio-format-core**: Shared utilities including object storage support and table utilities
- **bio-format-fastq**: FASTQ file format support with BGZF parallel reading
- **bio-format-vcf**: VCF file format support
- **bio-format-bam**: BAM file format support
- **bio-format-bed**: BED file format support
- **bio-format-gff**: GFF file format support
- **bio-format-fasta**: FASTA file format support
- **benchmarks/**: Performance benchmark framework
  - **benchmarks/common**: Shared benchmark infrastructure (harness, data downloader)
  - **benchmarks/runner**: Generic benchmark runner binary
  - **benchmarks/configs**: YAML configuration files for each format
  - **benchmarks/python**: Report generation scripts

### Key Components
Each format crate follows a consistent pattern:
- `table_provider.rs`: Implements DataFusion's TableProvider trait
- `physical_exec.rs`: Implements the physical execution plan
- `storage.rs`: File reading and parsing logic
- `lib.rs`: Public API and module exports

### Object Storage Support
The `bio-format-core` crate provides cloud storage integration via OpenDAL with support for:
- Google Cloud Storage (GCS)
- Amazon S3 
- Azure Blob Storage
- Compression detection (GZIP, BGZF, AUTO)

### Key Dependencies
- DataFusion 50.3.0 - SQL query engine
- Noodles 0.93.0 - Bioinformatics file parsing
- OpenDAL 0.53.3 - Object storage abstraction
- Tokio - Async runtime

## Development Environment

- Rust toolchain: 1.85.0 (specified in rust-toolchain.toml)
- Rustfmt version requirement: 1.8.0
- The CI workflow runs formatting checks, so ensure code is properly formatted before committing

## File Format Schemas

### FASTQ Schema
- `name`: String (required) - Sequence identifier
- `description`: String (optional) - Sequence description  
- `sequence`: String (required) - DNA/RNA sequence
- `quality_scores`: String (required) - Quality scores

Each format crate defines its schema in the `determine_schema()` function within the table provider.
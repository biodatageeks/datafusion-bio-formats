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
- `cargo run --example write_bam --package datafusion-bio-format-bam` - BAM/SAM write examples
- `cargo run --example write_cram --package datafusion-bio-format-cram` - CRAM write examples

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
- `table_provider.rs`: Implements DataFusion's TableProvider trait (with `insert_into()` for write support)
- `physical_exec.rs`: Implements the physical execution plan for reads
- `storage.rs`: File reading and parsing logic
- `lib.rs`: Public API and module exports

#### Write Support (BAM, CRAM, VCF, FASTQ)
Some formats include write support with additional modules:
- `writer.rs`: File writer with compression support
- `write_exec.rs`: Physical execution plan for writes
- `serializer.rs`: Converts Arrow RecordBatch to format-specific records
- `header_builder.rs`: Reconstructs format headers from Arrow schema metadata

Write operations use SQL `INSERT OVERWRITE` syntax:
```sql
-- BAM/SAM write
INSERT OVERWRITE output_table SELECT * FROM input_table WHERE mapping_quality >= 30

-- CRAM write (requires reference sequence)
INSERT OVERWRITE cram_output SELECT * FROM bam_input WHERE chrom = 'chr1'
```

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

## Metadata Schema Conventions

For round-trip read/write operations, format-specific metadata is preserved in Arrow schema metadata:

### BAM/SAM Metadata Keys
**Schema-level** (in `schema.metadata()`):
- `bio.bam.file_format_version` - SAM format version (default: "1.6")
- `bio.bam.sort_order` - Sort order: "coordinate", "queryname", or "unsorted"
- `bio.bam.reference_sequences` - JSON array of reference sequences: `[{"name": "chr1", "length": 249250621}, ...]`
- `bio.bam.read_groups` - JSON array of read group metadata: `[{"id": "RG1", "sample": "sample1", ...}, ...]`
- `bio.bam.program_info` - JSON array of program records: `[{"id": "bwa", "name": "bwa", "version": "0.7.17"}, ...]`
- `bio.bam.comments` - JSON array of comment lines
- `bio.coordinate_system_zero_based` - "true" for 0-based (default), "false" for 1-based

**Field-level** (tag column metadata):
- `bio.bam.tag.tag` - SAM tag name (e.g., "NM", "MD", "AS")
- `bio.bam.tag.type` - SAM type: 'i' (int), 'Z' (string), 'f' (float), 'B' (array)
- `bio.bam.tag.description` - Human-readable tag description

### CRAM Metadata Keys
CRAM uses BAM metadata keys for common fields (reference sequences, read groups, programs, comments) to maintain consistency across alignment formats. Only CRAM-specific fields use the `bio.cram.*` prefix:
- `bio.cram.file_format_version` - CRAM version (3.0 or 3.1, overrides BAM version)
- `bio.cram.reference_path` - Path to reference FASTA file (CRAM-specific)
- `bio.cram.reference_md5` - Reference checksum (CRAM-specific, optional)

**Note:** CRAM reuses these BAM keys:
- `bio.bam.file_format_version` - Fallback if CRAM version not specified
- `bio.bam.reference_sequences` - Reference sequence definitions
- `bio.bam.read_groups` - Read group metadata
- `bio.bam.program_info` - Program records
- `bio.bam.comments` - Comment lines

### VCF/FASTQ Metadata Keys
Similar patterns are followed for other formats with write support. See respective `header_builder.rs` files for details.
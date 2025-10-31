# Project Context

## Purpose
Datafusion-bio-formats provides Apache DataFusion table providers for bioinformatics file formats, enabling SQL-based analysis of genomic data. The project supports querying FASTQ, VCF, BAM, BED, GFF, FASTA, and CRAM files using DataFusion's distributed query engine.
It is used by [polars-bio](https://github.com/biodatageeks/polars-bio) project 

## Tech Stack
- Rust 1.86.0 (specified in rust-toolchain.toml)
- Apache DataFusion 50.3.0 (SQL query engine)
- Noodles 0.93.0 (bioinformatics file parsing library)
- OpenDAL 0.53.3 (object storage abstraction for GCS, S3, Azure)
- Tokio 1.43.0 (async runtime)
- BGZF compression support via noodles-bgzf with libdeflate

## Project Conventions

### Code Style
- Use cargo fmt for all code formatting (rustfmt 1.8.0+)
- Formatting is enforced in CI via `cargo fmt --all -- --check`
- Run `cargo clippy` to catch common mistakes
- Follow standard Rust naming conventions:
  - Snake_case for functions and variables
  - PascalCase for types and traits
  - SCREAMING_SNAKE_CASE for constants
- Keep modules organized: table_provider.rs, physical_exec.rs, storage.rs pattern

### Architecture Patterns
- Each bioinformatics format is implemented as a separate crate in `datafusion/bio-format-{format}/`
- Shared utilities are in `datafusion/bio-format-core/` including:
  - Object storage support via OpenDAL
  - Common table utilities
  - Compression detection (GZIP, BGZF, AUTO)
- Consistent pattern across format crates:
  - `table_provider.rs`: Implements DataFusion's TableProvider trait
  - `physical_exec.rs`: Physical execution plan implementation
  - `storage.rs`: File reading and parsing logic
  - `lib.rs`: Public API exports
- Schema definition in `determine_schema()` function within table provider
- Support for both local files and cloud storage (GCS, S3, Azure)

### Testing Strategy
- Unit tests in each crate under `#[cfg(test)]` modules
- Integration tests in `tests/` directories
- Example programs in `examples/` for manual testing
- CI runs `cargo test` across all workspace members
- Test files stored in crate-specific test directories
- Performance benchmarks in dedicated binary targets

### Git Workflow
- Main branch: `master`
- CI runs on pushes to `main` and all pull requests
- Format checks must pass before merge
- All tests must pass in CI
- Standard PR workflow for feature additions

## Domain Context

### Bioinformatics File Formats
- **FASTQ**: DNA sequencing reads with quality scores (name, description, sequence, quality_scores)
- **VCF**: Variant Call Format for genetic variations (case-sensitive INFO and FORMAT fields)
- **BAM/CRAM**: Binary/compressed alignment format for mapped sequencing reads
- **BED**: Browser Extensible Data for genomic regions
- **GFF**: General Feature Format for genome annotations (attributes parsing critical)
- **FASTA**: Simple sequence format (header + sequence)

### BGZF Compression
- Block-level GZIP compression allowing random access
- Parallel decompression support for performance
- Used extensively in BAM, VCF.gz, and FASTQ.gz files

### Cloud Storage Integration
- Support for accessing genomic data from cloud object stores
- Important for large-scale genomic datasets (often terabytes)
- OpenDAL provides unified interface across providers

## Important Constraints

### Performance
- Genomic files can be very large (multi-GB to TB scale)
- Parallel processing is critical for acceptable performance
- BGZF parallel decompression must be efficient
- Memory usage must be controlled for streaming operations

### Data Integrity
- Case sensitivity matters in VCF INFO/FORMAT fields
- Attribute parsing in GFF must preserve exact field names and values
- Quality scores and sequences must be preserved exactly
- Coordinate systems vary by format (0-based vs 1-based)

### Compatibility
- Must work with standard bioinformatics tools (samtools, bcftools, etc.)
- File format compliance with official specifications
- Support for indexed files (BAI, TBI)

### Rust Version
- Locked to Rust 1.85.0 via rust-toolchain.toml
- Rustfmt component required

## External Dependencies

### Core Libraries
- **Apache DataFusion**: SQL query engine and execution framework
- **Noodles**: Bioinformatics file format parsing (some crates use forked version from biodatageeks/noodles)
- **OpenDAL**: Object storage abstraction layer

### Cloud Providers
- Google Cloud Storage (GCS)
- Amazon S3
- Azure Blob Storage
- HTTP sources

### Compression
- libdeflate via noodles-bgzf for fast BGZF decompression
- async-compression for streaming compression support

### Custom Forks
- noodles-cram, noodles-sam, noodles-fasta use custom fork:
  - Repository: https://github.com/biodatageeks/noodles.git
  - Revision: d6f205e9bb5369762236724fb54d24c5e1d9b79a (for CRAM)
  - Revision: d6f205e9bb5369762236724fb54d24c5e1d9b79a (for SAM/FASTA)

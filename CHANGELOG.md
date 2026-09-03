# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- PGEN text companions are now streamed: the `.pvar` (plain, gzip, or zstd) is
  decoded and parsed in bounded, newline-aligned blocks on a worker pool, and
  the parsed variants live in a columnar `PvarTable` (interned contigs,
  positions, and one byte arena for IDs and alleles) instead of one heap
  struct per row. Transient memory no longer grows with the companion, and
  the resident table costs tens of bytes per variant rather than hundreds.
- PGEN `PgenReadOptions` defaults: `max_companion_bytes` 512 MiB → 4 GiB,
  `max_decompressed_companion_bytes` 1 GiB → 16 GiB, `max_variants`
  100M → 250M, so the published PGS Catalog 1000 Genomes panels
  (`pgsc_1000G_v1`, 75–85M variants, 541–592 MiB `.pvar.zst`) open with
  default options. `max_variants` above `u32::MAX` is rejected.
- PGEN scans represent their variant selection compactly (`All`, `Range`,
  or `u32` indices) instead of a `Vec<usize>` of every selected row.

### Fixed

- PGEN companion limit errors name the option to raise
  (`max_companion_bytes`, `max_decompressed_companion_bytes`) and its
  configured value, as `max_variants` and `max_samples` already did
  (biodatageeks/polars-bio#453).
- FASTQ gzip reader now decodes all members of multi-member (concatenated /
  block) gzip files (pigz, bgzip-as-gzip, fastp, etc.). Previously only the first
  gzip member was read, causing silent truncation or an `UnexpectedEof` crash
  depending on where the member boundary fell. Covers local and remote gz paths.
- FASTA, VCF, GFF, GTF, BED, and Pairs readers now also decode all members of
  multi-member gzip files (follow-up to the FASTQ fix). The async local gz
  readers (FASTA/VCF/GFF/BED) use `gzip_multi_member_decoder` and the sync gz
  readers (GFF/GTF/Pairs) use `flate2::read::MultiGzDecoder`. As part of this,
  BED's local and remote readers gained the previously-missing (and unwired)
  GZIP branch, and the Pairs header reader now distinguishes plain gzip from
  BGZF (by the BGZF `BC` extra subfield) so plain-gzip Pairs files load
  correctly. Each format has multi-member gzip regression tests.

### Added

- `gzip_multi_member_decoder` helper in `bio-format-core` for multi-member gzip decoding
- CRAM file format support with reference-based compression
- FASTA file format support for biological sequences
- GFF file format support for genome annotations
- BED file format support for genomic intervals
- BAM file format support for sequence alignments
- VCF file format support for genetic variants with case-sensitive INFO/FORMAT fields
- FASTQ file format support with parallel BGZF reading
- Ensembl VEP cache source metadata for explicit `ensembl`, `merged`, and
  `refseq` cache modes, including RefSeq cache schema support
- Core utilities crate with object storage support (GCS, S3, Azure)
- Comprehensive documentation for all crates with usage examples
- CI workflow with formatting, linting, documentation, and testing checks
- Dependabot configuration for automated dependency updates
- Apache-2.0 licensing
- Workspace metadata inheritance across all crates

### Changed

- Upgraded DataFusion to version 50.3.0
- **BREAKING**: `datafusion-bio-format-ensembl-cache` table providers now
  require an explicit `EnsemblCacheOptions::with_cache_source_type(...)` value,
  and `translation_core_schema` / `translation_sift_schema` require a
  `CacheSourceType` argument so generated schemas carry cache source metadata
- Enhanced README with badges, quick start examples, and development instructions
- Improved crate-level documentation with `#![warn(missing_docs)]` lint

### Fixed

- Preserved case sensitivity for VCF INFO and FORMAT fields
- GTF `attr_fields` now concatenates duplicate attribute keys with commas instead of silently dropping subsequent values (#164)

## [0.1.0] - 2025-01-XX

### Added

- Initial release of datafusion-bio-formats workspace
- Support for 8 bioinformatics file formats (FASTQ, VCF, BAM, BED, GFF, FASTA, CRAM)
- Integration with Apache DataFusion query engine
- Cloud storage support via OpenDAL
- BGZF parallel reading for compressed genomic files

[Unreleased]: https://github.com/biodatageeks/datafusion-bio-formats/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/biodatageeks/datafusion-bio-formats/releases/tag/v0.1.0

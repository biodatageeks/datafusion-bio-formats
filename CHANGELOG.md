# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- CRAM file format support with reference-based compression
- FASTA file format support for biological sequences
- GFF file format support for genome annotations
- BED file format support for genomic intervals
- BAM file format support for sequence alignments
- VCF file format support for genetic variants with case-sensitive INFO/FORMAT fields
- FASTQ file format support with parallel BGZF reading
- Core utilities crate with object storage support (GCS, S3, Azure)
- Comprehensive documentation for all crates with usage examples
- CI workflow with formatting, linting, documentation, and testing checks
- Dependabot configuration for automated dependency updates
- Apache-2.0 licensing
- Workspace metadata inheritance across all crates

### Changed

- Upgraded DataFusion to version 50.3.0
- Enhanced README with badges, quick start examples, and development instructions
- Improved crate-level documentation with `#![warn(missing_docs)]` lint

### Fixed

- Preserved case sensitivity for VCF INFO and FORMAT fields

## [0.1.0] - 2025-01-XX

### Added

- Initial release of datafusion-bio-formats workspace
- Support for 8 bioinformatics file formats (FASTQ, VCF, BAM, BED, GFF, FASTA, CRAM)
- Integration with Apache DataFusion query engine
- Cloud storage support via OpenDAL
- BGZF parallel reading for compressed genomic files

[Unreleased]: https://github.com/biodatageeks/datafusion-bio-formats/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/biodatageeks/datafusion-bio-formats/releases/tag/v0.1.0

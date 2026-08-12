# Add Genotype Format Providers

## Why

Population genotype datasets are commonly distributed as BCF, PLINK 1
BED/BIM/FAM, BGEN, PLINK 2 PGEN/PVAR/PSAM, or GRG files. The project can query
VCF today, but users must convert these other formats before using DataFusion.
Conversion is expensive, can discard format-native allele or probability
semantics, and prevents the scan from exploiting native indexes, fixed-width
records, sample selection, and metadata-only queries.

The formats have materially different physical layouts and genotype models.
A useful implementation therefore needs a shared query contract without
pretending that all allele definitions are VCF `REF`/`ALT`, and it needs
format-specific pushdowns that avoid reading or decoding genotype payloads
when a query does not need them.

## What Changes

- Add a shared, read-only genotype-provider contract for variant-major Arrow
  batches, sample selection, genotype field selection, coordinate conversion,
  projection pruning, filter exactness, limit handling, partition planning,
  companion files, object storage, metrics, and bounded memory.
- Add BCF 2.2 input to the existing VCF crate while preserving the current VCF
  logical schema and FORMAT-field behavior by default. Add an explicit
  biallelic GT-dosage output mode that changes only the requested GT child type.
- Add a PLINK 1 BED/BIM/FAM provider with explicit `A1`/`A2` semantics,
  fixed-offset variant reads, exact BIM pruning, and selected-sample decoding.
- Add a BGEN 1.2/1.3 provider supporting Layout 1 compatibility, full Layout 2
  probability semantics, BGI pruning, phased or unphased data, variable
  ploidy, multiallelic variants, and optional biallelic dosage output.
- Add a PLINK 2 PGEN/PVAR/PSAM provider supporting standard header modes,
  hardcalls, phase, biallelic dosage, phased dosage, multiallelic hardcalls,
  PVAR pruning, and LD-dependency-aware partitioning.
- Add a gated, read-only GRG mutation view with haplotype and fixed-ploidy
  individual output modes, mutation and sample pruning, graph-aware traversal,
  and an explicit local-file scope for the first implementation.
- Define canonical format specifications and independent compatibility
  oracles. `snputils`, `pgenlib`, and `grgl` may be used for differential
  testing, but are not the physical implementation design.
- Stage delivery so each format capability can be implemented, benchmarked,
  and approved independently after the shared contract is established.

This proposal does not change the default existing VCF schema, add writers,
perform automatic format conversion, or add genotype association algorithms.

## Impact

- Affected specs:
  - `genotype-provider-core` (new)
  - `bcf` (new)
  - `plink1` (new)
  - `bgen` (new)
  - `pgen` (new)
  - `grg` (new)
- Expected affected code:
  - `datafusion/bio-format-core`
  - `datafusion/bio-format-vcf`
  - new format crates under `datafusion/bio-format-*`
  - workspace dependency and feature configuration
  - format examples, fixtures, conformance tests, and benchmarks
- API compatibility:
  - Existing VCF APIs and schemas remain compatible.
  - New genotype output modes are additive, explicit, and read-only.
- Operational impact:
  - Indexed and fixed-offset scans may issue object-store range requests.
  - Remote SQLite BGI files require a bounded local cache.
  - Parallel scans do not guarantee global input order without an explicit
    DataFusion sort.
- Licensing:
  - Apache-2.0/MIT Rust libraries may be considered as runtime dependencies.
  - LGPL `pgenlib` and GPL `grgl` are test or behavioral references only unless
    a separate licensing review explicitly approves another integration.

## Delivery Order

1. Shared genotype-provider contract and storage/index infrastructure.
2. BCF, reusing the current VCF logical and execution model.
3. PLINK 1, using its simple fixed-width variant-major payload.
4. BGEN, including BGI and probability-preserving output.
5. PGEN, including header modes and LD-compression dependencies.
6. GRG, after its compatibility and licensing gates are satisfied.

Implementation SHALL NOT start until this proposal and its per-capability
requirements are approved.

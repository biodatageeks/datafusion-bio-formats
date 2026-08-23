# Genotype Format Provider Implementation Tasks

## 1. Approval And Baselines

- [ ] 1.1 Approve the shared variant-major schema and nested sample contract.
- [ ] 1.2 Approve format-native allele naming and counted-allele metadata.
- [ ] 1.3 Approve default strict missing-sample behavior.
- [ ] 1.4 Record runtime dependency and license decisions for every new crate.
- [ ] 1.5 Reconfirm the pinned normative specification and compatibility-oracle
  versions before decoder implementation begins.
- [ ] 1.6 Capture current VCF schema, pushdown, memory, and throughput baselines.
- [ ] 1.7 Add a feature support matrix to user documentation.

## 2. Shared Genotype Provider Core

- [ ] 2.1 Define internal genotype provider, planner, partition, and decoder
  contracts in `bio-format-core`.
- [ ] 2.2 Define shared coordinate-system conversion and site-span helpers.
- [ ] 2.3 Define sample selection, duplicate handling, and missing-sample policy.
- [ ] 2.4 Define genotype-field projection and hidden filter dependencies.
- [ ] 2.5 Define Arrow metadata keys for sample names, allele counting, state
  order, format version, and output mode.
- [ ] 2.6 Extend genomic filter extraction with identifier equality/`IN` where
  supported without regressing existing providers.
- [ ] 2.7 Implement exact, inexact, unsupported, and unsatisfiable predicate
  planning helpers.
- [ ] 2.8 Implement safe pushed-limit planning around exact and residual filters.
- [ ] 2.9 Implement OpenDAL-aware companion discovery with sanitized errors.
- [ ] 2.10 Implement checked object identity and file-set consistency helpers.
- [ ] 2.11 Implement range coalescing with maximum gap and size thresholds.
- [ ] 2.12 Implement byte-weighted partition balancing for sparse ranges.
- [ ] 2.13 Implement session row and genotype-byte batch limit helpers.
- [ ] 2.14 Implement checked allocation limits for file-declared dimensions.
- [ ] 2.15 Define genotype scan metrics and attach them to execution plans.
- [ ] 2.16 Implement a bounded, identity-aware remote companion cache suitable
  for SQLite BGI.
- [ ] 2.17 Ensure concurrent cache opens coalesce downloads and active leases
  survive eviction attempts.
- [ ] 2.18 Add shared conformance helpers for projection, filter, limit, order,
  empty selection, sample order, and metrics.
- [ ] 2.19 Add property tests for coordinate conversion, range coalescing,
  partition ownership, and checked offset arithmetic.
- [ ] 2.20 Document that DataFusion owns outer concurrency and parallel output
  has no implicit global order.

## 3. BCF Provider

- [x] 3.1 Add BCF input detection and explicit format selection to the VCF crate.
- [x] 3.2 Parse and validate BCF 2.2 headers and string dictionaries.
- [x] 3.3 Map BCF records to the existing VCF logical schema without API changes.
- [x] 3.4 Preserve scalar/vector missing values, vector-end markers, phase,
  ploidy, multiallelic alleles, and case-sensitive INFO/FORMAT names.
- [x] 3.5 Add streaming BGZF BCF decoding with reusable record and value buffers.
- [x] 3.6 Apply INFO, FORMAT, and sample projection before Arrow conversion.
- [x] 3.7 Discover explicit and conventional CSI companions locally and through
  the configured object store.
- [x] 3.8 Plan CSI regions, merge overlapping chunks, and balance compressed
  byte ranges across target partitions.
- [x] 3.9 Apply record-level validation after inexact CSI pruning and remove
  duplicate chunk records.
- [x] 3.10 Add a one-partition sequential fallback for unindexed BCF.
- [x] 3.11 Support local, HTTP, S3, GCS, and Azure range reads through existing
  storage abstractions.
- [ ] 3.12 Add BCF corruption, truncation, invalid dictionary, and allocation
  limit tests.
- [x] 3.13 Add differential tests against htslib/bcftools and `noodles-bcf`.
- [x] 3.14 Add BCF-to-VCF logical equivalence fixtures for INFO/FORMAT schemas.
- [ ] 3.15 Benchmark sequential, indexed-region, metadata-only, sparse-sample,
  and parallel scans.
- [x] 3.16 Add an explicit BCF genotype output mode while preserving string GT
  and the current schema as the default.
- [x] 3.17 Refactor BCF FORMAT parsing into borrowed validated series views and
  an extensible projected typed-sink dispatch.
- [x] 3.18 Implement fused biallelic GT validation and direct nullable `Int8`
  ALT-dosage construction without intermediate genotype strings.
- [x] 3.19 Cover dosage missingness, phase independence, sample selection,
  encoded integer widths, dosage overflow independently of ploidy, and
  multiallelic rejection.
- [x] 3.20 Expose BCF dosage mode through polars-bio and verify complete output
  equivalence against the independent snputils result.
- [x] 3.21 On the representative public cohort, require the median of at least
  three fresh one-thread release/native runs to beat the pinned snputils
  baseline while reporting peak RSS.

## 4. PLINK 1 Provider

- [ ] 4.1 Create the `bio-format-plink1` crate and public read options.
- [ ] 4.2 Resolve explicit and conventional BED/BIM/FAM filesets.
- [ ] 4.3 Parse FAM sample IDs with an explicit documented identifier policy.
- [ ] 4.4 Parse BIM columns, chromosome values, positions, IDs, centimorgan
  values, `A1`, and `A2` without assigning reference semantics.
- [ ] 4.5 Validate BED magic bytes and require current variant-major mode.
- [ ] 4.6 Validate BIM/FAM counts against exact BED length with checked
  arithmetic.
- [ ] 4.7 Reject non-zero unused padding bits in each variant's final BED byte.
- [ ] 4.8 Decode two-bit calls to documented A1 dosage and null missingness.
- [ ] 4.9 Apply sample selection directly to packed calls without materializing
  unselected genotypes.
- [ ] 4.10 Apply exact variant filters and limits to BIM rows before BED I/O.
- [ ] 4.11 Skip BED entirely for metadata-only and empty-sample queries.
- [ ] 4.12 Plan contiguous fixed-offset variant ranges and coalesce adjacent
  object-store requests.
- [ ] 4.13 Partition selected variants by payload bytes up to target partitions.
- [ ] 4.14 Add local and object-store fileset consistency tests.
- [ ] 4.15 Add differential tests against PLINK and `bed-reader`.
- [ ] 4.16 Add malformed BIM/FAM, sample-major, legacy BED, truncation, padding,
  and overflow tests.
- [ ] 4.17 Benchmark dense and sparse variants, sparse samples, metadata-only
  queries, range coalescing, and parallel scans.

## 5. BGEN Provider

- [x] 5.1 Create the `bio-format-bgen` crate and output-mode read options.
- [x] 5.2 Parse BGEN 1.2/1.3 headers, flags, counts, offsets, free data, and
  optional embedded sample identifiers.
- [x] 5.3 Resolve external sample metadata and validate sample counts and IDs.
- [x] 5.4 Implement Layout 2 variant metadata parsing and independent blocks.
- [x] 5.5 Implement Layout 2 uncompressed, zlib, and zstd decompression.
- [x] 5.6 Decode Layout 2 phased/unphased probabilities, variable ploidy,
  missingness, multiallelic states, and source bit precision.
- [x] 5.7 Implement Layout 1 compatibility for its biallelic diploid encoding.
- [x] 5.8 Preserve ordered alleles and publish the exact probability-state order.
- [x] 5.9 Implement default-layout probability output without padding variable
  state vectors.
- [x] 5.10 Implement biallelic dosage output and reject selected multiallelic
  variants in dosage mode.
- [x] 5.11 Apply sample selection while unpacking probability bits and skip
  unrequested Arrow value construction.
- [x] 5.12 Open local BGI directly and remote BGI through the bounded cache.
- [x] 5.13 Validate BGI file identity, offsets, sizes, and variant counts.
- [x] 5.14 Push exact chromosome, position, and rsid filters into BGI and
  varid filters into the transient identifying-metadata catalog.
- [x] 5.15 Build a transient lightweight offset catalog when BGI is absent,
  without decoding probability payloads.
- [x] 5.16 Skip probability blocks for metadata-only and empty-sample scans.
- [x] 5.17 Partition independent selected blocks and coalesce adjacent ranges.
- [x] 5.18 Add quantization-aware differential tests against bgenix/qctool,
  `limix/bgen`, and `snputils`.
- [x] 5.19 Add malformed header, invalid flags, inconsistent sample count,
  decompression bomb, truncation, and invalid probability total tests.
- [x] 5.20 Benchmark Layout 1/2, compression codecs, sparse BGI filters, sparse
  samples, probability/dosage modes, and parallel range reads.
- [x] 5.21 Decode genotypes directly into batch-level Arrow buffers instead of
  staging each variant, and decide the batch flush after the row is written.
- [x] 5.22 Derive the fixed probability width from the catalog, NaN-pad narrower
  and missing samples per sample, and bound the derived width by
  `max_states_per_sample`.
- [x] 5.23 Size coalesced payload ranges for several per partition, with a floor
  that never starves a partition, so partition scaling is not capped by an
  unbalanced split.
- [x] 5.24 Bound a variant's probability reconstruction by the decompressed
  block budget, bind the metadata limit on parsed rather than fetched bytes, and
  attribute only payload bytes to the compressed-bytes counter.

## 6. PGEN Provider

- [x] 6.1 Create the `bio-format-pgen` crate and allele/dosage output options.
- [x] 6.2 Resolve standard PGEN/PVAR/PSAM filesets and reject unsupported hybrid
  companions explicitly.
- [x] 6.3 Parse PVAR variants, allele lists, IDs, positions, and optional metadata.
- [x] 6.4 Parse PSAM sample identifiers with an explicit documented ID policy.
- [x] 6.5 Parse and validate PGEN modes `0x01`, `0x02`, `0x03`, `0x04`, `0x10`,
  `0x11`, `0x20`, and `0x21`.
- [x] 6.6 Parse embedded and external PGEN indexes and validate counts/offsets.
- [x] 6.7 Implement biallelic hardcall and missingness decoding.
- [x] 6.8 Implement difflist and one-bit genotype representations.
- [x] 6.9 Implement phase-present and phase-information decoding.
- [x] 6.10 Implement biallelic dosage and dosage-present decoding.
- [x] 6.11 Implement phased dosage decoding.
- [x] 6.12 Implement multiallelic hardcall patch decoding with PVAR allele indices.
- [x] 6.13 Reject unsupported multiallelic dosage portions with a distinct
  unsupported-feature error.
- [x] 6.14 Implement LD base tracking and dependency-prelude planning.
- [x] 6.15 Prove partitions emit owned variants once while dependency records
  remain internal.
- [x] 6.16 Apply exact PVAR filters and limits before PGEN payload reads.
- [x] 6.17 Apply sample selection within packed, sparse, and dosage paths.
- [x] 6.18 Skip PGEN variant record payloads for PVAR-only, exact count, and
  empty-sample scans after minimal fileset validation.
- [x] 6.19 Support local and object-store range reads with index/header
  coalescing.
- [x] 6.20 Add differential tests against PLINK 2 and external `pgenlib`.
- [x] 6.21 Add fuzz/property tests for varints, difflists, offsets, LD chains,
  allele patches, and dosage bounds.
- [x] 6.22 Benchmark each record representation, sparse variants, sparse
  samples, LD-heavy files, dosage output, and parallel record blocks.
- [x] 6.23 Build projection and sample plans once per scan and use reusable
  partition-local decode workspaces.
- [x] 6.24 Replace generic dense per-sample bit extraction with a specialized
  packed-byte/word kernel and direct selected-sample gather path.
- [ ] 6.25 Append decoded values directly to Arrow builders without retaining
  full-cohort `DecodedRecord` and `DecodedRow` copies.
- [ ] 6.26 Retain only the latest eligible LD base, update it in place, and add
  allocation/copy assertions for LD-heavy scans.
- [ ] 6.27 Reuse one local read handle and bounded range buffer per partition;
  decode header indexes by block and compact or stream PVAR/PSAM catalogs.
- [x] 6.28 Define effective `DS`, optional `DS_STORED`, missingness, and stored
  dosage override behavior in oracle and hand-checked tests.
- [ ] 6.29 Publish encoded-diploid ploidy metadata and reject unsupported
  chromosome-aware requests instead of inferring genome-build/PAR rules.
- [ ] 6.30 Enforce the public `UInt16` allele-width limit and maintain a separate
  current-`pgenlib` oracle compatibility limit.
- [x] 6.31 Rebase the PGEN implementation on the current genotype-core head and
  verify that it does not revert intervening BCF/core changes.

## 7. GRG Provider

- [x] 7.1 Record the supported GRG on-disk versions and complete an independent
  implementation/license review.
- [ ] 7.2 Create the `bio-format-grg` crate behind an explicit workspace feature
  until compatibility gates pass.
- [ ] 7.3 Implement local immutable GRG opening with version and size validation.
- [ ] 7.4 Reject remote GRG locations with a clear local-only error.
- [ ] 7.5 Expose mutation ID, configured/stored contig, position, reference,
  alternate, and genotype columns.
- [ ] 7.6 Implement mutation catalog filtering before graph traversal.
- [ ] 7.7 Implement selected haplotype-node mapping and stable output order.
- [ ] 7.8 Implement haplotype presence output with graph missingness as null.
- [ ] 7.9 Implement fixed-ploidy individual grouping and alternate counts.
- [ ] 7.10 Validate individual grouping, sample count, and incomplete final groups.
- [ ] 7.11 Traverse only descendants required for selected mutations/samples and
  avoid optional upward-edge construction.
- [ ] 7.12 Partition mutation ranges over a shared immutable graph.
- [ ] 7.13 Skip traversal for metadata-only and empty-sample scans.
- [ ] 7.14 Add differential tests against external Python `grgl` on synthetic
  graphs without linking it into runtime code.
- [ ] 7.15 Add graph version, cycle/invariant, missingness, ploidy, mutation
  density, and corrupted offset tests.
- [ ] 7.16 Benchmark sparse/dense descendant sets, sample subsets, haplotype and
  individual modes, and parallel mutation ranges.

## 8. Cross-Format Integration

- [ ] 8.1 Register providers through consistent constructors and explicit
  format-specific read options.
- [ ] 8.2 Add SQL examples for projected metadata, selected samples, regions,
  identifiers, probabilities, dosages, and raw allele calls.
- [ ] 8.3 Add cross-format semantic fixtures where one cohort is encoded in
  multiple losslessly comparable formats.
- [ ] 8.4 Verify exact/inexact pushdown declarations by comparing provider-owned
  filtering with DataFusion residual evaluation.
- [ ] 8.5 Verify pushed limits with no filter, exact filters, inexact filters,
  unsupported filters, and multiple partitions.
- [ ] 8.6 Verify projected schemas and sample metadata are stable across batch
  and partition boundaries.
- [ ] 8.7 Verify memory limits on wide cohorts and adversarial declared sizes.
- [ ] 8.8 Verify no provider creates unbounded inner concurrency.
- [ ] 8.9 Add object-store integration tests with request/byte-count assertions
  for all range-capable providers.
- [ ] 8.10 Add CI jobs or feature groups that keep optional format build times
  manageable while exercising every provider.

## 9. Performance And Release Gates

- [ ] 9.1 Define representative public or generated benchmark datasets without
  committing large fixtures.
- [ ] 9.2 Record latest released-version and independent-tool baselines.
- [ ] 9.3 Require metadata-only scans to read zero genotype payload bytes when
  companion metadata can answer the query.
- [ ] 9.4 Require sparse indexed scans to read a bounded superset of selected
  blocks and report the amplification.
- [ ] 9.5 Require sparse sample scans to reduce decoded sample values even when
  compressed bytes cannot be reduced.
- [ ] 9.6 Check for throughput regressions in existing VCF providers.
- [ ] 9.7 Run `cargo fmt --all -- --check`.
- [ ] 9.8 Run `cargo check --workspace`.
- [ ] 9.9 Run affected crate tests and `cargo test --workspace`.
- [ ] 9.10 Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] 9.11 Publish the support matrix, limitations, pushdown behavior, ordering,
  sample/allele semantics, and benchmark results.
- [ ] 9.12 Obtain per-format approval before enabling it in default release
  artifacts.
- [x] 9.13 Add a pinned, official-writer PGEN parity harness that constrains all
  runtime pools to one thread and compares at least ten post-warmup medians with
  `snputils`/`pgenlib` on identical selections.
- [x] 9.14 Require PGEN one-thread biallelic phased `GT` decode-plus-materialize
  time to be no slower than the pinned `snputils` median; report provider-open,
  full-fileset, output-byte, and peak-RSS results separately.
- [ ] 9.15 Publish PGEN 1/2/4/8-partition scaling, balance, dependency-prelude,
  decoded-value, allocation, and peak-memory metrics without nested pools.

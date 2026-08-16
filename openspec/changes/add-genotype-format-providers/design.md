# Genotype Format Provider Design

## Context

The existing VCF table provider already establishes several useful project
patterns:

- a variant-major row model;
- nested per-sample FORMAT values for multi-sample data;
- projection pruning;
- genomic filter extraction;
- CSI/TBI-derived scan partitions;
- OpenDAL-backed local and object storage; and
- session-sized RecordBatch output.

BCF, PLINK 1, BGEN, PGEN, and GRG should reuse those architectural patterns,
but they do not share one physical encoding or one allele model. A unified
decoder would either erase semantics or accumulate format branches in a hot
loop. The design therefore standardizes planning and query behavior while
leaving physical decoding in format-specific crates.

The normative format references are:

- BCF 2.2 in `samtools/hts-specs`;
- PLINK 1 binary format documentation from the PLINK project;
- BGEN 1.2 and 1.3 specifications from the BGEN project;
- `pgen_spec.pdf`/`pgen_spec.tex` in the PLINK 2 source tree; and
- for GRG, an implementation-gated on-disk contract derived independently from
  the published model, supported fixtures, and declared serialization versions.

PGEN is explicitly a draft specification and must be re-pinned when its decoder
work begins. GRG does not currently have a standalone, mature byte-level
specification comparable to BCF or BGEN. Official GRGL documentation and
fixtures define behavior, while GPL serialization source is not a porting
source. GRG implementation cannot pass its gate until an independently
reviewable byte-level contract exists.

Reference implementations are compatibility oracles, not substitutes for the
normative documents. The initial oracle set is `noodles-bcf`/htslib for BCF,
`bed-reader` and PLINK for PLINK 1, `limix/bgen`, bgenix, and qctool for BGEN,
PLINK 2 `pgenlib` for PGEN, and Python `grgl` for GRG. `snputils` is useful for
cross-format behavioral comparisons, but its whole-file and delegated-reader
paths are not the target execution architecture.

### Reference baseline (reviewed 2026-08-16)

| Capability | Normative source | Implementation/oracle snapshot |
| --- | --- | --- |
| BCF | [`BCFv2_qref`](https://github.com/samtools/hts-specs/blob/da617203a9527537746e200abda2885bec3a822c/BCFv2_qref.pdf) and [`CSIv1`](https://github.com/samtools/hts-specs/blob/da617203a9527537746e200abda2885bec3a822c/CSIv1.pdf) | `noodles` `fe2b112566a5d509910303841bb9df47dd007fcf`; htslib/bcftools |
| PLINK 1 | [PLINK binary format appendix](https://www.cog-genomics.org/plink/1.9/formats#bed) | Apache-2.0 `bed-reader` `0128fc755745c8e1cbe49d677479e5cfc3b2f49e`; PLINK |
| BGEN | [BGEN v1.3 latest specification](https://www.chg.ox.ac.uk/~gav/bgen_format/spec/latest.html) | MIT `limix/bgen` `0c3bae807d00e03499f8b04ac0db83f7f20dd1c4`; bgenix/qctool |
| PGEN | [`pgen_spec` draft](https://github.com/chrchang/plink-ng/tree/9ee41ce224ea7cd091760d69392a98835715b5b2/pgen_spec) | PLINK 2 at `7b30cf1733c4f50c6699268a9f07fb6af206ed49` and Python `pgenlib` 0.94.1; external oracle only for LGPL components |
| GRG | [Official GRG model/documentation](https://github.com/aprilweilab/grgl/tree/7b896a00d8b23821e5a779048580f64ae9c34368) plus the required independent on-disk contract | Python/C++ GRGL at `7b896a00d8b23821e5a779048580f64ae9c34368`; external GPL oracle only |
| Cross-format | Format sources above | BSD-3-Clause `snputils` at `482c6d1dfd6c4001935dfaec81ae01a5e0ec3e53` |

These commit pins make conformance results reproducible; they are not permission
to copy implementation code. Before implementation, each pin is reviewed for
newer format corrections and upgraded only with corresponding fixture changes.
The PGEN specification tree at the PLINK 2 oracle commit above is byte-for-byte
unchanged from the normative `9ee41ce` pin. The newer PLINK 2 commit is therefore
an implementation-oracle update, not a format-contract update.

## Goals

- Preserve format-native genotype, phase, ploidy, probability, missingness,
  sample-order, and allele semantics.
- Give all providers consistent DataFusion behavior for projection, filters,
  limits, partitions, errors, and metrics.
- Push selection into metadata and native indexes before payload I/O.
- Avoid genotype payload reads for metadata-only queries.
- Decode only requested samples and genotype fields when the encoding permits.
- Stream bounded RecordBatches without materializing the complete dataset.
- Support local files and object stores where the format admits range access.
- Keep DataFusion in control of scan concurrency.
- Make compatibility and performance independently testable.
- Meet or beat the pinned `snputils` single-thread biallelic `GT` baseline before
  enabling PGEN in release artifacts, while retaining bounded-memory streaming.

## Non-Goals

- Writing, mutating, indexing, or converting genotype files.
- A universal lossless conversion between unlike genotype models.
- A default sample-major row explosion with one row per variant/sample pair.
- Genotype predicates over nested sample arrays in the first implementation.
- Population statistics, association testing, imputation, or phasing.
- Inferring biological reference/alternate orientation from PLINK 1 or BGEN.
- Transparent support for every historical, undocumented, or hybrid PLINK
  fileset.
- Linking LGPL `pgenlib` or GPL `grgl` into default production artifacts.
- Globally ordered parallel scan output without an explicit sort.

## Decisions

### 1. Crate and provider boundaries

The shared planning types, companion resolution, coordinate helpers, range
coalescing, metrics, and conformance utilities belong in
`bio-format-core`. BCF support belongs in `bio-format-vcf` because its logical
schema is the VCF schema. PLINK 1, BGEN, PGEN, and GRG receive separate format
crates so their dependencies and hot loops remain isolated.

A physical decoder SHALL expose records through a small internal interface
that can:

1. report header, variant, sample, and field metadata;
2. plan candidate variant ranges;
3. read one physical partition;
4. decode a projected row range into Arrow builders; and
5. report pruning and I/O metrics.

This is an internal contract, not a promise that every physical encoding shares
one decoder implementation.

### 2. Variant-major logical model

One output row represents one source variant or, for GRG, one source mutation.
Sample values remain nested inside a `genotypes` struct. This avoids multiplying
row counts by the number of samples and matches columnar scans that commonly
select variant metadata across a cohort.

Common logical columns are exposed when the source can supply them:

| Column | Type | Meaning |
| --- | --- | --- |
| `chrom` | `Utf8`, nullable | Source contig or configured GRG contig |
| `start` | integer | Position in the requested coordinate system |
| `end` | integer | End in the requested coordinate system |
| `id` | `Utf8`, nullable | Primary source variant or mutation identifier |
| `genotypes` | `Struct`, nullable | Projected per-sample genotype fields |

Format-specific metadata columns remain first-class columns rather than being
packed into a generic map.

For site-only sources, one-based coordinates use `start = POS` and `end = POS`.
Zero-based half-open coordinates use `start = POS - 1` and
`end = start + 1`. Invalid zero or negative one-based positions are rejected.
BCF uses the existing VCF span rules, including reference-allele length and
symbolic-variant handling.

### 3. Allele identity is format-native

The providers SHALL NOT silently rename ordered alleles to `ref` and `alt`.

- BCF retains VCF `ref` and `alt`.
- PLINK 1 exposes `a1` and `a2`. Genotype counts are documented relative to
  `a1`; no biological reference orientation is inferred.
- BGEN exposes `alleles: List<Utf8>` in encoded order. It does not manufacture a
  reference allele.
- PGEN exposes PVAR `ref` and `alt`, retaining all PVAR alternate alleles.
- GRG exposes the mutation's reference and alternate alleles where stored.

Any future normalization or reference-genome validation is a separate,
explicit transform above the scan.

### 4. Nested genotype representations

The selected sample order is stable across every child of `genotypes`.
Selected sample names are recorded in Arrow field metadata so consumers can
map list offsets to samples without reading a separate table.

The format-specific child fields are:

| Format/mode | Genotype children |
| --- | --- |
| BCF string mode (default) | Existing VCF FORMAT-to-Arrow mapping |
| BCF dosage mode | `GT: List<Int8>` containing biallelic ALT dosage 0..ploidy or null |
| PLINK 1 | `GT: List<UInt8>` containing A1 dosage 0, 1, 2, or null |
| BGEN probability | `GP: List<List<Float32>>`, `PLOIDY: List<UInt8>`, and variant `phased` |
| BGEN dosage | `DS: List<Float32>` and `PLOIDY: List<UInt8>` |
| PGEN allele | `GT: List<FixedSizeList<UInt16, 2>>`, `PHASED: List<Boolean>` |
| PGEN dosage | effective `DS`, optional source-only `DS_STORED`, `HDS`, and requested hardcall fields |
| GRG haplotype | `GT: List<UInt8>` with mutation presence 0/1 or null |
| GRG individual | `GT: List<UInt8>` with alternate count 0..ploidy or null |

Null represents missing sample genotype data. Empty inner allele/probability
lists are not used as a substitute for missingness.

BCF dosage mode is explicit and defined only for records with exactly one ALT
allele. It counts allele index 1 across the called GT alleles, ignores phase,
uses null when any allele is missing, and rejects multiallelic records rather
than collapsing distinct alternate alleles. The `Int8` representation supports
first-ALT dosage through 127. Ploidy is tracked independently, so higher-ploidy
genotypes remain valid when their observed dosage is representable; an observed
dosage above 127 is rejected and remains available through default string mode.
Schema metadata records the output mode and that dosage counts the first ALT
allele.

BGEN `GP` state ordering SHALL be the exact ordering defined by the BGEN
specification for phased/unphased data, ploidy, and allele count. PGEN raw
allele indices SHALL refer to PVAR allele order. A metadata key records the
counted allele or state-order definition for machine-readable interpretation.

### 5. Sample and genotype field selection

Sample selection and genotype-field selection are table read options, because
ordinary SQL filters cannot efficiently describe positions within nested lists.
The default is all samples and all fields included by the selected output mode.

Requested sample order is preserved. Repeated names are de-duplicated at their
first occurrence. Unknown sample names fail by default. An explicit
`missing_sample_policy = ignore` option permits unknown names to be skipped,
with the final selected-name list recorded in metadata. An empty explicit
selection is valid and produces metadata-only rows.

The planner maps requested sample names to physical indices once. Decoders
SHALL avoid materializing unselected sample values where their physical
encoding permits; compressed variant blocks may still require whole-block
decompression.

### 6. Scan planning order

Every provider follows this observable planning order:

1. resolve the primary object and explicit or conventional companions;
2. read the minimum header and sample metadata;
3. validate format version, counts, and required companions;
4. resolve the logical schema, projection, sample set, and genotype fields;
5. identify filter columns needed for correctness even if not projected;
6. select candidate variants through an index or lightweight metadata;
7. return an empty execution plan if selection is unsatisfiable;
8. form balanced physical partitions from selected byte/range estimates;
9. read and decode only the selected payload and fields;
10. evaluate provider-owned exact predicates;
11. preserve unsupported or inexact predicates as DataFusion residuals; and
12. emit batches bounded by session and genotype memory limits.

This ordering is the basis for pushdown metrics and conformance tests.

### 7. Filter pushdown and exactness

Providers reuse the core genomic filter vocabulary for equality or `IN` on
`chrom`, comparisons and `BETWEEN` on `start`/`end`, conjunctions, and
unsatisfiable expressions. Format metadata can add equality/`IN` pruning for
identifiers.

`supports_filters_pushdown` SHALL report:

- `Exact` only when the provider evaluates the complete expression and no
  residual is needed;
- `Inexact` when an index produces a superset or coordinate conversion can
  admit boundary candidates; and
- `Unsupported` when no provider-owned evaluation is guaranteed.

BCF CSI region queries are inexact because chunks can contain records outside
the requested interval. BIM, BGI, PVAR, and a loaded GRG mutation catalog may
produce exact candidate sets for the predicate forms they fully evaluate.
Unsupported genotype-array predicates remain residual filters.

Columns used by residual or provider-owned predicates are read as hidden
dependencies when not projected and are removed before final output.

### 8. Projection and metadata-only execution

Projection is applied before payload decoding:

- unprojected metadata columns are not materialized unless filters need them;
- an unprojected `genotypes` column prevents genotype payload decoding;
- unrequested genotype children are not decoded;
- an explicit empty sample set prevents sample genotype decoding; and
- an unfiltered exact count may use trusted header/index counts without reading
  variant payloads.

Metadata-only variant queries may still read BIM, BGI, PVAR, or BCF records as
needed to obtain projected metadata, but SHALL not read the genotype payload
object when the companion already contains the requested columns.

### 9. Limit correctness

A scan may stop after a pushed limit only after every predicate assigned to the
provider has been evaluated. A limit SHALL NOT be applied to an inexact index
candidate set before record-level validation, and it SHALL NOT be pushed below
an unsupported DataFusion residual. Across multiple partitions, early stopping
is an optimization only; correctness does not depend on deterministic
partition completion order.

### 10. Companion discovery and object storage

A shared companion resolver accepts explicit locations first. Otherwise it
tries format-defined suffix conventions next to the primary object. Every
resolved companion uses the same configured OpenDAL operator unless its
location explicitly selects another supported store.

The resolver records attempted locations in missing-companion errors without
printing credentials or signed query parameters. It checks object identity
metadata when caching and rejects inconsistent file-set counts before scanning.

SQLite BGI cannot be queried through arbitrary range reads. A remote BGI is
downloaded to a bounded local cache keyed by canonical object identity,
including ETag/version when available. Downloads are atomic, concurrent opens
are coalesced, and cache size/age is configurable. Deleting the provider does
not invalidate a file still leased by an active scan.

The initial GRG provider is local-file-only and SHALL reject object-store
locations explicitly. This exception is part of the GRG capability rather than
an implicit fallback that downloads an unbounded graph.

### 11. Physical partitions and concurrency

DataFusion target partitions control outer scan parallelism. A decoder does not
create an additional unbounded thread pool. Compression and parsing inside one
partition use explicit single-concurrency options unless a measured,
bounded exception is documented.

Partition units are format-specific:

- BCF: CSI-derived virtual-offset chunks, grouped by estimated compressed bytes;
- PLINK 1: contiguous variant ranges with fixed byte offsets;
- BGEN: independent variant blocks from BGI or a lightweight offset scan;
- PGEN: `2^16`-variant record blocks plus required LD dependency preludes; and
- GRG: mutation-index ranges over a shared immutable graph.

One physical source range is owned by at most one output partition. Overlapping
BCF chunks are merged before assignment. PGEN dependency records may be read by
more than one partition but are emitted only by their owning partition.

With more than one partition, output is complete but global source order is not
promised. One-partition scans preserve source order.

### 12. Range I/O and buffer management

Object-store readers coalesce adjacent selected ranges subject to configurable
maximum gap and range-size thresholds. They do not replace sparse selection
with an unbounded read from the first selected byte through the last.

Decoders reuse record and decompression buffers within a partition. Output is
bounded by both `SessionConfig::batch_size()` and a configurable soft genotype
byte budget so a wide cohort does not create an unexpectedly large batch. A
single valid row larger than the soft budget may be emitted alone. Hard limits
cover declared sample, allele, ploidy, field, string, and decompressed-block
sizes before allocation.

### 13. Format-specific physical strategy

#### BCF

BCF uses a streaming BCF 2.2 decoder over BGZF and reuses record buffers. Header
string dictionaries are resolved once. CSI provides sparse range reads and
parallel chunks; unindexed input falls back to one sequential partition.
Samples and FORMAT children are projected before typed value conversion.
CSI companions have a hard byte ceiling. Local files are rejected from
metadata before parsing, while remote companions use a single-request stream
whose observed chunks are checked before extending the bounded buffer. The
single-request storage primitive intentionally avoids a HEAD preflight so it
also works with GET-only signed URLs.
Remote CSI-selected BCF spans are not materialized even when one index chunk is
large: an explicit range stream caps each sequential backend read at 8 MiB and
feeds those bytes directly into the asynchronous BGZF and BCF decoders. Outer
DataFusion partitions provide scan concurrency without multiplying the cap
inside one partition.

The BCF individual section is scanned once into validated typed FORMAT-series
views containing the dictionary key, encoded primitive type, per-sample width,
and borrowed payload. Projected series dispatch to preselected typed sinks;
unprojected series are validated and skipped without constructing noodles
trait objects or intermediate per-sample values. Validation and materialization
are fused where the selected sink can enforce the same integrity checks.

GT has representation-specific sinks. Default string mode retains the existing
lossless VCF-compatible path. Dosage mode decodes BCF allele integers directly
into contiguous Arrow `Int8` values and validity bits, without constructing GT
strings, boxed per-cell iterators, or a complete-file genotype matrix. The
FORMAT-series scanner and sink boundary are intentionally field-generic so
integer, float, vector, and string FORMAT children can migrate to direct typed
decoders without changing the record reader or public mode semantics.

#### PLINK 1

The provider supports the current variant-major BED encoding with magic bytes
`0x6c 0x1b 0x01`. Each variant occupies `ceil(sample_count / 4)` bytes, enabling
constant-time offsets and parallel ranges. Two-bit codes map to A1 dosage as:
`00 -> 2`, `10 -> 1`, `11 -> 0`, and `01 -> null`. BIM is the exact variant
metadata catalog and FAM is the sample catalog.

Sample-major and pre-current BED encodings are rejected with an actionable
message rather than guessed or silently transposed.

#### BGEN

Layout 2 is the primary implementation and supports uncompressed, zlib, and
zstd blocks, multiallelic variants, phase, variable ploidy, and missingness.
Layout 1 is a compatibility path for its restricted biallelic diploid model.
Each selected variant block is decompressed independently. BGI supplies exact
row offsets and metadata pruning; without BGI the provider builds a transient
offset catalog by scanning variant metadata without decoding probabilities.

Probability mode is lossless up to the source quantization. Dosage mode is
defined only for biallelic variants; selecting a multiallelic variant in dosage
mode is an error rather than a lossy collapse.

#### PGEN

The provider implements standard PGEN header modes and consumes PVAR/PSAM for
variant and sample identity. PGEN's index may be embedded or external. Decoder
features are enabled from header flags, including hardcalls, phase, biallelic
dosage, phased dosage, and multiallelic hardcalls.

LD-compressed records depend only on the most recent eligible non-LD record.
Partition plans include the required dependency anchor and decode forward to
the first owned record. Unsupported or not-yet-standardized multiallelic dosage
sections fail explicitly. Hybrid `.pgen + .bim + .fam` filesets are outside the
initial contract.

PGEN itself does not encode biological ploidy. Initial raw `GT` output therefore
reports the two encoded allele slots and labels them with
`ploidy_semantics=encoded_diploid`; it does not guess chromosome build, PAR
boundaries, or sex-chromosome ploidy from PVAR/PSAM. A future chromosome-aware
mode requires an explicit genome-build/ploidy policy. The performance oracle is
run with `snputils`' `chromosome_ploidy="autosomal"` so both sides implement the
same contract.

The two encoded allele slots use an Arrow `FixedSizeList<UInt16, 2>` per sample
instead of a variable inner list. Missing samples remain null at the fixed-size
list level. This removes one 32-bit offset per sample while preserving the exact
two-slot PGEN representation and materially reduces wide-cohort memory traffic.

`DS` uses effective biallelic alternate-allele dosage semantics, matching
PLINK 2/`pgenlib`: a stored dosage overrides the hardcall, an otherwise-called
hardcall contributes exact dosage 0, 1, or 2, and the value is null only when
both are missing. `DS_STORED`, when projected, exposes only the physically
stored dosage and is null for hardcall fallback. This keeps the common fast path
oracle-compatible without losing the distinction between source dosage and a
hardcall-derived value.

The first implementation supports allele indices representable by the public
`UInt16` schema (at most 65,536 alleles per variant) and rejects larger encoded
widths before allocation. Differential fixtures additionally stay within the
current official `pgenlib` limit of 255 alleles; this oracle limit is not
misreported as a file-format limit.

#### GRG

GRG is graph-native rather than record-native. The provider opens one immutable
local graph and exposes a mutation view. For a selected mutation, it traverses
only the graph paths needed to determine selected descendant samples. It avoids
constructing upward edges unless a documented algorithm requires them.

Haplotype mode returns presence on haploid sample nodes. Individual mode groups
consecutive haplotypes using an explicit fixed ploidy and returns alternate
counts. Graph missingness maps to null. A `contig_name` option supplies `chrom`
when the graph does not store one. Remote GRG access and graph analytics remain
out of scope for the first provider.

### 14. PGEN hot-path and workspace design

PGEN has a dedicated projection-driven decode pipeline. Planning creates a
`ProjectionPlan` bitset and a `SamplePlan` once. Each physical partition owns a
single reusable `DecodeWorkspace` containing its range buffer, packed hardcall
scratch, selected-sample scratch, auxiliary-track scratch, Arrow builders, and
one LD-base workspace. The partition appends decoded values directly to Arrow
builders; it does not retain a `DecodedRecord` or a second row representation.

Unprojected tracks are validated and skipped from declared lengths without
allocating logical values. A `GT`-only scan does not allocate `PHASED`, `DS`,
`DS_STORED`, or `HDS`; a metadata-only scan does not read record payloads. The
identity-sample path uses specialized packed-byte/word decoding, while sparse
selection gathers directly into request order. Generic per-sample bit readers
remain a correctness fallback, not the dense hot loop.

LD state contains only the most recent eligible non-LD base required by the
specification. It is updated in place and represented at the narrowest level
needed by the projection/sample plan. The decoder does not keep a map of every
base variant and does not clone a full cohort vector for each LD record.

Local scans open one read handle per partition and reuse buffers with positional
reads. Object-store scans retain bounded coalesced range reads. The header index
is decoded by `2^16`-variant blocks on demand instead of requiring an owned
record descriptor for every variant. PVAR/PSAM parsing is streaming or compactly
interned so metadata memory does not multiply the source text with per-cell
`String` allocations.

Partitions remain contiguous for locality and are balanced with a cost model
combining encoded bytes, projected decoded values, record representation, and
LD prelude work. Boundaries prefer valid non-LD anchors. DataFusion target
partitions are the only outer parallelism; the decoder creates no nested pool.

### 15. PGEN performance qualification

The initial audit of draft PR #221 used an official-writer, fully phased
biallelic fixture with 16,384 variants and 1,024 samples on an Apple M3 Max.
Both implementations read all variants and samples and materialized `GT`; all
Python/native numerical thread pools and DataFusion target partitions were set
to one for the single-thread comparison.

| Implementation | Median/representative wall time | Materialized value bytes |
| --- | ---: | ---: |
| `snputils` `482c6d1` / `pgenlib` 0.94.1 | 70.4 ms | 32.0 MiB compact `int8` alleles |
| Draft Rust PR #221, one partition | 391.4 ms | 203.6 MB Arrow array estimate |
| Draft Rust PR #221, four partitions | 114.3 ms | 203.6 MB Arrow array estimate |

The formats differ in output width, so qualification reports output bytes and
does not claim identical memory bandwidth. The observed roughly 5.5x
single-partition gap is nevertheless a release blocker. The dominant structural
causes are eager construction of every genotype child, full-cohort intermediate
rows, generic per-sample packed reads, repeated LD-base copies, and per-range
file/buffer setup.

The hard gate uses a release build, identical fixture and selection, warm page
cache, one DataFusion partition/Tokio worker, one thread for every oracle pool,
and at least ten measured iterations after warmup. It compares median steady
state decode-plus-materialize time for biallelic phased `GT`; provider-open and
full-fileset end-to-end times are reported separately. Rust SHALL be no slower
than the pinned `snputils` median on the same host. The benchmark command,
fixture generator seed, hardware, compiler flags, oracle versions, result
samples, output bytes, and peak RSS are published with the result.

Additional suites cover dense, one-bit, difflist, LD-heavy, phase, dosage,
multiallelic hardcall, metadata-only, sparse-variant, sparse-sample, local, and
object-store paths. Multi-partition results must remain complete and bounded;
on a host with at least four physical cores, four partitions target at least
2.5x the one-partition throughput without nested concurrency. A noisy general
CI runner may track regressions with a tolerance, but cannot waive the dedicated
no-slower-than-`snputils` release gate.

Gate 1 implementation on 2026-08-16 replaced the draft row intermediates with
a direct fixed-size allele-pair Arrow path and a four-call dense/phase lookup
kernel. On the pinned generated fixture, ten post-warmup iterations produced
32.92 ms for one Rust partition versus 71.60 ms for one-thread `snputils`.
Rust output was 69,272,392 bytes with maximum RSS 163,889,152 bytes; `snputils`
output was 33,554,432 bytes with maximum RSS 587,841,536 bytes. Both produced
the same genotype digest. Rust scans measured 18.26, 9.72, and 7.03 ms with two,
four, and eight partitions respectively, so the one-thread parity and four-core
scaling gates pass for this fixture.

### 16. Error model

Format, version, count, offset, compression, allocation-limit, and companion
errors are detected as early as possible and include the primary object and
record/variant context. Corruption is never converted to missing genotypes.
Unsupported-but-valid features produce a distinct unsupported-feature error,
not a generic parse failure.

No hot-path decoder uses `unwrap` or trusts file-declared sizes without checked
arithmetic. Partial output followed by a corruption error is allowed for
streaming execution, but the error is not suppressed.

### 17. Metrics and observability

Every scan exposes, at minimum:

- primary and companion bytes read;
- range request count and coalesced range count;
- compressed and decompressed bytes;
- metadata candidates, selected variants, and emitted variants;
- genotype payloads skipped by projection;
- samples requested, samples decoded, and sample values skipped;
- partitions and dependency-prelude records;
- rows rejected by exact record-level filters; and
- batch rows and estimated genotype bytes.

Metrics permit proving that a pushdown avoided I/O rather than merely discarding
values after decoding.

### 18. Testing and benchmarking

Each format receives:

- small hand-checked fixtures for every supported encoding feature;
- conformance tests derived from the normative specification;
- differential tests against at least one independent oracle;
- projection, sample-selection, filter, limit, and empty-selection tests;
- local and supported object-store range tests;
- malformed header, truncation, overflow, and inconsistent companion tests;
- property/fuzz tests for pure decoders and offset arithmetic; and
- benchmarks for metadata-only, sparse-variant, sparse-sample, dense-cohort,
  single-partition, and multi-partition scans.

Differential comparisons normalize only declared semantic differences such as
floating-point quantization and coordinate presentation. Benchmarks report
wall time, throughput, peak memory, bytes read, ranges, and decoded values.

The BCF dosage release gate uses fresh one-thread processes, release builds
with native CPU tuning, the same source file and selected samples, equivalent
biallelic hard-call dosage values, and both timing and peak RSS. At least three
interleaved runs are summarized by their median. The optimized streaming BCF
path must have a lower median wall time than the pinned independent snputils
GT-dosage baseline before the performance task is complete.

### 19. Licensing boundary

Runtime code may depend on libraries with project-compatible licenses after
normal dependency review. LGPL `pgenlib` and GPL `grgl` SHALL NOT be linked,
vendored, translated line-by-line, or copied into default project artifacts.
They may run as optional external test executables on synthetic fixtures.
PGEN and GRG production decoders require independent implementation from the
public format description and a recorded licensing review.

## Alternatives Considered

### Convert every input to VCF/Arrow first

Rejected because it adds an expensive preprocessing step, loses probability or
graph structure, duplicates storage, and prevents native pushdowns.

### Use `snputils` as the decoding engine

Rejected as the production architecture because several readers inflate whole
files or delegate to native libraries and do not expose DataFusion partition or
range planning. It remains useful as a behavior oracle.

### Emit one row per sample genotype

Rejected as the default because cohort scans would multiply row counts and
repeat variant metadata. A future relational view can explicitly unnest the
nested representation.

### Force a universal `GT`/`DS` schema

Rejected because BGEN probability states, PGEN phased dosage, and GRG haplotype
presence cannot all be represented losslessly by one scalar dosage.

### Put all formats in the VCF crate

Rejected because only BCF shares VCF semantics. Separate crates contain
dependencies, features, tests, and performance-sensitive implementations.

## Risks / Trade-offs

- Nested Arrow fields are compact but require explicit sample metadata and
  consumer support. Conformance tests will validate name/order stability.
- BGEN decompression cannot skip unselected sample bits inside a compressed
  block. Metrics distinguish decompression from value decoding.
- PGEN LD dependencies complicate random access. Partition preludes trade small
  duplicate reads for independent DataFusion partitions.
- Remote SQLite BGI requires local storage. A bounded identity-aware cache
  prevents stale or unbounded downloads.
- Graph traversal costs depend on topology rather than just row count. GRG
  benchmarks include sparse and dense mutation descendants.
- Supporting Layout 1 BGEN adds a compatibility decoder. It remains isolated
  from the Layout 2 hot path.
- Exact pushdown claims can cause wrong results if overstated. Every exact
  predicate form receives result-equivalence tests against DataFusion residual
  evaluation.

## Rollout Plan

1. Approve the shared schema, allele semantics, and licensing boundaries.
2. Implement core contracts and conformance harnesses without changing current
   providers.
3. Add BCF behind an additive input-format path and compare against VCF output.
4. Add PLINK 1 and establish fixed-offset/range benchmarks.
5. Add BGEN Layout 2, BGI, and probability mode; add Layout 1 and dosage mode
   after lossless tests pass.
6. Add PGEN header modes incrementally, keeping unsupported features explicit
   until each decoder is conformant.
7. Add GRG only after on-disk compatibility and independent-implementation
   review; initially expose local immutable graphs.
8. Enable each provider in normal workspace builds after its correctness,
   object-store where applicable, and benchmark gates pass.

Each format can be disabled independently through crate selection or a
workspace feature if a regression is discovered. Existing VCF behavior remains
the compatibility baseline throughout rollout.

## Open Questions

- Whether the public API should expose one shared `GenotypeReadOptions` base
  type embedded by format options, or only shared traits plus format-owned
  option types.
- Whether wide genotype fields need an additional hard per-batch byte cap
  separate from the existing session memory pool.
- Whether remote GRG should later use an explicit managed download/cache or a
  native range-capable graph reader.
- Which compatible SQLite crate and cache implementation should back BGI
  without introducing native build requirements.

These questions do not change the normative query semantics in the delta
specifications.

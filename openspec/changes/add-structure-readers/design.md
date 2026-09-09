# Native structural providers: implementation plan

## Ownership and public contract

This repository owns the complete native path from structure files/database entries to atom or residue Arrow batches. `polars-bio` owns option/source bindings, Python and SQL APIs, end-to-end tests, and wheels. This is the two-repository scope agreed with the user for #455.

The authoritative proposed schemas, model/altloc/connectivity policies, six angle definitions, API scope, and measured oracle findings live in the [shared design](../../../../polars-bio/openspec/changes/add-structure-readers/design.md) and [oracle plan](../../../../polars-bio/openspec/changes/add-structure-readers/oracles.md). This document assigns native files, interfaces, dependencies, and execution work; implementation must pass those shared semantics. Both atom and residue output are part of the format providers.

## Crates, files, and dependency direction

```text
datafusion/bio-format-structure/
  Cargo.toml, README.md, build.rs
  src/lib.rs                 public providers/options/schema and shared entry API
  src/options.rs             level/model/altloc/connectivity/source options
  src/schema.rs              fixed atom/residue schemas, units and schema version
  src/model.rs               normalized entries/sites/atoms, stable source ordinals
  src/manifest.rs            explicit lists/globs, file/block identity and scheduling
  src/storage.rs             core storage reuse, gzip and bounded payload loading
  src/pdb.rs                 fixed-column records and metadata
  src/mmcif.rs               raw CIF category mapping without hierarchy coercion
  src/native_cif.rs          checked native parser ownership/error boundary
  native/cif_bridge.*        minimal Gemmi adapter, batched raw field access
  src/residue.rs             site assembly, component/altloc selection, connectivity
  src/geometry.rs            pure Float64 point/angle/dihedral functions
  src/batch_builder.rs       shared atom/residue projection and Arrow builders
  src/table_provider.rs      StructureTableProvider and scan planning
  src/physical_exec.rs       per-partition entry cursor and output stream
  tests/                     native parsing, residue, query, storage and oracle tests

datafusion/bio-format-foldcomp/
  Cargo.toml, README.md, build.rs
  src/lib.rs, options.rs      FoldcompTableProvider/options exports
  src/database.rs            checked index/lookup/dbtype and source identity
  src/selection.rs           names/keys, empty/missing/duplicate request semantics
  src/codec.rs                checked FCZ decoder to normalized entries
  native/codec_bridge.*       upstream codec interface and allocation ownership
  src/table_provider.rs      standalone FCZ/database planning
  src/physical_exec.rs       selected-entry scheduling and shared batch builder
  tests/                     official codec, sidecars, subsets and failure behavior

testing/data/structure/        canonical small inputs, expected outputs, manifest
testing/oracles/structure/     pinned independent generators and hash verification
```

Names are a proposed file map; combine small modules if their code does not justify separate files. Use two format crates, without introducing a new generic framework or a public transform over arbitrary DataFrames.

Dependency direction is `bio-format-foldcomp -> bio-format-structure -> bio-format-core`; no reverse edge. The structure crate owns shared entry/schema/residue/batch APIs, so Foldcomp never copies geometry logic. Keep these APIs narrowly scoped and version them together. `text-formats` enables PDB/mmCIF source modules and the native Gemmi build; it is default-on for the ordinary structure crate. Foldcomp uses `default-features = false` for that dependency so its standalone build need not compile the text parser. Verify the feature-off build; Cargo feature unification is expected when polars-bio enables both readers.

Use workspace versions and metadata consistent with current manifests; do not copy stale DataFusion 50.x numbers from old project documents. If registry publication is used, supply matching path+version dependencies for workspace crates and package native headers/sources/notices in the crate. Existing release automation currently centers on a repository tag; inspect new-crate inclusion and dependency updates explicitly.

## Native provider interface

Provide `StructureReadOptions` with output level, model selection, altloc policy, connectivity cutoff, supported residue population, storage/size controls, and explicit/auto text format. Foldcomp options embed the same structural output options and add a local database/FCZ source and mutually exclusive name/key selectors. Native options validate defaults and combinations once. Python converts user values to these options without reproducing selection or geometry rules.

`StructureTableProvider::try_new(sources, options)` and `FoldcompTableProvider::try_new(source, options)` are proposed constructors. Their schemas are determined by `level`; construction/schema access may perform bounded source/metadata validation but not decode every coordinate payload. Both provide finished atom/residue schemas and batches. Public point-math helpers remain independently testable Rust functions; they are not SQL functions in this scope.

A `NormalizedEntry` preserves available raw author/label identifiers, row ordinals, entity/component metadata, structural boundaries, and coordinates. Native parser/codec adapters return entries with owned lifetimes; borrowed data cannot escape a released CIF document or codec buffer. Source/entry indices come from the source manifest or database index before selection and never from names alone. Keep input-specific absent metadata null and source reconstruction provenance explicit.

`EntryBatchBuilder` is the shared provider utility for converting one normalized entry into projected atom or residue batches. Its contract includes schema/options, projection dependency closure, retained site/neighbor state, and terminal flushing. Prefer passing normalized Rust data directly; do not first build a full atom DataFrame/Arrow table only to regroup it for residue output.

## Execution sequence

1. Fix source options and discover an immutable source manifest. Explicit lists preserve occurrences; glob results are sorted. A new execution obtains fresh file identities; caches must validate those identities and mid-execution changes must not mix versions.
2. Prune whole sources/entries using proven exact selectors. For Foldcomp, scan index/lookup metadata and retain selected byte ranges before any payload decode.
3. Assign whole files/entries to partitions under the DataFusion target-partition/thread budget. Parse/decode each selected entry once in that execution.
4. Normalize identity and site order within the entry. Handle interleaved CIF rows explicitly. Keep model/chain/TER/polymer breaks in the normalized model.
5. For atom output, build the requested atom columns, retaining temporary predicate dependencies. For residue output, resolve coherent component/conformer candidates, establish links, then calculate requested descriptors from original neighboring context.
6. Apply any fully supported row predicate after its dependencies exist, build output batches, and let DataFusion handle unsupported residual predicates and global limits. Stop early only when doing so cannot remove needed neighbor data or bypass an unevaluated predicate.
7. Release current entry/parser/codec resources before advancing to the next entry, and drop them on cancellation/error.

An internal per-entry cursor carries assembly/lookahead across output batches. There is no optimizer-visible atom-to-residue plan edge through which DataFusion could repartition individual atom inputs. Repartitioning completed output rows is fine. Whole-entry parsing may retain a CIF document plus normalized data; report this memory honestly as a function of the largest selected entry and active workers. Use measured size limits and avoid a claim of constant memory within an arbitrarily large file.

## Pushdown contract

| Request | Correct implementation |
|---|---|
| File or FCZ entry selector | Schedule only selected source entries before payload reading |
| Whole-model/chain filter | Prune only after native identity is available and all selected sites retain required local context |
| Residue position/amino-acid/geometry filter | Evaluate after original-neighbor geometry; otherwise leave unsupported for DataFusion |
| `SELECT phi_deg` | Parse/retain previous C and current N/CA/C internally without emitting those columns |
| Atom/residue count only | Correct level-specific row count, including a zero-column RecordBatch row count; decoding can still be required |
| `LIMIT` | Keep global semantics in DataFusion; source hints must be safe with residual filters and next-residue lookahead |

Start conservatively with unsupported row filters and exact explicit source/entry selection; add exact native row filtering only after it enforces complete semantics. `Inexact` is appropriate only when a real pruning approximation is applied and the residual is preserved. Projection reduces Arrow/geometry work, not necessarily parser/codec bytes. Do not claim columnar text/FCZ I/O or count-from-header behavior without format-specific evidence.

## Parsing and geometry modules

Use a fixed-column Rust reader for the supported PDB records and a checked Gemmi low-level CIF adapter for mmCIF; both are subject to BF-0 corpus/packaging results. The low-level path is required to preserve valid nonnumeric author IDs and missing-value semantics. Do not use the inspected pdbtbx parser unchanged or convert CIF to PDB as an intermediate format.

`residue.rs` owns component classification/parent mapping, coherent altloc selection, missing-backbone retention, structural boundaries, standardized sequence adjacency where available, and configured C-N link checking. It takes normalized source metadata, not guessed sequence order from lexically sorted author IDs.

`geometry.rs` owns only deterministic Float64 calculations, degenerate/null results, units conversion, and the shared signed-angle convention. It receives selected point coordinates; it does not choose neighbors or interpret biological identity. `batch_builder.rs` combines these outputs with the fixed residue schema and flags. Unit tests for point math and policy tests for residue assembly remain independent even though both modules live in the same crate.

## Foldcomp specialization

The Foldcomp crate uses the upstream codec library build, excluding the CLI structure reader and its bundled Gemmi where possible. Verify the actual linked sources in BF-0. Decode arrays directly into normalized entries and reuse the same builder; never serialize PDB text or create Python atom lists in the production path.

Database metadata must distinguish names, titles, numeric keys and index positions. Define None as all entries, empty selectors as zero entries, deduplicated selectors as set membership, and absent/ambiguous names as errors. Numeric keys can select without lookup names if a valid supported index exists. `.source` is optional provenance. Validate dbtype, required sidecars, terminators, integer/range bounds and payload sizes before decoding.

For K requested entries in a large database, initially stream O(N) text metadata and retain O(K) selected metadata, decoding only K payloads. Instrument both costs; a full in-memory string catalog or O(K)-total-time claim is not required. Reusable compact catalogs are a later measured optimization. Local subsets are required for issue completion; remote database ranges and general tar archives are follow-ups.

Recompute descriptors from reconstructed coordinates. The official codec is the decoding compatibility oracle; source PDB measures compression loss separately. Float64 widening of raw Float32 codec coordinates does not restore lost precision or original metadata. Record compiler/architecture tolerance findings before finalizing native parity bounds.

## Test corpus and handoff to polars-bio

Own the canonical corpus and generator here. Freeze inputs, package/binary versions, source checksums, policy/schema versions, expected keys/counts/types/null masks/values, and any deliberate oracle discrepancies. Use independent raw CIF/PDB parsers, analytical vector examples, independent numerical libraries and the official codec according to the shared oracle plan. Normal test jobs read committed goldens offline; generation jobs require their pinned dependencies and cannot silently skip.

polars-bio receives a minimal content-addressed subset via a documented sync/copy tool that carries source hashes and corpus revision. Its wheel tests use local committed copies and fail on provenance drift. Research material already saved in the public proposal remains immutable evidence and is not automatically regenerated while revising this plan.

Native coverage must include all six descriptor definitions; model/chain/TER gaps; insertion codes and nonnumeric author IDs; whole-residue altloc policies; missing atoms; mixed file collections; interleaved CIF records; gzip; malformed inputs; tiny output batches; count/projection/filter/limit equivalence; and native cancellation/error cleanup. Subset tests must assert payload-decode counts, not just result equality.

## Implementation sequence and release boundary

Use [tasks.md](tasks.md): BF-0 contracts/oracles/dependency spike; BF-1 atom providers; BF-2 residue output; BF-3 Foldcomp; BF-4 benchmarks/packaging/release. polars-bio PB-1 consumes BF-2, PB-2 consumes BF-3, and final PB-3 consumes the BF-4 release result. Native residue output is complete before Python integration starts claiming residue support.

Release matching versions/commits for the two new formats crates and any shared formats dependencies. Update polars-bio's formats pins coherently; existing functions dependencies are outside this feature's release path. Native dependency/build feasibility is the first unresolved gate, followed by measured collection memory and supported platform checks. No product implementation was performed while preparing this plan.

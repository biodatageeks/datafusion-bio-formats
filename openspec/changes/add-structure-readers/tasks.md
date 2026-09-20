# datafusion-bio-formats implementation tasks

This checklist owns native work for #455. The [shared design](../../../../polars-bio/openspec/changes/add-structure-readers/design.md) defines the schemas and biological/numerical policies. [polars-bio tasks](../../../../polars-bio/openspec/changes/add-structure-readers/tasks.md) own Python/SQL integration and wheels. Implementation and validation evidence is recorded in IMPLEMENTATION.md. Unchecked platform/release-scale benchmark gates remain visible for review; they are not claimed by the local smoke runs.

## BF-0. Freeze fixtures and prove native dependencies

- [x] BF-0.1 Freeze options/schema/policy version 1 with polars-bio PB-0: identity fields, atom/residue levels, model selection, coherent conformers, peptide population/connectivity, outgoing omega, physical units, missing values, and selector semantics. The two-repository ownership is already agreed.
- [x] BF-0.2 Create testing/oracles/structure/ and testing/data/structure/ with pinned manifests, independent generators, tiny hand-authored fields/null masks/analytical geometry, 1UBQ paired formats, and the official small Foldcomp database. Inspect each additional real fixture before asserting it covers a domain feature.
- [ ] BF-0.3 Produce canonical atom/residue expected tables using independent raw parsing and numerical oracles; compare full keys/counts/types/null masks and freeze tolerances/discrepancies. Implement regeneration --check and reject input/output hash drift.
- [x] BF-0.4 Build a minimal Gemmi low-level CIF native bridge; demonstrate author ID X1, distinct auth/label fields, absent optional values, quoted/multiline tokens, multiple blocks, and row-order preservation. Avoid high-level hierarchy coercion and per-cell FFI calls.
- [x] BF-0.5 Build a checked official Foldcomp decoder-to-arrays bridge; inspect coordinate/name/numbering availability, angle padding/alignment, supported dbtype/layout, standalone FCZ, and record-terminator handling. Measure raw-array parity against the pinned codec.
- [ ] BF-0.6 Validate native builds and polars-bio PB-0 wheel imports on Linux/macOS/Windows targets; pin source versions/notices and feature combinations, exclude unnecessary CLI/Gemmi/OpenMP dependencies, and establish cross-platform numerical bounds.
- [x] BF-0.7 Record supported PDB extensions and Foldcomp layouts, default size/geometry-conditioning limits, known backend limitations, and explicit rejection behavior. Define the version/hash-checked fixture subset export to polars-bio.

Gate: reproducible independent expected tables, correct low-level identifiers/nulls, native decoder arrays, supported-platform smoke results, and a concrete parser/codec version choice. Revise the adapter/backend before implementation if a required gate fails.

## BF-1. Canonical structure crate and atom providers

- [x] BF-1.1 Add datafusion/bio-format-structure with workspace metadata, options.rs, schema.rs, model.rs and documented public provider/entry interfaces. Implement the text-formats feature boundary and a feature-off shared-model build.
- [x] BF-1.2 Implement the required PDB fixed-column records, model/TER preservation, original atom/author IDs, blank optional values, element/charge parsing, MODRES/HEADER metadata, checked limits, and contextual errors.
- [x] BF-1.3 Implement mmCIF raw category mapping through the BF-0 bridge: both identifier namespaces, entity/polymer/component metadata, numeric/missing tokens, coordinate-bearing blocks, interleaved rows, and rejection of unsupported coordinate layouts.
- [x] BF-1.4 Implement manifest.rs/storage.rs using core local/remote/gzip helpers, stable explicit-list occurrences and sorted glob expansion, source/block identities, whole-entry grouping, and configurable input/decompressed bounds. Reject ambiguous duplicate atom sites rather than overwriting them.
- [x] BF-1.5 Implement StructureTableProvider/StructureExec with fixed atom schema, immutable read options, fresh execution cursors, whole-entry/file partitioning and early resource cleanup. Schema discovery must not decode the whole collection.
- [x] BF-1.6 Implement atom EntryBatchBuilder projection and zero-column counts. Preserve temporary predicate dependencies and residual/global-limit semantics; start with conservative pushdown declarations. Add source/entry/bytes/decode metrics needed by correctness and performance tests.
- [x] BF-1.7 Add native tests for parsing, full atom oracles, models/altloc/TER/IDs, PDB/CIF common fields, gzip/plain, controlled remote storage, mixed collections, duplicate source names, malformed data, output batch sizes, and 1-versus-many partitions.

Gate: all atom keys/counts/types/null masks/values match frozen expectations, schema access avoids collection decoding, source identities survive projection/partition changes, and payload errors are recoverable with source context.

## BF-2. Residue output in the structure provider

- [x] BF-2.1 Implement geometry.rs with pure Float64 bond-angle/dihedral functions, canonical degree ranges, finite/degenerate handling, and analytical +/-90/180/null cases. Test identical-coordinate parity against independent libraries.
- [x] BF-2.2 Implement residue.rs grouping by structural site; classification and pinned parent/one-letter mapping; coherent altloc/component selection; explicit-alt mode; occupancy/tie rules; and incomplete-residue retention. Never mix mutually exclusive backbone atoms.
- [x] BF-2.3 Implement model/chain/segment/polymer boundaries, standardized sequence continuity where available, and configured C-N link checking. Test author gaps versus label gaps, TER with reused chain IDs, insertion codes, conformer conflicts, and cutoff boundaries.
- [x] BF-2.4 Extend EntryBatchBuilder with a residue assembler and previous/current/next context over normalized entries. Compute N/CA/C/O coordinates, all six backbone quantities and status/link columns; flush termini on entry changes and EOF.
- [x] BF-2.5 Select atom/residue schema and builders inside StructureTableProvider/StructureExec using level. Calculate neighbor-dependent quantities before unsafe residue predicates/limits; compute a projection dependency closure without constructing an intermediate atom DataFrame.
- [x] BF-2.6 Validate output batches splitting residue/neighbor context, phi-only projections, filtered middle residues, rootless/count queries, limits, 1/many partitions, and entry cleanup. Use full-scan-then-transform as the query result oracle.
- [x] BF-2.7 Pass synthetic and real residue oracles, exact null masks, full key/count equality, numeric tolerances and metamorphic checks. Export the pinned corpus subset and a compatible native provider revision for PB-1.

Gate: both output levels are complete in the native provider. Missing atoms, chain breaks, conformers and batch sizes do not change unrelated descriptors; PB-1 can expose PDB/mmCIF residues without any geometry implementation.

## BF-3. Foldcomp sources using the shared residue implementation

- [x] BF-3.1 Add datafusion/bio-format-foldcomp, depending one-way on the structure crate's shared API with text parsing features disabled. Package the checked codec adapter and upstream sources/notices; keep versions compatible with BF-2.
- [x] BF-3.2 Implement standalone FCZ and supported local database recognition; validate dbtype and required index/lookup sidecars, optional source metadata, integer conversions, duplicate keys, selected byte ranges, terminators and payload limits.
- [x] BF-3.3 Implement mutually exclusive ids/entry_keys selectors, None/all versus empty/zero, deduplication, absent/ambiguous ID errors, and numeric selection without lookup. Keep lookup name, internal title, key and database ordinal separate.
- [ ] BF-3.4 Stream/select text metadata with O(K) retained selection state for small subsets, then partition selected entries by available compressed sizes. Validate cached companion identities and avoid a per-query full string hash map unless measured workloads require it.
- [x] BF-3.5 Map codec arrays and available metadata to NormalizedEntry, preserving reconstructed provenance/null fields. Feed the same EntryBatchBuilder for atom or residue output; recompute geometry from reconstructed coordinates instead of copying codec angle arrays.
- [x] BF-3.6 Implement FoldcompTableProvider/FoldcompExec with fixed schemas, native worker bounds, fresh cursors and cleanup on cancel/error. Instrument metadata reads, payload bytes, entries selected and entries decoded separately.
- [x] BF-3.7 Pass official raw-array/decoded-identity parity, common residue goldens, empty/missing/duplicate selectors, title/lookup differences, changed/corrupt sidecars and malformed FCZ tests. Use sanitizers/subprocess fuzzing for the native boundary. Export a compatible revision and fixtures for PB-2.

Gate: exactly K selected distinct payloads decode, zero for an empty selector; coordinates and descriptor null masks/values match pinned oracles; index/codec failures are contextual errors. Metadata O(N) scanning is reported separately from K payload decodes.

## BF-4. Native performance, packaging, and release

- [ ] BF-4.1 Benchmark parse-only, atom-to-Arrow, residue-to-Arrow and selective FCZ reads against matched oracle work. Record versions/inputs, cold/warm cache, wall time, first batch, RSS, bytes, decode counts and 1/2/4/8-worker scaling; coordinate with PB-3's full Python-visible benchmark.
- [x] BF-4.2 Verify decoded structures are released between entries, collection memory is bounded by active entry buffers, and native work stays within the configured worker budget. Document parser DOM overhead, largest-entry bounds and supported layouts.
- [x] BF-4.3 Optimize only measured bottlenecks, re-running affected golden/policy/execution tests after parser, projection, scheduling or decoder changes. Preserve current correctness gates during any fast-path work.
- [ ] BF-4.4 Run new crate tests, feature-off/shared-module tests, format/clippy checks, required workspace CI and native boundary jobs; validate package contents/native build inputs across supported platforms together with PB-3 wheel tests.
- [x] BF-4.5 Document both Rust provider constructors/options/schemas and native memory/error/selector behavior. Add new crates to release/version/CI handling using the repository's actual publication workflow.
- [ ] BF-4.6 Release compatible structure/Foldcomp/shared formats versions or a supported repository tag/commit and hand that pin to polars-bio. No functions version bump is required. Close native work only after its release artifacts and fixture version are consumable by PB-3.

Native verification:

```sh
cargo test -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp
cargo test -p datafusion-bio-format-structure --no-default-features --lib
cargo fmt --all -- --check
cargo clippy -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp --all-targets --all-features -- -D warnings
openspec validate add-structure-readers --strict
```

Run required workspace CI before release, and run the pinned oracle generator's verification job separately from offline correctness tests. A native codec/parser failure or a missing required oracle dependency must not silently skip the corresponding gate.

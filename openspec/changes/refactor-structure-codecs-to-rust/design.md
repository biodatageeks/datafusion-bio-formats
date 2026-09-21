# Rust structure parser and Foldcomp decoder migration

## Baseline and boundaries

The fetched formats baseline is `fd17754`; the consumer baseline is polars-bio
PR #461 at `ea24d4a`. Current manifests and `rust-toolchain.toml` are authoritative:
the formats toolchain is Rust 1.91.0. Some older OpenSpec project/dependency text
still describes earlier DataFusion and Rust versions; this port does not upgrade
the workspace toolchain, DataFusion or Arrow.

The original `add-structure-readers` implementation is merged upstream but its
proposal remains active with unfinished release/oracle/benchmark tasks. Reuse
its corpus and record unresolved checks; do not assume every original gate passed.
This change supersedes its Gemmi-specific backend decision while preserving the
`text-formats` feature boundary and the shared structural contract.

| Area | Current implementation | Planned change |
| --- | --- | --- |
| CIF syntax/document ownership | `src/native_cif.rs`, `native/cif_bridge.cpp`, Gemmi/PEGTL headers | Rust raw document/token parser |
| mmCIF category-to-atom mapping | `bio-format-structure/src/mmcif.rs` | Retain mapping; adapt its document source |
| PDB and structural processing | `pdb.rs`, `model.rs`, `residue.rs`, `geometry.rs`, `batch_builder.rs` | Keep behavior and existing tests |
| FCZ decode/FFI | `bio-format-foldcomp/src/codec.rs`, `native/codec_bridge.cpp`, vendored codec | Checked byte reader and Rust reconstruction |
| Foldcomp local database handling | `bio-format-foldcomp/src/manifest.rs` and `src/lib.rs` | Retain selection, limits and provider execution |
| Packaging | Both crate `build.rs` files and direct `cc` dependencies; consumer notices | Remove replaced C++ build inputs and revise provenance |

## Decisions

1. Preserve provider and consumer APIs. Both new backends feed the existing
   `NormalizedEntry` model, so the port does not require a second residue model.
2. Implement only functionality used by the current readers. FCZ encoding,
   full Gemmi functionality and new file formats are separate work.
3. Implement the CIF tokenizer/document parser inside this repository, as
   explicitly requested by the user. Use the published CIF grammar and frozen
   compatibility tests to define behavior. Existing implementations can inform
   the design and tests; no external parser crate becomes the production backend.
4. Implement the Foldcomp decoder inside this repository as requested. Port the
   needed decoding algorithms with attribution and checked Rust data access.
   [Research](research.md) records reference implementations and their limits;
   none substitutes for our decoder or its differential tests.
5. Keep reference execution outside production crates: build the pinned legacy
   implementation in a separate checkout/process for comparisons, and use frozen
   goldens for ordinary offline tests. Do not add public backend flags or silently
   fall back to C++ when Rust decoding fails.
6. Use safe Rust for the replacement modules. Parse integers and floats from
   explicit little-endian bytes with checked arithmetic and bounded allocations;
   do not reproduce C struct casts, pointer ownership or unchecked buffer reads.
7. First match current memory behavior and results. Streaming optimizations can
   follow once parity is established; the port does not promise constant memory
   for a single large document or decoded entry.

## CIF implementation

Introduce `bio-format-structure/src/cif/` with tokenizer/document modules.
Preserve the operations currently consumed
by `mmcif::Blocks`: parse input, count/access blocks, expose block names and raw
tag columns. Update `mmcif.rs` and `lib.rs`; remove `native_cif.rs` after cutover.
Use owned input/storage and checked views; borrowing details must not leak into
public provider APIs.

Freeze a compatibility table before implementation:

- comments and whitespace; LF/CRLF; both quote styles, embedded quotes and
  semicolon text fields; loop values crossing physical lines;
- unquoted `.` and `?` as missing, quoted equivalents as literal strings;
- case-insensitive tags without changing case-sensitive values or block labels;
- multiple blocks, scalar items, reordered columns and unknown categories;
- both author and label namespaces, author ID `X1`, insertion codes, all atoms,
  all models, altlocs, nonpeptides and original row order;
- `_atom_site`, `_entry`, `_struct_asym`, `_entity_poly`, `_chem_comp`, and
  `_pdbx_struct_mod_residue`, including metadata located after atom rows;
- invalid loop widths, missing values, malformed quoting, duplicate names and
  encoding errors, with source/block context and bounded failure;
- save frames/STAR extensions and numeric standard-uncertainty notation: measure
  actual baseline acceptance and exposed behavior. Do not infer support from the
  old proposal or expand/narrow it incidentally during this port.

Current `mmcif.rs` converts numeric strings with Rust parsing and preserves
biological policy in Rust. Syntax parsing and numeric/schema interpretation must
remain separate. Keep input/decompressed/atom limits and release completed entry
buffers as execution advances. Validate ignored categories syntactically even
when their values are not needed for structural output.

Our parser must expose raw syntax before filtering, normalization or Float32
conversion. Malformed input must retain the current explicit failure behavior;
partial silent success is unacceptable.

## Foldcomp implementation

Replace the C ABI implementation behind `codec::decode` with modules under
`bio-format-foldcomp/src/codec/`:

| Module | Responsibility |
| --- | --- |
| `header.rs` / `bitstream.rs` | Explicit FCZ layout, offsets, packed backbone records, checked lengths |
| `discretize.rs` | Restore encoded torsions, bond angles and B factors |
| `backbone.rs` | NeRF construction, anchor segments and reverse correction |
| `sidechain.rs` / `residue_tables.rs` | Full residue reconstruction, atom order and residue constants |
| `mod.rs` | Decode orchestration and mapping to `NormalizedEntry` |

Names are a proposed file split, not a new public crate API. Stage 0 must document
the byte layout from the pinned upstream reader/writer, including header padding,
field sizes, first/last residue fields, title bytes, anchors, OXT, eight-byte
backbone records, side-chain bytes and per-residue B factors. Validate the layout
with hand-authored byte fixtures, not only upstream-generated examples.

Port the decoder in dependency order:

1. Parse/validate the header and all variable sections with checked additions and
   multiplications. Establish bounds before allocation or indexing.
2. Decode packed residue/angle fields and restore discretizers; compare these
   intermediate arrays with the pinned reference before coordinate generation.
3. Reconstruct backbone segments, forward and reverse anchor correction, and
   joins. Multi-anchor chains are mandatory fixtures.
4. Reconstruct all supported residue side chains with upstream atom ordering,
   residue numbering, chain IDs, OXT and B factors. Include backbone-only UNK,
   short chains, nonzero starting indices and every supported residue code.
5. Map reconstructed Float32 values into the existing Float64 public model and
   invoke the same Rust normalization/residue/geometry path.

Preserve the current supported FCZ/layout contract. No claim of arbitrary FCZ
version support or new big-endian support follows from explicit byte parsing.
For malformed input, safety takes precedence over reproducing an upstream crash;
document deterministic rejection and add the minimized input as a regression.

Copy the current adapter's validation intent: magic/size checks, increasing anchor
indices, finite scalar fields, first/last residue consistency, side-chain count,
OXT validity, and both declared and reconstructed atom limits. Derive decoded
atom counts from residue codes before allocation; `nAtom` alone is insufficient.

Keep local type-12 database selection in the existing Rust manifest/provider.
Exactly K distinct selected records must invoke the decoder; empty selections
invoke it zero times, and corruption in an unselected payload stays irrelevant.

## Acceptance gates

### Results and numerical compatibility

First require identical schemas, row counts/order, full identifiers, atom names,
null masks, selection behavior and contextual errors. Test atom and residue
levels, SQL, projection/counts, filters/limits, repeated/concurrent collection,
gzip and controlled remote text sources.

The committed oracle manifest supplies the initial numerical ceilings:

| Comparison | Existing ceiling |
| --- | --- |
| Text coordinates | `1e-9` angstrom |
| Text-derived angles | `1e-6` degrees, circular difference for torsions |
| Foldcomp reconstructed coordinates | `1e-4` angstrom |
| Foldcomp-derived angles | `0.01` degrees, circular difference for torsions |

Retain any stricter existing test assertions. Compare FCZ decoding with the same
encoded input and upstream decoded arrays, never with the original precompression
PDB using a looser compression RMSD. Report worst-case and percentile coordinate
errors, not only an aggregate RMSD. Preserve Float32 operation ordering initially;
do not introduce FMA, fast-math or Float64 reconstruction as an incidental change.
Measure B-factor parity and freeze its bound during baseline capture.

Bitwise equality across different compilers/platforms is not promised. Characterize
the existing reference on each supported architecture and freeze tolerances before
cutover. Failures must be investigated; widening tolerances is a separate explicit
contract decision. Check near-degenerate geometry and peptide-link thresholds
because small coordinate errors can change null masks and connectivity.

### Coverage and robustness

Expand beyond 1UBQ and the small example database: checked small fixtures must
cover all residue codes, long/multi-anchor proteins, terminal OXT, metadata-rich
mmCIF, multiple models/blocks and syntax boundaries. Store provenance, generator
versions, hashes and coverage rationale. Keep ordinary tests offline; put larger
benchmark inputs in a reproducible download manifest rather than Git.

Use three comparison levels: handwritten byte/syntax cases; pinned native decoded
arrays; independently parsed/analytical output checks. Add property tests and
fuzz targets for CIF lexing/document construction, FCZ headers/records/full decode,
and sidecar-selected ranges. Malformed inputs must terminate with errors without
panics or unbounded allocation. Proposed pre-cutover fuzz budget: at least 24 CPU
hours per decoder target, with seeds and run metadata recorded; smoke budgets run
on PRs and all discovered regressions run offline thereafter.

### Performance and platform checks

Compare release builds of the old and new implementations using identical input,
schema, filters, worker counts and materialization. Separate syntax/codec timing,
atom-to-Arrow, residue-to-Arrow, Python collection and small-subset database reads.
Record wall time, peak RSS, first-batch latency, bytes and decoded-entry counts for
1/2/4/8 workers; measure warm and cold-cache cases separately where feasible.

Proposed decision threshold: no repeatable regression greater than 10% in median
end-to-end time or peak RSS on the agreed representative cases. This is a proposed
gate, not a measured result or a speedup claim. Stage 0 freezes inputs, repetitions,
measurement noise and acceptance thresholds. Correctness cannot be traded for speed.

Run Rust tests and installed-wheel tests on Linux x86_64/arm64, macOS x86_64/arm64,
and Windows x64. A cross-built wheel must be tested on a compatible architecture.
Check structure default/no-default features and Foldcomp with/without text formats.
The final structure/codec packages and build logs must contain no Gemmi/Foldcomp
C++ compilation, `bio_cif_*`/`bio_fc_*` bridge symbols or replacement-module FFI.
Other workspace dependencies may still use `cc` or native libraries.

## Delivery sequence and effort

| Stage / review unit | Deliverable and exit gate | Dependency | Rough engineer-days |
| --- | --- | --- | --- |
| R0: contract and internal design | Corpus map, raw-reference harness, parser/codec design, numeric/performance budgets | None | 2–4 |
| R1: Rust CIF parser | Raw syntax parity, unchanged mmCIF/provider tests, remove Gemmi production path | R0 | 4–8 |
| R2: FCZ byte decoding | Documented layout, bounded parser, exact discrete/intermediate-array parity | R0 | 3–5 |
| R3: full Rust FCZ reconstruction | Anchor/side-chain/all-atom/metadata parity and unchanged subset execution | R2 | 8–15 |
| R4: hardening and cutover evidence | Expanded corpus, fuzzing, measured release benchmarks, platform/feature matrix | R1 + R3 | 5–8 |
| R5: consumer and distributions | Compatible formats pin, Python/wheel parity, package/provenance cleanup | R4 | 2–4 |

Budget approximately 24–44 focused engineer-days (roughly 5–9 working weeks for
one developer), plus review/release waiting time. These are planning estimates,
not commitments. CIF syntax coverage can expand R1; numerical drift or uncovered
FCZ variants can expand R3. Re-estimate after R0. R1 and R2/R3 have independent
implementation paths once the shared contract is frozen.

Merge in reviewable units. On 2026-09-21 the user explicitly authorized switching
both production readers to Rust, removing native sources and updating licenses
while the outstanding performance/fuzz checks continue. This supersedes the
previous requirement to defer the production switch until every gate completes;
it does not mark those open checks passed or relax numerical/performance limits.
Each switch remains reversible by reverting its commit or restoring the recorded
consumer dependency pin. Retain the external, immutable reference tooling and
its source/license provenance independently of production dependencies.

## Packaging, provenance and consumer handoff

After each accepted switch, remove its C++ bridge/vendor build and direct `cc`
dependency if unused. Keep `text-formats` behavior useful for Foldcomp-only users.
If code/constants are translated or adapted, preserve source attribution and
applicable notices; changing the implementation language alone is not a basis
for deleting them. Independently implemented CIF parsing should cite the syntax
specification and its actual dependencies. Preserve fixture provenance separately.

In polars-bio, update all formats pins together, run the existing structure,
Foldcomp, MSA and shared IO regressions, build wheels/sdist, and test installed
wheels outside the checkout. Audit contents before changing
`polars_bio/licenses/structure/` and the `pyproject.toml` include list. Keep every
notice still needed by translated code or other bundled material.

## Open decisions to close in R0

- Freeze the internal CIF tokenizer/document interface and supported syntax
  using the published grammar and baseline fixture probes.
- Freeze the expanded corpus, B-factor/numerical bounds and performance budgets.
- Decide whether a reusable standalone decoder crate is warranted; default to
  internal modules, with no new public API or publication overhead.
- Record any baseline behavior that is unsafe, unspecified or contradicts older
  design text; separate fixes from compatibility-preserving port commits.

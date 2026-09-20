# R0 contract and internal design

Recorded 2026-09-20 on branch `feat/rust-structure-codecs`. This document closes
the local source/design/fixture work and identifies the remaining R0 gate.
Actual runs are recorded separately in [IMPLEMENTATION.md](IMPLEMENTATION.md).

## Revisions and inherited requirements

Formats source: `fd17754c55c63394717967c18b7a45cf8aeb48ee`.
Consumer reference: polars-bio PR #461 at
`ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa`. Neither reference is advanced here.
Use Rust 1.91.0 (the repository toolchain), DataFusion 53.0.0 and the existing
Arrow 58 dependency family. No dependency/toolchain upgrade is part of the port.

The active `add-structure-readers` requirements for schemas, raw identifiers,
encounter order, model/altloc policy, peptide links, geometry, size limits,
lazy scans and database selectors remain authoritative. Its outstanding
BF-0.3 (complete independent tables), BF-3.4 (metadata selection memory),
BF-4.1 (performance), BF-4.4 (platform/package matrix) and BF-4.6 (release)
are not silently marked complete by this new baseline. O(K) database metadata
work stays outside the codec replacement. This proposal replaces only the
Gemmi/native-backend decision, not the structural/provider contract.

The source reader already checks `max_input_bytes` and `max_decoded_bytes` before
constructing CIF documents. `mmcif::Blocks::entry` checks `max_atoms` before atom
construction. Defaults remain 256 MiB, 512 MiB and 5,000,000 atoms respectively.
The low-level `Blocks::parse` accepts bytes without `StructureOptions`; do not
invent new public limits in the port. Allocation/indexing inside the parser must
be checked and proportional to actual input, never an input-declared count alone.

Feature contract: structure's default `text-formats` enables CIF/PDB/storage;
feature-off retains the common model/providers without a CIF backend. Foldcomp
depends on that common model with default features disabled and forwards its
optional `text-formats` feature. PDB, residue/geometry and database selection
algorithms remain unchanged.

## Measured CIF compatibility

The [355-case manifest](../../../testing/oracles/structure-codecs/manifest.json)
identifies every input/hash and the associated observation. All 56 CIF cases
are exercised through the private `Document` adapter by offline Rust tests.

| Input | Measured exposed behavior |
| --- | --- |
| Empty input/comments | Zero blocks, successful raw parse |
| Tags | ASCII case-insensitive, exposed in lowercase |
| Values/block labels | Preserve spelling, case, row order and empty strings |
| Unquoted `.` / `?` | Missing (`None`); quoted equivalents remain strings |
| Single/double quotes | Embedded quote allowed unless it closes at whitespace/comment/EOF; LF within quotes rejected, bare CR accepted |
| Semicolon fields | Column-one delimiter; strip delimiters and the final LF/CRLF, preserve interior CRLF and leading newline |
| Comments | Ignored at token boundaries; `alpha#beta` is one unquoted value |
| Loops | Row width enforced, `stop_` accepted, empty loops accepted at EOF/next block |
| Blocks | Original labels preserved; bare `data_` label becomes one space; `global_` label is empty and can repeat |
| Duplicate names | Case-insensitive duplicate block, top-level tag and frame names rejected |
| Save frames | Syntax checked, values hidden by the adapter; duplicate tags inside a frame accepted by this baseline |
| UTF-8 | Quoted non-ASCII accepted; unquoted non-ASCII rejected; exposed invalid UTF-8 rejected when materializing the block |
| Ignored bytes | Invalid UTF-8 in comments/ignored frames accepted; NUL in exposed quoted CIF values preserved by length-aware strings |
| Unsupported syntax | BOM, nested frames, standalone `stop_`, unquoted `$` references, missing values, incomplete loops rejected |
| Raw numeric tokens | `12.3(4)`, `nan`, `inf` remain raw strings; biological numeric conversion is separate |

The retained Rust mapping uses `FromStr` and finite-float checks, so accepting a
raw uncertainty token is not a promise to parse that coordinate as a number.
Unknown categories remain syntactically validated. Preserve `_atom_site`,
`_entry`, `_struct_asym`, `_entity_poly`, `_chem_comp` and
`_pdbx_struct_mod_residue`, including metadata after atom rows. The corpus
preserves author ID `X1`, nonmonotonic atom/model IDs and quoted author chain IDs.
Existing structure/policy tests remain responsible for their biological mapping.

Do not silently fix surprising baseline behavior during the replacement. An
intentional behavior change needs a separate regression/contract decision.
Error text need not reproduce upstream messages, but parse/view failures must
retain a byte/line and block/tag context where known. Callers add source paths.

## Internal CIF interface

Keep the operations consumed by `mmcif.rs`:

```rust
Document::parse(data: &[u8]) -> Result<Document>
Document::block_count(&self) -> usize
Document::block(&self, index: usize) -> Result<CategoryBlock<'_>>
```

`CategoryBlock` retains its borrowed name and
`HashMap<&str, Vec<Option<&str>>>` columns during initial integration. No parser
types become public provider API. The document owns a single byte buffer and
private blocks/columns; tokens/cells hold checked byte ranges and a flavor
(bare, quoted, semicolon, dot-null or question-null), not self-referential Rust
borrows. Normalize tag names once. Preserve lexical null provenance internally
while mapping both null kinds to `None` in the existing view.

Scan bytes, not a whole-input `str`: validating all UTF-8 eagerly would reject
accepted ignored comments/frames. Validate exposed ranges when building views.
No `unsafe`, lifetime extension or native handle is needed. Borrowed views
cannot outlive the document. Parse frames with bounded, nonrecursive state;
validate their loops/values without retaining hidden columns. Check duplicate
names at the same scopes shown by the probes. Checked source ranges and token
counts bound storage by input length; do not preallocate from untrusted counts.

Implementation split: `cif/tokenizer.rs` (byte syntax, spans, locations),
`cif/document.rs` (blocks, columns, duplicate/loop validation), `cif/mod.rs`
(private adapter API). Move `cif_contract_tests.rs` with the adapter at cutover.
Use the published CIF syntax and these measured compatibility extensions for
the repository implementation; do not translate Gemmi's PEGTL implementation.

## FCZ layout

All offsets are absolute from the beginning of the FCMP file. This is the one
legacy layout currently supported; it has no explicit version field. Endian
assumptions are explicit, not Rust/C struct casts.

| Offset | Bytes | Field |
| --- | ---: | --- |
| 0 | 4 | ASCII `FCMP` |
| 4, 6, 8, 10 | 2 each | LE `u16`: residue count, declared atom count, first residue index, first atom index |
| 12, 13 | 1 each | Anchor count, chain byte |
| 14 | 2 | Ignored C struct padding |
| 16 | 4 | LE `u32` sidechain torsion byte count |
| 20, 21 | 1 each | First/last one-letter residue |
| 22 | 2 | Ignored padding |
| 24 | 4 | LE `u32` title byte length |
| 28 | 24 | Six LE Float32 minima: phi, psi, omega, N-CA-C, CA-C-N, C-N-CA |
| 52 | 24 | Six Float32 continuation factors in the same order |
| 76 | `4*A` | LE signed 32-bit anchor indices |
| `76+4*A` | `T` | Title bytes, including embedded NULs |
| `C=76+4*A+T` | `36*A` | Each anchor's N/CA/C xyz Float32 coordinates |
| `C+36*A` | 13 | OXT byte flag then three Float32 coordinates (present even when flag is zero) |
| `B=C+36*A+13` | `8*R` | Packed backbone records |
| `B+8*R` | `S` | One unsigned byte per sidechain torsion, **not** packed 4-bit fields |
| `B+8*R+S` | 8 | Float32 B-factor minimum and continuation factor |
| `B+8*R+S+8` | `R` | One B-factor code per residue |

Exact length is `B + 9*R + S + 8`. Require 2+ residues, 2..R anchors,
declared atoms >=3*R and <=max_atoms. Anchors strictly increase from 0 to R-1.
Require finite minima/factors/anchor/OXT/B-factor fields, OXT flag 0/1, first/last
residue consistency, exact sidechain count, and no trailing bytes. Padding has
no meaning. Validate checked lengths before borrowing sections or allocating.

Packed backbone bytes `b0..b7` decode as:

```text
residue = b0 >> 3
omega   = ((b0 & 7) << 8) | b1
psi     = (b2 << 4) | (b3 >> 4)
phi     = ((b3 & 15) << 8) | b4
CA-C-N  = b5; C-N-CA = b6; N-CA-C = b7
```

Codes 0..19 are the twenty standard amino acids. Codes 20/21/22 map to
ASX/GLX/STOP and are rejected because reconstruction tables are absent.
Codes 23..31 all map to backbone-only UNK and are accepted. The output bound is
the sum of `max(3, residue_table_atom_count)` plus OXT, independent of `nAtom`.
The corpus includes a header that fits the limit but reconstructs too many atoms.

The legacy FFI truncates title/chain at NUL before UTF-8 checking. Preserve this
observable behavior at initial cutover. OXT's residue number comes from R, not
`first_residue_index + R - 1`; the shifted-index OXT fixture exposes that quirk.
Final atom IDs are sequential from the header's first atom index.

## Foldcomp algorithm/module map and attribution

The internal crate module boundary stays as designed: no extra published crate.
Construct a private validated `EncodedEntry<'a>` only after checked header,
section, code and bound validation. Borrow packed/sidechain/B-factor bytes;
allocate decoded parameter and coordinate arrays only after counts are proven.
Keep header fields/indices private to prevent unchecked construction.

| Rust module | Pinned upstream behavior to reproduce |
| --- | --- |
| `header.rs`, `bitstream.rs` | `codec_bridge.cpp::validate`, `Foldcomp::read/read_header`, `convertBytesToBackboneChain` |
| `discretize.rs` | `Discretizer::continuize`, `decompressBackboneChain`, `FixedAngleDiscretizer(255)`; Float32 multiplication then addition |
| `backbone.rs` | `reconstructBackboneAtoms`, `Nerf::place_atom`, `reconstructBackboneReverse`, `reconstructWithReversed`, `weightedAverage` |
| `residue_tables.rs`, `sidechain.rs` | `AminoAcid::AminoAcids`, residue/code/count maps, `Nerf::reconstructAminoAcid` |
| `mod.rs` | `Foldcomp::decompress` orchestration, OXT/B factors/sequential numbering, existing `codec::decode` mapping and normalization |

Match torsion interleave psi/omega/phi and bond-angle interleave CA-C-N/C-N-CA/
N-CA-C. Segments overlap at anchors; the final segment includes the last residue.
Reverse correction uses bond angles measured from the forward reconstruction
and weights atom i as `((forward*(N-i)) + reverse*i)/N`. The last atom is not
weighted entirely to the reverse result. Join by dropping the last three atoms
of every nonfinal segment. Sidechains use a full byte mapped from -180..180,
the table's predecessor atoms/bond lengths/angles, and original table atom order
(`useAltAtomOrder=false`). B factors apply once per residue, including terminal
OXT's final-residue value.

Maintain Float32 storage and operation order; the upstream angle-to-radian
expression promotes through double-precision `M_PI` before assignment to float.
An incidental all-Float32 expression or fused multiply-add can change results.
Use existing Float64 widening only when constructing the shared model.
Retain applicable Foldcomp MIT copyright/license notices for translated
algorithms/constants. The upstream sources remain the attribution authority;
changing language does not remove attribution obligations.

## Remaining gate

The local reproducible harness, corpus and internal interfaces are in place.
R0.7 remains open: characterize the reference on Linux x86_64/arm64, macOS
x86_64/arm64 and Windows x64, freeze representative release benchmarks and
noise/repetition budgets, and establish a cross-platform B-factor ceiling.
Local synthetic B factors and independent 1UBQ coordinates match exactly.
That observation is not cross-platform evidence. Exact B-factor comparison is
retained in the new local Rust test until measured evidence justifies a change.

Uncovered before production cutover: large/long-chain and near-degenerate
geometry, threshold-sensitive peptide links, sustained fuzz budgets,
performance/RSS, full workspace and distribution matrix. Retain the design's
existing coordinate/angle tolerances and proposed performance/fuzz thresholds.
Do not remove either native backend while these migration gates remain open.

### Measured inverse-discretization contraction (2026-09-20)

The captured Apple Clang 16 arm64 reference emits FMADD for `code * factor +
minimum`. Separate Float32 operations differ by two ULP for side-chain code 93.
The Rust candidate therefore explicitly uses `mul_add`; all captured restored
parameter arrays match bit-for-bit. The earlier warning about incidental fusion
remains applicable: contraction decisions must follow measured reference
behavior. This observation does not establish other-platform compatibility.

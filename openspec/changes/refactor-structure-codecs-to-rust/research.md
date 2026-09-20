# Planning evidence — 2026-09-20

This is source inspection and an initial alternative survey. The user subsequently
confirmed that both replacements must be implemented in this repository. The
external projects below are reference material, not proposed production backends.
No candidate integration,
performance benchmark, Rust parser prototype or codec port was run for this plan.

## Local implementation inspected

Formats baseline: `fd17754c55c63394717967c18b7a45cf8aeb48ee`.

- `datafusion/bio-format-structure/src/native_cif.rs`: raw `Document`/`CategoryBlock`
  interface and C ABI ownership. `mmcif.rs` already owns category interpretation.
- `datafusion/bio-format-structure/native/cif_bridge.cpp`: Gemmi parsing, raw/null
  value extraction, lowercased tags and source block labels.
- `datafusion/bio-format-foldcomp/src/codec.rs`: decoded Float32 arrays widened
  into the common model, with shared normalization afterward.
- `datafusion/bio-format-foldcomp/native/codec_bridge.cpp`: header/section/anchor
  validation, residue-derived output bounds and direct upstream decode calls.
- `native/vendor/foldcomp.cpp` in that crate: packed records, discretization,
  forward/reverse anchor reconstruction, side chains, B factors and atom order.
- `testing/oracles/structure/manifest.json` and `generate.py`: frozen inputs,
  independent oracle generation and current numeric tolerances.
- `.github/workflows/structures.yml`: current portability, oracle and native
  sanitizer checks. Feature and architecture coverage must be expanded/verified.

The original planning documents mention broader gates than their checked tasks
establish. Their remaining release-scale/performance/platform work is not evidence
that a Rust replacement is already characterized.

## CIF alternatives surveyed before the implementation decision

| Candidate | Evidence | Planning decision |
| --- | --- | --- |
| Repository-owned raw CIF parser | Existing adapter exposes a narrow raw-document interface; IUCr defines syntax | Selected direction, explicitly confirmed by the user |
| `cifflow_core` | Rust event parser with raw values and missing-token provenance; Apache-2.0 repository | Useful comparison material for events/errors; high-level objects normalize names |
| `pdbtbx` 0.12.0 | Rust PDB/mmCIF library with a protein hierarchy | Comparison material; its high-level hierarchy is not evidence of our raw identifier/category contract |
| `kira-mmcif` 0.3.0 | Documented protein/backbone output, numeric author IDs, Float32 coordinates and first-coordinate-block behavior | High-level API is not a compatible replacement for all-atom/raw-category output |
| `ustar-parser` 0.1.4 | General STAR/CIF parser; crate metadata reports LGPL-3.0-only and Rust 1.83 | General syntax reference; no dependency adoption planned |

At inspected cifflow commit `acb83c3f50de3b3d1cfa05fc6c43f4c44ba3d812`, the
`cifflow_core` manifest unconditionally depends on PyO3 and Arrow with Python
support. A parser-only integration would need an upstream feature split or a
small attributed extraction; the Python package is not a runtime solution here.
Its recovery and name-normalization behavior also need an explicit adapter.

Useful primary sources:

- [IUCr CIF 1.1 syntax](https://www.iucr.org/what-we-do/digital-standards/cif/cif1/file-syntax)
- [Gemmi CIF parser behavior](https://github.com/project-gemmi/gemmi/blob/5cc1c23c6007e0e6cbd69289c6f7c0bff50e943e/docs/cif.rst)
- [cifflow source](https://github.com/rowlesmr/cifflow/tree/acb83c3f50de3b3d1cfa05fc6c43f4c44ba3d812/cifflow_core)
- [pdbtbx API](https://docs.rs/pdbtbx/0.12.0/pdbtbx/)
- [kira-mmcif 0.3.0 contract](https://docs.rs/crate/kira-mmcif/0.3.0)
- [ustar-parser registry metadata](https://crates.io/api/v1/crates/ustar-parser)

## Existing Rust Foldcomp work

Registry metadata lists `proxide-io` 0.1.0-alpha.16, MIT, in
`maraxen/proxide`. Source at commit
`bf1286ccdc580cf2e03cab4ebc3576e1d6c6a21b` includes a Rust FCZ reader with
packed-backbone extraction. The inspected reader transmutes the header and skips
anchor indices, title and inner/last anchor records before reconstruction. Those
behaviors require scrutiny against our validated layout, metadata and anchor
requirements; this is not evidence of a compatible full decoder.

- [Inspected Rust decoder](https://github.com/maraxen/proxide/blob/bf1286ccdc580cf2e03cab4ebc3576e1d6c6a21b/crates/proxide-io/src/formats/foldcomp.rs)
- [Pinned upstream Foldcomp decoder](https://github.com/steineggerlab/foldcomp/blob/89e37195d3c8ade8d40ead91ad82e6cd2964a967/src/foldcomp.cpp)
- [Pinned upstream residue/geometry code](https://github.com/steineggerlab/foldcomp/tree/89e37195d3c8ade8d40ead91ad82e6cd2964a967/src)

Planning conclusion: implement a complete checked decoder in this repository,
using upstream algorithms with attribution and the stated acceptance gates.
This bounded search does not establish that no other Rust implementation exists.

## Consumer baseline

[polars-bio PR #461](https://github.com/biodatageeks/polars-bio/pull/461) at
`ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa` consumes the formats pin above and
packages third-party notices. Its API/tests are the downstream compatibility
target. The previous rebase validation recorded 454 passing Python tests and 19
passing Rust binding tests on macOS arm64; these results validate that baseline,
not this planned port or all other architectures.

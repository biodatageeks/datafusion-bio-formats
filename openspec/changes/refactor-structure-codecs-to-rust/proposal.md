# Change: Replace the structure parser and Foldcomp codec with Rust

## Why

The structure readers currently compile vendored Gemmi and Foldcomp C++ into
the native extension. The requested direction is to implement both production
paths in Rust while preserving their existing public behavior. This removes
the two C++ adapters and their unsafe FFI boundaries and makes these components
maintainable within the Rust formats workspace.

## What Changes

- Replace the Gemmi-backed raw CIF document parser used by mmCIF with a
  repository-owned Rust implementation.
- Implement the supported Foldcomp FCZ decoder in this repository's Rust code, including packed-field
  decoding, anchor correction, full side chains, atom identity and B factors.
- Preserve the Rust PDB reader, residue/geometry algorithms, schemas, providers,
  local database selectors, and Python/SQL entry points.
- Keep pinned upstream implementations as external test oracles during the
  migration. Production builds will use the Rust implementations exclusively
  after their respective acceptance gates pass.
- Remove superseded native build scripts, vendored C++ and FFI wrappers, and
  update packaging and notices to reflect the actual remaining provenance.
- Integrate the resulting compatible formats revision into polars-bio and
  verify installed wheels on supported platforms.

This covers the Gemmi CIF parsing functionality and Foldcomp decoding used by
the existing readers. Full Gemmi functionality, FCZ compression, database
creation, BinaryCIF, remote Foldcomp, new schemas and a new Python API are
outside this change.

## Impact

- Affected specs: `structure-providers`; additive requirements extend the
  capability introduced by `add-structure-readers`.
- Main implementation: `datafusion/bio-format-structure` and
  `datafusion/bio-format-foldcomp`.
- Verification: `testing/oracles/structure`, `testing/data/structure`, focused
  Rust tests, benchmarks, fuzz targets, and `.github/workflows/structures.yml`.
- Consumer: polars-bio dependency pins, distribution notices and wheel tests.
- No intended public breaking changes. Backend changes must not narrow accepted
  valid inputs or change schema, ordering, nulls, identifiers or selectors.
- Rust-only describes the replacement parser and codec. Other dependencies in
  the formats workspace and Python extension can still contain native code.

## Delivery and review

The [design](design.md) defines six stages and acceptance gates; [tasks](tasks.md)
contains the implementation checklist; [research](research.md) records inspected
sources and reference implementation limitations. The [spec delta](specs/structure-providers/spec.md)
states the required behavior.

Planning baseline, inspected 2026-09-20:

- formats `fd17754c55c63394717967c18b7a45cf8aeb48ee`;
- polars-bio PR #461 at `ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa`.

This is a follow-up to the existing readers, with its own branch and review.
It does not change PR #461 or decide when that PR should merge. If integration
starts before #461 merges, test against its exact head and revalidate against
the eventual merged revision. Implementation tasks remain unstarted.

Confirmed user decision: implement both replacements in this repository.
External parser/codec implementations serve as reference material and test
oracles, not as substitute production backends. The plan does not port every
feature of either upstream project.

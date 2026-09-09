# Implementation evidence — 2026-09-09

Both native crates are implemented. Structure owns PDB/mmCIF parsing, normalized
atom/residue models, conformer/connectivity policy, six Float64 angles, Arrow
construction and execution. Foldcomp owns checked local database selection and
its native decoder, then reuses the same provider/builder. No bio-functions change
is required. Public behavior, supported layouts and defaults are in each crate's
README; the code is the delivered version-1 contract.

Local macOS arm64 validation:

- `cargo test --workspace`: 1,862 passed, 8 ignored, no failures at the broad
  regression run. Additional structure policy/metric tests were run afterward.
- New structure/Foldcomp tests: paired atom identities/coordinates, 451 six-angle
  values, raw CIF tokens/IDs, modified parents, model/TER/label gaps, alternate
  selection, incomplete backbones, rigid motion, gzip, schema-only access,
  query filters/projections/counts/limits/repeated scans, selected FCZ coordinates,
  empty/duplicate/missing selectors, corrupt unselected entries and changed files.
- `cargo test -p datafusion-bio-format-structure --no-default-features --lib` passes.
- Workspace Clippy with all targets/features and warnings denied passes; the new
  crates were checked again after later native changes. Formatting and strict
  OpenSpec validation pass. New crate documentation builds with warnings denied.
- Pinned Gemmi/Biopython/Foldcomp oracle regeneration `--check` passes. Exact input
  and output hashes are in testing/oracles/structure/manifest.json.
- ASan/UBSan standalone codec harness passes all truncations of the 1UBQ FCZ and
  1,000 deterministic byte-mutation cases. This is a bounded smoke corpus, not a
  proof that every possible upstream decoder input is safe.
- Development benchmark covers parsing, Arrow assembly, full scans, first batch
  and 1/2/4/8 workers. Raw results and limits are in testing/benchmarks/structure.
- Companion Python integration passes its golden/SQL/concurrency/HTTP tests and
  existing selected regressions (257 tests); it carries a hash-identical fixture
  export and will pin this PR's exact Git commit.

Implementation choices relative to the proposed file map:

- Small provider/execution modules are combined; both providers share one execution
  plan. The Python binding retains an immutable native provider via a DataFusion
  DataFrame, which provides fresh cursors without catalog naming/lease machinery.
- Projection constructs only requested Arrow columns. Residue geometry is computed
  together before projection, rather than maintaining separate per-angle execution
  paths. No intermediate atom DataFrame is created.
- Whole entries/files use bounded round-robin partitions. Selective metadata uses
  O(K) retained state; compressed-size weighted scheduling remains unimplemented.
- Text limits bound encoded/decompressed file bytes and atom counts; active memory
  also includes the CIF DOM and Arrow buffers. Foldcomp enforces payload and atom
  limits. No constant-memory or count-without-decoding claim is made.
- The independent corpus freezes common atom identities/coordinates and full six-
  angle residue results. Less common raw fields use focused synthetic expectations;
  a full combinatorial, every-column real-world corpus remains an extension.

Review/release gates deliberately not claimed as locally complete:

- Linux/Windows execution and built-wheel checks run in the added/existing CI
  matrices; local execution here validates macOS arm64 only.
- Release-profile, cold-cache, large-database comparisons against matched oracle
  work remain open. Development smoke measurements do not satisfy that performance
  gate or establish production RSS/throughput guarantees.
- Distribution for the dependent PR uses an exact repository commit. Publishing
  a repository release/tag and merging the two PRs are maintainer actions.

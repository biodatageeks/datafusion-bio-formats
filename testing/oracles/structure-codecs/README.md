# Pinned structure-codec reference

This is development tooling for the repository-owned Rust CIF parser and FCZ
decoder, which now provide both production readers without native builds. The reference uses
formats commit `fd17754c55c63394717967c18b7a45cf8aeb48ee`, regardless of the
currently checked-out branch, including after removal of the native directories.

## Reproduce

Requires Python 3.12+, Git with the pinned commit available locally, and a C++17
compiler. No Python packages, network requests, Cargo builds or Python extension
installation are needed. Run from the repository root:

```sh
python3 testing/oracles/structure-codecs/capture.py --check
python3 testing/oracles/structure-codecs/cif_probes.py --check
python3 testing/oracles/structure-codecs/foldcomp_tables.py --check
python3 testing/oracles/structure-codecs/foldcomp_stress.py --check
python3 testing/oracles/structure-codecs/reference.py cif testing/data/structure/1ubq.cif
python3 testing/oracles/structure-codecs/reference.py fcz testing/data/structure/1ubq.fcz
python3 testing/oracles/structure-codecs/reference.py tables
```

`reference.py` extracts the two native directories with `git archive` into a
temporary checkout, builds `reference.cpp` against those exact files, and runs
the executable as a separate process with a per-input timeout. The FCZ adapter
validates each payload before the unchecked upstream reader sees it. The build
is cached under ignored `target/structure-codec-reference/`, keyed by source,
driver, compiler, flags and platform. Each cache has `build.json` provenance.
Set `CXX` to select a compiler; MSVC needs a developer shell and `CXX=cl`.
The MSVC command path is exercised on the Windows x64 portability runner.

`--check` regenerates observations in memory, compares them and their hashes,
and never rewrites committed files. `capture.py --record` intentionally replaces
the baseline; review its diff. A mismatch is a failed check, not permission to
update goldens automatically. The current strict recording is macOS arm64 with
Apple Clang 16. Other platforms may differ in floating-point bits; investigate
those differences before establishing platform acceptance. This reproducibility
check does not replace the migration's numerical-tolerance/platform gates.

## Artifacts and independent checks

- `corpus.py`: handwritten CIF probes and explicitly packed FCZ byte layouts;
  neither uses a production Rust parser/decoder to construct expected fields.
- `inputs.json`: tiny probe bytes in hex and `max_atoms` settings, consumable by
  offline Rust tests without executing Python or C++ reference tooling.
- `golden.json`: raw CIF block/column/null values; FCZ header, anchors, packed
  fields, discretizers, restored angles, and full decoded atoms for small cases
  and 1UBQ. The existing 24 database entries retain full-array hashes and coverage
  summaries instead of duplicating their larger decoded datasets in Git.
- `residue-codes.json`: all 32 code mappings, support decisions, sidechain and
  reconstructed atom counts from the pinned upstream tables.
- `manifest.json`: input/output/source hashes, original build provenance,
  per-case coverage, and measured independent checks.
- `cif_probes.py` / `cif-probes.json`: 412 additional pinned lexical boundary
  cases, including keyword-adjacent comments, context-sensitive save-frame
  endings, quoted/unquoted control and UTF-8 bytes. The Rust parser runs these observations in offline tests; the external
  reference comparison verifies the legacy parser separately.
- `foldcomp_tables.py` / `export_foldcomp_tables.cpp`: export exact residue
  geometry bits and predecessor indices from the immutable Foldcomp header,
  generating `src/fcz/tables.rs`; `--check` also requires rustfmt on PATH.
- `foldcomp_stress.py` / `fcz-stress/`: 1,040 mixed residues with 18 anchors and
  a 4,096-residue single segment. The independent packer constructs the inputs;
  the pinned process freezes full-output hashes separately for the five measured
  OS/architecture/toolchain profiles. Input bytes/counts/anchors and canonical
  source hashes remain shared; CRLF checkouts use LF-normalized source hashes.
  Each profile retains its compiler, raw archive/driver hashes and capture
  provenance. Unknown profiles fail explicitly instead of using another host's
  output. `--record` updates the current profile; other profiles are retained only
  while their shared inputs/reference source remain unchanged. The checker runs
  on all five portability jobs.
  Explicit Rust release tests compare complete arrays, normalized identities and
  residue geometry with the external legacy process.
- `check_contract.py`: compares handwritten nulls, packed integers, residue
  identities, counts, numbering and analytical B factors. It also checks all
  602 1UBQ decoded atom identities/coordinates against the pre-existing,
  independently generated Python Foldcomp oracle.
- `compare_candidate.py`: compares all 769 original/lexical/stress cases with a
  source-identical standalone Rust probe and a freshly selected pinned native
  process. Reports identities, coordinate percentiles, exact B factors and
  executable/input/output hashes; it never rewrites original goldens.
- `unfused_parameters.py` / `unfused-parameters.json`: 22 separately rounded
  restored-parameter/B-factor overrides captured on x86-64. Check with
  `CXX='clang++ -arch x86_64' python3 testing/oracles/structure-codecs/unfused_parameters.py --check`
  on Apple Silicon with the Intel toolchain/runtime installed. These expectations
  do not replace original coordinate goldens or relax their tolerance.

Float32 fields are stored as unsigned IEEE-754 bit patterns, preserving signed
zero and exact values. Atom rows are
`[name_hex, residue_hex, chain_hex, atom_id, residue_id, x_bits, y_bits, z_bits, b_bits]`.
Backbone rows are `[residue, phi, psi, omega, n_ca_c, ca_c_n, c_n_ca]`.
The parameter bit rows use the same six-angle order, excluding the residue code.
Raw strings use hex across the subprocess boundary so UTF-8 validation can be
distinguished from native syntax acceptance. CIF views use explicit lengths;
FCZ decoded strings reproduce the legacy adapter's NUL-terminated conversion.

The corpus contains 355 cases: 56 CIF and 299 FCZ, including the 24 database
entries. The Rust contract tests consume all 331 small/1UBQ cases; existing
database/provider integration tests continue to cover database execution.
Synthetic anchors test codec mechanics, not biological plausibility.

## Provenance and limits

The immutable reference archive retains its original source/license files:

- Gemmi 0.7.5, commit `5cc1c23c6007e0e6cbd69289c6f7c0bff50e943e`, MPL-2.0;
  bundled PEGTL, MIT. The reference uses the existing `cif_bridge.cpp`.
- Foldcomp commit `89e37195d3c8ade8d40ead91ad82e6cd2964a967`, MIT;
  bundled span, Boost Software License 1.0, and Windows dirent, MIT.
  The byte layout and synthetic residue code/count constants follow
  `foldcomp.h`, `foldcomp.cpp` and `utility.h` at that revision.
- Existing 1UBQ/database fixture provenance remains in
  [the original oracle manifest](../structure/manifest.json) and
  [oracle README](../structure/README.md).

Production codecs contain no C++ sources, build scripts or FFI. The test-only
Rust clients in `reference_process.rs`, `reference_cif.rs` and
`reference_foldcomp.rs` exchange JSON with the separate pinned process. Run the
four explicitly ignored provider comparisons with:

```sh
python3 testing/oracles/structure-codecs/check_provider_reference.py
```

This command builds the reference independently, sets `BIO_CODEC_REFERENCE`,
and runs the named comparisons in **release** mode against the optimized native
reference. Normal offline Cargo tests need neither Python nor a C++ compiler;
they retain the frozen 331-case contract and 412 lexical probes. The provider
comparisons cover full database/stress arrays, normalized identity, all six
angles, null/connectivity masks, malformed geometry and complete CIF batches.
Missing or failing external references fail this explicit command. The supported
platform workflow runs it on all five targets.

The concrete interfaces, measured compatibility rules, layout and algorithm
mapping are in [BASELINE.md](../../../openspec/changes/refactor-structure-codecs-to-rust/BASELINE.md).
Long-chain and degenerate-frame regressions now supplement the small corpus.
The [probe guide](../../fuzz/structure-codecs/README.md) documents local comparison
and CI setup. The [five hosted targets](platform-results/2026-09-20-hosted.json)
pass all 769 cases: coordinate drift is zero on Linux/macOS, and the Windows
maximum is 1.1444091796875e-5 angstrom, below the unchanged 1e-4 ceiling. B factors
match exactly everywhere. Sustained fuzz budgets, stable release performance/RSS,
full workspace checks and consumer wheels remain open. Shared provider suites exercise the same Rust backend in unit and integration
builds. The captured error strings document the reference; future Rust
errors must retain useful source/block context but need not copy Gemmi wording.

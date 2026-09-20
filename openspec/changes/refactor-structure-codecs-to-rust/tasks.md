# Implementation checklist

All items are planned, not completed. The design defines stage gates and the
immutable baseline. Keep implementation and measured validation evidence in a
future `IMPLEMENTATION.md`; do not convert proposed thresholds into claimed results.

## R0. Freeze the contract and internal Rust design

- [ ] R0.1 Reconcile the source contract with existing OpenSpec requirements and remaining `add-structure-readers` gates; record the actual toolchain and provider/consumer revisions.
- [ ] R0.2 Inventory CIF categories/syntax and FCZ layouts actually used; define accepted inputs, errors, limits, metadata, ordering and feature combinations.
- [ ] R0.3 Build a reference harness in a separate pinned checkout/process that emits raw CIF values and FCZ intermediate/decoded arrays. Keep production dependencies untouched.
- [ ] R0.4 Expand the small hashed corpus to cover quoting/nulls/blocks/models/metadata and all supported residue codes, multi-anchor chains, OXT, B factors and malformed FCZ fields. Document coverage gaps explicitly.
- [ ] R0.5 Design the repository-owned CIF tokenizer/document API, raw-value representation, contextual errors and bounded storage; validate the design against syntax/metadata fixtures and record source provenance.
- [ ] R0.6 Map the required upstream Foldcomp algorithms and residue constants to internal Rust modules, including anchor correction, side chains and numeric semantics; record attribution for translated portions.
- [ ] R0.7 Capture supported-platform reference results, current numerical tolerances including B factors, release benchmarks and budgets; re-estimate the remaining work.

Gate: reproducible baseline, concrete internal module designs and explicit acceptance criteria.

## R1. Replace Gemmi's CIF parsing path

- [ ] R1.1 Add repository-owned Rust tokenizer/document modules with raw string/null provenance, original block labels and bounded storage.
- [ ] R1.2 Implement quoted/multiline values, loops/scalars, case-insensitive tags, multiple blocks, comments, syntax errors and measured baseline extensions.
- [ ] R1.3 Adapt `mmcif::Blocks` and category views while retaining the existing identifier, metadata, atom and residue mapping.
- [ ] R1.4 Pass raw-parser differential tests, existing structure/policy tests, metadata-after-atoms cases and independent numerical/identity checks.
- [ ] R1.5 Verify feature-off/shared-model builds, input limits, contextual errors and resource release; switch production mmCIF parsing only after its gate passes.
- [ ] R1.6 Remove the Gemmi bridge/vendor build and unused direct build dependency; update source/provenance documentation without discarding applicable notices.

## R2. Parse FCZ bytes safely

- [ ] R2.1 Document header/section offsets, padding, bit widths, encoded counts and the supported version/layout from pinned upstream source and hand-built bytes.
- [ ] R2.2 Implement checked little-endian readers, magic/length checks, finite-field validation, anchor/side-chain/OXT checks, and reconstructed atom limits before allocation.
- [ ] R2.3 Decode packed backbone/residue fields, anchors, title, side-chain values and B factors; compare intermediate arrays to the pinned reference.
- [ ] R2.4 Cover truncation at field boundaries, size overflow, inconsistent counts, unknown/UNK residue handling, invalid indices and exact section exhaustion.

## R3. Reconstruct full Foldcomp output

- [ ] R3.1 Port inverse discretization with baseline Float32 semantics and unit-test all decoded parameter arrays.
- [ ] R3.2 Port backbone NeRF construction, anchor segmentation, reverse correction and segment joins; pass short/long/multi-anchor fixtures.
- [ ] R3.3 Port residue tables and full side-chain reconstruction with attribution; preserve unknown-residue behavior, atom ordering, OXT, numbering, chain and B factors.
- [ ] R3.4 Return the existing `NormalizedEntry` through `codec::decode`; retain common Float64 widening, normalization and residue/geometry behavior.
- [ ] R3.5 Pass full decoded-array parity, coordinate/angle/null/connectivity gates and all database selector tests, including zero/K decode counts and corrupt unselected records.
- [ ] R3.6 Switch the production codec after acceptance and remove the Foldcomp FFI/bridge/vendor build; preserve applicable translated-code and fixture notices.

## R4. Harden and validate both production paths

- [ ] R4.1 Add parser/decoder property tests, bounded fuzz targets and minimized regressions; record fuzz budgets, seeds, executions and findings.
- [ ] R4.2 Run the full small offline corpus and external-oracle verification; investigate numerical drift without relaxing established tolerances implicitly.
- [ ] R4.3 Compare release performance and peak RSS on representative parse/decode, Arrow/residue, query and subset workloads using equal work and frozen inputs.
- [ ] R4.4 Validate Linux x86_64/arm64, macOS x86_64/arm64 and Windows x64, plus structure/Foldcomp feature combinations and all required workspace checks.
- [ ] R4.5 Audit package/build artifacts for removed C++ sources, build invocations and bridge symbols; retain external reference execution separately from production.
- [ ] R4.6 Update `.github/workflows/structures.yml` for Rust robustness checks and retained independent oracles; retire obsolete C++ sanitizer jobs only after replacement coverage exists.

## R5. Integrate polars-bio and prepare release

- [ ] R5.1 Hand off the compatible formats revision and fixture manifest; update all polars-bio formats pins together on a dedicated follow-up branch.
- [ ] R5.2 Rebuild the extension; pass structure/Foldcomp, MSA and shared lazy/pushdown/metadata tests against the merged consumer baseline.
- [ ] R5.3 Build and inspect wheels/sdist; run installed-wheel tests outside the source checkout on every supported architecture.
- [ ] R5.4 Update README/build instructions, native notices and package include rules to match the actual code/dependencies; retain attribution for translated portions.
- [ ] R5.5 Record acceptance evidence and tested rollback pins; prepare the release handoff and archive/reconcile the relevant OpenSpec changes when appropriate.

## Verification commands available today

```sh
cargo test -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp
cargo test -p datafusion-bio-format-structure --no-default-features --lib
cargo test -p datafusion-bio-format-foldcomp --features text-formats
cargo fmt --all -- --check
cargo clippy -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp --all-targets --all-features -- -D warnings
python testing/oracles/structure/generate.py --check
openspec validate refactor-structure-codecs-to-rust --strict
```

The oracle command requires the pinned development requirements. New comparison,
fuzz and benchmark commands must be documented when implemented. Targeted tests
supplement the repository's required workspace CI and consumer wheel matrix.

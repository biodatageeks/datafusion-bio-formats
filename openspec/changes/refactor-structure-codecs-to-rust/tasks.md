# Implementation checklist

The design defines stage gates and the immutable baseline. Completed local
work is checked below; [IMPLEMENTATION.md](IMPLEMENTATION.md) records measured
evidence and [BASELINE.md](BASELINE.md) records the concrete contract/design.
Both production readers now use Rust following the explicit 2026-09-21 cutover
instruction. Remaining performance/fuzz/platform delivery checks stay tracked
separately; the switch does not imply those checks passed.

## R0. Freeze the contract and internal Rust design

- [x] R0.1 Reconcile the source contract with existing OpenSpec requirements and remaining `add-structure-readers` gates; record the actual toolchain and provider/consumer revisions.
- [x] R0.2 Inventory CIF categories/syntax and FCZ layouts actually used; define accepted inputs, errors, limits, metadata, ordering and feature combinations.
- [x] R0.3 Build a reference harness in a separate pinned checkout/process that emits raw CIF values and FCZ intermediate/decoded arrays. Keep production dependencies untouched.
- [x] R0.4 Expand the small hashed corpus to cover quoting/nulls/blocks/models/metadata and all supported residue codes, multi-anchor chains, OXT, B factors and malformed FCZ fields. Document coverage gaps explicitly.
- [x] R0.5 Design the repository-owned CIF tokenizer/document API, raw-value representation, contextual errors and bounded storage; validate the design against syntax/metadata fixtures and record source provenance.
- [x] R0.6 Map the required upstream Foldcomp algorithms and residue constants to internal Rust modules, including anchor correction, side chains and numeric semantics; record attribution for translated portions.
- [x] R0.7 Capture supported-platform reference results, current numerical tolerances including B factors, release benchmarks and budgets; re-estimate the remaining work.

Gate: reproducible baseline, concrete internal module designs and explicit acceptance criteria.

## R1. Replace Gemmi's CIF parsing path

- [x] R1.1 Add repository-owned Rust tokenizer/document modules with raw string/null provenance, original block labels and bounded storage.
- [x] R1.2 Implement quoted/multiline values, loops/scalars, case-insensitive tags, multiple blocks, comments, syntax errors and measured baseline extensions.
- [x] R1.3 Adapt `mmcif::Blocks` and category views while retaining the existing identifier, metadata, atom and residue mapping.
- [x] R1.4 Pass raw-parser differential tests, existing structure/policy tests, metadata-after-atoms cases and independent numerical/identity checks.
- [x] R1.5 Verify feature-off/shared-model builds, input limits, contextual errors and resource release; switch production mmCIF parsing under the explicit cutover instruction.
- [x] R1.6 Remove the Gemmi bridge/vendor build and unused direct build dependency; update source/provenance documentation without discarding applicable notices.

R1.3/R1.4 now apply to both ordinary and unit-test builds using Rust `Blocks`.
The provider suite and separate pinned reference comparisons cover block-at-a-time
errors, input limits, identifiers, metadata, Arrow mapping and independent geometry.

## R2. Parse FCZ bytes safely

- [x] R2.1 Document header/section offsets, padding, bit widths, encoded counts and the supported version/layout from pinned upstream source and hand-built bytes.
- [x] R2.2 Implement checked little-endian readers, magic/length checks, finite-field validation, anchor/side-chain/OXT checks, and reconstructed atom limits before allocation.
- [x] R2.3 Decode packed backbone/residue fields, anchors, title, side-chain values and B factors; compare intermediate arrays to the pinned reference.
- [x] R2.4 Cover truncation at field boundaries, size overflow, inconsistent counts, unknown/UNK residue handling, invalid indices and exact section exhaustion.

## R3. Reconstruct full Foldcomp output

- [x] R3.1 Port inverse discretization with baseline Float32 semantics and unit-test all decoded parameter arrays.
- [x] R3.2 Port backbone NeRF construction, anchor segmentation, reverse correction and segment joins; pass short/long/multi-anchor fixtures.
- [x] R3.3 Port residue tables and full side-chain reconstruction with attribution; preserve unknown-residue behavior, atom ordering, OXT, numbering, chain and B factors.
- [x] R3.4 Return the existing `NormalizedEntry` through `codec::decode`; retain common Float64 widening, normalization and residue/geometry behavior.
- [x] R3.5 Pass full decoded-array parity, coordinate/angle/null/connectivity gates and all database selector tests, including zero/K decode counts and corrupt unselected records.
- [x] R3.6 Switch the production codec under the explicit cutover instruction and remove the Foldcomp FFI/bridge/vendor build; preserve applicable translated-code and fixture notices.

R3.2–R3.5 are verified locally with Rust production routing. Coverage
includes the frozen small corpus, all 24 database records, 1,040-/4,096-residue
stress fixtures, all residue tables, selectors and decode-count metrics. These
checkmarks do not claim sustained fuzz, release-performance or final wheel acceptance.

## R4. Harden and validate both production paths

- [ ] R4.1 Add parser/decoder property tests, bounded fuzz targets and minimized regressions; record fuzz budgets, seeds, executions and findings.
- [x] R4.2 Run the full small offline corpus and external-oracle verification; investigate numerical drift without relaxing established tolerances implicitly.
- [ ] R4.3 Compare release performance and peak RSS on representative parse/decode, Arrow/residue, query and subset workloads using equal work and frozen inputs.
- [ ] R4.4 Validate Linux x86_64/arm64, macOS x86_64/arm64 and Windows x64, plus structure/Foldcomp feature combinations and all required workspace checks.
- [x] R4.5 Audit package/build artifacts for removed C++ sources, build invocations and bridge symbols; retain external reference execution separately from production.
- [x] R4.6 Update `.github/workflows/structures.yml` for Rust robustness checks and retained independent oracles; retire obsolete C++ sanitizer jobs only after replacement coverage exists.

R4 now has source-identical ASan/libFuzzer targets, separate-process comparisons
on all five targets, 93-case paired release measurements on macOS/Linux, and a
five-platform CI matrix. R0.7 freezes the baseline and budgets; it does not claim
that noisy or regressing benchmark cases passed their R4 acceptance gate.
These tasks stay open: smoke runs do not meet the 24-CPU-hour decoder budgets,
the isolated Arrow regression and some benchmark cases remain unresolved,
while the production paths and native build inputs have now been replaced at
the user's explicit instruction. See the latest checkpoint
in [IMPLEMENTATION.md](IMPLEMENTATION.md) for evidence and scope.

## R5. Integrate polars-bio and prepare release

- [x] R5.1 Hand off the compatible formats revision and fixture manifest; update all polars-bio formats pins together on a dedicated follow-up branch.
- [x] R5.2 Rebuild the extension; pass structure/Foldcomp, MSA and shared lazy/pushdown/metadata tests against the merged consumer baseline.
- [ ] R5.3 Build and inspect wheels/sdist; run installed-wheel tests outside the source checkout on every supported architecture.
- [x] R5.4 Update README/build instructions, native notices and package include rules to match the actual code/dependencies; retain attribution for translated portions.
- [ ] R5.5 Record acceptance evidence and tested rollback pins; prepare the release handoff and archive/reconcile the relevant OpenSpec changes when appropriate.

An isolated Rust-backed consumer wheel and a matching native wheel each pass
231 installed tests on macOS ARM64, with four explicit external/network skips.
The 64-case Python/storage benchmark has matching output, 45 cases within the
local budget and 19 noisy cases; separate SQL counters preserve 0/2/24 selected
decodes. All five candidate wheel jobs pass: 231 tests plus four skips on each
Linux/macOS target; Windows passes 178 and skips the MSA module. Those are historical
trial results. The production consumer branch now pins all 17 formats to
`7681a92c26d9748f588ef517b5036538aaebaeba`. Its final macOS ARM64 wheel passes the
same 231 tests/four skips, and both wheel and sdist pass license/native-input
audits. The codec crates have no C++ build targets, and the extension has no
codec bridge symbols. Five-platform production delivery and release acceptance
remain open; see the [production evidence](../../../testing/consumer/structure-codecs/results/2026-09-21-production.json).

## Verification commands available today

```sh
cargo test -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp
cargo test -p datafusion-bio-format-structure --no-default-features --lib
cargo test -p datafusion-bio-format-foldcomp --features text-formats
cargo fmt --all -- --check
cargo clippy -p datafusion-bio-format-structure -p datafusion-bio-format-foldcomp --all-targets --all-features -- -D warnings
python testing/oracles/structure/generate.py --check
python3 testing/oracles/structure-codecs/foldcomp_tables.py --check
python3 testing/oracles/structure-codecs/foldcomp_stress.py --check
cargo build --locked --manifest-path testing/fuzz/structure-codecs/Cargo.toml --release --no-default-features --features probe --bin codec_probe
python3 testing/oracles/structure-codecs/compare_candidate.py --candidate testing/fuzz/structure-codecs/target/release/codec_probe --output target/codec-platform-comparison.json --label actual-host-target
python3 testing/fuzz/structure-codecs/seed.py
python3 testing/fuzz/structure-codecs/run.py --toolchain nightly-2026-02-02 --seconds 60
python3 testing/benchmarks/structure-codecs/run.py --output target/codec-benchmark-release.json --samples 9 --seconds 0.2
openspec validate refactor-structure-codecs-to-rust --strict
```

The original oracle command requires the pinned development requirements. The
[fuzz/probe guide](../../../testing/fuzz/structure-codecs/README.md) and
[benchmark protocol](../../../testing/benchmarks/structure-codecs/README.md)
record prerequisites and measurement limits. Targeted tests
supplement the repository's required workspace CI and consumer wheel matrix.

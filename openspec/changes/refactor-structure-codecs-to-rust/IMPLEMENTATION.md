# Implementation evidence

## 2026-09-20: Separate branch and compatibility baseline

Branch: `feat/rust-structure-codecs`, based on the committed plan `4cd3064` and
formats baseline `fd17754`. Worktree:
`/Users/mwiewior/research/git/datafusion-bio-formats-rust-structure-plan`.
User authorized continuation on a separate feature branch and confirmed both
replacement implementations must be maintained inside this repository.

Completed R0.1–R0.6: source/requirement reconciliation, concrete internal
interfaces, byte layout and reconstruction mapping, isolated legacy reference
process, hashed fixture corpus, and offline Rust contract tests. The
[baseline contract](BASELINE.md) records discoveries and known gaps.

The reference archives only native source objects from immutable
`fd17754c55c63394717967c18b7a45cf8aeb48ee`, builds in a temporary checkout, and
runs separately from production. Compiler/flags/platform/source hashes are
recorded in [the manifest](../../../testing/oracles/structure-codecs/manifest.json).
It emits raw CIF columns and FCZ headers, packed fields, restored parameter
arrays, full atom identities/coordinates and B factors. Existing production
code gains only test-module declarations; no runtime path or dependency changes.

Coverage: 355 probes, including 56 CIF cases (31 accepted/25 rejected), 275
small/1UBQ FCZ cases, and 24 official database records. FCZ totals are 62 accepted
and 237 rejected. Cases cover all 32 residue codes, three/adjacent anchors, OXT,
shifted numbering, title/chain byte behavior, declared versus reconstructed
atom bounds, finite-field checks and every truncation point of a small file.
The new offline Rust tests exercise all 331 small/1UBQ cases through the actual
adapters, preserving full columns and decoded atom identities/order.

Independent checks compare explicit handwritten packed bit fields and synthetic
residue atom counts/numbering/B factors. The 602 1UBQ atoms match the pre-existing
Python Foldcomp oracle with maximum coordinate difference **0.0 angstrom** on
this host. Synthetic B-factor maximum error is **0.0**, and the Rust adapter
tests require exact captured B-factor bits. No cross-platform B-factor bound is
inferred from this local observation.

Local environment: macOS arm64, Apple Clang 16.0.0, Rust 1.91.0. The fresh
library-workspace resolution uses DataFusion 53.0.0, Arrow 58.4.0, Parquet 58.0.0
and cc 1.2.59; the ignored Cargo.lock is local test state, not a dependency change.
Cargo reused the existing PR-review target cache without modifying that checkout's
tracked files. Apple's `ar` rejects an optional deterministic flag during cc's
probe; cc falls back and the build succeeds.

Validation completed on this host:

| Check | Result |
| --- | --- |
| `capture.py --check` | All 355 cases and input/output/source hashes reproduced |
| Both structure/Foldcomp crate tests | 29 passed, including the two new tests covering 331 cases |
| Structure `--no-default-features --lib` | 1 passed |
| Foldcomp alone, with/without `text-formats` | 11 passed in each configuration |
| Both crates, Clippy `--all-targets --all-features -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Passed |
| Python Ruff format/check | Passed |
| Strict OpenSpec validation, local Markdown links, `git diff --check` | Passed |

The commands are in [tasks.md](tasks.md) and the
[reference README](../../../testing/oracles/structure-codecs/README.md).
Full workspace/release/platform verification is still pending; the table is
targeted evidence for this baseline-only change.

## 2026-09-20: Rust CIF parser candidate

Implemented `bio-format-structure/src/cif/{tokenizer,document,mod}.rs`, compiled
only for unit tests while cutover gates remain open. The implementation owns
one input byte buffer and checked cell spans with lexical flavor, preserves
null/string provenance and original block labels, and validates UTF-8 on block
exposure. It has no new dependency, FFI or unsafe code. Frames are parsed with
nonrecursive state, and ignored frame values are not retained.

The new parser matches the original 56 CIF observations plus 412 additional
lexical probes captured by `cif_probes.py` from the pinned reference process.
The added probes identified keyword-adjacent comments and context-dependent
`save_#comment` endings, both now covered by offline regression tests. The
shared contract tests execute once against each backend without duplicating
test module definitions.

A test-only adapter feeds Rust category views to the existing `mmcif::decode`
mapping. Complete atom/residue Arrow batches, including null masks, identifiers
and geometry, match exactly for 1UBQ and a synthetic fixture with metadata after
atom rows, modified residues and nonstandard author IDs. Input ownership,
cross-thread movement, deferred per-block UTF-8 errors, contextual syntax errors,
truncations, delimiter mutations and 4,096 deterministic arbitrary-byte inputs
are tested. These short mutation runs are not the planned sustained fuzz gate.

At this checkpoint R1.1/R1.2 were implemented. The next checkpoint below
completes candidate provider routing; R1.5/R1.6 retain switch/removal gates.

## 2026-09-20: Checked Rust FCZ reader and inverse discretization

Implemented a test-only candidate with checked little-endian reads, validated
section lengths/counts, finite fields, ordered anchors, residue codes, side-chain
counts, OXT and reconstructed atom limits. The parsed entry borrows encoded
sections and exposes validated records; no coordinate allocation precedes count
validation. Upstream MIT attribution is retained beside the translated modules.

All 275 small/1UBQ FCZ cases match the captured acceptance/error stages and exact
intermediate parameter bits. The 24 database records match counts and anchors.
Tests also cover every 1UBQ truncation, mutated headers, all byte-valued residue
codes and reader cursor stability after failed reads.

Compiler inspection corrected a numeric assumption: Clang arm64 emits FMADD for
inverse discretization. Side-chain code 93 exposed a two-ULP difference with
separate multiplication/addition. Explicit Rust `mul_add` reproduces the frozen
bits for every captured parameter. This is a measured compatibility choice;
no tolerance changed, and other-platform reference characterization remains open.

Both crate suites now pass 40 tests. Clippy with all targets/features and denied
warnings, formatting, the structure feature-off test, and Foldcomp with text
formats also pass locally. R2.1–R2.4 and R3.1 are implemented.

## 2026-09-20: Full Rust reconstruction and candidate provider integration

Implemented forward backbone NeRF, anchor segmentation, reverse correction,
weighted joins, all 20 residue side chains plus UNK, OXT and normalized atom
mapping. The implementation preserves legacy ordering, Float32-to-Float64
widening, numbering and the preceding-residue proline bond-length rule.
The residue geometry generator reads an immutable upstream header through Git,
exports exact Float32 bits, and records its source hash. Translated algorithms
and constants retain Foldcomp's MIT license and attribution.

Compiler inspection also establishes the norm's Float64 intermediates, Float32
`acos`, and contraction order in cross/dot products. Explicit Rust operations
reproduce the reference without introducing fast-math or changing tolerances.
Both debug tests and the native optimized reference execute on macOS arm64;
Rust release/platform characterization remains pending.

The same 11 structure and 10 Foldcomp provider tests now run twice: ordinary
integration builds retain native backends, and unit-test builds route through the
Rust candidates. This covers lazy blocks, late block errors, metadata, input
bounds, projections, selectors, repeated execution, zero/K decoded payloads and
corrupt unselected records. No public backend option or fallback is introduced.

Local reconstruction evidence:

| Input/check | Result |
| --- | --- |
| 275 frozen small/1UBQ cases | Acceptance/errors, full identities, coordinates and exact B factors pass |
| 24 database entries (131–157 residues) | 27,131 atoms / 81,393 components; median/p95/p99/max coordinate error all 0 |
| 1,040-residue mixed chain, 18 anchors | 8,685 atoms; median/p95/p99/max coordinate error all 0 |
| 4,096-residue single segment | 16,384 atoms; median/p95/p99/max coordinate error all 0 |
| Residue geometry | Angle tolerances, null masks, completeness and connectivity pass |
| Degenerate/extreme fields | Collinear/coincident frames and overflow reject; finite extreme minima retain native acceptance |
| Deterministic FCZ mutations | 4,096 three-byte mutations; no panic or non-finite accepted output |
| Generated tables and stress fixtures | Reproduced from pinned source; hashes verified |

Database/stress comparisons use the retained native adapter, whose vendored
source is unchanged from the pinned baseline. The separate process reproduces
frozen full-output hashes for database/stress fixtures; large decoded arrays
are not duplicated in Git. The two stress inputs total about 57 KB. These
bounded mutation runs are not sustained fuzzing.

R1.3/R1.4 and R3.2–R3.5 now have candidate implementation and local acceptance
evidence. Final checks on this host:

| Check | Result |
| --- | --- |
| Combined structure/Foldcomp suites | 66 passed |
| Structure without default features | 6 passed |
| Foldcomp alone, with/without text formats | 32 passed in each configuration |
| Both crates, all targets/features, Clippy with denied warnings | Passed |
| Rust formatting, Python Ruff, diff whitespace | Passed |
| Generated residue table and long-chain fixture verification | Passed |
| Strict OpenSpec validation | Passed |

Full workspace, Rust release, supported-platform and consumer wheel checks
remain outside this local candidate checkpoint.

## Remaining work

R0.7 is open: platform reference characterization, release benchmark cases and
noise/repetition budgets, a cross-platform B-factor ceiling, and re-estimation.
Both production backends still use the existing C++ implementations. Both
Rust candidates are implemented and exercised through test-only provider routing. No CI workflow,
polars-bio pin, PR #461, published artifact or remote branch is changed.

Next stages are sustained robustness testing, release performance/RSS, supported
platform verification, then production cutover/removal and consumer integration.
Do not equate local oracle success with full fuzz/performance/platform gates.

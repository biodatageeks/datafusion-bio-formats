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

## 2026-09-20: Platform arithmetic, fuzz targets and release measurements

Added a standalone validation workspace that includes the actual CIF, FCZ and
normalization source files. Only its error carrier is substituted for DataFusion;
the parser/decoder algorithms are not duplicated. It provides three ASan/libFuzzer
targets and an optional separate-process probe. The selected-range target uses
the production index parser, extracted without changing selection semantics.
Malformed unselected payload ranges remain irrelevant.

The probe checks all 355 original cases, 412 lexical cases and two long chains.
The initial Intel and Linux runs exposed accumulated coordinate errors caused
by compiler contraction and C++ math overloads. The corrected candidate uses
the measured per-target arithmetic profile; no tolerance was increased and no
original coordinate golden was rewritten. The 22 unfused parameter/B-factor
overrides are separately captured from the pinned x86 reference. Even the
original small coordinate goldens remain within 1e-4 on the measured Intel
reference (maximum cross-target difference 5.7220458984375e-5 angstrom).

[Platform observations](../../../testing/oracles/structure-codecs/platform-results/2026-09-20.json):

| Executable target | Execution environment | Cases | Coordinate p50/p95/p99/max | B-factor max |
| --- | --- | ---: | --- | --- |
| macOS arm64 | Native, Apple Clang 16 | 769 | 0 / 0 / 0 / 0 | 0 |
| macOS x86_64 | Rosetta, Apple Clang 16 | 769 | 0 / 0 / 0 / 0 | 0 |
| Linux arm64 | Docker VM, GCC 14.2 | 769 | 0 / 0 / 0 / 0 | 0 |
| Linux x86_64 | Docker emulation, GCC 14.2 | 769 | 0 / 0 / 0 / 0 | 0 |

Each platform compares 159,915 coordinate components and all accepted identities,
titles and B factors. Reports preserve executable/compiler/source hashes and
record emulation explicitly. These are core parser/decoder/model observations;
full DataFusion/platform wheel acceptance remains separate.

Final-source [fuzz smoke metadata](../../../testing/fuzz/structure-codecs/results/2026-09-20-smoke.json)
records nightly-2026-02-02, cargo-fuzz 0.13.2, RNG seed 20260920 and one 60-second
ASan run per target: CIF 542,238 executions; FCZ 14,825; selected ranges
22,891,334. All exited successfully without discovered failures. Input, atom,
time and RSS bounds are in the [guide](../../../testing/fuzz/structure-codecs/README.md).
Child CPU includes Cargo overhead; these smoke runs do **not** meet the required
24 CPU hours per decoder target.

The [release benchmark](../../../testing/benchmarks/structure-codecs/README.md)
uses identical binaries, schemas, bytes and materialization for paired backends.
It covers 93 cases: raw CIF, normalized decode, atom/residue Arrow conversion,
decode-to-Arrow and full DataFusion queries at 1/2/4/8 workers. Nine fresh-process
samples per backend alternate order, with two untimed warmups and equal iteration
counts calibrated to 0.2 seconds. All paired row/decoder-call/byte counts match.
[All samples and hashes](../../../testing/benchmarks/structure-codecs/results/2026-09-20-macos-arm64.json)
are retained. Seventeen cases meet the local 10% time/RSS budget; 76 exceed the
5% MAD/median noise limit and are inconclusive. One noisy CIF pipeline case has a
1.138 median RSS ratio. No performance acceptance or general speedup is claimed.
Inputs are preloaded; cold storage, actual sidecar-selection cost, Python
collection and large external databases remain unmeasured by this harness.

The portability workflow now compares the candidate against a pinned native
process on five supported platforms, then runs debug/release/provider/feature
checks. Three bounded fuzz jobs retain logs, artifacts and evolved corpora.
The existing native sanitizer and independent Python oracle jobs remain while
the production backends are native.

Local checks after these changes: 66 combined tests pass (plus two ignored
benchmark workers); structure feature-off passes six; Foldcomp alone and with
text formats pass 32 each. Release libraries pass 40 tests plus the two ignored
workers. Both production crates and the standalone harness pass Clippy with
warnings denied; formatting, Ruff, unfused-parameter reproduction and strict
OpenSpec validation pass.

## Remaining work

Both production backends still use C++. R4 remains open for complete hosted and
workspace/distribution checks, stable performance/RSS including storage/consumer
cases, and sustained fuzz campaigns. No polars-bio pin, PR #461 or published
package is changed by these checkpoints.

Re-estimate after the implemented candidates: allow roughly 3–6 engineering days
for remaining validation, numerical/performance findings and cutover, then 2–4
for consumer/wheel integration, plus review and campaign wall time. This estimate
assumes no new format or numerical failure; the 24-CPU-hour decoder budgets still
have to be measured, and noisy benchmarks cannot be counted as acceptance.

## 2026-09-20: Hosted reference profiles and Linux release measurements

R0.7 baseline characterization is complete. All five
[hosted core comparisons](../../../testing/oracles/structure-codecs/platform-results/2026-09-20-hosted.json)
pass 769 cases: Linux/macOS coordinate drift is zero, Windows x64/MSVC reaches
1.1444091796875e-5 angstrom, and B factors match exactly on all targets. The
1e-4 coordinate ceiling remains unchanged. Native Intel runners supplement
the earlier emulated measurements.

The first full Linux ARM64 job identified a legacy build-mode difference: GCC's
unoptimized C++ adapter does not contract the same expressions as the optimized
reference. The candidate matched the pinned reference, but the unoptimized native
adapter differed in B-factor bits and by 0.01792 angstrom on the mixed long chain.
Commit `1be733a` pins only Foldcomp's test profile to optimization level 2 while
retaining debug assertions and overflow checks. Production dev/release profiles
and the Rust numerical implementation are unchanged. The temporary override is
documented in [BASELINE.md](BASELINE.md) and must be removed with the native
test adapter. The corrected [five-platform workflow](https://github.com/biodatageeks/datafusion-bio-formats/actions/runs/35518305873)
passes all nine jobs, including Linux x86_64/ARM64, macOS x86_64/ARM64 and Windows
x64 debug/provider/feature/release/Clippy checks, the independent oracle/native
sanitizer job, and all three Rust fuzz smoke jobs. The
[complete job record](../../../testing/oracles/structure-codecs/platform-results/2026-09-20-ci.json)
is retained with the tested revision.

[Linux ARM64 release samples](../../../testing/benchmarks/structure-codecs/results/2026-09-20-linux-arm64.json)
cover the same 93 cases and nine-sample protocol. Eighty-five cases are within the
local budget, seven are noisy, and one isolated residue-to-Arrow case after mixed
long-chain decoding regresses by 17.7%. All 28 CIF cases are within budget. The
[one-second diagnostic](../../../testing/benchmarks/structure-codecs/results/2026-09-20-linux-arm64-arrow-diagnostic.json)
repeats that isolated finding at 10.6%; corresponding full pipeline/query cases
remain within budget. These observations do not close the whole performance
gate. Measurements are from a Linux ARM64 Docker VM; image/core-array provenance
and all samples are retained.

Broader local checks now include whole-workspace Clippy with all targets/features
and denied warnings, 32 Foldcomp tests with the corrected test profile, and the
40 release library tests executed directly in Linux ARM64. All pass. The default
`cargo test --workspace` run also passes: 1,982 tests across 126 suites, with ten
ignored tests. Optional external oracles for unrelated formats were not forced
on in this local run; it does not substitute for the repository CI's required
external-oracle environment.

The new sustained runner executes prebuilt ASan binaries directly and measures
only child fuzzer CPU. Corpus/report work and compilation are excluded. It records
source/binary/lock hashes, seeds, actual executions and per-segment CPU time.
Aggregation requires every decoder/shard, matching source/compiler/revision and
the requested measured CPU budgets. Local short checks demonstrate successful
aggregation and rejection of insufficient CPU or absent fuzzer statistics.
The optional workflow campaign uses eight three-CPU-hour shards per decoder;
short harness checks never count as the 24-hour acceptance run.

The [hosted campaign smoke](https://github.com/biodatageeks/datafusion-bio-formats/actions/runs/35519673026)
passes two ten-CPU-second shards per target and the aggregation check: 26.27
measured CIF CPU seconds / 68,532 executions and 26.99 FCZ CPU seconds / 9,591
executions. The full [sustained campaign](https://github.com/biodatageeks/datafusion-bio-formats/actions/runs/35519841823)
has been dispatched against `f96d1b0d82b709f161196c3b426f09bdc3d2b0f1`, requesting
eight 10,800-CPU-second shards per decoder. It remains pending until every shard
and the final measured-budget aggregation succeed. No sustained acceptance is
claimed by dispatching it.

## 2026-09-20: Installed consumer trial

An isolated formats worktree routes ordinary builds to both Rust candidates;
the main feature branch still uses the native production backends. The
[reproducible routing patch and consumer harness](../../../testing/consumer/structure-codecs/README.md)
retain that experiment without adding a public backend switch. Both comparison
wheels use consumer `ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa`, identical locked
dependencies, release settings and default mimalloc, with all 17 formats pins
pointing together to the appropriate checkout. The local consumer feature branch
is `feat/rust-structure-codecs`; its absolute path overrides are validation-only,
not release pins. PR #461 and its original review checkout are unchanged.

Both macOS ARM64 wheels pass **231 installed-wheel tests**, with four explicit
skips: three need the external HMMER executable, and one opts into an 89 MB S3
download. The eight files cover structures, Foldcomp, MSA and shared metadata,
lazy execution and pushdown. Tests run with isolated Python outside the source
checkout and import the installed package/extension. [Wheel, lock and test
provenance](../../../testing/consumer/structure-codecs/results/2026-09-20-macos-arm64.json)
records both results. The candidate still compiles native build inputs and has
two unused inspection-helper warnings; this is runtime compatibility evidence,
not the final package/artifact gate.

A manual five-platform workflow now builds that isolated candidate and runs the
same installed-wheel tests on compatible native runners. Its results remain
pending. The new [paired consumer benchmark](../../../testing/benchmarks/structure-codecs/README.md)
includes public Python collection, real local file/sidecar reads, full/subset/empty
database selection and all output columns at 1/2/4/8 configured workers. It
verifies matching Python/package versions and installed-extension/wheel hashes
before measurement. Warm filesystem observations do not establish cold-storage
or large external-database performance; actual I/O/decode counters are not
exposed by the Python API.

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

## Remaining work

R0.7 is open: platform reference characterization, release benchmark cases and
noise/repetition budgets, a cross-platform B-factor ceiling, and re-estimation.
R1–R5 remain unstarted. Both production backends still use the existing C++
implementations; the repository-owned Rust ports have not been implemented or
switched on. No CI workflow, polars-bio pin, PR #461, published artifact or remote
branch is changed by this checkpoint.

Next implementation unit: the private CIF tokenizer/document parser, reusing
the frozen raw-column tests, followed by the checked FCZ reader and reconstruction.
Do not equate local oracle success with full fuzz/performance/platform gates.

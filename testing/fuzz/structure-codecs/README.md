# Structure codec robustness and platform probes

This unpublished, separate workspace compiles the **same source files** as the
production Rust CIF/FCZ codecs and shared atom normalization. Its only substitute is the
error carrier (`std::io::Error` instead of `DataFusionError`). Parser control flow,
geometry, limits, normalization and data structures are not reimplemented.
No native CIF/Foldcomp code is linked into the fuzz targets. The production crate
suites separately verify the real DataFusion error wrappers and provider behavior.

## Bounded fuzzing

The workflow follows the [Rust Fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html).
The recorded toolchain is nightly-2026-02-02, cargo-fuzz 0.13.2 and
libfuzzer-sys 0.4.13; Cargo.lock pins the validation workspace dependencies.

```sh
rustup toolchain install nightly-2026-02-02 --profile minimal
cargo +nightly-2026-02-02 install cargo-fuzz --version 0.13.2 --locked
python3 testing/fuzz/structure-codecs/seed.py
python3 testing/fuzz/structure-codecs/run.py --toolchain nightly-2026-02-02 --seconds 60
```

`seed.py` deduplicates the frozen corpus by SHA256; it never deletes evolved
corpora. The runner records initial/final corpus hashes, compiler/version, RNG
seed, command, executions, child CPU time, exit status and log hashes. Logs,
evolved corpora and artifacts are ignored locally and uploaded by CI. A nonzero
fuzzer exit fails the runner; the failing artifact is retained for minimization.

| Target | Exercised code | Bound |
| --- | --- | --- |
| `cif_document` | Tokenizer, document construction, all block views and invalid block access | 256 KiB input |
| `fcz_decode` | Checked sections, full reconstruction, normalization and finite output | 64 KiB input, 20,000 atoms |
| `selected_range` | Actual index-row parser, ordered keys, selected range bounds and u64 overflow | 4 KiB fuzzer input |

AddressSanitizer, overflow checks and debug assertions remain enabled. Each input
has a 10-second timeout; libFuzzer has a 2 GiB RSS limit. The index target uses
three little-endian u64 values (previous key, file length, input limit), then a
UTF-8 index row. Range validation stays conditional on selection in production,
so malformed unselected payload ranges remain irrelevant.

These are smoke runs. The sustained acceptance budget remains **at least 24 CPU hours per
decoder target**; do not count wall time, compilation or index-target CPU toward
that requirement. Longer campaigns can use `--target cif_document --seconds
86400` or `--target fcz_decode --seconds 86400`; inspect actual execution/CPU
statistics and extend runs as necessary. No sustained budget is claimed here.
The input bounds are harness limits, not replacements for provider limits.

## Sustained CPU campaigns

`campaign.py` executes already-built fuzz binaries directly. Its Unix child CPU
samples surround only each fuzzer process; builds, Cargo startup, corpus hashing
and report generation are excluded. It runs bounded segments until each shard's
actual CPU budget is met, recording binary/source/lock hashes, seeds, execution
counts, corpus changes and log hashes. A crash, absent statistics, missing shard,
source/toolchain mismatch or insufficient measured budget fails verification.

The portability workflow has an optional manual campaign mode. A zero CPU budget
(the default) runs the normal platform/oracle/smoke jobs. For the planned sustained
gate, eight independent shards each measure three CPU hours per decoder:

```sh
gh workflow run structures.yml --ref feat/rust-structure-codecs \
  -f fuzz_cpu_seconds=10800 -f fuzz_shards=8
```

This schedules 16 Linux jobs and a final aggregation check. Each job has a six-hour
wall timeout, with the same per-input 10-second / 2-GiB bounds as smoke fuzzing.
The input limits are unchanged. Reports, crash artifacts and evolved corpora are
retained for seven days; archive the accepted evidence before expiry. The final
summary must show at least 86,400 **measured fuzzer CPU seconds per decoder**.
Job startup, compilation and queue time do not satisfy that budget.

For a local short runner check, first build with `cargo +nightly-2026-02-02 fuzz
build --fuzz-dir testing/fuzz/structure-codecs cif_document`, then run:

```sh
python3 testing/fuzz/structure-codecs/campaign.py run \
  --target cif_document --cpu-seconds 10 --output target/codec-campaign-check
```

Short runner checks are explicitly not sustained-budget evidence. Campaign mode
does not run the full platform matrix again; retain that matrix's separate result.

## Separate-process comparison

The optional probe uses the same source inclusion but no libFuzzer dependency:

```sh
cargo build --locked --manifest-path testing/fuzz/structure-codecs/Cargo.toml \
  --release --no-default-features --features probe --bin codec_probe
python3 testing/oracles/structure-codecs/compare_candidate.py \
  --candidate testing/fuzz/structure-codecs/target/release/codec_probe \
  --output target/codec-platform-comparison.json --label macos-arm64-native
```

The comparison covers 769 cases: the 355-case original corpus, 412 CIF lexical
probes and two long FCZ chains. Raw CIF values, atom identities/order and titles
must match. Coordinate errors must remain <=1e-4 angstrom; B factors retain exact
per-target bits. Reports retain source/compiler provenance, executable/input/output
hashes, per-case differences and percentile/worst-case coordinate errors. A
failed comparison exits nonzero and still writes its report. The frozen goldens
are never rewritten by this command.

Local Intel-on-Apple-Silicon comparison uses an actual x86-64 Rust executable and
`CXX='clang++ -arch x86_64'`, executed through Rosetta. This is emulation evidence,
not a native Intel runner or installed-wheel result.

`linux_check.py --arch arm64` (or `amd64`) builds an isolated Linux image with
Python 3.12, Rust 1.91.0 and GCC. Source/Git mounts are read-only; build output and
reports go only to `target/codec-linux-ARCH/`. Image metadata is retained there.
The amd64 run on an ARM host is emulated. The image tag is a build recipe, while
its recorded digest identifies the actual image used. Docker builds require
network access to toolchain/package registries.

Add `--benchmark` to run the full nine-sample paired release protocol after the
comparison. This installs GNU time in the image and writes `benchmark.json`
beside the comparison report. It builds the actual DataFusion unit-test binaries
with four Cargo build jobs; container timings are labeled as such and must be
interpreted separately from native-host measurements.
For a diagnostic subset, use `--datasets`, `--stages`, `--seconds` and a distinct
`--report` filename. These subsets do not replace the complete acceptance run.

Platform probes validate the decoder/parser/model core, now including the hosted
Windows x64/MSVC target. Full DataFusion suites, installed wheels and the release
performance gate remain separate checks.

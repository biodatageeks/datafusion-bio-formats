# Rust structure codec consumer validation

This harness builds polars-bio at `ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa`
against the production Rust readers in a disposable formats checkout. Both
readers use Rust by default; no routing patch, FFI backend or fallback is added.

Use **clean, disposable** checkouts. `prepare` rejects codec crates that still
contain native build inputs, updates all 17 formats dependencies together and
removes only their old Git source records and the two retired direct `cc`
dependencies from the consumer lockfile. It synchronizes Foldcomp MIT notices
and removes the obsolete structure-specific Gemmi/PEGTL/Boost notices. Locked
builds verify that the rest of the dependency graph stays unchanged.

```sh
python -m pip install -r formats/testing/consumer/structure-codecs/requirements.txt
python formats/testing/consumer/structure-codecs/validate.py prepare \
  --consumer consumer --formats formats --output evidence
cd consumer
python -m maturin build --locked --release --out ../wheels --interpreter python
cd ..
python -m pip install --no-deps --force-reinstall wheels/*.whl
python -I formats/testing/consumer/structure-codecs/validate.py test \
  --consumer consumer --output evidence
```

The manual workflow uses native Linux x86_64/ARM64, macOS x86_64/ARM64 and
Windows x64 runners, with denied Rust warnings. It records source, lock, wheel
and installed-extension hashes, JUnit/logs and explicit skip reasons. Tests
import the installed package with isolated Python outside both checkouts.
Windows omits pyhmmer and skips the MSA module; external HMMER and opt-in network
checks also remain explicit skips. These are not passing oracle tests.

```sh
gh workflow run structures.yml --repo biodatageeks/datafusion-bio-formats \
  --ref feat/rust-structure-codecs -f consumer_validation=true
```

This workflow has no publish or merge action. Other polars-bio dependencies can
still require native toolchains; removal applies to the two structure codecs.
Use separate installed environments for [paired consumer measurements](../../benchmarks/structure-codecs/README.md).
Historical native baseline preparation is available at commit
`fe9c879b87e71b61f39c16d8cccd4f607b3f08eb`.

The [earlier hosted evidence](results/2026-09-21-hosted.json) records five
successful runtime-trial jobs before native build removal: 231 tests with four
skips per Linux/macOS target, and 178 tests plus a skipped MSA module on Windows.
Those historical results do not claim to test the final package contents.

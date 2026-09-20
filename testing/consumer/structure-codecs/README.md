# Isolated consumer acceptance trial

This harness enables the Rust candidates in a disposable formats checkout and
builds polars-bio at `ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa`. It does not
change the main feature branch's production routing or PR #461. The small
`candidate-routing.patch` preserves the local trial commit `0fcf32d5fe3ad1730aa2826829bbade0ba9eff01`
relative to `f96d1b0d82b709f161196c3b426f09bdc3d2b0f1`; it routes ordinary mmCIF
and FCZ calls to the existing Rust modules and keeps native reference calls
test-only. No parser/decoder algorithm is changed.

Use **clean, disposable** checkouts. `prepare` changes their files, replaces all
17 formats dependencies together, and removes only those Git source records
from the frozen consumer lockfile. `maturin --locked` then verifies dependency
resolution without upgrading it:

```sh
python -m pip install -r formats/testing/consumer/structure-codecs/requirements.txt
python formats/testing/consumer/structure-codecs/validate.py prepare \
  --consumer consumer --formats formats --candidate --output evidence
cd consumer
python -m maturin build --locked --release --out ../wheels --interpreter python
cd ..
python -m pip install --no-deps --force-reinstall wheels/*.whl
python -I formats/testing/consumer/structure-codecs/validate.py test \
  --consumer consumer --output evidence
```

Omit `--candidate` to prepare the matching native baseline. Use separate Python
environments for paired measurements; see the [consumer benchmark](../../benchmarks/structure-codecs/README.md).
The manual `structure-codecs-consumer.yml` workflow builds and tests on native
Linux x86_64/ARM64, macOS x86_64/ARM64 and Windows x64 runners. It records source,
lock, wheel and extension hashes plus test logs/JUnit and every skip reason.
Tests import the installed extension with isolated Python in a temporary directory
outside either checkout. Windows omits pyhmmer and therefore skips the MSA module; missing external HMMER and opt-in network tests also remain
explicit skips. These gaps do not count as passing oracle tests.

This is runtime compatibility evidence, **not** final package acceptance. Native
sources/build scripts remain in the trial, and two decoder inspection helpers
still warn in ordinary builds. Final cutover must remove native artifacts, resolve
those warnings, restore denied-warning distribution checks, test the final pins
and inspect wheels/sdist. This manual workflow has no publish or merge action.

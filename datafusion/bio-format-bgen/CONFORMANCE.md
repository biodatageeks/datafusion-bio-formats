# BGEN conformance

The BGEN provider is tested in four layers:

1. Unit tests cover checked phased/unphased state counts and unaligned
   little-endian bit extraction.
2. Generated integration fixtures cover Layout 1 and Layout 2; no compression,
   zlib, and zstd; phased and unphased probabilities; variable ploidy;
   missingness; multiallelic states; dosage; sample reordering; exact
   predicate/limit pushdown; batch bounds; and malformed inputs.
3. Local and HTTP tests create a standard SQLite BGI, validate object identity
   and every indexed range, exercise stale-index policy, and verify bounded
   remote cache and BGEN range behavior.
4. Process-level differential tests pass the same quantized generated fixture
   to `bgenix`, qctool, the MIT-licensed `limix/bgen` reader, and `snputils`.
   Their values are compared with tolerance `1 / (2^B - 1)` plus floating
   conversion tolerance.

The regular suite runs without external tools and executes every oracle that is
already installed:

```shell
cargo test -p datafusion-bio-format-bgen
```

The conformance job must install pinned reference tools and make their absence
fatal:

```shell
BGEN_REQUIRE_REFERENCE_ORACLES=1 \
  cargo test -p datafusion-bio-format-bgen differential_reference_oracles
```

`BGEN_REFERENCE_PYTHON` selects the Python interpreter. The intended reference
matrix pins each tool by release or commit in CI:

| Oracle | Role |
| --- | --- |
| Oxford `bgenix` | Official-format metadata and VCF probability export |
| Oxford qctool | Independent BGEN-to-VCF probability export |
| `limix/bgen` | Independent MIT-licensed probability decoder |
| `snputils` | User-requested high-level genotype reader |

Reference readers are test-only processes. None are linked into the Rust
runtime. Generated cases are compared state by state in encoded sample and
allele order; randomized cases use deterministic seeds and retain any failing
seed as a regression fixture.

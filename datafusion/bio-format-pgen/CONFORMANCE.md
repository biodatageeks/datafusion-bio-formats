# PGEN conformance

The normative baseline is the `plink-ng` PGEN specification at commit
`9ee41ce224ea7cd091760d69392a98835715b5b2` (2026-07-27). Production code is an
independent Rust decoder. It does not link or distribute PLINK 2, `pgenlib`,
or `snputils`.

The test strategy has four layers:

1. Unit and deterministic property tests cover varint boundaries, packed
   padding, difflist groups and deltas, phase tracks, dosage bounds, allele
   patches, checked lengths, invalid LD chains, and partition ownership.
2. Generated integration files cover all eight supported storage modes,
   embedded and external indexes, dense, one-bit, difflist, LD/inverted-LD,
   multiallelic hardcalls, sparse/all/bitarray dosage, phase-present and
   phase-information tracks, explicit and implicit haplotype dosage, sample
   reordering, filters, limits, and projection.
3. Local and HTTP tests use the same fileset and assert exact query results,
   metadata payload skipping, LD dependency reads, and bounded remote PGEN
   range requests.
4. Process-level differential tests have pinned Python reference readers
   generate or read the same hardcall, phase, and dosage files. The Rust
   output is compared sample by sample in PVAR allele order.

The validated reference versions are:

| Oracle | Version | Role |
| --- | --- | --- |
| PLINK `pgenlib` | 0.94.1 | Authoritative writer and hardcall/phase/dosage reader |
| `snputils` | 1.1.1.dev19+g482c6d1df | Independent high-level hardcall and phased-call reader |
| PLINK 2 CLI | 2.0.0-a.7.1 (4 May 2026) | End-to-end PGEN-to-VCF hardcall export |

External tools are test-only. The regular suite executes `pgenlib` and
`snputils` when they are installed:

```shell
cargo test -p datafusion-bio-format-pgen
```

A conformance job makes absence fatal and supplies the isolated Python
environment:

```shell
PGEN_REFERENCE_PYTHONPATH=/path/to/pinned/python/site-packages \
PGEN_REQUIRE_REFERENCE_ORACLES=1 \
PGEN_REFERENCE_PLINK2=/path/to/plink2 \
PGEN_REQUIRE_PLINK2_ORACLE=1 \
  cargo test -p datafusion-bio-format-pgen \
    differential_pgenlib_and_snputils_oracles_when_installed
```

For randomized expansion, use a deterministic seed to generate calls through
`pgenlib.PgenWriter`, read the result through all available oracles, and retain
the seed and generated companions for every failure. Comparisons are exact for
hardcall allele indices and phase. Dosage comparisons use the PGEN
quantization step of `1 / 16384` plus floating conversion tolerance.

Phased dosage is covered by specification-derived byte fixtures because the
current `pgenlib` Python writer rejects `dosage_phase_present`; this is an
explicit oracle gap rather than an inferred external comparison.

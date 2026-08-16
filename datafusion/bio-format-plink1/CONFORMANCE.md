# PLINK 1 conformance

The test strategy has four layers:

1. Unit tests exhaustively cover the four two-bit BED states, padding masks,
   checked size arithmetic, and exact filter classification.
2. Integration fixtures exercise DataFusion projection, exact BIM filters,
   limits, selected and reordered samples, Arrow metadata, and contextual
   malformed-input errors.
3. A local HTTP range server verifies that registration reads only the BED
   header, metadata-only queries read no BED payload, and sparse ID queries
   fetch only the selected fixed-width payload.
4. External-oracle tests read the same generated fileset with the independent
   Apache-2.0 `bed-reader` implementation and PLINK `--recode A`, then compare
   every A1 dosage and missing call.

The normal test suite skips the external oracle when it is not installed:

```shell
cargo test -p datafusion-bio-format-plink1
```

CI conformance jobs install the pinned oracle and require it:

```shell
python3 -m pip install \
  'bed-reader @ git+https://github.com/fastlmm/bed-reader.git@0128fc755745c8e1cbe49d677479e5cfc3b2f49e'
REQUIRE_PLINK_ORACLE=1 \
  cargo test -p datafusion-bio-format-plink1 differential_against_bed_reader
```

The PLINK CLI oracle is also opt-in:

```shell
REQUIRE_PLINK_CLI_ORACLE=1 \
  cargo test -p datafusion-bio-format-plink1 differential_against_plink
```

`snputils` can be added as another process-level oracle over the same fixtures.
All reference readers remain test-only executables and are not linked into the
Rust provider.

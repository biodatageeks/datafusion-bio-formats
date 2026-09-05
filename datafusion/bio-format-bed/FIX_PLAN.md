# BED reader fixes (polars-bio #456)

Restore documented BED behavior and prevent failed records from silently
disappearing. This document records the implementation contract and regression
test coverage for [polars-bio #456](https://github.com/biodatageeks/polars-bio/issues/456).

## Contract

- Read the three required tab-delimited BED fields, with optional later fields.
- Preserve the BED4 output used by polars-bio; absent names become null.
- Make the existing BED3/BED5/BED6 output modes executable with matching schemas.
  Missing optional output fields are null; fields beyond the selected mode are
  not interpreted. BED7–BED12 and custom suffix fields remain accepted.
- Treat LF, CRLF, final EOF, plain, gzip and BGZF consistently. Skip blank lines,
  comments and UCSC track/browser directives without dropping data records.
- Propagate structural, numeric, encoding, decompression and I/O failures.
  Include the physical line number for record parsing failures.
- Check UInt32 coordinate bounds, reject reversed intervals, preserve valid empty
  intervals (including 0–0), and check overflow during one-based conversion.
- Preserve metadata and row counts under projection, COUNT(*), filters, LIMIT
  and batch boundaries. Report invalid partitions/projections as errors.
- Exercise remote async reading through a local HTTP fixture server, without
  cloud credentials; honor explicit compression options and file:// paths.

UCSC's BED definition is the format reference:
https://genome.ucsc.edu/FAQ/FAQformat.html#format1
The reader remains permissive about mixed widths and custom suffix fields for
compatibility; this is not full BED12 block-structure validation.

## Steps

- [x] Add failing regression tests for BED3 and adjacent failure modes.
- [x] Share line preparation/validation between synchronous and async readers.
- [x] Fix error propagation, compression options and remote construction.
- [x] Unify batch construction; implement declared output modes and projections.
- [x] Expand integration tests and update public format documentation.
- [x] Run BED tests, formatting and Clippy; validate polars-bio's reproducer
      against the patched dependency in an isolated build.

## Test matrix

| Area | Cases |
| --- | --- |
| Width/schema | BED3–BED12; all four output modes; absent/dot/empty names; score/strand |
| Framing | LF, CRLF, no final newline; comments/blanks/directives; tiny read buffers |
| Coordinates | both systems; zero/empty intervals; UInt32 boundary/overflow; reversed/negative/non-numeric |
| Errors | short/empty fields; invalid UTF-8; malformed first/middle/last rows; read/decode errors |
| Storage | plain/gzip/BGZF; multi-member gzip including split records; file URI; explicit compression; HTTP |
| Queries | full/reordered/empty projection; COUNT(*); filters/limits; multiple batches; schema metadata |

## Implemented and validated (2026-09-05)

- Added 32 tests (40 total including the existing tests and doctests). Matrix
  tests exercise hundreds of combinations without external services. In
  particular, width/mode/framing covers 360 combinations and malformed
  records under projections cover 378 combinations.
- Captured failing tests before fixing BED3 row loss and swallowed short records.
  The expanded tests also exposed a Noodles edge case rejecting end `000`;
  a failing regression was captured and the zero representation is normalized.
- Fixed zero-length intervals, narrowing/conversion overflow, declared schemas,
  zero-column count batches, physical projection validation and scan limits.
- Tested the shared async path through actual HTTP range requests, including
  missing objects, truncated responses and split gzip members. Cloud-specific
  authentication/services were not exercised.
- Ran `cargo test --release -p datafusion-bio-format-bed`: 40 passed.
- Ran `cargo clippy --release -p datafusion-bio-format-bed --all-targets -- -D warnings`:
  passed. This also checks all BED targets compile.
- Ran `cargo fmt -p datafusion-bio-format-bed -- --check` and `git diff --check`:
  passed.
- Built a polars-bio 0.35.1 wheel with this BED dependency overridden locally.
  The core path override required for Cargo type unification uses the same
  core source as the existing polars-bio dependency pin.
- Downstream validation: the original 180-case reproducer and the 11 existing
  Python BED tests pass in Python 3.11 / Polars 1.44.1. Another 12 smoke cases
  cover the corrected leading-zero end representation across both coordinate
  systems, three compression formats, and eager/lazy readers.

Compatibility note: `BedRemoteReader::new` now returns `Result<Self, io::Error>`;
low-level callers must handle initialization errors. The `BedTableProvider::new`
signature and BED4 schema used by polars-bio are preserved. BED3/BED5/BED6 now
produce their declared schemas. Fields beyond the selected mode remain opaque;
this work does not implement full BED12 block validation or output columns.

## Debug-build CI follow-up

The initial CI run exposed an Arrow 58.0.0 debug assertion in primitive batch
coalescing: `Vec::reserve(2)` can allocate capacity 4, so asserting that capacity
equals the requested batch size is invalid. Release tests did not exercise that
assertion. The failure was reproduced locally with the same debug test command
as CI.

The lockfile now resolves the Arrow crates consistently to 58.4.0, which includes
the upstream correction. The two-row regression batches remain in place, and
the query helper explicitly requests two partitions so the repartition path is
covered independently of available CPU cores. Validation now also includes
`cargo test --locked -p datafusion-bio-format-bed` with debug assertions enabled.

# Tasks: add-msa-format-provider

## 1. Crate foundation

- [x] 1.1 Add `datafusion-bio-format-msa` to the workspace with the shared
  DataFusion, Arrow, async-stream, tokio and logging dependencies.
- [x] 1.2 Reuse `bio-format-core` object storage so local paths, object stores
  and `gz`/`bgz` compression are resolved once, behind a single boxed async
  buffered line source that both parsers consume.
- [x] 1.3 Strip a `file://` scheme before every filesystem open, matching the
  BED and Cooler providers.

## 2. A2M and A3M provider

- [x] 2.1 Implement `MsaFlavor` and `FastaLikeTableProvider` so both formats
  share one parse path and differ only in the name reported by plans.
- [x] 2.2 Emit the FASTA schema with verbatim sequences: no case folding, gap
  rewriting, padding or alignment validation.
- [x] 2.3 Skip `#` lines before the first `>` record and reject other data
  ahead of it with an error naming the path.
- [x] 2.4 Split the definition line on whitespace only, leaving commas inside
  the description.
- [x] 2.5 Implement projection, `LIMIT`, and empty-projection row counting.

## 3. Stockholm parser

- [x] 3.1 Implement the line reader: header validation, `#=GF`/`#=GS`/`#=GC`/
  `#=GR` dispatch, block concatenation by name and feature, `//` termination,
  and tolerance of a missing final terminator.
- [x] 3.2 Validate the header exactly: accept `# STOCKHOLM 1.0` with optional
  trailing whitespace, reject other versions and malformed headers as Easel
  does.
- [x] 3.3 Skip ordinary `#` comments between alignments without consuming an
  alignment ordinal, while still treating annotation or sequence data as the
  start of an alignment that omits its header.
- [x] 3.4 Provide an annotations-only mode that never allocates sequence text.

## 4. Stockholm provider and execution

- [x] 4.1 Implement the row schema, the `ID → AC → ordinal` identifier
  fallback, and the `gs`/`gr` list-of-struct bags.
- [x] 4.2 Implement `gs_fields` promotion with the `gs` sentinel.
- [x] 4.3 Build only projected columns; serve an empty projection as a row
  count.
- [x] 4.4 Plan partitions from `//` byte offsets for local uncompressed inputs,
  assigning ordinals globally, and keep one partition otherwise.
- [x] 4.5 Require the compulsory header only in the partition that opens at
  byte 0, so a mid-input partition may start on a headerless alignment and the
  result does not depend on `target_partitions`.
- [x] 4.6 Implement `read_stockholm_annotations` in long format with
  `n_sequences` and `alignment_length`.

## 5. Fixtures and tests

- [x] 5.1 Vendor fixtures covering the real shapes: Pfam PF00001 seed (repeated
  `#=GF DR`/`CC`), interleaved Rfam RF00001 seed, an `hmmalign` output with
  `#=GR PP` and `#=GC PP_cons`, hh-suite `query.a3m`, and an hhpred example
  carrying `ss_*` pseudo-sequences.
- [x] 5.2 Generate expectations from reference implementations: `esl-alistat`
  counts, `esl-reformat pfam` canonical single-block forms, and a dotted A2M
  asserted byte-identical between hh-suite `reformat.pl` and Easel.
- [x] 5.3 Cover edge cases: missing `//`, unsupported and malformed headers,
  header with trailing whitespace, empty input, single record, comments between
  alignments, headerless later alignments across partition counts, `file://`
  URIs for both scanning and partition planning, and `gz`/`bgz` variants.
- [x] 5.4 `cargo fmt`, `cargo clippy --all-targets --all-features`,
  `cargo doc --no-deps --all-features` with `-D warnings`, and
  `cargo test -p datafusion-bio-format-msa` all green.

## 6. Follow-ups (out of scope here)

- [ ] 6.1 Writing A2M/A3M and Stockholm.
- [ ] 6.2 A3M → A2M insert expansion.
- [ ] 6.3 `#=GC` exposed as per-column columns.

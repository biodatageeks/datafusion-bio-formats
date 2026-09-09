# msa-format-provider Specification (delta)

## ADDED Requirements

### Requirement: A2M and A3M logical schema

The provider SHALL expose A2M and A3M records with the FASTA schema `name` (Utf8, non-null), `description` (Utf8, nullable) and `sequence` (LargeUtf8, non-null), splitting the definition line on the first whitespace only.

#### Scenario: Definition line with a description

- **WHEN** a record header is `>sp|Q5VUD6|FA69B_HUMAN Protein FAM69B OS=Homo sapiens`
- **THEN** `name` is `sp|Q5VUD6|FA69B_HUMAN` and `description` is `Protein FAM69B OS=Homo sapiens`

#### Scenario: Definition line without a description

- **WHEN** a record header contains no whitespace after the identifier
- **THEN** `description` is null

#### Scenario: A comma does not terminate the identifier

- **WHEN** a record header is `>tr|Q4S137|Q4S137_TETNG Chromosome 1 SCAF14770, whole genome`
- **THEN** `name` is `tr|Q4S137|Q4S137_TETNG` and the comma stays inside `description`

### Requirement: Verbatim alignment sequences

The provider SHALL return the `sequence` of an A2M or A3M record as the exact concatenation of its sequence lines, preserving letter case, `-` and `.`, and SHALL NOT expand, pad or validate the alignment.

#### Scenario: Ragged A3M rows

- **WHEN** an A3M input contains records of differing length because insert-state gaps are omitted
- **THEN** every record is returned at its own length and no error is raised

#### Scenario: Insert states and gap characters survive

- **WHEN** a record contains lowercase insert states and `.` insert-column gaps
- **THEN** both appear unchanged at the same offsets in `sequence`

### Requirement: A3M header lines and pseudo-sequences

The provider SHALL skip lines beginning with `#` that precede the first `>` record, and SHALL emit reserved pseudo-sequence records as ordinary rows in file order.

#### Scenario: hh-suite header marker

- **WHEN** an input begins with `#A3M#` and further `#` lines before the first `>`
- **THEN** those lines are ignored and every `>` record is returned

#### Scenario: Secondary-structure pseudo-sequences

- **WHEN** an input carries `>ss_pred`, `>ss_conf` and `>ss_dssp` records ahead of the query
- **THEN** they are returned as the first three rows, in file order

#### Scenario: Data before the first record is an error

- **WHEN** a non-`#` line precedes the first `>` record
- **THEN** the scan fails with an error naming the path

### Requirement: Stockholm logical schema

The provider SHALL expose a Stockholm input as one row per sequence per alignment with `alignment_id` (Utf8, non-null), `name` (Utf8, non-null), `sequence` (LargeUtf8, non-null) and `gs` / `gr` annotation bags typed `List<Struct<tag: Utf8, value: Utf8>>`.

#### Scenario: Alignment identifier and its fallbacks

- **WHEN** an alignment carries `#=GF ID`
- **THEN** every row of that alignment takes that value as `alignment_id`
- **AND** WHEN it carries only `#=GF AC`, that value is used instead
- **AND** WHEN it carries neither, the alignment's 0-based ordinal within the input is used

#### Scenario: Sequence names are verbatim

- **WHEN** a sequence line begins with `NPY1R_HUMAN/57-320`
- **THEN** `name` is `NPY1R_HUMAN/57-320` and is not split into name, start and end

#### Scenario: Per-sequence annotations in file order

- **WHEN** a sequence carries several `#=GS` lines
- **THEN** its `gs` bag lists one `{tag, value}` entry per line, in file order, with repeats preserved

#### Scenario: Sequences without annotations

- **WHEN** a sequence has no `#=GS` or `#=GR` lines
- **THEN** its `gs` and `gr` values are null

### Requirement: Stockholm header validation

The provider SHALL accept exactly `# STOCKHOLM 1.0` — ignoring trailing whitespace only, and requiring the single separating space — as the first non-blank line of an input, and SHALL reject every other first line, including other version numbers, other spellings and leading whitespace.

#### Scenario: The supported header

- **WHEN** an input begins with `# STOCKHOLM 1.0`, with or without trailing whitespace
- **THEN** the input is parsed

#### Scenario: An unsupported version

- **WHEN** an input begins with `# STOCKHOLM 2.0`
- **THEN** the scan fails with an error naming the path and the expected header
- **AND** the input is not parsed under 1.0 semantics

#### Scenario: A malformed header

- **WHEN** an input begins with `# STOCKHOLM garbage`, `# STOCKHOLMX`, `#STOCKHOLM 1.0`, `# STOCKHOLM1.0`, `# STOCKHOLM  1.0` or a comment line
- **THEN** the scan fails with an error naming the path and the expected header

#### Scenario: Leading whitespace before the header

- **WHEN** the first line is `# STOCKHOLM 1.0` preceded by spaces or a tab
- **THEN** the scan fails with an error naming the path and the expected header

### Requirement: Stockholm block and alignment structure

The provider SHALL concatenate sequence lines, `#=GR` values and `#=GC` values that repeat across interleaved blocks for the same name and feature, SHALL treat `//` as the end of an alignment, and SHALL support many alignments per input.

#### Scenario: Interleaved alignment

- **WHEN** an alignment is wrapped into two blocks so each sequence name appears twice
- **THEN** each row's `sequence` is the concatenation of both lines and the row count equals the number of distinct names

#### Scenario: Several alignments in one input

- **WHEN** an input contains two alignments separated by `//`
- **THEN** rows from both are returned and are distinguishable by `alignment_id`

#### Scenario: Missing final terminator

- **WHEN** the last alignment is not terminated by `//` before end of input
- **THEN** its rows are still returned

#### Scenario: Comments between alignments

- **WHEN** a `#` comment line appears after one alignment's `//` and before the next header
- **THEN** it is ignored
- **AND** it does not consume an alignment ordinal, so a following alignment without `ID` or `AC` keeps the ordinal it would otherwise have had

### Requirement: Stockholm named annotation promotion

The provider SHALL accept a list of `#=GS` feature names to promote to top-level nullable Utf8 columns, using the first occurrence per sequence, and SHALL keep the full `gs` bag when the list contains the `gs` sentinel.

#### Scenario: Promoting a feature

- **WHEN** a caller requests promotion of `AC`
- **THEN** the schema gains a nullable Utf8 column `AC` holding each sequence's first `#=GS … AC` value, or null when absent

#### Scenario: Keeping the bag alongside promoted columns

- **WHEN** a caller requests `AC` and the `gs` sentinel
- **THEN** the schema contains both the promoted column and the full `gs` bag

### Requirement: Alignment-level annotation reader

The provider SHALL expose the `#=GF` and `#=GC` lines of every alignment in long format — `alignment_id`, `kind`, `feature`, `value`, `n_sequences`, `alignment_length` — preserving repeats and file order across both kinds, without materialising sequences.

#### Scenario: Repeated file annotations

- **WHEN** an alignment contains several `#=GF DR` and `#=GF CC` lines
- **THEN** one row is returned per line, in the order they appear

#### Scenario: Column annotations

- **WHEN** an alignment contains `#=GC RF` and `#=GC SS_cons` lines spread over interleaved blocks
- **THEN** one row per feature is returned whose `value` length equals `alignment_length`

#### Scenario: Interleaved kinds keep their file order

- **WHEN** an alignment interleaves `#=GF` and `#=GC` lines
- **THEN** the rows follow the order of the lines in the input rather than being grouped by kind
- **AND** a `#=GC` feature repeated across blocks is reported once, at the position of its first block

### Requirement: Storage, compression and local URIs

The provider SHALL read every supported format from local paths and object stores, with `gz` and `bgz` compression, and SHALL accept a `file://` URI wherever a local path is accepted.

#### Scenario: Compressed input

- **WHEN** a caller scans a `gz`- or `bgz`-compressed input
- **THEN** the rows are identical to scanning the uncompressed input

#### Scenario: Local file URI

- **WHEN** a caller supplies `file:///path/to/alignment.sto`
- **THEN** the provider opens the local file, both for scanning and for partition planning

### Requirement: Projection and partitioning

The provider SHALL build only the projected columns, SHALL serve an empty projection as a row count without reading sequence text, and SHALL split local uncompressed multi-alignment Stockholm inputs on `//` boundaries across `target_partitions` without changing the result.

#### Scenario: Empty projection

- **WHEN** a caller counts rows without selecting any column
- **THEN** the count is returned and no sequence strings are materialised

#### Scenario: Unprojected columns are never built

- **WHEN** a caller selects a subset of the A2M/A3M columns
- **THEN** only the requested columns are accumulated and finished
- **AND** sequence text is neither buffered nor decoded when `sequence` is not requested

#### Scenario: Partitioned multi-alignment scan

- **WHEN** a local uncompressed input holds more alignments than `target_partitions`
- **THEN** the scan reports that many partitions and the union of their rows equals the single-partition result

#### Scenario: Single-alignment input

- **WHEN** an input holds exactly one alignment
- **THEN** the scan uses one partition regardless of `target_partitions`

#### Scenario: Comment-only tail after the final terminator

- **WHEN** blank lines and ordinary `#` comments follow the last `//`
- **THEN** they earn no partition of their own, because the reader treats them as no alignment
- **AND** WHEN annotation or sequence data follows the last `//` instead, it is planned and read as a further alignment

#### Scenario: Planning reads the input once

- **WHEN** partitions are planned for a file whose final alignment has no `//`
- **THEN** the boundary scan does not re-read that alignment or buffer it whole

#### Scenario: A pushed-down limit of zero

- **WHEN** a scan is planned with a limit of zero
- **THEN** no rows are returned and the input is not opened
- **AND** this holds however many partitions the scan uses

#### Scenario: A positive pushed-down limit under partitioning

- **WHEN** a scan with a positive limit is planned over several partitions
- **THEN** the limit is not applied independently in each partition, so the plan never returns the limit multiplied by the partition count
- **AND** WHEN the scan uses a single partition, the limit stops the read early

#### Scenario: Unprojected alignment-wide annotations are not materialised

- **WHEN** a Stockholm scan projects neither `sequence` nor `gr`
- **THEN** neither those payloads nor the `#=GC` tracks, which no table column exposes, are accumulated

#### Scenario: Headerless later alignments under partitioning

- **WHEN** an input's later alignments omit their own `# STOCKHOLM 1.0` header
- **THEN** the rows are the same at every `target_partitions` value
- **AND** a partition that opens mid-input does not reject the alignment it starts on for lacking a header

### Requirement: Reference-implementation parity

The provider SHALL be verified against independent reference implementations for every fixture: Easel (HMMER) for Stockholm structure and A2M/A3M alignment semantics, and hh-suite `reformat.pl` for A3M/A2M conversion.

#### Scenario: Stockholm parity

- **WHEN** the Pfam and Rfam seed fixtures are scanned
- **THEN** row counts, names and aligned sequences equal Easel's parse of the same inputs

#### Scenario: A3M parity

- **WHEN** an A3M fixture is scanned with reserved pseudo-sequences excluded
- **THEN** every row has the same number of match columns, equal to the count Easel reports for the same input

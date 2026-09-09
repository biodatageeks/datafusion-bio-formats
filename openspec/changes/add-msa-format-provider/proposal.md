# Change: Add A2M, A3M and Stockholm (MSA) format provider

## Why

A2M, A3M and Stockholm are the multiple-sequence-alignment formats produced and
consumed by hh-suite, HMMER and the Pfam/Rfam databases, and they are the input
of most structure-prediction pipelines. DataFusion has no table provider for
them, so clients cannot query alignments lazily. A3M in particular cannot be
read as plain FASTA: it omits insert-state gaps, so its rows are ragged, and
Stockholm is a block-structured format with four annotation kinds that needs a
parser of its own — no usable Rust crate exists for it.

The companion polars-bio `add-msa-alignment-formats` change is the original
cross-repository feature plan. This local change records the provider's own
schema, parsing, partitioning and correctness acceptance criteria so they can be
reviewed and archived in this repository.

## What Changes

- Add the `datafusion-bio-format-msa` workspace crate with two read-only
  providers over the shared `bio-format-core` storage layer (local paths and
  object stores, plain / `gz` / `bgz`).
- `FastaLikeTableProvider` reads A2M and A3M through one code path selected by
  `MsaFlavor`, exposing the FASTA schema (`name`, `description`, `sequence`)
  with **verbatim** sequences: no case folding, no `.`/`-` rewriting, and no
  padding, so ragged rows are preserved.
- Skip `#` lines that precede the first `>` record (hh-suite's `#A3M#` marker)
  and emit reserved pseudo-sequences (`ss_pred`, `ss_conf`, `ss_dssp`, …) as
  ordinary rows; split the definition line on whitespace only.
- `StockholmTableProvider` exposes one row per sequence per alignment with
  `alignment_id`, `name`, `sequence` and the `#=GS` / `#=GR` annotations as
  `List<Struct<tag, value>>` bags, concatenating interleaved blocks and
  supporting many alignments per input.
- Promote named `#=GS` features to top-level columns via `gs_fields`, with a
  `"gs"` sentinel that keeps the bag alongside them.
- Partition local uncompressed multi-alignment inputs on `//` boundaries across
  `target_partitions`, assigning alignment ordinals globally so results do not
  depend on the partition count.
- Add `read_stockholm_annotations` for the alignment-level `#=GF` / `#=GC` lines
  in long format, preserving repeats and file order, without materialising
  sequences.
- Add fixtures and integration coverage generated from reference
  implementations (Easel, hh-suite `reformat.pl`).

Out of scope for this provider version: writing any of the three formats,
A3M→A2M insert expansion, and `#=GC` exposed as per-column columns.

## Impact

- Affected specs: `msa-format-provider` (new capability)
- Affected code:
  - workspace `Cargo.toml`
  - `datafusion/bio-format-msa/`
- New dependencies: none beyond the existing workspace set
- Runtime scope: byte-range partitioning applies to local uncompressed inputs;
  object-store and compressed inputs are read as a single partition
- Companion consumer: `biodatageeks/polars-bio` `scan_a2m` / `scan_a3m` /
  `scan_sto` / `describe_sto` (polars-bio#460, issue #459)

# datafusion-bio-format-msa

A2M, A3M and Stockholm multiple-sequence-alignment readers for Apache DataFusion.

| Format | Extensions | Provider | Schema |
|---|---|---|---|
| A2M | `.a2m` | `FastaLikeTableProvider` (`MsaFlavor::A2m`) | `name` Utf8, `description` Utf8?, `sequence` LargeUtf8 |
| A3M | `.a3m` | `FastaLikeTableProvider` (`MsaFlavor::A3m`) | same as A2M |
| Stockholm | `.sto`, `.stk`, `.stockholm` | `StockholmTableProvider` | `alignment_id` Utf8, `name` Utf8, `sequence` LargeUtf8, `gs` List<Struct<tag,value>>?, `gr` List<Struct<tag,value>>? |

All three accept local paths and object-store URIs, plain or `gz`/`bgz` compressed.

## A2M / A3M

Rows are returned verbatim: letter case, `-` and `.` are preserved and rows may
be ragged (A3M omits insert-state gaps; so does Easel's own A2M writer). Lines
starting with `#` before the first `>` are skipped. Reserved pseudo-sequences
(`ss_pred`, `ss_conf`, `ss_dssp`, …) are ordinary rows. The header is split on
the first whitespace only — a comma is not a separator.

## Stockholm

One row per sequence per alignment. `alignment_id` is `#=GF ID`, else
`#=GF AC`, else the alignment's 0-based ordinal. Interleaved blocks are
concatenated; a file may contain many alignments; a missing trailing `//` is
tolerated. `gs_fields = ["AC", "DE"]` promotes those `#=GS` features to columns
(add `"gs"` to keep the bag as well). Alignment-level `#=GF` / `#=GC` lines are
available via `read_stockholm_annotations`, one row per line, repeats preserved.

Local uncompressed multi-alignment files are split across DataFusion
`target_partitions` on `//` boundaries; a single-alignment file is one
partition and holds that alignment in memory while it is parsed.

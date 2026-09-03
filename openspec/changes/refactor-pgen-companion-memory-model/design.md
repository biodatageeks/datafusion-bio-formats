## Context

`PgenFileset::open` (fileset.rs:278) resolves the PVAR, reads the entire
object with `read_all_bounded` (compressed cap), decodes it fully with
`decode_text_companion` (decoded cap), and calls `parse_pvar_chunked`, which
splits the body at newlines and parses chunks on scoped threads into
`Vec<Vec<PvarVariant>>`, then extends them into one `Vec<PvarVariant>`. Each
`PvarVariant` owns `chrom: String`, `id: Option<String>`, `reference: String`,
`alternate: Vec<String>`, so a biallelic SNP row costs 112 B inline plus four
to five small allocations. The transient `parsed` plus final vector doubles
the inline part during assembly.

Measured on `chr22.full` from the PGS Catalog panel (993,881 variants, 108 MiB
decoded, 12.5 MiB zstd), on macOS with mimalloc:

| step | peak RSS above process baseline |
|---|---|
| decoded text | 108 MiB |
| `describe` (open only) | ~600 MiB |
| metadata-only scan | ~1.1 GiB |

The full GRCh38 panel is 75.2M variants and 2.3 GiB decoded; GRCh37 is 84.8M
and 2.5 GiB (measured; the PVARs carry `CHROM POS ID REF ALT` only, with rs
IDs averaging 14 bytes and alleles 3 bytes per row). `pgenlib` never reads the PVAR, which is why it opens the panel and
we do not.

## Goals / Non-Goals

- Goals: open the published 1000G panels with the default options; make peak
  transient memory independent of companion size; make resident variant
  memory a small constant per variant; keep parse throughput at or above the
  current chunk-parallel parser; keep every existing error, line number, and
  ordering guarantee; name the relevant option in each limit error.
- Non-Goals: caching a parsed companion across opens; lazy or partial PVAR
  parsing (every consumer needs all five columns, or the matrix reader needs
  positions for every row); changing the emitted schema; touching PSAM
  beyond routing it through the same reader; genotype record decoding.

## Decisions

- **Decision: stream-decode into newline-aligned blocks with a bounded
  in-flight window.** A producer reads the companion through a `Read`
  (local `std::fs::File`, or the remote object's byte stream), detects
  zstd/gzip by magic as today, and fills blocks of `PVAR_BLOCK_BYTES` (64 MiB)
  extended to the next newline. Blocks go through a `sync_channel` sized to
  the parse worker count; workers parse a block with the existing
  `parse_pvar_chunk` and return `(block_index, rows, stop, line_count)`; the
  collector appends in block order. Peak text held is
  `(workers + 1) × PVAR_BLOCK_BYTES`, independent of the file. `max_variants`
  and malformed-line reporting keep first-in-file semantics by resolving in
  block order with cumulative line counts. The header is parsed from the first
  block before any worker starts. No new dependency: `std::thread::scope` and
  `std::sync::mpsc` suffice.
  - Alternatives: keep whole-buffer decode and only drop it earlier (still a
    4.6 GiB spike); memory-map a plain PVAR (does not help `.pvar.zst`, which
    is what the published panels ship).
- **Decision: columnar `PvarTable` instead of `Vec<PvarVariant>`.**
  `contigs: Vec<String>` with `chrom: Vec<u32>` indices; `start: Vec<u64>`
  with `end` derived through the coordinate system as it is today from the
  position alone; `id_offsets: Vec<u64>` (n+1) into `id_bytes: Vec<u8>` with
  an empty span meaning `.`; `allele_start: Vec<u32>` (n+1) into
  `allele_offsets: Vec<u64>` into `allele_bytes: Vec<u8>`, ref first then alts
  in source order. Fixed cost ≈ 40 B per variant plus the bytes themselves,
  ~55–60 B for the panel's rows, so ~4.5 GB resident for the 75M-variant
  GRCh38 panel and ~5 GB for GRCh37.
  Accessors: `chrom(i) -> &str`, `start(i)`, `end(i)`, `id(i) -> Option<&str>`,
  `reference(i)`, `alternates(i) -> impl Iterator<Item=&str>`,
  `allele_count(i)`, `len()`, `heap_bytes()`. Each worker builds a private
  `PvarTable` for its block; the collector keeps the block tables in a
  `Vec` with a prefix-sum of row starts and resolves an index with
  `partition_point`, so no per-row `String` is ever allocated and the
  blocks are never concatenated. Concatenating into one growing table
  would double the resident size at the last reallocation (~9 GB
  transient for the GRCh38 panel); with ~40 blocks the lookup cost is
  negligible.
  - Alternatives: `Arc<str>` interning of alleles (still one allocation per
    row); Arrow arrays directly (offsets are i32 and a 100M-row string column
    would need chunking, and the table also feeds filter.rs which wants
    random access).
- **Decision: keep all four caps, raise three defaults, name the option in
  the error.** `max_companion_bytes` 512 MiB → 4 GiB and
  `max_decompressed_companion_bytes` 1 GiB → 16 GiB are now sanity bounds on
  work, not memory. `max_variants` 100M → 250M: it is the one cap that still
  bounds resident memory (~15 GB at the pinned per-variant cost), and the
  GRCh37 panel already sits at 85M, so 100M leaves no headroom for the next
  release of the same resource. `max_samples` is unchanged. Limit errors read
  `... exceeding max_companion_bytes (536870912)` so a caller learns what to
  raise without reading source.
- **Decision: `VariantSelection` enum instead of `Arc<Vec<usize>>`.**
  `All(len)`, `Range(Range<usize>)`, or `Sparse(Arc<[u32]>)`, chosen by
  `scan()` after exact-filter evaluation and by the matrix reader. It exposes
  `len()`, `get(i)`, `iter()`, and `binary_search(index)` so
  `plan_partitions`, `contiguous_partition_bounds`, the LD dependency
  lookup, and the matrix partitioner keep their shape. A full scan then
  carries no per-variant vector; a sparse selection costs 4 B per selected
  row instead of 8. `u32` is sufficient because `max_variants` bounds the
  index space well below 2^32 and the header count is validated against it.
- **Decision: pin the cost with a test.** `PvarTable::heap_bytes()` on a
  fixture of biallelic SNP rows must stay under 80 B per variant; a second
  test drives a synthetic multi-block `.pvar.zst` through the streaming reader
  with a small `PVAR_BLOCK_BYTES` override and checks rows, order, and the
  reported line number of an injected malformed line in a later block. The
  real-panel run is an opt-in example gated on an env var, like the oracle
  tests.

## Risks / Trade-offs

- Sequential zstd decode bounds throughput at roughly 1–2 GB/s; for the 4.6
  GiB panel that is a few seconds, comparable to the current parse. Mitigated
  by overlapping decode with parallel parsing.
- Transient memory that remains after this change, on the GRCh38 panel:
  the PGEN record index (~570 MB: retained header bytes, relative offsets,
  LD deltas) and the parsed variant table (~4.5 GB). Both are per open and
  released with the fileset. Removing the resident table needs an on-disk
  parsed sidecar, proposed separately as
  `add-pgen-companion-sidecar-cache`.
- The header-then-body split must handle a header longer than the first
  block; treat it as a malformed companion with a clear error rather than
  growing the block.
- Accessor-based consumers change many call sites in `physical_exec.rs`,
  `filter.rs`, and `matrix.rs`; the existing oracle and conformance tests
  cover them, and no schema changes, so the diff is mechanical.
- Remote companions are streamed rather than fetched whole, changing the
  request pattern from one range read to a streaming get. OpenDAL's reader
  already backs `get_remote_stream`.

## Migration Plan

Pure internal change plus default bumps; released as a minor version. Callers
that relied on the old defaults to reject large companions can set the caps
explicitly. Rollback is reverting the release tag.

## Open Questions

- Whether `PVAR_BLOCK_BYTES` should follow `batch_soft_byte_limit` or stay a
  crate constant. Proposal: constant, overridable in tests only.

## 1. Streaming companion reader
- [x] 1.1 Add `ObjectAccess::companion_reader` in `source.rs` returning a `Read` over a local file or a remote byte stream, with the compressed size check against `max_companion_bytes` kept and the error naming the option.
- [x] 1.2 Replace `decode_text_companion` with a block producer: magic-based zstd/gzip/plain detection, newline-aligned blocks of `PVAR_BLOCK_BYTES`, cumulative decoded-byte accounting against `max_decompressed_companion_bytes` with the option named in the error, cumulative line counts per block.
- [x] 1.3 Parse the header from the first block; error clearly if the header does not end within it.
- [x] 1.4 Route PSAM through the same reader (single small block) so both companions share one path.

## 2. Columnar variant table
- [x] 2.1 Add `PvarTable` with the columns and accessors in design.md, a chunked container over block tables with prefix-sum row starts, and `heap_bytes()`.
- [x] 2.2 Make `parse_pvar_chunk` build a `PvarTable` per block instead of `Vec<PvarVariant>`; keep `PvarStop` semantics.
- [x] 2.3 Rewrite `parse_pvar_chunked` as producer / bounded worker pool / in-order collector; preserve first-in-file `max_variants` and malformed-line reporting.
- [x] 2.4 Switch `PgenFileset.variants` to `Arc<PvarTable>` and update `physical_exec.rs` metadata builders, `filter.rs` pruning, `matrix.rs` positions, and the header variant-count check.
- [x] 2.5 Remove `PvarVariant` from the non-test API; keep a test-only constructor if fixtures need it.
- [x] 2.6 Add `VariantSelection` (`All`, `Range`, `Sparse(Arc<[u32]>)`) and use it in `scan()`, `plan_partitions`, `plan_metadata_partitions`, `PgenPartition`, and `matrix.rs`; a full scan must allocate no per-variant index vector.

## 3. Limits and messages
- [x] 3.1 Raise defaults: `max_companion_bytes` 4 GiB, `max_decompressed_companion_bytes` 16 GiB, `max_variants` 250M; document each option's role in the `PgenReadOptions` doc comments.
- [x] 3.2 Name the option in every limit error (companion, decompressed, variants, samples).

## 4. Tests and verification
- [x] 4.1 Unit test: `heap_bytes()` per variant ≤ 80 B on a biallelic SNP fixture.
- [x] 4.2 Unit test: multi-block `.pvar.zst` through the streaming path with a small block size; rows, order, `max_variants` across a block boundary, and malformed-line line number in a later block.
- [x] 4.3 Unit test: limit errors name the option and the configured value.
- [x] 4.3a Unit test: a full scan and a contiguous filtered scan produce `All`/`Range` selections; a sparse filter produces `Sparse`; partition plans are identical to the previous `Vec<usize>` plans on the fixtures.
- [x] 4.4 Existing PGEN oracle, conformance, and pushdown suites green; `cargo clippy` and `cargo fmt` clean.
- [ ] 4.5 Opt-in example against `pgsc_1000G_v1/GRCh38_1000G_ALL.pgen`: open time and peak RSS recorded in `PERF_HANDOVER.md`.
- [ ] 4.6 Update CHANGELOG and cut a release tag for polars-bio to consume.

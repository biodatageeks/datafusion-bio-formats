## 1. Core helper + FASTQ (Phase 1)
- [ ] 1.1 Add `sync_batch_stream(schema, next_batch)` in `bio-format-core/src/sync_stream.rs` with a unit test; export from `lib.rs`
- [ ] 1.2 Add a FASTQ multi-partition correctness test (4-partition scan == 1-partition scan) against current code
- [ ] 1.3 Replace FASTQ `execute_bgzf_partition` / `execute_byte_range_partition` with a synchronous `batch_producer` closure driven by `sync_batch_stream`; remove `thread::spawn` + `mpsc`
- [ ] 1.4 Remove benchmark scaffolding: `FASTQ_EXEC_MODE`/`FASTQ_READER_POOL` modes, `libc` dev-dep, `examples/thread_model_bench.rs`
- [ ] 1.5 `cargo fmt` + `cargo clippy` + `cargo test -p datafusion-bio-format-fastq` green
- [ ] 1.6 Document the thread-usage contract in FASTQ module doc + `CLAUDE.md`; commit; STOP for review

## 2. VCF (Phase 2)
- [ ] 2.1 Convert `get_indexed_vcf_stream` (`:2659`) and `get_local_vcf_sync` (`:819`) to `sync_batch_stream`; remove channel/thread
- [ ] 2.2 4-partition == 1-partition parity test; `cargo test -p datafusion-bio-format-vcf` green; commit; STOP

## 3. BAM (Phase 3)
- [ ] 3.1 Convert `get_indexed_stream` (`:888`) and `get_local_bam_sync` (`:383`) to `sync_batch_stream`
- [ ] 3.2 Parity test; `cargo test -p datafusion-bio-format-bam` green; commit; STOP

## 4. CRAM (Phase 4)
- [ ] 4.1 Convert `get_indexed_stream` (`:867`) to `sync_batch_stream` (leave already-async full scan; preserve `no_ref` decode)
- [ ] 4.2 Parity test; `cargo test -p datafusion-bio-format-cram` green; commit; STOP

## 5. GFF (Phase 5)
- [ ] 5.1 Convert `get_indexed_gff_stream` (`:1284`) to `sync_batch_stream`
- [ ] 5.2 Parity test; `cargo test -p datafusion-bio-format-gff` green; commit; STOP

## 6. GTF (Phase 6)
- [ ] 6.1 Convert `get_indexed_gtf_stream` (`:729`) to `sync_batch_stream`
- [ ] 6.2 Parity test; `cargo test -p datafusion-bio-format-gtf` green; commit; STOP

## 7. Pairs (Phase 7)
- [ ] 7.1 Convert `get_indexed_pairs_stream` (`:440`) to `sync_batch_stream`
- [ ] 7.2 Parity test; `cargo test -p datafusion-bio-format-pairs` green; commit; STOP

## 8. FASTA + BED + repo-wide docs (Phase 8)
- [ ] 8.1 Convert FASTA `get_local_fasta_sync` (`:291`, single partition) to `sync_batch_stream`
- [ ] 8.2 Verify BED needs no change (already fully async)
- [ ] 8.3 Add repo-wide "Thread usage" contract section to `CLAUDE.md` + `README`
- [ ] 8.4 Workspace-wide `cargo test` + `clippy` + `fmt` green; commit; STOP

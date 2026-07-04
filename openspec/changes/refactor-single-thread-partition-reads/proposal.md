# Fold Partition Decompression Into The Scan Stream (Single Thread Per Partition)

## Why

Every indexed/parallel format reader currently spawns a dedicated `std::thread` per DataFusion partition that decompresses and parses records off the caller's Tokio runtime, feeding batches to the async compute stream through a bounded `mpsc` channel. Because these reader threads are raw `std::thread`s, they are **not** bounded by the consumer's runtime worker count, so a scan with `target_partitions = N` uses ~**2·N** busy OS threads (N reader threads + N compute streams). A downstream consumer (e.g. polars-bio) cannot cap total cores by sizing its Tokio runtime, and `target_partitions = N` silently oversubscribes on shared/CI nodes (issue #212).

Benchmarking on a 523 MB BGZF FASTQ (26.5M reads) confirmed effective cores ≈ 2·`target_partitions`, and that folding read+decompress+compute onto one thread per partition uses exactly `target_partitions` cores while being ~18% **faster per core** (it drops the cross-thread channel handoff).

## What Changes

- Add a shared `sync_batch_stream` helper in `bio-format-core` that turns a synchronous "produce one RecordBatch" closure into a `SendableRecordBatchStream`, decoding each batch inline on the consuming Tokio worker.
- Replace the per-partition `std::thread` + `mpsc` reader in every affected crate (FASTQ, VCF, BAM, CRAM, GFF, GTF, pairs) and the single-partition sync fallbacks (VCF, BAM, FASTA) with `sync_batch_stream`.
- **BREAKING (performance contract):** a scan now uses **one OS thread per partition** (`target_partitions`), not ~2·`target_partitions`. To saturate N cores, set `target_partitions = N` (previously N used ~2N). Decompression parallelism is bounded by the caller's Tokio worker-thread count.
- Preserve byte-identical results (rows, values, projection, `COUNT(*)` empty-projection, `LIMIT`) — only the threading model and per-`target_partitions` wall time change.
- Document the uniform thread-usage contract across all format crates.

## Impact

- Affected specs: **NEW** `partition-thread-model` capability.
- Affected code:
  - `datafusion/bio-format-core/src/sync_stream.rs` (new), `lib.rs`
  - `datafusion/bio-format-fastq/src/physical_exec.rs`
  - `datafusion/bio-format-vcf/src/physical_exec.rs`
  - `datafusion/bio-format-bam/src/physical_exec.rs`
  - `datafusion/bio-format-cram/src/physical_exec.rs`
  - `datafusion/bio-format-gff/src/physical_exec.rs`
  - `datafusion/bio-format-gtf/src/physical_exec.rs`
  - `datafusion/bio-format-pairs/src/physical_exec.rs`
  - `datafusion/bio-format-fasta/src/physical_exec.rs`
  - Docs: `CLAUDE.md`, workspace `README`
- No breaking API changes. Existing SQL queries remain valid; only physical execution threading and per-`target_partitions` wall time change. BED already executes fully async (no reader thread) and needs no code change.
